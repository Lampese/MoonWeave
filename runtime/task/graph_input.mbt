///|
/// A graph stage admits all causal witnesses, but executes only its key shard.
/// Intermediate stages emit one durable packet per admitted owned input.
pub(all) struct TaskGraphConfig {
  partition_count : Int
  emit_packets : Bool
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct TaskGraphPacket {
  graph_packet : Bool
  input : @protocol.EventRecord
  outputs : Array[TaskOutput]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn graph_skip_payload() -> Json {
  { "$moonweave": "skip" }
}

///|
pub fn is_graph_skip(payload : Json) -> Bool {
  payload == graph_skip_payload()
}

///|
pub fn Task::with_graph_input(
  self : Task,
  config : TaskGraphConfig,
) -> Task raise TaskError {
  if self.phase != Created || config.partition_count <= self.partition {
    raise TaskError::InvalidTransition
  }
  let log = @causal.CausalLog::new() catch {
    error => raise TaskError::Causal(error)
  }
  { ..self, graph_config: Some(config), graph_log: Some(log), }
}

///|
fn Task::restore_graph_state(
  self : Task,
  snapshot : TaskSnapshot,
) -> Unit raise TaskError {
  match (snapshot.graph_config, snapshot.graph_state) {
    (None, None) => ()
    (Some(config), Some(state)) => {
      if config.partition_count <= self.partition {
        raise TaskError::InvalidTransition
      }
      let saved : @causal.CausalLogSnapshot[TaskEventPayload] = @json.from_json(
        state,
      ) catch {
        _ => raise TaskError::InvalidTransition
      }
      self.graph_log = Some(
        @causal.CausalLog::restore(saved) catch {
          error => raise TaskError::Causal(error)
        },
      )
      self.graph_config = Some(config)
    }
    _ => raise TaskError::InvalidTransition
  }
}

///|
fn Task::dispatch_input(
  self : Task,
  input : TaskInput,
) -> Array[TaskOutput] raise TaskError {
  guard self.graph_config is Some(config) else {
    return match self.event_time_config {
      None => self.apply_input(input)
      Some(_) => self.buffer_event_time_input(input)
    }
  }
  let causal = task_event_causal(input.event())
  let admission = self.graph_log.unwrap().submit(causal) catch {
      error => raise TaskError::Causal(error)
    }
  let outputs : Array[TaskOutput] = []
  for event in admission.admitted() {
    let record = task_event_record(event)
    if @plan.key_partition(record.key(), config.partition_count) !=
      self.partition {
      continue
    }
    let values = if is_graph_skip(record.payload()) {
      []
    } else {
      // The graph gate already admitted every parent, including foreign-key
      // and filtered witnesses. Built-in state only contains this shard's data.
      let own_record = if self.operator == SecurityChain {
        record
      } else {
        { ..record, parents: [], }
      }
      match self.event_time_config {
        None =>
          self.apply_input(
            TaskInput::new(own_record),
            callback_input=TaskInput::new(record),
          )
        Some(_) =>
          self.buffer_event_time_input(TaskInput::new(own_record), flush=false)
      }
    }
    if config.emit_packets {
      outputs.push(
        TaskOutput::Custom(
          @json.to_json(TaskGraphPacket::{
            graph_packet: true,
            input: record,
            outputs: values,
          }),
        ),
      )
    } else {
      outputs.push_iter(values.iter())
    }
  }
  if self.event_time_config is Some(_) {
    // The admission batch may contain several newly unblocked descendants.
    // Buffer all of them before a requested watermark can close a window.
    outputs.push_iter(self.advance_event_time().iter())
  }
  outputs
}
