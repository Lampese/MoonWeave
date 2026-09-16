///|
struct GraphOutputTarget {
  edge : GraphEdge
  client : DriverClient
}

///|
struct GraphOutputSink {
  transformed : Bool
  targets : Array[GraphOutputTarget]
}

///|
fn graph_event_id(
  origin : String,
  sequence : Int64,
) -> @causal.EventId raise ApiError {
  @causal.EventId::new(origin, sequence) catch {
    error => raise driver_error("graph event identity", error)
  }
}

///|
fn graph_identity(
  edge : GraphEdge,
  id : @causal.EventId,
) -> @causal.EventId raise ApiError {
  graph_scoped_identity(edge, "input", id)
}

///|
fn graph_scoped_identity(
  edge : GraphEdge,
  kind : String,
  id : @causal.EventId,
) -> @causal.EventId raise ApiError {
  let name = edge.identity()
  graph_event_id(
    "graph/" +
    name.length().to_string() +
    ":" +
    name +
    "/" +
    kind +
    "/" +
    id.origin(),
    id.sequence(),
  )
}

///|
fn graph_value(
  output : @task.TaskOutput,
  original : @protocol.EventRecord?,
  transformed : Bool,
) -> GraphValue raise ApiError {
  let key = original.map(record => record.key()).unwrap_or("output")
  let timestamp = original.map(record => record.event_time()).unwrap_or(None)
  match output {
    KeyedSum(value) =>
      {
        key: value.key(),
        payload: { "value": @json.to_json(value.total()) },
        event_time: timestamp,
      }
    WindowedSum(value) =>
      {
        key: value.key(),
        payload: {
          "value": @json.to_json(value.total()),
          "start_ms": @json.to_json(value.start_ms()),
          "end_ms": @json.to_json(value.end_ms()),
        },
        event_time: Some(value.end_ms() - 1L),
      }
    Custom(value) if transformed =>
      @json.from_json(value) catch {
        error => raise driver_error("decode transformed value", error)
      }
    Custom(value) => {
      let output_key = match value {
        Object(fields) =>
          match fields.get("key") {
            Some(String(key)) => key
            _ => key
          }
        _ => key
      }
      let timestamp = match (timestamp, value) {
        (None, Object(fields)) =>
          fields
          .get("event_time")
          .map(value => {
            @json.from_json(value) catch {
              error => raise driver_error("graph output timestamp", error)
            }
          })
        _ => timestamp
      }
      { key: output_key, payload: value, event_time: timestamp, }
    }
    Reachable(value) =>
      { key, payload: @json.to_json(value), event_time: timestamp, }
    SecurityAlert(value) =>
      { key, payload: @json.to_json(value), event_time: timestamp, }
  }
}

///|
fn graph_record(
  edge : GraphEdge,
  original : @protocol.EventRecord,
  value : GraphValue,
) -> Event raise ApiError {
  {
    record: {
      ..original,
      id: graph_identity(edge, original.id()),
      parents: original.parents().map(parent => graph_identity(edge, parent)),
      key: value.key,
      payload: value.payload,
      event_time: value.event_time,
      input_id: edge.input_id,
    },
  }
}

///|
fn GraphOutputSink::events(
  self : GraphOutputSink,
  edge : GraphEdge,
  record : @runtime.TaskOutputRecord,
) -> Array[Event] raise ApiError {
  let packet : @task.TaskGraphPacket? = match record.output() {
    Custom(json) => Some(@json.from_json(json)) catch { _ => None }
    _ => None
  }
  match packet {
    Some(packet) if packet.graph_packet => {
      let original = packet.input
      if packet.outputs.length() == 1 {
        return [
          graph_record(
            edge,
            original,
            graph_value(packet.outputs[0], Some(original), self.transformed),
          ),
        ]
      }
      let result = []
      let parents = original
        .parents()
        .map(parent => graph_identity(edge, parent))
      for index, output in packet.outputs {
        let id = graph_scoped_identity(
          edge,
          "item/" + index.to_string(),
          original.id(),
        )
        parents.push(id)
        let value = graph_record(
          edge,
          original,
          graph_value(output, Some(original), self.transformed),
        )
        result.push({ record: { ..value.record, id, }, })
      }
      // A filtered input still supplies its identity. A multi-output callback
      // supplies that identity only after every expanded output is admitted.
      let marker = graph_record(edge, original, {
        key: original.key(),
        payload: @task.graph_skip_payload(),
        event_time: original.event_time(),
      })
      result.push({ record: { ..marker.record, parents, }, })
      result
    }
    _ => {
      let value = graph_value(record.output(), None, false)
      let event = Event::new(
        graph_event_id("output/" + record.task_id(), record.sequence()),
        [],
        value.key,
        value.payload,
      )
      let mapped = graph_record(edge, event.record, value)
      [
        {
          record: {
            ..mapped.record,
            id: graph_scoped_identity(edge, "output", event.id()),
          },
        },
      ]
    }
  }
}

///|
async fn GraphOutputSink::write(
  self : GraphOutputSink,
  record : @runtime.TaskOutputRecord,
) -> Unit raise ApiError {
  for target in self.targets {
    let events = self.events(target.edge, record)
    // Every branch ACKs durable input before the source output consumer ACKs.
    // A crash in the middle repeats the same edge/input identities.
    ignore(target.client.input(events))
  }
}

///|
fn GraphOutputSink::close(self : GraphOutputSink) -> Unit {
  for target in self.targets {
    target.client.close()
  }
}

///|
/// Run every stage shard assigned to this Worker through real TCP sessions.
/// Stage output ACKs include durable downstream ingress and propagate pressure.
pub async fn CompiledJob::run_graph_worker(
  self : CompiledJob,
  config : WorkerNodeConfig,
  control_endpoint : NodeEndpoint,
) -> WorkerNodeSetSummary raise ApiError {
  guard self.definition.graph is Some(_) else {
    raise ApiError::Cluster("not a graph job")
  }
  control_endpoint.validate_connect()
  let registry = self.worker_registry()
  // Concurrent recursive mkdir calls can race on shared ancestors. Establish
  // this Worker's common directory before starting independent stage tasks.
  ensure_live_storage(config.storage_dir + "/stages")
  let summaries : Array[WorkerNodeSetSummary] = []
  @async_runtime.with_task_group(fn(group) {
    for (id, job) in self.graph_stages {
      group.spawn_bg() <| () => {
        let stage_config = {
          ..config,
          storage_dir: config.storage_dir + "/stages/" + id,
          assignment_known: false,
        }
        let summary = job.run_worker_service_registered(stage_config, registry)
        if summary.failed_tasks() > 0 {
          raise ApiError::Cluster("graph stage " + id + " failed")
        }
        summaries.push(summary)
      }
    }
  }) catch {
    error => raise driver_error("graph Worker", error)
  }
  if summaries.is_empty() {
    raise ApiError::InvalidWorkerAssignment(config.worker_id)
  }
  let mut completed_tasks = 0
  let mut failed_tasks = 0
  let mut replayed_batches = 0
  let mut live_batches = 0
  for summary in summaries {
    completed_tasks += summary.completed_tasks
    failed_tasks += summary.failed_tasks
    replayed_batches += summary.replayed_batches
    live_batches += summary.live_batches
  }
  {
    worker_id: config.worker_id,
    completed_tasks,
    failed_tasks,
    replayed_batches,
    live_batches,
  }
}
