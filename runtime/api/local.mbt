///|
pub struct LocalRun {
  compiled : CompiledJob
  task_set : @task.TaskSet
  next_offsets : Map[Int, Int64]
  mut closed : Bool
}

///|
/// Start the synchronous local runner. It supports deterministic key routing
/// across local Tasks and one-hop replication along declared exchange routes.
/// This synchronous mode is in-memory; it does not provide a durable sink.
pub fn CompiledJob::run_local(self : CompiledJob) -> LocalRun raise ApiError {
  match self.definition.sink {
    SinkSpec::Discard => ()
    SinkSpec::Durable(_) =>
      raise ApiError::Cluster(
        "LocalRun has no durable sink; use run_local_cluster",
      )
  }
  let task_set = @task.TaskSet::new(self.job_id(), self.operator().id()) catch {
    error => raise ApiError::TaskFailed(@debug.to_string(error))
  }
  for partition in 0..<self.spec.partition_count() {
    let task = self.descriptor.create_task(
      self.job_id() + "/p" + partition.to_string() + "/e0",
      partition,
      0,
      0L,
    )
    task_set.add(task) catch {
      error => raise ApiError::TaskFailed(@debug.to_string(error))
    }
  }
  task_set.start() catch {
    error => raise ApiError::TaskFailed(@debug.to_string(error))
  }
  let next_offsets : Map[Int, Int64] = Map([])
  for partition in 0..<self.spec.partition_count() {
    next_offsets[partition] = 0L
  }
  { compiled: self, task_set, next_offsets, closed: false, }
}

///|
pub fn LocalRun::job_id(self : LocalRun) -> String {
  self.compiled.definition.job_id
}

///|
pub fn LocalRun::plan_digest(self : LocalRun) -> String {
  self.compiled.plan_digest
}

///|
pub fn LocalRun::phase(self : LocalRun) -> LocalRunPhase {
  if self.closed {
    return LocalRunPhase::Stopped
  }
  match self.task_set.phase() {
    @task.TaskSetPhase::Empty => LocalRunPhase::Created
    @task.TaskSetPhase::Created => LocalRunPhase::Created
    @task.TaskSetPhase::Running => LocalRunPhase::Running
    @task.TaskSetPhase::Draining => LocalRunPhase::Draining
    @task.TaskSetPhase::Stopped => LocalRunPhase::Stopped
    @task.TaskSetPhase::Failed => LocalRunPhase::Failed
    @task.TaskSetPhase::Mixed => LocalRunPhase::Mixed
  }
}

///|
pub fn LocalRun::progress(self : LocalRun) -> LocalProgress {
  self.progresses()[0]
}

///|
/// Return progress for every locally hosted partition in deterministic order.
pub fn LocalRun::progresses(self : LocalRun) -> Array[LocalProgress] {
  self.task_set
  .progress()
  .map(progress => {
    job_id: self.compiled.definition.job_id,
    task_id: progress.task_id(),
    partition: progress.partition(),
    epoch: progress.epoch(),
    offset: progress.offset(),
    frontier_digest: progress.frontier_digest(),
    emitted_outputs: progress.emitted_outputs(),
    phase: self.phase(),
  })
}

///|
fn local_output(output : @task.TaskOutput) -> Output {
  match output {
    @task.TaskOutput::KeyedSum(value) => Output::KeyedSum(value)
    @task.TaskOutput::Reachable(value) => Output::Reachable(value)
    @task.TaskOutput::SecurityAlert(value) => Output::SecurityAlert(value)
  }
}

///|
/// Submit causal events to the local Task. This path deliberately uses the
/// same contiguous offset and operator validation as Worker execution.
pub fn LocalRun::submit(
  self : LocalRun,
  events : ArrayView[Event],
) -> Array[Output] raise ApiError {
  if self.closed {
    raise ApiError::Closed
  }
  if events.is_empty() {
    return []
  }
  for event in events {
    self.compiled.descriptor.validate_event(event)
  }
  let grouped : Map[Int, Array[Event]] = Map([])
  for event in events {
    let partition = self.compiled.spec.route_event(event.record).partition()
    match grouped.get(partition) {
      Some(current) => current.push(event)
      None => grouped[partition] = [event]
    }
  }
  let partitions = grouped.keys().to_array()
  partitions.sort()
  let outputs : Array[Output] = []
  for partition in partitions {
    guard grouped.get(partition) is Some(partition_events) else { continue }
    let first_offset = self.next_offsets.get(partition).unwrap_or(0L)
    let records = partition_events.map(event => event.record)
    let batch = @protocol.EventBatch::new(partition, 0, first_offset, records) catch {
      error => raise ApiError::InvalidEvent(@debug.to_string(error))
    }
    let dispatch = self.task_set.process_batch(batch) catch {
      error => raise ApiError::TaskFailed(@debug.to_string(error))
    }
    self.next_offsets[partition] = first_offset +
      partition_events.length().to_int64()
    for output in dispatch.outputs() {
      outputs.push(local_output(output))
    }
  }
  // Route the same committed input records through explicitly declared
  // exchange edges. The target Task applies them without advancing its
  // primary offset cursor.
  for source_partition in partitions {
    guard grouped.get(source_partition) is Some(source_events) else { continue }
    let records = source_events.map(event => event.record)
    let deltas = self.compiled.spec.route_batch(source_partition, 0, 0, records) catch {
      error => raise ApiError::Plan(error)
    }
    for delta in deltas {
      let dispatch = self.task_set.process_delta(delta) catch {
        error => raise ApiError::TaskFailed(@debug.to_string(error))
      }
      for output in dispatch.outputs() {
        outputs.push(local_output(output))
      }
    }
  }
  outputs
}

///|
/// Stop the local run after draining the causal pending set.
pub fn LocalRun::close(self : LocalRun) -> Unit raise ApiError {
  if self.closed {
    return
  }
  self.task_set.drain() catch {
    error => raise ApiError::TaskFailed(@debug.to_string(error))
  }
  self.task_set.stop() catch {
    error => raise ApiError::TaskFailed(@debug.to_string(error))
  }
  self.closed = true
}
