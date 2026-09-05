///|
/// Lifecycle summary for a collection of Tasks hosted by one Worker.
pub(all) enum TaskSetPhase {
  Empty
  Created
  Running
  Draining
  Stopped
  Failed
  Mixed
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// Errors raised by the synchronous multi-partition Task manager.
pub(all) suberror TaskSetError {
  EmptyJobId
  EmptyOperatorId
  EmptyTaskSet
  DuplicatePartition(Int)
  DuplicateTaskId(String)
  OperatorMismatch(expected~ : String, actual~ : String)
  WrongPhase(expected~ : TaskSetPhase, actual~ : TaskSetPhase)
  UnknownPartition(Int)
  Task(partition~ : Int, error~ : TaskError)
} derive(Debug, Eq)

///|
/// Result of routing one input batch through a TaskSet. Keeping the partition
/// identity beside outputs prevents downstream adapters from conflating facts
/// emitted by different tasks when they share one Worker process.
pub(all) struct TaskDispatch {
  task_id : String
  partition : Int
  epoch : Int
  outputs : Array[TaskOutput]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn TaskDispatch::task_id(self : TaskDispatch) -> String {
  self.task_id
}

///|
pub fn TaskDispatch::partition(self : TaskDispatch) -> Int {
  self.partition
}

///|
pub fn TaskDispatch::epoch(self : TaskDispatch) -> Int {
  self.epoch
}

///|
pub fn TaskDispatch::outputs(self : TaskDispatch) -> ArrayView[TaskOutput] {
  self.outputs[:]
}

///|
/// Synchronous registry and router for all assignments owned by one Worker.
/// It deliberately contains no sockets, timers, WALs, or checkpoint files.
pub struct TaskSet {
  job_id : String
  operator_id : String
  tasks : Map[Int, Task]
  task_ids : Map[String, Int]
}

///|
pub fn TaskSet::new(
  job_id : String,
  operator_id : String,
) -> TaskSet raise TaskSetError {
  if job_id.is_empty() {
    raise TaskSetError::EmptyJobId
  }
  if operator_id.is_empty() {
    raise TaskSetError::EmptyOperatorId
  }
  { job_id, operator_id, tasks: Map([]), task_ids: Map([]), }
}

///|
/// Construct a Worker-local TaskSet from the assignments currently owned by
/// that Worker. Assignments may be a strict subset of the Job's partitions.
pub fn TaskSet::from_job(
  job : @plan.JobSpec,
  assignments : ArrayView[@protocol.AssignmentSpec],
) -> TaskSet raise TaskSetError {
  let set = TaskSet::new(job.job_id(), job.operator_id())
  if assignments.is_empty() {
    raise TaskSetError::EmptyTaskSet
  }
  for assignment in assignments {
    let spec = TaskSpec::from_job(job, assignment) catch {
      error =>
        raise TaskSetError::Task(partition=assignment.partition(), error~)
    }
    let task = Task::from_spec(spec) catch {
      error =>
        raise TaskSetError::Task(partition=assignment.partition(), error~)
    }
    set.add(task)
  }
  set
}

///|
/// Register one Task before the set is started. A partition and task identity
/// can each be owned by at most one entry.
pub fn TaskSet::add(self : TaskSet, task : Task) -> Unit raise TaskSetError {
  let set_phase = self.phase()
  if set_phase != TaskSetPhase::Empty && set_phase != TaskSetPhase::Created {
    raise TaskSetError::WrongPhase(
      expected=TaskSetPhase::Created,
      actual=set_phase,
    )
  }
  if self.tasks.contains(task.partition()) {
    raise TaskSetError::DuplicatePartition(task.partition())
  }
  match self.task_ids.get(task.task_id()) {
    Some(_) => raise TaskSetError::DuplicateTaskId(task.task_id())
    None => ()
  }
  if task.operator_id() != self.operator_id {
    raise TaskSetError::OperatorMismatch(
      expected=self.operator_id,
      actual=task.operator_id(),
    )
  }
  if task.phase() != TaskPhase::Created {
    raise TaskSetError::Task(
      partition=task.partition(),
      error=TaskError::WrongPhase(
        expected=TaskPhase::Created,
        actual=task.phase(),
      ),
    )
  }
  self.tasks[task.partition()] = task
  self.task_ids[task.task_id()] = task.partition()
}

///|
pub fn TaskSet::job_id(self : TaskSet) -> String {
  self.job_id
}

///|
pub fn TaskSet::operator_id(self : TaskSet) -> String {
  self.operator_id
}

///|
pub fn TaskSet::length(self : TaskSet) -> Int {
  self.tasks.length()
}

///|
pub fn TaskSet::partitions(self : TaskSet) -> Array[Int] {
  let result = self.tasks.keys().to_array()
  result.sort()
  result
}

///|
pub fn TaskSet::task(self : TaskSet, partition : Int) -> Task? {
  self.tasks.get(partition)
}

///|
pub fn TaskSet::tasks(self : TaskSet) -> Array[Task] {
  let result = self.tasks.values().to_array()
  result.sort_by((left, right) => left.partition().compare(right.partition()))
  result
}

///|
pub fn TaskSet::progress(self : TaskSet) -> Array[TaskProgress] {
  self.tasks().map(task => task.progress())
}

///|
pub fn TaskSet::phase(self : TaskSet) -> TaskSetPhase {
  if self.tasks.is_empty() {
    return TaskSetPhase::Empty
  }
  let phases = self.tasks.values().map(task => task.phase()).to_array()
  let first = phases[0]
  if phases.all(phase => phase == first) {
    match first {
      TaskPhase::Created => TaskSetPhase::Created
      TaskPhase::Running => TaskSetPhase::Running
      TaskPhase::Draining => TaskSetPhase::Draining
      TaskPhase::Stopped => TaskSetPhase::Stopped
      TaskPhase::Failed => TaskSetPhase::Failed
    }
  } else {
    TaskSetPhase::Mixed
  }
}

///|
fn TaskSet::validate_phase(
  self : TaskSet,
  expected : TaskPhase,
) -> Unit raise TaskSetError {
  if self.tasks.is_empty() {
    raise TaskSetError::EmptyTaskSet
  }
  for task in self.tasks.values() {
    if task.phase() != expected {
      raise TaskSetError::Task(
        partition=task.partition(),
        error=TaskError::WrongPhase(expected~, actual=task.phase()),
      )
    }
  }
}

///|
pub fn TaskSet::start(self : TaskSet) -> Unit raise TaskSetError {
  self.validate_phase(TaskPhase::Created)
  for task in self.tasks.values() {
    task.start() catch {
      error => raise TaskSetError::Task(partition=task.partition(), error~)
    }
  }
}

///|
pub fn TaskSet::drain(self : TaskSet) -> Unit raise TaskSetError {
  self.validate_phase(TaskPhase::Running)
  for task in self.tasks.values() {
    task.drain() catch {
      error => raise TaskSetError::Task(partition=task.partition(), error~)
    }
  }
}

///|
pub fn TaskSet::stop(self : TaskSet) -> Unit raise TaskSetError {
  self.validate_phase(TaskPhase::Draining)
  for task in self.tasks.values() {
    if task.pending_count() > 0 {
      raise TaskSetError::Task(
        partition=task.partition(),
        error=TaskError::PendingEvents(task.pending_count()),
      )
    }
  }
  for task in self.tasks.values() {
    task.stop() catch {
      error => raise TaskSetError::Task(partition=task.partition(), error~)
    }
  }
}

///|
/// Fail all currently non-terminal tasks after an adapter-level error.
pub fn TaskSet::fail(self : TaskSet) -> Unit {
  for task in self.tasks.values() {
    match task.phase() {
      TaskPhase::Stopped | TaskPhase::Failed => ()
      _ => task.fail()
    }
  }
}

///|
/// Route a primary batch to its owning Task. The Task itself enforces epoch
/// and contiguous offset rules, while this layer enforces partition ownership.
pub fn TaskSet::process_batch(
  self : TaskSet,
  batch : @protocol.EventBatch,
) -> TaskDispatch raise TaskSetError {
  let task = match self.tasks.get(batch.partition()) {
    Some(value) => value
    None => raise TaskSetError::UnknownPartition(batch.partition())
  }
  let outputs = task.process_batch(batch) catch {
    error => {
      task.fail()
      raise TaskSetError::Task(partition=batch.partition(), error~)
    }
  }
  {
    task_id: task.task_id(),
    partition: task.partition(),
    epoch: task.epoch(),
    outputs,
  }
}

///|
/// Route an exchange batch by target partition without consuming its primary
/// offset cursor.
pub fn TaskSet::process_delta(
  self : TaskSet,
  delta : @protocol.DeltaBatch,
) -> TaskDispatch raise TaskSetError {
  let task = match self.tasks.get(delta.target_partition()) {
    Some(value) => value
    None => raise TaskSetError::UnknownPartition(delta.target_partition())
  }
  let outputs = task.process_delta(delta) catch {
    error => {
      task.fail()
      raise TaskSetError::Task(partition=delta.target_partition(), error~)
    }
  }
  {
    task_id: task.task_id(),
    partition: task.partition(),
    epoch: task.epoch(),
    outputs,
  }
}
