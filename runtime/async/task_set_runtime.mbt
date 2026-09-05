///|
/// One independently connected Task hosted by a Worker process. The
/// connection and durable paths belong to this work item and are consumed by
/// `run_worker_task` exactly once.
pub struct WorkerTaskWork {
  connection : @socket.Tcp
  cluster_id : String
  coordinator_id : String
  worker_id : String
  incarnation : String
  capabilities : Array[String]
  wal_path : String
  task : @task.Task
  checkpoint_path : String?
  delta_wal_path : String?
  output_log_path : String?
  maximum_frame_bytes : Int
  initial_credit_events : Int64
  initial_credit_bytes : Int64
  heartbeat_interval_ms : Int
}

///|
pub fn WorkerTaskWork::new(
  connection : @socket.Tcp,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  incarnation : String,
  capabilities : ArrayView[String],
  wal_path : String,
  task : @task.Task,
  checkpoint_path? : String,
  delta_wal_path? : String,
  output_log_path? : String,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
  initial_credit_events? : Int64 = 128L,
  initial_credit_bytes? : Int64 = 4L * 1024L * 1024L,
  heartbeat_interval_ms? : Int = 0,
) -> WorkerTaskWork {
  {
    connection,
    cluster_id,
    coordinator_id,
    worker_id,
    incarnation,
    capabilities: capabilities.to_owned(),
    wal_path,
    task,
    checkpoint_path,
    delta_wal_path,
    output_log_path,
    maximum_frame_bytes,
    initial_credit_events,
    initial_credit_bytes,
    heartbeat_interval_ms,
  }
}

///|
pub fn WorkerTaskWork::task_id(self : WorkerTaskWork) -> String {
  self.task.task_id()
}

///|
pub fn WorkerTaskWork::partition(self : WorkerTaskWork) -> Int {
  self.task.partition()
}

///|
pub fn WorkerTaskWork::task(self : WorkerTaskWork) -> @task.Task {
  self.task
}

///|
/// Result of one Task session. A failed entry keeps the stable task identity so
/// a scheduler can retry only that assignment.
pub(all) struct WorkerTaskResult {
  task_id : String
  partition : Int
  summary : WorkerRunSummary?
  failure : String?
} derive(Debug, Eq)

///|
pub fn WorkerTaskResult::task_id(self : WorkerTaskResult) -> String {
  self.task_id
}

///|
pub fn WorkerTaskResult::partition(self : WorkerTaskResult) -> Int {
  self.partition
}

///|
pub fn WorkerTaskResult::summary(self : WorkerTaskResult) -> WorkerRunSummary? {
  self.summary
}

///|
pub fn WorkerTaskResult::failure(self : WorkerTaskResult) -> String? {
  self.failure
}

///|
/// Aggregate result for one Worker process hosting multiple independent Task
/// sessions.
pub(all) struct WorkerTaskSetSummary {
  completed_tasks : Int
  failed_tasks : Int
  results : Array[WorkerTaskResult]
  live_batches : Int
  replayed_batches : Int
  replayed_deltas : Int
} derive(Debug, Eq)

///|
pub fn WorkerTaskSetSummary::completed_tasks(
  self : WorkerTaskSetSummary,
) -> Int {
  self.completed_tasks
}

///|
pub fn WorkerTaskSetSummary::failed_tasks(self : WorkerTaskSetSummary) -> Int {
  self.failed_tasks
}

///|
pub fn WorkerTaskSetSummary::results(
  self : WorkerTaskSetSummary,
) -> ArrayView[WorkerTaskResult] {
  self.results[:]
}

///|
pub fn WorkerTaskSetSummary::live_batches(self : WorkerTaskSetSummary) -> Int {
  self.live_batches
}

///|
pub fn WorkerTaskSetSummary::replayed_batches(
  self : WorkerTaskSetSummary,
) -> Int {
  self.replayed_batches
}

///|
pub fn WorkerTaskSetSummary::replayed_deltas(
  self : WorkerTaskSetSummary,
) -> Int {
  self.replayed_deltas
}

///|
fn validate_worker_task_work(
  works : ArrayView[WorkerTaskWork],
) -> Unit raise FrameIoError {
  if works.is_empty() {
    raise FrameIoError::TaskSet(@task.TaskSetError::EmptyTaskSet)
  }
  let partitions : Set[Int] = Set([])
  let task_ids : Set[String] = Set([])
  for work in works {
    if !partitions.add_and_check(work.partition()) {
      raise FrameIoError::TaskSet(
        @task.TaskSetError::DuplicatePartition(work.partition()),
      )
    }
    if !task_ids.add_and_check(work.task_id()) {
      raise FrameIoError::TaskSet(
        @task.TaskSetError::DuplicateTaskId(work.task_id()),
      )
    }
    if work.task().phase() != @task.TaskPhase::Created {
      raise FrameIoError::Task(
        @task.TaskError::WrongPhase(
          expected=@task.TaskPhase::Created,
          actual=work.task().phase(),
        ),
      )
    }
  }
}

///|
/// Run independent Task sessions concurrently. All IO remains inside the
/// spawned async workers; this function only validates the static work list and
/// aggregates their terminal results.
pub async fn run_worker_task_set(
  works : Array[WorkerTaskWork],
  output_handler : (String, Int, @task.TaskOutput) -> Unit raise,
) -> WorkerTaskSetSummary raise FrameIoError {
  validate_worker_task_work(works[:])
  (@async.with_task_group <| group => {
    let tasks : Array[@async.Task[Result[WorkerRunSummary, FrameIoError]]] = []
    for work in works {
      let task_id = work.task_id()
      let partition = work.partition()
      let task = work.task()
      let handler : (@task.TaskOutput) -> Unit raise = output => {
        output_handler(task_id, partition, output)
      }
      let spawned = group.spawn(allow_failure=true) <| () => {
        Ok(
          run_worker_task(
            work.connection,
            work.cluster_id,
            work.coordinator_id,
            work.worker_id,
            work.incarnation,
            work.capabilities,
            work.wal_path,
            task,
            handler,
            checkpoint_path?=work.checkpoint_path,
            maximum_frame_bytes=work.maximum_frame_bytes,
            initial_credit_events=work.initial_credit_events,
            initial_credit_bytes=work.initial_credit_bytes,
            heartbeat_interval_ms=work.heartbeat_interval_ms,
            delta_wal_path?=work.delta_wal_path,
            output_log_path?=work.output_log_path,
          ),
        ) catch {
          error => Err(error)
        }
      }
      tasks.push(spawned)
    }
    let results : Array[WorkerTaskResult] = []
    let mut live_batches = 0
    let mut replayed_batches = 0
    let mut replayed_deltas = 0
    let mut failed_tasks = 0
    for index in 0..<tasks.length() {
      let work = works[index]
      let outcome : Result[WorkerRunSummary, FrameIoError] = tasks[index].wait() catch {
        error => Err(FrameIoError::Io(@debug.to_string(error)))
      }
      match outcome {
        Ok(summary) => {
          live_batches += summary.live_batches()
          replayed_batches += summary.replayed_batches()
          replayed_deltas += summary.replayed_deltas()
          results.push({
            task_id: work.task_id(),
            partition: work.partition(),
            summary: Some(summary),
            failure: None,
          })
        }
        Err(error) => {
          failed_tasks += 1
          results.push({
            task_id: work.task_id(),
            partition: work.partition(),
            summary: None,
            failure: Some(@debug.to_string(error)),
          })
        }
      }
    }
    {
      completed_tasks: works.length() - failed_tasks,
      failed_tasks,
      results,
      live_batches,
      replayed_batches,
      replayed_deltas,
    }
  }) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
}
