///|
/// One source-backed execution owned by a Coordinator listener. DriverStore
/// persists user intent, while this object owns the durable control, job, and
/// source stores that turn that intent into live Worker delivery.
pub struct SourceBackedExecution {
  listener : CoordinatorListener
  job : CompiledJob
  coordinator : @runtime.CoordinatorStore
  job_store : @runtime.JobStore
  source : @runtime.SourceBackedJob
  output_manifest : @runtime.JobOutputManifest?
  mut kafka_source : KafkaSource?
  mut kafka_source_started : Bool
  mut kafka_source_stopped : Bool
  mut kafka_stop_requested : Bool
  mut closed : Bool
}

///|
fn live_execution_error(stage : String, error : Error) -> ApiError {
  ApiError::Cluster(stage + ": " + @debug.to_string(error))
}

///|
async fn ensure_live_storage(path : String) -> Unit raise ApiError {
  let exists = @fs.exists(path) catch {
    error => raise live_execution_error("check Coordinator storage", error)
  }
  if !exists {
    @fs.mkdir(path, recursive=true) catch {
      error => raise live_execution_error("mkdir Coordinator storage", error)
    }
  }
}

///|
fn live_worker_capabilities(job : CompiledJob) -> Array[String] {
  let descriptor = job.descriptor
  [
    job.definition.operator.id(),
    descriptor.capability(),
    "causal-frontier",
    descriptor.code_digest(),
  ]
}

///|
async fn CoordinatorListener::open_source_backed_execution(
  self : CoordinatorListener,
  job : CompiledJob,
  inputs : ArrayView[Event],
) -> SourceBackedExecution raise ApiError {
  validate_coordinator_workers(job, self.config)
  ensure_live_storage(self.config.storage_dir)
  let coordinator = @runtime.CoordinatorStore::open(
    self.config.cluster_id,
    self.config.coordinator_id,
    self.config.storage_dir + "/coordinator.wal",
    checkpoint_path=self.config.storage_dir + "/coordinator.json",
    maximum_bytes=self.config.storage_budget_bytes,
  ) catch {
    error => raise live_execution_error("open Coordinator store", error)
  }
  errdefer coordinator.close()
  let job_store = @runtime.JobStore::open(
    job.spec,
    self.config.storage_dir + "/job.wal",
    checkpoint_path=self.config.storage_dir + "/job.json",
    maximum_bytes=self.config.storage_budget_bytes,
  ) catch {
    error => raise live_execution_error("open Job store", error)
  }
  errdefer job_store.close()
  let capabilities = live_worker_capabilities(job)
  for worker in self.config.workers {
    let incarnation = @protocol.Incarnation::new(worker.incarnation()) catch {
      error => raise live_execution_error("invalid Worker incarnation", error)
    }
    ignore(
      coordinator.register_worker(worker.worker_id(), incarnation, capabilities) catch {
        @runtime.CoordinatorStoreError::Coordinator(
          @control.CoordinatorError::StaleIncarnation
        ) => {
          let fenced = coordinator
            .snapshot()
            .workers()
            .any(value => {
              value.worker_id() == worker.worker_id() &&
              value.lifecycle() == @control.WorkerLifecycle::Fenced
            })
          if fenced {
            @control.RegisterStatus::Duplicate
          } else {
            raise live_execution_error(
              "register Worker",
              @control.CoordinatorError::StaleIncarnation,
            )
          }
        }
        error => raise live_execution_error("register Worker", error)
      },
    )
  }
  ignore(restore_node_assignments(coordinator, job_store, job, self.config))
  for partition in 0..<job.spec.partition_count() {
    let worker_id = job.spec.owner(partition) catch {
      error => raise ApiError::Plan(error)
    }
    let assignment = match
      snapshot_assignment(coordinator.snapshot(), partition) {
      Some(existing) => existing
      None =>
        coordinator.assign(partition, worker_id) catch {
          error => raise live_execution_error("assign partition", error)
        }
    }
    let saved = job_store.snapshot().partitions()[partition]
    if saved.phase() == @control.JobPartitionPhase::Unassigned {
      ignore(
        job_store.assign(
          partition,
          assignment.worker_id(),
          assignment.epoch(),
          assignment.start_offset(),
        ),
      ) catch {
        error =>
          raise live_execution_error("persist partition assignment", error)
      }
    } else if saved.worker_id() != assignment.worker_id() ||
      saved.epoch() != assignment.epoch() ||
      saved.start_offset() != assignment.start_offset() {
      raise ApiError::Cluster(
        "restored live assignment does not match Coordinator",
      )
    }
  }
  let source = @runtime.SourceBackedJob::open(
    self.config.storage_dir + "/source.wal",
    job.spec,
    coordinator,
    job_store,
    required_capability=job.descriptor.capability(),
    maximum_bytes=self.config.storage_budget_bytes,
  ) catch {
    error => raise live_execution_error("open source-backed execution", error)
  }
  errdefer source.close()
  match job.definition.source {
    SourceSpec::Kafka(_) => {
      if !inputs.is_empty() {
        raise ApiError::InvalidDriverCommand(
          "Kafka source Jobs do not accept Driver input",
        )
      }
      source.refresh_work_plans() catch {
        error =>
          raise live_execution_error("recover Kafka source work plans", error)
      }
    }
    _ => {
      source.ingest_all(inputs.map(event => event.record)) catch {
        error => raise live_execution_error("bind source work plans", error)
      }
      if source.event_count() != inputs.length() {
        raise ApiError::Cluster(
          "SourceLog contains input absent from Driver intent",
        )
      }
    }
  }
  ignore(
    @runtime.reconcile_job_coordinator(
      coordinator,
      job_store,
      reconcile_path=node_reconciliation_path(self.config),
    ),
  ) catch {
    error => raise live_execution_error("reconcile live execution", error)
  }
  match job_store.phase() {
    @control.JobPhase::Created =>
      job_store.start() catch {
        error => raise live_execution_error("start live Job", error)
      }
    @control.JobPhase::Running
    | @control.JobPhase::Draining
    | @control.JobPhase::Succeeded => ()
    @control.JobPhase::Failed | @control.JobPhase::Cancelled =>
      raise ApiError::InvalidJobPhase(
        expected=JobRunPhase::Running,
        actual=match job_store.phase() {
          @control.JobPhase::Succeeded => JobRunPhase::Succeeded
          @control.JobPhase::Failed => JobRunPhase::Failed
          @control.JobPhase::Cancelled => JobRunPhase::Cancelled
          _ => JobRunPhase::Created
        },
      )
  }
  let output_manifest = match job.definition.sink {
    SinkSpec::Discard => None
    SinkSpec::Durable(_) | SinkSpec::JsonLines(_) | SinkSpec::Kafka(_) =>
      Some(
        @runtime.JobOutputManifest::open(
          self.config.storage_dir + "/manifest.wal",
          job.definition.job_id,
          job.definition.operator.id(),
          job.spec.partition_count(),
          maximum_bytes=self.config.storage_budget_bytes,
        ) catch {
          error => raise live_execution_error("open output manifest", error)
        },
      )
  }
  {
    listener: self,
    job,
    coordinator,
    job_store,
    source,
    output_manifest,
    kafka_source: None,
    kafka_source_started: false,
    kafka_source_stopped: false,
    kafka_stop_requested: false,
    closed: false,
  }
}

///|
fn SourceBackedExecution::ensure_open(
  self : SourceBackedExecution,
) -> Unit raise ApiError {
  if self.closed {
    raise ApiError::Closed
  }
}

///|
fn SourceBackedExecution::event_count(self : SourceBackedExecution) -> Int {
  self.source.event_count()
}

///|
async fn SourceBackedExecution::compact_completed_source(
  self : SourceBackedExecution,
) -> Unit raise ApiError {
  self.source.compact_completed_source() catch {
    error => raise live_execution_error("compact completed source", error)
  }
}

///|
fn SourceBackedExecution::phase(self : SourceBackedExecution) -> JobRunPhase {
  match self.job_store.phase() {
    @control.JobPhase::Created => JobRunPhase::Created
    @control.JobPhase::Running => JobRunPhase::Running
    @control.JobPhase::Draining => JobRunPhase::Draining
    @control.JobPhase::Succeeded => JobRunPhase::Succeeded
    @control.JobPhase::Failed => JobRunPhase::Failed
    @control.JobPhase::Cancelled => JobRunPhase::Cancelled
  }
}

///|
/// Synchronize the complete durable Driver input prefix. Retrying after a
/// crash is safe because SourceLog rejects conflicting identities and accepts
/// equal retransmissions at their original offsets.
async fn SourceBackedExecution::sync_inputs(
  self : SourceBackedExecution,
  events : ArrayView[Event],
) -> Unit raise ApiError {
  self.ensure_open()
  for event in events {
    self.job.descriptor.validate_event(event)
  }
  let records = events.map(event => event.record)
  self.source.ingest_all(records[:]) catch {
    error =>
      raise live_execution_error("persist Driver input in SourceLog", error)
  }
}

///|
/// Seal ingress after the Driver has durably entered Draining. Each live
/// Worker session observes this signal only after its source prefix has been
/// appended and bound into JobStore.
async fn SourceBackedExecution::drain(
  self : SourceBackedExecution,
) -> Unit raise ApiError {
  self.ensure_open()
  if self.phase() == JobRunPhase::Succeeded {
    return
  }
  self.kafka_stop_requested = true
  self.wait_for_kafka_source_stop("drain Kafka source")
  self.source.seal() catch {
    error => raise live_execution_error("seal live source", error)
  }
}

///|
async fn SourceBackedExecution::wait_for_kafka_source_stop(
  self : SourceBackedExecution,
  stage : String,
) -> Unit raise ApiError {
  while self.kafka_source_started && !self.kafka_source_stopped {
    @async_runtime.sleep(1) catch {
      error => raise live_execution_error(stage, error)
    }
  }
}

///|
async fn SourceBackedExecution::cancel(
  self : SourceBackedExecution,
) -> Unit raise ApiError {
  self.ensure_open()
  if self.phase() == JobRunPhase::Succeeded ||
    self.phase() == JobRunPhase::Failed {
    return
  }
  self.kafka_stop_requested = true
  self.wait_for_kafka_source_stop("cancel Kafka source")
  self.source.cancel() catch {
    error => raise live_execution_error("cancel live source", error)
  }
}

///|
/// Poll the Kafka consumer-group assignment until Driver drain closes ingress.
/// A batch is committed per Kafka partition only after its events and refreshed
/// delivery plans are durable in SourceLog/JobStore.
async fn SourceBackedExecution::pump_kafka_source(
  self : SourceBackedExecution,
) -> Unit raise ApiError {
  defer {
    self.kafka_source_stopped = true
  }
  guard self.kafka_source is Some(source) else {
    raise ApiError::Source("Kafka source pump has no connected consumer")
  }
  while !self.kafka_stop_requested && self.phase() == JobRunPhase::Running {
    let batch = source.poll(max_wait_ms=100) catch {
      error => raise ApiError::Source(@debug.to_string(error))
    }
    // Drain may have started while the broker poll was in flight. Leave the
    // batch uncommitted so the next execution can read it again.
    if self.kafka_stop_requested || self.phase() != JobRunPhase::Running {
      break
    }
    for event in batch.events() {
      self.job.descriptor.validate_event(event)
    }
    if !batch.events().is_empty() {
      self.source.ingest_all(batch.events().map(event => event.record)) catch {
        error => raise live_execution_error("persist Kafka source batch", error)
      }
    }
    source.commit_offsets(batch.next_offsets()) catch {
      error => raise ApiError::Source(@debug.to_string(error))
    }
  }
}

///|
fn source_backed_summary_failures(
  failures : ArrayView[@runtime.SupervisorFailure],
) -> Array[String] {
  coordinator_failures(failures)
}

///|
fn live_replacement_worker(
  config : CoordinatorNodeConfig,
  failed_worker : String,
) -> String? {
  if config.workers.length() <= 1 {
    return None
  }
  for slot in config.workers {
    if slot.worker_id() != failed_worker {
      return Some(slot.worker_id())
    }
  }
  None
}

///|
async fn SourceBackedExecution::recover_live_failures(
  self : SourceBackedExecution,
  summary : @runtime.LiveSupervisorSummary,
) -> Bool raise ApiError {
  let failed_workers : Array[String] = []
  let failed_partitions : Map[String, Array[Int]] = Map([])
  for failure in summary.failures() {
    guard failure.partition() is Some(partition) else { return false }
    let worker = failure.worker_id()
    if worker.is_empty() {
      return false
    }
    if !failed_workers.any(value => value == worker) {
      failed_workers.push(worker)
    }
    match failed_partitions.get(worker) {
      Some(values) =>
        if !values.any(value => value == partition) {
          values.push(partition)
        }
      None => failed_partitions[worker] = [partition]
    }
  }
  if failed_workers.length() != 1 {
    return false
  }
  let failed_worker = failed_workers[0]
  match live_replacement_worker(self.listener.config, failed_worker) {
    Some(_) => ()
    None => return false
  }
  let owned = self.coordinator
    .snapshot()
    .assignments()
    .filter(assignment => assignment.worker_id() == failed_worker)
  if owned.is_empty() {
    return false
  }
  guard failed_partitions.get(failed_worker) is Some(failed) else {
    return false
  }
  let owned_ids = owned.map(assignment => assignment.partition())
  let failed_ids = failed.copy()
  owned_ids.sort()
  failed_ids.sort()
  if owned_ids != failed_ids {
    return false
  }
  let job_snapshot = self.job_store.snapshot()
  for assignment in owned {
    let saved = job_snapshot.partitions()[assignment.partition()]
    if saved.worker_id() != assignment.worker_id() ||
      saved.epoch() != assignment.epoch() ||
      saved.phase() == @control.JobPartitionPhase::Succeeded {
      return false
    }
  }
  let assignments : Map[Int, @control.PartitionAssignment] = Map([])
  for item in self.coordinator.snapshot().assignments() {
    let assignment = @control.PartitionAssignment::from_snapshot(item) catch {
      error =>
        raise live_execution_error("decode live Worker assignment", error)
    }
    assignments[assignment.partition()] = assignment
  }
  ignore(
    recover_node_worker(
      self.coordinator,
      self.job_store,
      self.job,
      self.listener.config,
      assignments,
      failed_worker,
      failed[:],
    ) catch {
      error => raise live_execution_error("recover live Worker leases", error)
    },
  )
  true
}

///|
/// Run all assigned Worker sessions until ingress is sealed and every source
/// cursor has passed the existing drain barrier. This function is intentionally
/// long-lived while the Driver phase is Running.
async fn SourceBackedExecution::run(
  self : SourceBackedExecution,
) -> CoordinatorNodeSummary raise ApiError {
  guard self.job.definition.source is SourceSpec::Kafka(spec) &&
    self.phase() == JobRunPhase::Running &&
    !self.kafka_stop_requested else {
    return self.run_source_supervisor()
  }
  let source_config = self.listener.config.kafka_runtime
    .unwrap_or(KafkaRuntimeConfig::new())
    .source_config(spec)
  let mut outcome : Result[CoordinatorNodeSummary, ApiError]? = None
  (@async_runtime.with_task_group <| group => {
    let connected : Result[KafkaSource, ApiError] = Ok(
      KafkaSource::connect(group~, source_config),
    ) catch {
      error => Err(error)
    }
    match connected {
      Err(error) => outcome = Some(Err(error))
      Ok(source) => {
        self.kafka_source = Some(source)
        defer {
          source.close()
          self.kafka_source = None
        }
        let mut pump : @async_runtime.Task[Unit]? = None
        if self.kafka_stop_requested || self.phase() != JobRunPhase::Running {
          self.kafka_source_stopped = true
        } else {
          // Mark the pump before spawning it so drain/cancel cannot observe a
          // transient "not started" state and seal ingress ahead of the poll.
          self.kafka_source_started = true
          self.kafka_source_stopped = false
          pump = Some(
            group.spawn(allow_failure=false) <| () => {
              self.pump_kafka_source()
            },
          )
        }
        let supervisor_outcome : Result[CoordinatorNodeSummary, ApiError] = {
          defer {
            // A supervisor failure or early completion must not leave the
            // Kafka poller keeping this structured task group alive.
            self.kafka_stop_requested = true
          }
          Ok(self.run_source_supervisor()) catch {
            error => Err(error)
          }
        }
        let pump_outcome : Result[Unit, ApiError] = match pump {
          None => Ok(())
          Some(task) =>
            Ok(task.wait()) catch {
              error =>
                Err(
                  ApiError::Source(
                    "Kafka source pump: " + @debug.to_string(error),
                  ),
                )
            }
        }
        outcome = Some(
          match pump_outcome {
            Ok(_) => supervisor_outcome
            Err(error) => Err(error)
          },
        )
      }
    }
  }) catch {
    error =>
      raise ApiError::Source("Kafka source runtime: " + @debug.to_string(error))
  }
  match outcome {
    Some(Ok(summary)) => summary
    Some(Err(error)) => raise error
    None => raise ApiError::Source("Kafka source runtime did not start")
  }
}

///|
async fn SourceBackedExecution::run_source_supervisor(
  self : SourceBackedExecution,
) -> CoordinatorNodeSummary raise ApiError {
  self.ensure_open()
  if self.phase() == JobRunPhase::Succeeded {
    return {
      endpoint: self.listener.endpoint(),
      plan_digest: self.job.plan_digest(),
      worker_count: self.listener.config.workers.length(),
      completed_partitions: self.job.partition_count(),
      failed_partitions: 0,
      sent_batches: 0,
      failures: [],
    }
  }
  let mut summary = @runtime.run_live_source_supervisor(
    self.listener.server,
    self.source,
    output_manifest?=self.output_manifest,
    auth_token?=self.listener.config.auth_token,
  ) catch {
    error => {
      if !@async_runtime.is_being_cancelled() &&
        (
          self.job_store.phase() == @control.JobPhase::Running ||
          self.job_store.phase() == @control.JobPhase::Draining
        ) {
        self.job_store.fail(@debug.to_string(error)) catch {
          write_error =>
            raise live_execution_error("persist live failure", write_error)
        }
      }
      raise live_execution_error("run live source supervisor", error)
    }
  }
  if summary.failed_partitions() > 0 {
    let recovered = if summary.recoverable_failures() {
      self.recover_live_failures(summary)
    } else {
      false
    }
    if recovered {
      summary = @runtime.run_live_source_supervisor(
        self.listener.server,
        self.source,
        output_manifest?=self.output_manifest,
        auth_token?=self.listener.config.auth_token,
      ) catch {
        error =>
          raise live_execution_error("resume live source supervisor", error)
      }
    }
    if summary.failed_partitions() > 0 {
      let reason = source_backed_summary_failures(summary.failures()).join(", ")
      if self.job_store.phase() == @control.JobPhase::Running ||
        self.job_store.phase() == @control.JobPhase::Draining {
        self.job_store.fail(
          if reason.is_empty() {
            "live Worker execution failed"
          } else {
            reason
          },
        ) catch {
          error => raise live_execution_error("record live Job failure", error)
        }
      }
    }
  }
  if summary.failed_partitions() == 0 {
    ignore(
      @runtime.reconcile_job_coordinator(
        self.coordinator,
        self.job_store,
        reconcile_path=node_reconciliation_path(self.listener.config),
      ),
    ) catch {
      error => raise live_execution_error("reconcile completed live Job", error)
    }
    self.coordinator.checkpoint_and_compact() catch {
      error => raise live_execution_error("checkpoint live Coordinator", error)
    }
    self.job_store.compact_completed() catch {
      error => raise live_execution_error("compact completed Job plans", error)
    }
    self.job_store.checkpoint_and_compact() catch {
      error => raise live_execution_error("checkpoint live Job", error)
    }
  }
  {
    endpoint: self.listener.endpoint(),
    plan_digest: self.job.plan_digest(),
    worker_count: self.listener.config.workers.length(),
    completed_partitions: summary.completed_partitions(),
    failed_partitions: summary.failed_partitions(),
    sent_batches: summary.sent_batches(),
    failures: source_backed_summary_failures(summary.failures()),
  }
}

///|
fn SourceBackedExecution::close(self : SourceBackedExecution) -> Unit {
  if !self.closed {
    self.closed = true
    self.kafka_stop_requested = true
    match self.kafka_source {
      Some(source) => source.close()
      None => ()
    }
    match self.output_manifest {
      Some(manifest) => manifest.close()
      None => ()
    }
    self.source.close()
    self.job_store.close()
    self.coordinator.close()
  }
}
