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
  ) catch {
    error => raise live_execution_error("open Coordinator store", error)
  }
  errdefer coordinator.close()
  let job_store = @runtime.JobStore::open(
    job.spec,
    self.config.storage_dir + "/job.wal",
    checkpoint_path=self.config.storage_dir + "/job.json",
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
        error => raise live_execution_error("register Worker", error)
      },
    )
  }
  for partition in 0..<job.spec.partition_count() {
    let worker_id = job.spec.owner(partition) catch {
      error => raise ApiError::Plan(error)
    }
    let assignment = coordinator.assign(partition, worker_id) catch {
      error => raise live_execution_error("assign partition", error)
    }
    let saved = job_store.snapshot().partitions()[partition]
    if saved.phase() == @control.JobPartitionPhase::Unassigned {
      ignore(
        job_store.assign(
          partition,
          worker_id,
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
  ) catch {
    error => raise live_execution_error("open source-backed execution", error)
  }
  errdefer source.close()
  source.ingest_all(inputs.map(event => event.record)) catch {
    error => raise live_execution_error("bind source work plans", error)
  }
  if source.event_count() != inputs.length() {
    raise ApiError::Cluster(
      "SourceLog contains input absent from Driver intent",
    )
  }
  ignore(
    @runtime.reconcile_job_coordinator(
      coordinator,
      job_store,
      reconcile_path=self.config.storage_dir + "/live-reconcile.wal",
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
    SinkSpec::Durable(_) =>
      Some(
        @runtime.JobOutputManifest::open(
          self.config.storage_dir + "/manifest.wal",
          job.definition.job_id,
          job.definition.operator.id(),
          job.spec.partition_count(),
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
  self.source.seal() catch {
    error => raise live_execution_error("seal live source", error)
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
  self.source.cancel() catch {
    error => raise live_execution_error("cancel live source", error)
  }
}

///|
fn source_backed_summary_failures(
  failures : ArrayView[@runtime.SupervisorFailure],
) -> Array[String] {
  coordinator_failures(failures)
}

///|
/// Run all assigned Worker sessions until ingress is sealed and every source
/// cursor has passed the existing drain barrier. This function is intentionally
/// long-lived while the Driver phase is Running.
async fn SourceBackedExecution::run(
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
  let summary = @runtime.run_live_source_supervisor(
    self.listener.server,
    self.source,
    output_manifest?=self.output_manifest,
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
  } else {
    ignore(
      @runtime.reconcile_job_coordinator(
        self.coordinator,
        self.job_store,
        reconcile_path=self.listener.config.storage_dir + "/live-reconcile.wal",
      ),
    ) catch {
      error => raise live_execution_error("reconcile completed live Job", error)
    }
    self.coordinator.checkpoint_and_compact() catch {
      error => raise live_execution_error("checkpoint live Coordinator", error)
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
    match self.output_manifest {
      Some(manifest) => manifest.close()
      None => ()
    }
    self.source.close()
    self.job_store.close()
    self.coordinator.close()
  }
}
