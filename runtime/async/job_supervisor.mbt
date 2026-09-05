///|
fn job_store_partition(
  job : JobStore,
  partition : Int,
) -> @control.JobPartition raise FrameIoError {
  for value in job.snapshot().partitions() {
    if value.partition() == partition {
      return value
    }
  }
  raise FrameIoError::JobStore(
    JobStoreError::Job(@control.JobError::UnknownPartition(partition)),
  )
}

///|
fn validate_job_control(
  job : JobStore,
  assignment : @control.PartitionAssignment,
) -> @control.JobPartition raise FrameIoError {
  let saved = job_store_partition(job, assignment.partition())
  if saved.worker_id() != assignment.worker_id() ||
    saved.epoch() != assignment.epoch() ||
    saved.start_offset() != assignment.start_offset() ||
    assignment.next_offset() > saved.next_offset() ||
    assignment.persisted_offset() > saved.persisted_offset() ||
    assignment.applied_offset() > saved.applied_offset() {
    raise FrameIoError::JobStore(
      JobStoreError::PlanMismatch(assignment.partition()),
    )
  }
  saved
}

///|
fn restore_job_control(
  control : @control.CoordinatorState,
  job : JobStore,
  assignment : @control.PartitionAssignment,
) -> Unit raise FrameIoError {
  let saved = validate_job_control(job, assignment)
  if assignment.next_offset() < saved.next_offset() {
    ignore(
      control.record_batch_sent(
        assignment.partition(),
        assignment.worker_id(),
        assignment.incarnation(),
        assignment.epoch(),
        assignment.next_offset(),
        saved.next_offset() - assignment.next_offset(),
      ) catch {
        error => raise FrameIoError::Coordinator(error)
      },
    )
  }
  control.record_progress(
    assignment.partition(),
    assignment.worker_id(),
    assignment.incarnation(),
    assignment.epoch(),
    saved.persisted_offset(),
    saved.applied_offset(),
  ) catch {
    error => raise FrameIoError::Coordinator(error)
  }
}

///|
async fn record_job_send(
  job : JobStore,
  assignment : @control.PartitionAssignment,
  batch : @protocol.EventBatch,
) -> Unit raise FrameIoError {
  let saved = job_store_partition(job, assignment.partition())
  // Reconnect may resend already journaled intent; the bound work plan checks
  // its contents, while the high-water cursor prevents reserving it twice.
  if batch.end_offset() < saved.next_offset() {
    return
  }
  ignore(
    job.record_batch_sent(
      assignment.partition(),
      assignment.worker_id(),
      assignment.epoch(),
      batch.first_offset(),
      batch.events().length().to_int64(),
    ) catch {
      error => raise FrameIoError::JobStore(error)
    },
  )
}

///|
async fn record_job_progress(
  job : JobStore,
  assignment : @control.PartitionAssignment,
  persisted : Int64,
  applied : Int64,
) -> Unit raise FrameIoError {
  job.record_progress(
    assignment.partition(),
    assignment.worker_id(),
    assignment.epoch(),
    persisted,
    applied,
  ) catch {
    error => raise FrameIoError::JobStore(error)
  }
}

///|
async fn wait_job_drain(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  session : @protocol.CoordinatorSession,
  job : JobStore,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  for ;; {
    let frame = read_frame(connection, maximum_frame_bytes~)
    session.accept_frame(frame) catch {
      error => raise FrameIoError::Session(error)
    }
    match frame.kind() {
      @protocol.FrameKind::DrainAck => {
        guard session.drain_ack() is Some(ack) else {
          raise FrameIoError::DrainMismatch
        }
        if ack.pending_events != 0 {
          raise FrameIoError::PendingEvents(ack.pending_events)
        }
        record_job_progress(
          job,
          assignment,
          ack.persisted_offset,
          ack.applied_offset,
        )
        job.stop_partition(
          assignment.partition(),
          worker.worker_id(),
          assignment.epoch(),
          ack.pending_events,
        ) catch {
          error => raise FrameIoError::JobStore(error)
        }
        return
      }
      @protocol.FrameKind::Progress => {
        let progress = @protocol.Progress::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        record_job_progress(
          job,
          assignment,
          progress.persisted_offset(),
          progress.applied_offset(),
        )
        supervisor_record_progress(
          control,
          None,
          worker,
          assignment,
          progress.persisted_offset(),
          progress.applied_offset(),
        )
      }
      @protocol.FrameKind::Heartbeat =>
        supervisor_record_heartbeat(control, None, worker)
      @protocol.FrameKind::Credit | @protocol.FrameKind::AckPersisted => ()
      _ => raise FrameIoError::DrainMismatch
    }
  }
}

///|
async fn confirm_job_drain(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  job : JobStore,
  target_offset : Int64,
  maximum_frame_bytes : Int,
  timeout_ms : Int,
) -> Unit raise FrameIoError {
  job.drain_partition(
    assignment.partition(),
    worker.worker_id(),
    assignment.epoch(),
  ) catch {
    error => raise FrameIoError::JobStore(error)
  }
  let request = @protocol.DrainRequest::new(
    assignment.partition(),
    assignment.epoch(),
    target_offset,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(
    @protocol.FrameKind::DrainRequest,
    request.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  session.begin_drain(frame.message_id(), request) catch {
    error => raise FrameIoError::Session(error)
  }
  let result : Result[Unit, FrameIoError] = @async.with_timeout(timeout_ms, () => {
    try {
      write_frame(connection, frame, maximum_frame_bytes~)
      wait_job_drain(
        connection, control, worker, assignment, session, job, maximum_frame_bytes,
      )
      Ok(())
    } catch {
      error => Err(error)
    }
  }) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  match result {
    Ok(_) => ()
    Err(error) => raise error
  }
}

///|
/// Run or resume a finite Job from its durable lifecycle and input plan.
/// Supply the full plan; completed partitions do not require another connection.
/// Coordinator membership/epochs come from the caller, while JobStore owns
/// send intent, progress, and completion for this driver.
pub async fn run_job_supervisor(
  server : @socket.TcpServer,
  control : @control.CoordinatorState,
  job_store : JobStore,
  works : Array[SupervisorWork],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
  drain_timeout_ms? : Int = 5000,
) -> SupervisorRunSummary raise FrameIoError {
  if !job_store.supervisor_lock.try_acquire() {
    raise FrameIoError::JobStore(JobStoreError::AlreadyRunning)
  }
  defer job_store.supervisor_lock.release()
  if drain_timeout_ms <= 0 {
    raise FrameIoError::Handler("drain timeout must be positive")
  }
  let phase = job_store.phase()
  if phase == @control.JobPhase::Failed || phase == @control.JobPhase::Cancelled {
    raise FrameIoError::JobStore(
      JobStoreError::Job(
        @control.JobError::InvalidTransition(
          expected=@control.JobPhase::Running,
          actual=phase,
        ),
      ),
    )
  }
  let snapshot = job_store.snapshot()
  if works.length() != snapshot.partition_count() {
    raise FrameIoError::JobStore(
      JobStoreError::Job(@control.JobError::NotFullyAssigned),
    )
  }
  let active = []
  let frozen_works = []
  let seen : Set[Int] = Set([])
  for work in works {
    if !seen.add_and_check(work.partition()) {
      raise FrameIoError::SupervisorPlan(
        SupervisorPlanError::DuplicatePartition(work.partition()),
      )
    }
    let plan = job_store.check_work(work) catch {
      error => raise FrameIoError::JobStore(error)
    }
    let frozen = SupervisorWork::new(
      work.worker_id(),
      work.session_id(),
      work.partition(),
      plan.batches,
      deltas=plan.deltas,
      probe?=plan.probe,
    )
    frozen_works.push(frozen)
    let partition = job_store_partition(job_store, work.partition())
    if partition.phase() == @control.JobPartitionPhase::Succeeded {
      continue
    }
    if partition.phase() == @control.JobPartitionPhase::Failed {
      raise FrameIoError::JobStore(
        JobStoreError::Job(@control.JobError::PartitionFailed(work.partition())),
      )
    }
    let (_, assignment, _) = validate_supervisor_work(control, work)
    if assignment.epoch() != partition.epoch() ||
      assignment.start_offset() != partition.start_offset() {
      raise FrameIoError::JobStore(
        JobStoreError::PlanMismatch(work.partition()),
      )
    }
    ignore(validate_job_control(job_store, assignment))
    active.push(frozen)
  }
  validate_supervisor_plan(control, active, maximum_frame_bytes)
  for work in frozen_works {
    job_store.bind_work(work) catch {
      error => raise FrameIoError::JobStore(error)
    }
  }
  if phase == @control.JobPhase::Created {
    job_store.start() catch {
      error => raise FrameIoError::JobStore(error)
    }
  }
  let retry = RetryPolicy::new(1, 0, 0) catch {
    _ => raise FrameIoError::Handler("invalid retry policy")
  }
  let summary = run_supervisor_impl(
    server,
    control,
    active,
    maximum_frame_bytes,
    None,
    retry,
    job_store~,
    drain_timeout_ms~,
  )
  job_store.checkpoint() catch {
    error => raise FrameIoError::JobStore(error)
  }
  summary
}
