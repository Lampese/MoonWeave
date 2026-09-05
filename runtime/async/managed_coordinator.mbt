///|
/// Send one batch while keeping the synchronous CoordinatorState in lockstep
/// with the wire session. The control state is updated only after all batch
/// identity checks have passed and the frame has been written.
async fn managed_send_batch(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  store : CoordinatorStore?,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  assignment : @control.PartitionAssignment,
  batch : @protocol.EventBatch,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  if batch.partition() != assignment.partition() ||
    batch.epoch() != assignment.epoch() ||
    batch.first_offset() != assignment.next_offset() {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::InvalidSendOffset(
        expected=assignment.next_offset(),
        actual=batch.first_offset(),
      ),
    )
  }
  let bytes = @utf8.encode(batch.to_payload()).length().to_int64()
  let expected = control.next_offset(
    assignment.partition(),
    assignment.worker_id(),
    assignment.incarnation(),
    assignment.epoch(),
  ) catch {
    error => raise FrameIoError::Coordinator(error)
  }
  if expected != batch.first_offset() {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::InvalidSendOffset(
        expected~,
        actual=batch.first_offset(),
      ),
    )
  }
  match store {
    Some(durable) => {
      // Persist intent before the write. A crash after this point is allowed
      // to resend the range, but recovery can never skip it.
      ignore(
        durable.record_batch_sent(
          assignment.partition(),
          assignment.worker_id(),
          assignment.incarnation(),
          assignment.epoch(),
          batch.first_offset(),
          batch.events().length().to_int64(),
        ) catch {
          error => raise FrameIoError::CoordinatorStore(error)
        },
      )
      coordinator_send_batch(
        connection,
        session,
        outbound,
        batch,
        bytes,
        maximum_frame_bytes~,
      )
    }
    None => {
      coordinator_send_batch(
        connection,
        session,
        outbound,
        batch,
        bytes,
        maximum_frame_bytes~,
      )
      ignore(
        control.record_batch_sent(
          assignment.partition(),
          assignment.worker_id(),
          assignment.incarnation(),
          assignment.epoch(),
          batch.first_offset(),
          batch.events().length().to_int64(),
        ) catch {
          error => raise FrameIoError::Coordinator(error)
        },
      )
    }
  }
}

///|
/// Drive one targeted query proof while keeping liveness and progress in the
/// same control-state stream as ordinary data frames.
async fn managed_probe_and_seal(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  store : CoordinatorStore?,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  probe : @protocol.Probe,
  maximum_frame_bytes : Int,
) -> @protocol.ProbeReply raise FrameIoError {
  coordinator_send_probe(
    connection,
    session,
    outbound,
    probe,
    maximum_frame_bytes~,
  )
  for ;; {
    let frame = read_frame(connection, maximum_frame_bytes~)
    session.accept_frame(frame) catch {
      error => raise FrameIoError::Session(error)
    }
    match frame.kind() {
      @protocol.FrameKind::ProbeReply => {
        let reply = @protocol.ProbeReply::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        if reply.query_id() != probe.query_id() ||
          reply.partition() != probe.partition() ||
          reply.epoch() != probe.epoch() {
          raise FrameIoError::ProbeMismatch
        }
        if reply.reached_offset() < probe.target_offset() {
          raise FrameIoError::ProbeNotReached(
            target=probe.target_offset(),
            actual=reply.reached_offset(),
          )
        }
        let seal = @protocol.Seal::new(
          reply.query_id(),
          reply.partition(),
          reply.epoch(),
          reply.reached_offset(),
          reply.frontier_digest(),
        ) catch {
          error => raise FrameIoError::Protocol(error)
        }
        coordinator_send_seal(
          connection,
          session,
          outbound,
          seal,
          maximum_frame_bytes~,
        )
        return reply
      }
      @protocol.FrameKind::Heartbeat => {
        let heartbeat = @protocol.Heartbeat::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        if heartbeat.partition() != assignment.partition() ||
          heartbeat.epoch() != assignment.epoch() {
          raise FrameIoError::Coordinator(
            @control.CoordinatorError::StaleEpoch(
              expected=assignment.epoch(),
              actual=heartbeat.epoch(),
            ),
          )
        }
        match store {
          Some(durable) =>
            durable.record_heartbeat(
              worker.worker_id(),
              worker.incarnation(),
              @async.now(),
            ) catch {
              error => raise FrameIoError::CoordinatorStore(error)
            }
          None =>
            control.record_heartbeat(
              worker.worker_id(),
              worker.incarnation(),
              @async.now(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
        }
      }
      @protocol.FrameKind::Progress => {
        let progress = @protocol.Progress::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        match store {
          Some(durable) =>
            durable.record_progress(
              assignment.partition(),
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              progress.persisted_offset(),
              progress.applied_offset(),
            ) catch {
              error => raise FrameIoError::CoordinatorStore(error)
            }
          None =>
            control.record_progress(
              assignment.partition(),
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              progress.persisted_offset(),
              progress.applied_offset(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
        }
      }
      @protocol.FrameKind::Credit | @protocol.FrameKind::AckPersisted => ()
      _ =>
        raise FrameIoError::Session(
          @protocol.SessionError::WrongPhase(
            expected=@protocol.SessionPhase::Running,
            actual=session.phase(),
          ),
        )
    }
  }
}

///|
/// Coordinator driver backed by a static membership/assignment state machine.
/// It owns one worker connection; callers may run several instances in an
/// async task group after making assignments through `CoordinatorState`.
pub async fn run_managed_coordinator(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  worker_id : String,
  session_id : String,
  partition : Int,
  batches : Array[@protocol.EventBatch],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
  probe? : @protocol.Probe,
) -> CoordinatorRunSummary raise FrameIoError {
  run_managed_coordinator_impl(
    connection,
    control,
    worker_id,
    session_id,
    partition,
    batches,
    maximum_frame_bytes,
    probe,
    None,
  )
}

///|
/// Durable managed driver. It journals source reservations and worker control
/// events, then compacts the journal into a checkpoint after graceful exit.
pub async fn run_durable_managed_coordinator(
  connection : @socket.Tcp,
  store : CoordinatorStore,
  worker_id : String,
  session_id : String,
  partition : Int,
  batches : Array[@protocol.EventBatch],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
  probe? : @protocol.Probe,
) -> CoordinatorRunSummary raise FrameIoError {
  let summary = run_managed_coordinator_impl(
    connection,
    store.state(),
    worker_id,
    session_id,
    partition,
    batches,
    maximum_frame_bytes,
    probe,
    Some(store),
  )
  store.checkpoint() catch {
    error => raise FrameIoError::CoordinatorStore(error)
  }
  summary
}

///|
async fn run_managed_coordinator_impl(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  worker_id : String,
  session_id : String,
  partition : Int,
  batches : Array[@protocol.EventBatch],
  maximum_frame_bytes : Int,
  probe : @protocol.Probe?,
  store : CoordinatorStore?,
) -> CoordinatorRunSummary raise FrameIoError {
  defer connection.close()
  let worker = match control.worker(worker_id) {
    Some(value) => value
    None =>
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::UnknownWorker(worker_id),
      )
  }
  if worker.lifecycle() != @control.WorkerLifecycle::Active {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::WorkerNotActive(worker_id),
    )
  }
  let assignment = match control.assignment(partition) {
    Some(value) => value
    None =>
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::UnknownPartition(partition),
      )
  }
  if assignment.worker_id() != worker.worker_id() ||
    assignment.incarnation() != worker.incarnation() {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::AssignmentConflict(assignment.partition()),
    )
  }
  let spec = @protocol.AssignmentSpec::new(
    assignment.partition(),
    assignment.epoch(),
    assignment.incarnation().value(),
    start_offset=assignment.start_offset(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  validate_batch_plan(spec, batches[:])
  let (session, outbound) = coordinator_handshake(
    connection,
    control.cluster_id(),
    control.coordinator_id(),
    worker_id,
    session_id,
    assignment=spec,
    maximum_frame_bytes~,
  )
  session.restore_send_cursor(
    assignment.partition(),
    assignment.epoch(),
    assignment.next_offset(),
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  let mut next_batch = 0
  let mut sent_batches = 0
  let mut probe_reply : @protocol.ProbeReply? = None
  for ;; {
    let frame = read_frame(connection, maximum_frame_bytes~)
    session.accept_frame(frame) catch {
      error => raise FrameIoError::Session(error)
    }
    match frame.kind() {
      @protocol.FrameKind::Resume => {
        let resume_message = @protocol.Resume::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        next_batch = resume_batch_index(
          batches[:],
          resume_message.persisted_offset(),
        )
        match store {
          Some(durable) =>
            durable.record_resume(
              assignment.partition(),
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              resume_message.persisted_offset(),
            ) catch {
              error => raise FrameIoError::CoordinatorStore(error)
            }
          None =>
            control.record_resume(
              assignment.partition(),
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              resume_message.persisted_offset(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
        }
      }
      @protocol.FrameKind::Credit | @protocol.FrameKind::AckPersisted => ()
      @protocol.FrameKind::Heartbeat => {
        let heartbeat = @protocol.Heartbeat::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        if heartbeat.partition() != assignment.partition() ||
          heartbeat.epoch() != assignment.epoch() {
          raise FrameIoError::Coordinator(
            @control.CoordinatorError::StaleEpoch(
              expected=assignment.epoch(),
              actual=heartbeat.epoch(),
            ),
          )
        }
        match store {
          Some(durable) =>
            durable.record_heartbeat(
              worker.worker_id(),
              worker.incarnation(),
              @async.now(),
            ) catch {
              error => raise FrameIoError::CoordinatorStore(error)
            }
          None =>
            control.record_heartbeat(
              worker.worker_id(),
              worker.incarnation(),
              @async.now(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
        }
        let persisted = heartbeat.persisted_offset()
        if persisted >= assignment.persisted_offset() &&
          persisted >= assignment.applied_offset() {
          match store {
            Some(durable) =>
              durable.record_progress(
                assignment.partition(),
                worker.worker_id(),
                worker.incarnation(),
                assignment.epoch(),
                persisted,
                assignment.applied_offset(),
              ) catch {
                error => raise FrameIoError::CoordinatorStore(error)
              }
            None =>
              control.record_progress(
                assignment.partition(),
                worker.worker_id(),
                worker.incarnation(),
                assignment.epoch(),
                persisted,
                assignment.applied_offset(),
              ) catch {
                error => raise FrameIoError::Coordinator(error)
              }
          }
        }
      }
      @protocol.FrameKind::Progress => ()
      @protocol.FrameKind::Drain => break
      _ =>
        raise FrameIoError::Session(
          @protocol.SessionError::WrongPhase(
            expected=@protocol.SessionPhase::Running,
            actual=session.phase(),
          ),
        )
    }
    match frame.kind() {
      @protocol.FrameKind::Progress => {
        let progress = @protocol.Progress::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        match store {
          Some(durable) =>
            durable.record_progress(
              assignment.partition(),
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              progress.persisted_offset(),
              progress.applied_offset(),
            ) catch {
              error => raise FrameIoError::CoordinatorStore(error)
            }
          None =>
            control.record_progress(
              assignment.partition(),
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              progress.persisted_offset(),
              progress.applied_offset(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
        }
      }
      _ => ()
    }
    while next_batch < batches.length() {
      let batch = batches[next_batch]
      if batch.events().length().to_int64() > session.credit_events() {
        break
      }
      let bytes = @utf8.encode(batch.to_payload()).length().to_int64()
      if bytes > session.credit_bytes() {
        break
      }
      managed_send_batch(
        connection, control, store, session, outbound, assignment, batch, maximum_frame_bytes,
      )
      next_batch += 1
      sent_batches += 1
    }
    let last_offset = match batches.last() {
      None => assignment.start_offset() - 1L
      Some(batch) => batch.end_offset()
    }
    if next_batch == batches.length() &&
      session.last_applied_offset() >= last_offset {
      match probe {
        Some(probe_value) => {
          let reply = managed_probe_and_seal(
            connection, control, store, worker, assignment, session, outbound, probe_value,
            maximum_frame_bytes,
          )
          probe_reply = Some(reply)
        }
        None => ()
      }
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}") catch {
        error => raise FrameIoError::Protocol(error)
      }
      write_frame(connection, drain, maximum_frame_bytes~)
      break
    }
  }
  {
    sent_batches,
    sent_deltas: 0,
    persisted_offset: session.last_persisted_offset(),
    applied_offset: session.last_applied_offset(),
    probe_reply,
  }
}
