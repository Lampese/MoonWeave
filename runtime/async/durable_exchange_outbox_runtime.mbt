///|
fn collect_durable_outbox(
  outbox : DurableExchangeOutbox,
  pending : Array[@protocol.DeltaBatch],
) -> Unit {
  for ;; {
    match outbox.try_pop() {
      Some(delta) => pending.push(delta)
      None => return
    }
  }
}

///|
async fn send_one_durable_delta(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  outbox : DurableExchangeOutbox,
  delta : @protocol.DeltaBatch,
  maximum_frame_bytes : Int,
) -> String raise FrameIoError {
  let delta_id = delta.delta_id()
  errdefer outbox.requeue(delta_id)
  coordinator_send_delta(
    connection,
    session,
    outbound,
    delta,
    @utf8.encode(delta.to_payload()).length().to_int64(),
    maximum_frame_bytes~,
  )
}

///|
async fn send_pending_durable_deltas(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  outbox : DurableExchangeOutbox,
  pending : Array[@protocol.DeltaBatch],
  sent_ids : Map[String, Bool],
  maximum_frame_bytes : Int,
) -> Int raise FrameIoError {
  let mut sent = 0
  while !pending.is_empty() {
    let delta = pending[0]
    let events = delta.events().length().to_int64()
    let bytes = @utf8.encode(delta.to_payload()).length().to_int64()
    if events > session.credit_events() || bytes > session.credit_bytes() {
      break
    }
    ignore(pending.remove(0))
    let delta_id = delta.delta_id()
    ignore(
      send_one_durable_delta(
        connection, session, outbound, outbox, delta, maximum_frame_bytes,
      ),
    )
    sent_ids[delta_id] = false
    sent += 1
  }
  sent
}

///|
/// Consume a WAL-backed exchange outbox. All unacknowledged entries survive a
/// process restart and are re-presented to the target Worker.
pub async fn run_managed_durable_exchange_outbox(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  worker_id : String,
  session_id : String,
  partition : Int,
  outbox : DurableExchangeOutbox,
  poll_interval_ms? : Int = 10,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> ExchangeRunSummary raise FrameIoError {
  if poll_interval_ms <= 0 {
    raise FrameIoError::Handler(
      "exchange outbox poll interval must be positive",
    )
  }
  defer connection.close()
  defer outbox.requeue_all()
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
      @control.CoordinatorError::AssignmentConflict(partition),
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
  let pending : Array[@protocol.DeltaBatch] = []
  let sent_ids : Map[String, Bool] = Map([])
  let mut sent_deltas = 0
  let mut acked_deltas = 0
  for ;; {
    let incoming = @async.with_timeout_opt(poll_interval_ms, () => {
      read_frame_for_timeout(connection, maximum_frame_bytes)
    }) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
    match incoming {
      None => ()
      Some(frame) => {
        session.accept_frame(frame) catch {
          error => raise FrameIoError::Session(error)
        }
        match frame.kind() {
          @protocol.FrameKind::Resume => {
            let resume_message = @protocol.Resume::from_payload(frame.payload()) catch {
              error => raise FrameIoError::Message(error)
            }
            control.record_resume(
              partition,
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              resume_message.persisted_offset(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
          }
          @protocol.FrameKind::Progress => {
            let progress = @protocol.Progress::from_payload(frame.payload()) catch {
              error => raise FrameIoError::Message(error)
            }
            control.record_progress(
              partition,
              worker.worker_id(),
              worker.incarnation(),
              assignment.epoch(),
              progress.persisted_offset(),
              progress.applied_offset(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
          }
          @protocol.FrameKind::Heartbeat =>
            control.record_heartbeat(
              worker.worker_id(),
              worker.incarnation(),
              @async.now(),
            ) catch {
              error => raise FrameIoError::Coordinator(error)
            }
          @protocol.FrameKind::DeltaAck => {
            let ack = @protocol.DeltaAck::from_payload(frame.payload()) catch {
              error => raise FrameIoError::Message(error)
            }
            if ack.target_partition() != partition ||
              ack.target_epoch() != assignment.epoch() {
              raise FrameIoError::DeltaMismatch
            }
            match sent_ids.get(ack.delta_id()) {
              Some(_) => ()
              None => raise FrameIoError::UnknownDeltaAck(ack.delta_id())
            }
            outbox.ack(ack.delta_id()) catch {
              error => raise FrameIoError::DurableOutbox(error)
            }
            if !sent_ids.get(ack.delta_id()).unwrap() {
              sent_ids[ack.delta_id()] = true
              acked_deltas += 1
            }
          }
          @protocol.FrameKind::Credit | @protocol.FrameKind::AckPersisted => ()
          @protocol.FrameKind::Drain => break
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
    collect_durable_outbox(outbox, pending)
    sent_deltas += send_pending_durable_deltas(
      connection, session, outbound, outbox, pending, sent_ids, maximum_frame_bytes,
    )
    let drained = outbox.is_drained() catch {
      error => raise FrameIoError::DurableOutbox(error)
    }
    if drained && pending.is_empty() && acked_deltas == sent_deltas {
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}") catch {
        error => raise FrameIoError::Protocol(error)
      }
      write_frame(connection, drain, maximum_frame_bytes~)
      break
    }
  }
  { sent_deltas, acked_deltas, }
}
