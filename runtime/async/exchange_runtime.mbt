///|
/// Summary of one Coordinator-to-Worker derived-delta stream.
pub struct ExchangeRunSummary {
  sent_deltas : Int
  acked_deltas : Int
}

///|
pub fn ExchangeRunSummary::sent_deltas(self : ExchangeRunSummary) -> Int {
  self.sent_deltas
}

///|
pub fn ExchangeRunSummary::acked_deltas(self : ExchangeRunSummary) -> Int {
  self.acked_deltas
}

///|
fn validate_delta_plan(
  assignment : @protocol.AssignmentSpec,
  deltas : ArrayView[@protocol.DeltaBatch],
) -> Unit raise FrameIoError {
  for delta in deltas {
    if delta.target_partition() != assignment.partition() ||
      delta.target_epoch() != assignment.epoch() {
      raise FrameIoError::DeltaMismatch
    }
  }
}

///|
/// Run a derived-event stream to one assigned Worker. Delta batches consume
/// target credit and are acknowledged by frame identity, but do not advance
/// the target's primary source cursor.
pub async fn run_managed_exchange(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  worker_id : String,
  session_id : String,
  partition : Int,
  deltas : Array[@protocol.DeltaBatch],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> ExchangeRunSummary raise FrameIoError {
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
  validate_delta_plan(spec, deltas[:])
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
  let sent_ids : Map[String, Bool] = Map([])
  let mut next_delta = 0
  let mut sent_deltas = 0
  let mut acked_deltas = 0
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
    while next_delta < deltas.length() {
      let delta = deltas[next_delta]
      let events = delta.events().length().to_int64()
      let bytes = @utf8.encode(delta.to_payload()).length().to_int64()
      if events > session.credit_events() || bytes > session.credit_bytes() {
        break
      }
      let delta_id = coordinator_send_delta(
        connection,
        session,
        outbound,
        delta,
        bytes,
        maximum_frame_bytes~,
      )
      sent_ids[delta_id] = false
      next_delta += 1
      sent_deltas += 1
    }
    if next_delta == deltas.length() && acked_deltas == sent_deltas {
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}") catch {
        error => raise FrameIoError::Protocol(error)
      }
      write_frame(connection, drain, maximum_frame_bytes~)
      break
    }
  }
  { sent_deltas, acked_deltas, }
}
