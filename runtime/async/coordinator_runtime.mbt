///|
/// Summary of one coordinator-to-worker connection run.
pub struct CoordinatorRunSummary {
  sent_batches : Int
  sent_deltas : Int
  persisted_offset : Int64
  applied_offset : Int64
  probe_reply : @protocol.ProbeReply?
}

///|
pub fn CoordinatorRunSummary::sent_batches(self : CoordinatorRunSummary) -> Int {
  self.sent_batches
}

///|
pub fn CoordinatorRunSummary::sent_deltas(self : CoordinatorRunSummary) -> Int {
  self.sent_deltas
}

///|
pub fn CoordinatorRunSummary::persisted_offset(
  self : CoordinatorRunSummary,
) -> Int64 {
  self.persisted_offset
}

///|
pub fn CoordinatorRunSummary::applied_offset(
  self : CoordinatorRunSummary,
) -> Int64 {
  self.applied_offset
}

///|
pub fn CoordinatorRunSummary::probe_reply(
  self : CoordinatorRunSummary,
) -> @protocol.ProbeReply? {
  self.probe_reply
}

///|
/// Validate the source plan before any session or control cursor mutates.
fn validate_batch_plan(
  assignment : @protocol.AssignmentSpec,
  batches : ArrayView[@protocol.EventBatch],
) -> Unit raise FrameIoError {
  let mut expected = assignment.start_offset()
  for batch in batches {
    if batch.partition() != assignment.partition() ||
      batch.epoch() != assignment.epoch() {
      raise FrameIoError::Protocol(
        @protocol.ProtocolError::StaleEpoch(
          expected=assignment.epoch(),
          actual=batch.epoch(),
        ),
      )
    }
    if batch.first_offset() != expected {
      raise FrameIoError::Protocol(
        @protocol.ProtocolError::InvalidOffset(batch.first_offset()),
      )
    }
    expected = batch.end_offset() + 1L
  }
}

///|
/// Find the first complete batch after a worker's persisted cursor.
fn resume_batch_index(
  batches : ArrayView[@protocol.EventBatch],
  persisted_offset : Int64,
) -> Int raise FrameIoError {
  let mut index = 0
  while index < batches.length() &&
        batches[index].end_offset() <= persisted_offset {
    index += 1
  }
  if index < batches.length() &&
    batches[index].first_offset() <= persisted_offset {
    raise FrameIoError::Handler("resume cursor splits an EventBatch")
  }
  index
}

///|
/// Send every batch that fits the currently granted event and byte window.
/// The session reserves credit before bytes enter the socket.
async fn send_available_batches(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  batches : Array[@protocol.EventBatch],
  next_batch : Int,
  maximum_frame_bytes : Int,
) -> (Int, Int) raise FrameIoError {
  let mut sent = 0
  let mut index = next_batch
  while index < batches.length() {
    let batch = batches[index]
    let events = batch.events().length().to_int64()
    let bytes = @utf8.encode(batch.to_payload()).length().to_int64()
    if events > session.credit_events() || bytes > session.credit_bytes() {
      break
    }
    coordinator_send_batch(
      connection,
      session,
      outbound,
      batch,
      bytes,
      maximum_frame_bytes~,
    )
    index += 1
    sent += 1
  }
  (index, sent)
}

///|
/// Run the coordinator side of one assigned worker connection.
///
/// This is intentionally a single-connection driver rather than a membership
/// service. A higher-level scheduler can own many of these tasks and decide
/// assignments, retries, and fencing policy.
pub async fn run_coordinator(
  connection : @socket.Tcp,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  session_id : String,
  assignment : @protocol.AssignmentSpec,
  batches : Array[@protocol.EventBatch],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> CoordinatorRunSummary raise FrameIoError {
  defer connection.close()
  validate_batch_plan(assignment, batches[:])
  let (session, outbound) = coordinator_handshake(
    connection,
    cluster_id,
    coordinator_id,
    worker_id,
    session_id,
    assignment~,
    maximum_frame_bytes~,
  )
  let mut next_batch = 0
  let mut sent_batches = 0
  let last_offset = match batches.last() {
    None => assignment.start_offset() - 1L
    Some(batch) => batch.end_offset()
  }
  for ;; {
    let frame = read_frame(connection, maximum_frame_bytes~)
    let resume_message = match frame.kind() {
      @protocol.FrameKind::Resume =>
        Some(
          @protocol.Resume::from_payload(frame.payload()) catch {
            error => raise FrameIoError::Message(error)
          },
        )
      _ => None
    }
    session.accept_frame(frame) catch {
      error => raise FrameIoError::Session(error)
    }
    match resume_message {
      Some(value) =>
        next_batch = resume_batch_index(batches[:], value.persisted_offset())
      None => ()
    }
    match frame.kind() {
      @protocol.FrameKind::Resume
      | @protocol.FrameKind::Credit
      | @protocol.FrameKind::AckPersisted
      | @protocol.FrameKind::Heartbeat
      | @protocol.FrameKind::Progress => {
        let (updated_next_batch, newly_sent) = send_available_batches(
          connection, session, outbound, batches, next_batch, maximum_frame_bytes,
        )
        next_batch = updated_next_batch
        sent_batches += newly_sent
        if next_batch == batches.length() &&
          session.last_applied_offset() >= last_offset {
          let drain = outbound.build(@protocol.FrameKind::Drain, "{}") catch {
            error => raise FrameIoError::Protocol(error)
          }
          write_frame(connection, drain, maximum_frame_bytes~)
          break
        }
      }
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
  {
    sent_batches,
    sent_deltas: 0,
    persisted_offset: session.last_persisted_offset(),
    applied_offset: session.last_applied_offset(),
    probe_reply: None,
  }
}
