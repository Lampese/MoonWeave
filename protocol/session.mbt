///|
/// Lifecycle of a worker connection after the transport handshake.
pub(all) enum SessionPhase {
  Disconnected
  Handshaking
  Running
  Draining
} derive(Debug, Eq)

///|
pub(all) suberror SessionError {
  WrongPhase(expected~ : SessionPhase, actual~ : SessionPhase)
  ClusterMismatch
  SessionMismatch
  SenderMismatch
  IncarnationMismatch
  MessageRegression(expected_at_least~ : Int64, actual~ : Int64)
  MessageGap(expected~ : Int64, actual~ : Int64)
  MessageIdConflict(String)
  Frame(ProtocolError)
  Payload(MessageError)
  Assignment(ProtocolError)
} derive(Debug, Eq)

///|
/// Pure protocol session state for one connected worker.
pub struct WorkerSession {
  cluster_id : String
  coordinator_id : String
  worker_id : String
  incarnation : String
  mut session_id : String
  mut phase : SessionPhase
  mut next_sequence : Int64
  seen_messages : Map[String, Frame]
  mut last_received_offset : Int64
  mut last_persisted_offset : Int64
  mut last_applied_offset : Int64
  state : WorkerState
}

///|
pub fn WorkerSession::new(
  cluster_id : String,
  worker_id : String,
  incarnation : String,
  coordinator_id? : String = "coordinator",
) -> WorkerSession raise SessionError {
  let worker_state = WorkerState::new(worker_id, Incarnation::new(incarnation)) catch {
    error => raise SessionError::Frame(error)
  }
  if cluster_id.is_empty() || coordinator_id.is_empty() {
    raise SessionError::ClusterMismatch
  }
  {
    cluster_id,
    coordinator_id,
    worker_id: worker_state.worker_id(),
    incarnation: worker_state.incarnation().value(),
    session_id: "",
    phase: Disconnected,
    next_sequence: 0L,
    seen_messages: {},
    last_received_offset: -1L,
    last_persisted_offset: -1L,
    last_applied_offset: -1L,
    state: worker_state,
  }
}

///|
pub fn WorkerSession::phase(self : WorkerSession) -> SessionPhase {
  self.phase
}

///|
pub fn WorkerSession::session_id(self : WorkerSession) -> String {
  self.session_id
}

///|
pub fn WorkerSession::next_sequence(self : WorkerSession) -> Int64 {
  self.next_sequence
}

///|
pub fn WorkerSession::state(self : WorkerSession) -> WorkerState {
  self.state
}

///|
/// Return whether a frame identity has already been admitted on this session.
/// A transport may use this to replay an idempotent acknowledgement without
/// applying the business payload twice.
pub fn WorkerSession::has_seen(
  self : WorkerSession,
  message_id : String,
) -> Bool {
  self.seen_messages.contains(message_id)
}

///|
pub fn WorkerSession::last_received_offset(self : WorkerSession) -> Int64 {
  self.last_received_offset
}

///|
pub fn WorkerSession::last_persisted_offset(self : WorkerSession) -> Int64 {
  self.last_persisted_offset
}

///|
pub fn WorkerSession::last_applied_offset(self : WorkerSession) -> Int64 {
  self.last_applied_offset
}

///|
/// Record that a received input offset has been durably written to the worker
/// WAL. The async storage adapter must call this only after its write barrier.
pub fn WorkerSession::mark_persisted(
  self : WorkerSession,
  offset : Int64,
) -> Unit raise SessionError {
  if offset < self.last_persisted_offset || offset > self.last_received_offset {
    raise SessionError::Frame(ProtocolError::InvalidOffset(offset))
  }
  self.last_persisted_offset = offset
}

///|
/// Record that a persisted input offset has been applied to the operator state.
pub fn WorkerSession::mark_applied(
  self : WorkerSession,
  offset : Int64,
) -> Unit raise SessionError {
  if offset < self.last_applied_offset || offset > self.last_persisted_offset {
    raise SessionError::Frame(ProtocolError::InvalidOffset(offset))
  }
  self.last_applied_offset = offset
}

///|
/// Restore input progress reconstructed from a trusted local WAL before the
/// next network batch is admitted.
pub fn WorkerSession::restore_progress(
  self : WorkerSession,
  received_offset : Int64,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise SessionError {
  guard self.state.assignment() is Some(assignment) else {
    raise SessionError::Assignment(ProtocolError::UnknownAssignment)
  }
  let minimum = assignment.next_offset() - 1L
  if received_offset < minimum ||
    persisted_offset < minimum ||
    applied_offset < minimum ||
    persisted_offset > received_offset ||
    applied_offset > persisted_offset {
    raise SessionError::Frame(ProtocolError::InvalidOffset(received_offset))
  }
  self.last_received_offset = received_offset
  self.last_persisted_offset = persisted_offset
  self.last_applied_offset = applied_offset
}

///|
fn WorkerSession::validate_frame(
  self : WorkerSession,
  frame : Frame,
) -> Unit raise SessionError {
  if frame.cluster_id() != self.cluster_id {
    raise SessionError::ClusterMismatch
  }
  if frame.sender_id() != self.coordinator_id {
    raise SessionError::SenderMismatch
  }
  if frame.worker_incarnation() != self.incarnation {
    raise SessionError::IncarnationMismatch
  }
  if frame.message_sequence() < self.next_sequence {
    raise SessionError::MessageRegression(
      expected_at_least=self.next_sequence,
      actual=frame.message_sequence(),
    )
  }
  if frame.message_sequence() > self.next_sequence {
    raise SessionError::MessageGap(
      expected=self.next_sequence,
      actual=frame.message_sequence(),
    )
  }
  if self.phase is Running && frame.session_id() != self.session_id {
    raise SessionError::SessionMismatch
  }
}

///|
/// Admit the next frame and advance the receive sequence exactly once.
/// The returned frame kind is left to the caller for typed payload handling.
pub fn WorkerSession::accept_frame(
  self : WorkerSession,
  frame : Frame,
) -> Unit raise SessionError {
  match self.seen_messages.get(frame.message_id()) {
    Some(previous) => {
      if previous == frame {
        return
      }
      raise SessionError::MessageIdConflict(frame.message_id())
    }
    None => ()
  }
  self.validate_frame(frame)
  match (self.phase, frame.kind()) {
    (Disconnected, FrameKind::Welcome) => {
      let welcome = Welcome::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      self.session_id = welcome.session_id()
      self.phase = Running
      match welcome.assignment() {
        None => ()
        Some(spec) => {
          if frame.assignment_epoch() != spec.epoch() {
            raise SessionError::Assignment(
              ProtocolError::StaleEpoch(
                expected=spec.epoch(),
                actual=frame.assignment_epoch(),
              ),
            )
          }
          let assignment = Assignment::new(
            spec.partition(),
            spec.epoch(),
            Incarnation::new(spec.incarnation()),
            start_offset=spec.start_offset(),
          ) catch {
            error => raise SessionError::Assignment(error)
          }
          self.state.assign(assignment) catch {
            error => raise SessionError::Assignment(error)
          }
          self.last_received_offset = spec.start_offset() - 1L
          self.last_persisted_offset = spec.start_offset() - 1L
          self.last_applied_offset = spec.start_offset() - 1L
        }
      }
    }
    (Running, FrameKind::Assign) => {
      let spec = AssignmentSpec::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != spec.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=spec.epoch(),
            actual=frame.assignment_epoch(),
          ),
        )
      }
      if spec.incarnation() != self.incarnation {
        raise SessionError::IncarnationMismatch
      }
      let assignment = Assignment::new(
        spec.partition(),
        spec.epoch(),
        self.state.incarnation(),
        start_offset=spec.start_offset(),
      ) catch {
        error => raise SessionError::Assignment(error)
      }
      self.state.assign(assignment) catch {
        error => raise SessionError::Assignment(error)
      }
      self.last_received_offset = spec.start_offset() - 1L
      self.last_persisted_offset = spec.start_offset() - 1L
      self.last_applied_offset = spec.start_offset() - 1L
    }
    (Running, FrameKind::EventBatch) => {
      let batch = EventBatch::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      guard self.state.assignment() is Some(assignment) else {
        raise SessionError::Assignment(ProtocolError::UnknownAssignment)
      }
      if batch.partition() != assignment.partition() ||
        batch.epoch() != assignment.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=assignment.epoch(),
            actual=batch.epoch(),
          ),
        )
      }
      let first = batch.first_offset()
      let end = batch.end_offset()
      if frame.assignment_epoch() != batch.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=batch.epoch(),
            actual=frame.assignment_epoch(),
          ),
        )
      }
      if first > self.last_received_offset + 1L {
        raise SessionError::Frame(ProtocolError::InvalidOffset(first))
      }
      if first <= self.last_received_offset && end > self.last_received_offset {
        raise SessionError::Frame(ProtocolError::InvalidOffset(first))
      }
      if end > self.last_received_offset {
        self.last_received_offset = end
      }
    }
    (Running, FrameKind::DeltaBatch) => {
      let delta = DeltaBatch::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != delta.target_epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=delta.target_epoch,
            actual=frame.assignment_epoch(),
          ),
        )
      }
      guard self.state.assignment() is Some(assignment) else {
        raise SessionError::Assignment(ProtocolError::UnknownAssignment)
      }
      if assignment.partition() != delta.target_partition ||
        assignment.epoch() != delta.target_epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=assignment.epoch(),
            actual=delta.target_epoch,
          ),
        )
      }
    }
    (Running, FrameKind::Probe) => {
      let probe = Probe::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != probe.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=probe.epoch(),
            actual=frame.assignment_epoch(),
          ),
        )
      }
      guard self.state.assignment() is Some(assignment) else {
        raise SessionError::Assignment(ProtocolError::UnknownAssignment)
      }
      if assignment.partition() != probe.partition() ||
        assignment.epoch() != probe.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=assignment.epoch(),
            actual=probe.epoch(),
          ),
        )
      }
    }
    (Running, FrameKind::Seal) => {
      let seal = Seal::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != seal.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=seal.epoch(),
            actual=frame.assignment_epoch(),
          ),
        )
      }
      guard self.state.assignment() is Some(assignment) else {
        raise SessionError::Assignment(ProtocolError::UnknownAssignment)
      }
      if assignment.partition() != seal.partition() ||
        assignment.epoch() != seal.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=assignment.epoch(),
            actual=seal.epoch(),
          ),
        )
      }
      if seal.sealed_offset() > self.last_applied_offset {
        raise SessionError::Frame(
          ProtocolError::InvalidOffset(seal.sealed_offset()),
        )
      }
    }
    (Running, FrameKind::Drain) => self.phase = Draining
    (Draining, FrameKind::Heartbeat) | (Draining, FrameKind::Progress) => ()
    _ =>
      raise SessionError::WrongPhase(
        expected=if self.phase is Disconnected { Handshaking } else { Running },
        actual=self.phase,
      )
  }
  self.seen_messages[frame.message_id()] = frame
  self.next_sequence += 1L
}

///|
/// Lifecycle of the coordinator side of one worker connection.
pub struct CoordinatorSession {
  cluster_id : String
  coordinator_id : String
  worker_id : String
  mut incarnation : String
  mut session_id : String
  mut phase : SessionPhase
  mut next_sequence : Int64
  seen_messages : Map[String, Frame]
  mut state : WorkerState?
  mut last_persisted_offset : Int64
  mut last_applied_offset : Int64
}

///|
pub fn CoordinatorSession::new(
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
) -> CoordinatorSession raise SessionError {
  if cluster_id.is_empty() || coordinator_id.is_empty() || worker_id.is_empty() {
    raise SessionError::ClusterMismatch
  }
  {
    cluster_id,
    coordinator_id,
    worker_id,
    incarnation: "",
    session_id: "bootstrap",
    phase: Disconnected,
    next_sequence: 0L,
    seen_messages: {},
    state: None,
    last_persisted_offset: -1L,
    last_applied_offset: -1L,
  }
}

///|
pub fn CoordinatorSession::phase(self : CoordinatorSession) -> SessionPhase {
  self.phase
}

///|
pub fn CoordinatorSession::incarnation(self : CoordinatorSession) -> String {
  self.incarnation
}

///|
pub fn CoordinatorSession::session_id(self : CoordinatorSession) -> String {
  self.session_id
}

///|
pub fn CoordinatorSession::next_sequence(self : CoordinatorSession) -> Int64 {
  self.next_sequence
}

///|
pub fn CoordinatorSession::state(self : CoordinatorSession) -> WorkerState? {
  self.state
}

///|
/// Restore the source send cursor for a newly opened connection from an
/// external Coordinator control snapshot.
pub fn CoordinatorSession::restore_send_cursor(
  self : CoordinatorSession,
  partition : Int,
  epoch : Int,
  next_offset : Int64,
) -> Unit raise SessionError {
  if self.phase != Running {
    raise SessionError::WrongPhase(expected=Running, actual=self.phase)
  }
  let state = self.check_assignment(partition, epoch)
  state.restore_send_cursor(epoch, next_offset) catch {
    error => raise SessionError::Assignment(error)
  }
}

///|
fn CoordinatorSession::validate_frame(
  self : CoordinatorSession,
  frame : Frame,
) -> Unit raise SessionError {
  if frame.cluster_id() != self.cluster_id {
    raise SessionError::ClusterMismatch
  }
  if frame.sender_id() != self.worker_id {
    raise SessionError::SenderMismatch
  }
  if self.phase is Disconnected {
    if !(frame.kind() is FrameKind::Hello) || frame.session_id() != "bootstrap" {
      raise SessionError::WrongPhase(expected=Handshaking, actual=Disconnected)
    }
  } else {
    if frame.worker_incarnation() != self.incarnation {
      raise SessionError::IncarnationMismatch
    }
    if frame.session_id() != self.session_id {
      raise SessionError::SessionMismatch
    }
  }
  if frame.message_sequence() < self.next_sequence {
    raise SessionError::MessageRegression(
      expected_at_least=self.next_sequence,
      actual=frame.message_sequence(),
    )
  }
  if frame.message_sequence() > self.next_sequence {
    raise SessionError::MessageGap(
      expected=self.next_sequence,
      actual=frame.message_sequence(),
    )
  }
}

///|
/// Accept a worker Hello and move the coordinator side into Handshaking.
pub fn CoordinatorSession::accept_hello(
  self : CoordinatorSession,
  frame : Frame,
) -> Hello raise SessionError {
  match self.seen_messages.get(frame.message_id()) {
    Some(previous) => {
      if previous == frame {
        return Hello::from_payload(frame.payload()) catch {
          error => raise SessionError::Payload(error)
        }
      }
      raise SessionError::MessageIdConflict(frame.message_id())
    }
    None => ()
  }
  self.validate_frame(frame)
  let hello = Hello::from_payload(frame.payload()) catch {
    error => raise SessionError::Payload(error)
  }
  if hello.worker_id() != self.worker_id ||
    hello.incarnation() != frame.worker_incarnation() {
    raise SessionError::IncarnationMismatch
  }
  let incarnation = Incarnation::new(hello.incarnation()) catch {
    error => raise SessionError::Frame(error)
  }
  let worker_state = WorkerState::new(self.worker_id, incarnation) catch {
    error => raise SessionError::Frame(error)
  }
  self.incarnation = hello.incarnation()
  self.state = Some(worker_state)
  self.phase = Handshaking
  self.seen_messages[frame.message_id()] = frame
  self.next_sequence += 1L
  hello
}

///|
/// Activate a handshaken worker link and install its optional assignment.
/// The returned payload is ready to be wrapped in a Welcome frame by the
/// transport layer.
pub fn CoordinatorSession::activate(
  self : CoordinatorSession,
  session_id : String,
  assignment? : AssignmentSpec,
) -> Welcome raise SessionError {
  if self.phase != Handshaking || session_id.is_empty() {
    raise SessionError::WrongPhase(expected=Handshaking, actual=self.phase)
  }
  match assignment {
    None => ()
    Some(spec) => {
      if spec.incarnation() != self.incarnation {
        raise SessionError::IncarnationMismatch
      }
      let state = match self.state {
        Some(state) => state
        None => raise SessionError::Frame(ProtocolError::UnknownAssignment)
      }
      let installed = Assignment::new(
        spec.partition(),
        spec.epoch(),
        state.incarnation(),
        start_offset=spec.start_offset(),
      ) catch {
        error => raise SessionError::Assignment(error)
      }
      state.assign(installed) catch {
        error => raise SessionError::Assignment(error)
      }
      self.last_persisted_offset = spec.start_offset() - 1L
      self.last_applied_offset = spec.start_offset() - 1L
    }
  }
  self.session_id = session_id
  self.phase = Running
  Welcome::new(session_id, assignment?) catch {
    error => raise SessionError::Frame(error)
  }
}

///|
fn CoordinatorSession::checked_state(
  self : CoordinatorSession,
) -> WorkerState raise SessionError {
  match self.state {
    Some(state) => state
    None => raise SessionError::Frame(ProtocolError::UnknownAssignment)
  }
}

///|
fn CoordinatorSession::check_assignment(
  self : CoordinatorSession,
  partition : Int,
  epoch : Int,
) -> WorkerState raise SessionError {
  let state = self.checked_state()
  guard state.assignment() is Some(assignment) else {
    raise SessionError::Frame(ProtocolError::UnknownAssignment)
  }
  if assignment.partition() != partition || assignment.epoch() != epoch {
    raise SessionError::Assignment(
      ProtocolError::StaleEpoch(expected=assignment.epoch(), actual=epoch),
    )
  }
  state
}

///|
pub fn CoordinatorSession::last_persisted_offset(
  self : CoordinatorSession,
) -> Int64 {
  self.last_persisted_offset
}

///|
pub fn CoordinatorSession::last_applied_offset(
  self : CoordinatorSession,
) -> Int64 {
  self.last_applied_offset
}

///|
pub fn CoordinatorSession::credit_events(self : CoordinatorSession) -> Int64 {
  match self.state {
    Some(state) =>
      match state.assignment() {
        Some(assignment) => assignment.credit_events()
        None => 0L
      }
    None => 0L
  }
}

///|
pub fn CoordinatorSession::credit_bytes(self : CoordinatorSession) -> Int64 {
  match self.state {
    Some(state) =>
      match state.assignment() {
        Some(assignment) => assignment.credit_bytes()
        None => 0L
      }
    None => 0L
  }
}

///|
/// Return the next source offset that this assignment may send.
pub fn CoordinatorSession::next_offset(
  self : CoordinatorSession,
  partition : Int,
  epoch : Int,
) -> Int64 raise SessionError {
  let state = self.check_assignment(partition, epoch)
  guard state.assignment() is Some(assignment) else {
    raise SessionError::Frame(ProtocolError::UnknownAssignment)
  }
  assignment.next_offset()
}

///|
/// Reserve worker-granted credit for a coordinator-to-worker event batch.
/// Returns the first input offset assigned to the batch.
pub fn CoordinatorSession::reserve_batch(
  self : CoordinatorSession,
  partition : Int,
  epoch : Int,
  events : Int64,
  bytes : Int64,
) -> Int64 raise SessionError {
  let state = self.check_assignment(partition, epoch)
  guard state.assignment() is Some(assignment) else {
    raise SessionError::Frame(ProtocolError::UnknownAssignment)
  }
  let first_offset = assignment.next_offset()
  state.reserve_batch(epoch, events, bytes) catch {
    error => raise SessionError::Assignment(error)
  }
  first_offset
}

///|
/// Reserve target Worker credit for a DeltaBatch without changing its source
/// offset cursor.
pub fn CoordinatorSession::reserve_delta(
  self : CoordinatorSession,
  partition : Int,
  epoch : Int,
  events : Int64,
  bytes : Int64,
) -> Unit raise SessionError {
  let state = self.check_assignment(partition, epoch)
  state.reserve_delta(epoch, events, bytes) catch {
    error => raise SessionError::Assignment(error)
  }
}

///|
/// Accept one worker-to-coordinator frame after handshake/activation.
pub fn CoordinatorSession::accept_frame(
  self : CoordinatorSession,
  frame : Frame,
) -> Unit raise SessionError {
  match self.seen_messages.get(frame.message_id()) {
    Some(previous) => {
      if previous == frame {
        return
      }
      raise SessionError::MessageIdConflict(frame.message_id())
    }
    None => ()
  }
  if self.phase is Disconnected {
    if frame.kind() is FrameKind::Hello {
      ignore(self.accept_hello(frame))
      return
    }
    raise SessionError::WrongPhase(expected=Handshaking, actual=Disconnected)
  }
  self.validate_frame(frame)
  match frame.kind() {
    FrameKind::Resume => {
      let resume_message = Resume::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != resume_message.epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=resume_message.epoch,
            actual=frame.assignment_epoch(),
          ),
        )
      }
      let state = self.check_assignment(
        resume_message.partition,
        resume_message.epoch,
      )
      if resume_message.persisted_offset < self.last_persisted_offset {
        raise SessionError::Frame(
          ProtocolError::AckRegression(
            expected_at_least=self.last_persisted_offset,
            actual=resume_message.persisted_offset,
          ),
        )
      }
      state.resume_from(resume_message.epoch, resume_message.persisted_offset) catch {
        error => raise SessionError::Assignment(error)
      }
      self.last_persisted_offset = resume_message.persisted_offset
    }
    FrameKind::Credit => {
      let credit = Credit::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != credit.epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=credit.epoch,
            actual=frame.assignment_epoch(),
          ),
        )
      }
      let state = self.check_assignment(credit.partition, credit.epoch)
      state.grant_credit(frame.assignment_epoch(), credit.events, credit.bytes) catch {
        error => raise SessionError::Assignment(error)
      }
    }
    FrameKind::AckPersisted => {
      let ack = AckPersisted::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != ack.epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=ack.epoch,
            actual=frame.assignment_epoch(),
          ),
        )
      }
      let state = self.check_assignment(ack.partition, ack.epoch)
      if ack.offset < self.last_persisted_offset {
        raise SessionError::Frame(
          ProtocolError::AckRegression(
            expected_at_least=self.last_persisted_offset,
            actual=ack.offset,
          ),
        )
      }
      state.ack_persisted(ack.epoch, ack.offset) catch {
        error => raise SessionError::Assignment(error)
      }
      self.last_persisted_offset = ack.offset
    }
    FrameKind::DeltaAck => {
      let ack = DeltaAck::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != ack.target_epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=ack.target_epoch,
            actual=frame.assignment_epoch(),
          ),
        )
      }
      ignore(self.check_assignment(ack.target_partition, ack.target_epoch))
    }
    FrameKind::Heartbeat => {
      let heartbeat = Heartbeat::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != heartbeat.epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=heartbeat.epoch,
            actual=frame.assignment_epoch(),
          ),
        )
      }
      let state = self.check_assignment(heartbeat.partition, heartbeat.epoch)
      if heartbeat.persisted_offset < self.last_persisted_offset {
        raise SessionError::Frame(
          ProtocolError::InvalidOffset(heartbeat.persisted_offset),
        )
      }
      state.ack_persisted(heartbeat.epoch, heartbeat.persisted_offset) catch {
        error => raise SessionError::Assignment(error)
      }
      self.last_persisted_offset = heartbeat.persisted_offset
    }
    FrameKind::Progress => {
      let progress = Progress::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != progress.epoch {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=progress.epoch,
            actual=frame.assignment_epoch(),
          ),
        )
      }
      let state = self.check_assignment(progress.partition, progress.epoch)
      if progress.persisted_offset < self.last_persisted_offset ||
        progress.applied_offset < self.last_applied_offset ||
        progress.applied_offset > progress.persisted_offset {
        raise SessionError::Frame(
          ProtocolError::AckRegression(
            expected_at_least=self.last_persisted_offset,
            actual=progress.persisted_offset,
          ),
        )
      }
      guard state.assignment() is Some(assignment) else {
        raise SessionError::Frame(ProtocolError::UnknownAssignment)
      }
      if progress.persisted_offset >= assignment.next_offset() ||
        progress.applied_offset >= assignment.next_offset() {
        raise SessionError::Assignment(
          ProtocolError::InvalidOffset(
            if progress.persisted_offset >= assignment.next_offset() {
              progress.persisted_offset
            } else {
              progress.applied_offset
            },
          ),
        )
      }
      self.last_persisted_offset = progress.persisted_offset
      self.last_applied_offset = progress.applied_offset
    }
    FrameKind::ProbeReply => {
      let reply = ProbeReply::from_payload(frame.payload()) catch {
        error => raise SessionError::Payload(error)
      }
      if frame.assignment_epoch() != reply.epoch() {
        raise SessionError::Assignment(
          ProtocolError::StaleEpoch(
            expected=reply.epoch(),
            actual=frame.assignment_epoch(),
          ),
        )
      }
      let state = self.check_assignment(reply.partition(), reply.epoch())
      guard state.assignment() is Some(assignment) else {
        raise SessionError::Frame(ProtocolError::UnknownAssignment)
      }
      if reply.reached_offset() >= assignment.next_offset() {
        raise SessionError::Frame(
          ProtocolError::InvalidOffset(reply.reached_offset()),
        )
      }
    }
    FrameKind::Drain => self.phase = Draining
    _ => raise SessionError::WrongPhase(expected=Running, actual=self.phase)
  }
  self.seen_messages[frame.message_id()] = frame
  self.next_sequence += 1L
}
