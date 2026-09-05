///|
/// Protocol wire version implemented by this module.
pub fn protocol_version() -> Int {
  1
}

///|
/// Stateful constructor for frames sent in one direction on one connection.
///
/// Each direction owns an independent sequence space. The builder is pure with
/// respect to transport: it only allocates metadata and never writes bytes.
pub struct FrameBuilder {
  version : Int
  cluster_id : String
  sender_id : String
  worker_incarnation : String
  mut session_id : String
  mut assignment_epoch : Int
  mut next_sequence : Int64
}

///|
pub fn FrameBuilder::new(
  cluster_id : String,
  sender_id : String,
  worker_incarnation : String,
  session_id? : String = "bootstrap",
  assignment_epoch? : Int = 0,
  start_sequence? : Int64 = 0L,
) -> FrameBuilder raise ProtocolError {
  if cluster_id.is_empty() || sender_id.is_empty() {
    raise ProtocolError::InvalidFrameField
  }
  if worker_incarnation.is_empty() || session_id.is_empty() {
    raise ProtocolError::InvalidFrameField
  }
  if assignment_epoch < 0 {
    raise ProtocolError::InvalidEpoch(assignment_epoch)
  }
  if start_sequence < 0L {
    raise ProtocolError::InvalidFrameField
  }
  {
    version: protocol_version(),
    cluster_id,
    sender_id,
    worker_incarnation,
    session_id,
    assignment_epoch,
    next_sequence: start_sequence,
  }
}

///|
pub fn FrameBuilder::sender_id(self : FrameBuilder) -> String {
  self.sender_id
}

///|
pub fn FrameBuilder::worker_incarnation(self : FrameBuilder) -> String {
  self.worker_incarnation
}

///|
pub fn FrameBuilder::session_id(self : FrameBuilder) -> String {
  self.session_id
}

///|
pub fn FrameBuilder::assignment_epoch(self : FrameBuilder) -> Int {
  self.assignment_epoch
}

///|
pub fn FrameBuilder::next_sequence(self : FrameBuilder) -> Int64 {
  self.next_sequence
}

///|
/// Change the session ID for subsequent frames after a Welcome/activation.
pub fn FrameBuilder::set_session_id(
  self : FrameBuilder,
  session_id : String,
) -> Unit raise ProtocolError {
  if session_id.is_empty() {
    raise ProtocolError::InvalidFrameField
  }
  self.session_id = session_id
}

///|
pub fn FrameBuilder::set_assignment_epoch(
  self : FrameBuilder,
  epoch : Int,
) -> Unit raise ProtocolError {
  if epoch < 0 {
    raise ProtocolError::InvalidEpoch(epoch)
  }
  self.assignment_epoch = epoch
}

///|
/// Build the next frame and advance this direction's sequence exactly once.
pub fn FrameBuilder::build(
  self : FrameBuilder,
  kind : FrameKind,
  payload : String,
) -> Frame raise ProtocolError {
  let sequence = self.next_sequence
  let message_id = self.sender_id +
    "/" +
    self.worker_incarnation +
    "/" +
    sequence.to_string()
  let frame = Frame::new(
    self.version,
    self.cluster_id,
    self.session_id,
    message_id,
    self.sender_id,
    self.worker_incarnation,
    sequence,
    self.assignment_epoch,
    kind,
    payload,
  )
  self.next_sequence += 1L
  frame
}
