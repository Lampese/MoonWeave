///|
/// Kinds of messages exchanged by the coordinator and workers.
pub(all) enum FrameKind {
  Hello
  Welcome
  Assign
  Revoke
  Drain
  Resume
  EventBatch
  DeltaBatch
  DeltaAck
  Credit
  AckReceived
  AckPersisted
  Progress
  Heartbeat
  Probe
  ProbeReply
  Seal
  Error
} derive(Debug, Eq, ToJson, FromJson)

///|
/// A transport-neutral protocol frame. The payload is an opaque UTF-8 string
/// owned by the message handler; the frame layer never parses it.
pub struct Frame {
  version : Int
  cluster_id : String
  session_id : String
  message_id : String
  sender_id : String
  worker_incarnation : String
  message_sequence : Int64
  assignment_epoch : Int
  kind : FrameKind
  payload : String
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Create a validated frame before it enters an I/O adapter.
pub fn Frame::new(
  version : Int,
  cluster_id : String,
  session_id : String,
  message_id : String,
  sender_id : String,
  worker_incarnation : String,
  message_sequence : Int64,
  assignment_epoch : Int,
  kind : FrameKind,
  payload : String,
) -> Frame raise ProtocolError {
  if version != protocol_version() {
    raise ProtocolError::InvalidProtocolVersion(version)
  }
  if cluster_id.is_empty() ||
    session_id.is_empty() ||
    message_id.is_empty() ||
    sender_id.is_empty() ||
    worker_incarnation.is_empty() {
    raise ProtocolError::InvalidFrameField
  }
  if message_sequence < 0L || assignment_epoch < 0 {
    raise ProtocolError::InvalidFrameField
  }
  {
    version,
    cluster_id,
    session_id,
    message_id,
    sender_id,
    worker_incarnation,
    message_sequence,
    assignment_epoch,
    kind,
    payload,
  }
}

///|
pub fn Frame::version(self : Frame) -> Int {
  self.version
}

///|
pub fn Frame::cluster_id(self : Frame) -> String {
  self.cluster_id
}

///|
pub fn Frame::session_id(self : Frame) -> String {
  self.session_id
}

///|
pub fn Frame::message_id(self : Frame) -> String {
  self.message_id
}

///|
pub fn Frame::sender_id(self : Frame) -> String {
  self.sender_id
}

///|
pub fn Frame::worker_incarnation(self : Frame) -> String {
  self.worker_incarnation
}

///|
pub fn Frame::message_sequence(self : Frame) -> Int64 {
  self.message_sequence
}

///|
pub fn Frame::assignment_epoch(self : Frame) -> Int {
  self.assignment_epoch
}

///|
pub fn Frame::kind(self : Frame) -> FrameKind {
  self.kind
}

///|
pub fn Frame::payload(self : Frame) -> String {
  self.payload
}

///|
/// Encode the complete length-prefixed frame.
///
/// The length prefix is a 32-bit unsigned big-endian byte count for the JSON
/// payload that follows. `maximum_frame_bytes` bounds allocation at the wire
/// boundary and should be configured consistently on both peers.
pub fn Frame::encode(
  self : Frame,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> Bytes raise ProtocolError {
  if maximum_frame_bytes <= 0 {
    raise ProtocolError::FrameTooLarge(actual=0, limit=maximum_frame_bytes)
  }
  let json = @json.to_json(self).stringify()
  let body = @utf8.encode(json)
  // `maximum_frame_bytes` is an Int and is already far below the 32-bit
  // length-prefix limit in every supported configuration. Avoid spelling the
  // unsigned 0xffffffff literal here: Int literals are signed 32-bit values.
  if body.length() > maximum_frame_bytes {
    raise ProtocolError::FrameTooLarge(
      actual=body.length(),
      limit=maximum_frame_bytes,
    )
  }
  let buffer = Buffer()
  buffer.write_uint_be(body.length().reinterpret_as_uint())
  buffer.write_bytes(body[:])
  buffer.to_bytes()
}

///|
/// Decode the JSON body after a transport has read and validated its length.
pub fn Frame::decode_payload(
  body : Bytes,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> Frame raise ProtocolError {
  if maximum_frame_bytes <= 0 || body.length() > maximum_frame_bytes {
    raise ProtocolError::FrameTooLarge(
      actual=body.length(),
      limit=maximum_frame_bytes,
    )
  }
  let text = @utf8.decode(body) catch {
    _ => raise ProtocolError::MalformedFrame
  }
  let json = @json.parse(text) catch {
    _ => raise ProtocolError::MalformedFrame
  }
  let frame : Frame = @json.from_json(json) catch {
    _ => raise ProtocolError::MalformedFrame
  }
  if frame.version() != protocol_version() {
    raise ProtocolError::InvalidProtocolVersion(frame.version())
  }
  if frame.cluster_id().is_empty() ||
    frame.session_id().is_empty() ||
    frame.message_id().is_empty() ||
    frame.sender_id().is_empty() ||
    frame.worker_incarnation().is_empty() ||
    frame.message_sequence() < 0L ||
    frame.assignment_epoch() < 0 {
    raise ProtocolError::MalformedFrame
  }
  frame
}
