///|
/// A finite assignment's completion barrier, distinct from closing a link.
pub(all) struct DrainRequest {
  partition : Int
  epoch : Int
  target_offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn DrainRequest::new(
  partition : Int,
  epoch : Int,
  target_offset : Int64,
) -> DrainRequest raise ProtocolError {
  if partition < 0 || epoch < 0 || target_offset < -1L {
    raise ProtocolError::InvalidFrameField
  }
  { partition, epoch, target_offset, }
}

///|
pub fn DrainRequest::to_payload(self : DrainRequest) -> String {
  encode_payload(self)
}

///|
pub fn DrainRequest::from_payload(
  text : String,
) -> DrainRequest raise MessageError {
  let value : DrainRequest = decode_payload(text)
  DrainRequest::new(value.partition, value.epoch, value.target_offset) catch {
    error => raise protocol_error(error)
  }
}

///|
/// Worker-observed progress after processing every input preceding the drain
/// request. Nonzero pending events explicitly refuse successful completion.
pub(all) struct DrainAck {
  request_id : String
  partition : Int
  epoch : Int
  persisted_offset : Int64
  applied_offset : Int64
  pending_events : Int
  frontier_digest : String
  task_id : String?
  output_count : Int64?
  output_digest : String?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn DrainAck::new(
  request_id : String,
  partition : Int,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
  pending_events : Int,
  frontier_digest : String,
  task_id? : String,
  output_count? : Int64,
  output_digest? : String,
) -> DrainAck raise ProtocolError {
  if request_id.is_empty() ||
    partition < 0 ||
    epoch < 0 ||
    persisted_offset < -1L ||
    applied_offset < -1L ||
    applied_offset > persisted_offset ||
    pending_events < 0 ||
    frontier_digest.is_empty() ||
    (
      (
        task_id is Some(_) ||
        output_count is Some(_) ||
        output_digest is Some(_)
      ) &&
      (task_id is None || output_count is None || output_digest is None)
    ) {
    raise ProtocolError::InvalidFrameField
  }
  match (task_id, output_count, output_digest) {
    (Some(id), Some(count), Some(digest)) if id.is_empty() ||
      count < 0L ||
      digest.is_empty() => raise ProtocolError::InvalidFrameField
    _ => ()
  }
  {
    request_id,
    partition,
    epoch,
    persisted_offset,
    applied_offset,
    pending_events,
    frontier_digest,
    task_id,
    output_count,
    output_digest,
  }
}

///|
pub fn DrainAck::task_id(self : DrainAck) -> String? {
  self.task_id
}

///|
pub fn DrainAck::output_count(self : DrainAck) -> Int64? {
  self.output_count
}

///|
pub fn DrainAck::output_digest(self : DrainAck) -> String? {
  self.output_digest
}

///|
pub fn DrainAck::to_payload(self : DrainAck) -> String {
  encode_payload(self)
}

///|
pub fn DrainAck::from_payload(text : String) -> DrainAck raise MessageError {
  let value : DrainAck = decode_payload(text)
  DrainAck::new(
    value.request_id,
    value.partition,
    value.epoch,
    value.persisted_offset,
    value.applied_offset,
    value.pending_events,
    value.frontier_digest,
    task_id?=value.task_id,
    output_count?=value.output_count,
    output_digest?=value.output_digest,
  ) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn CoordinatorSession::begin_drain(
  self : CoordinatorSession,
  request_id : String,
  request : DrainRequest,
) -> Unit raise SessionError {
  if self.phase != Running {
    raise SessionError::WrongPhase(expected=Running, actual=self.phase)
  }
  let request = DrainRequest::new(
    request.partition,
    request.epoch,
    request.target_offset,
  ) catch {
    error => raise SessionError::Frame(error)
  }
  let state = self.check_assignment(request.partition, request.epoch)
  guard state.assignment() is Some(assignment) else {
    raise SessionError::Assignment(ProtocolError::UnknownAssignment)
  }
  if request_id.is_empty() ||
    request.target_offset != self.last_applied_offset ||
    request.target_offset != self.last_persisted_offset ||
    request.target_offset != assignment.next_offset() - 1L {
    raise SessionError::Frame(ProtocolError::InvalidFrameField)
  }
  self.drain_request = Some((request_id, request))
  self.phase = Draining
}

///|
pub fn CoordinatorSession::drain_ack(self : CoordinatorSession) -> DrainAck? {
  self.drain_ack
}

///|
fn CoordinatorSession::accept_drain_ack(
  self : CoordinatorSession,
  frame : Frame,
) -> Unit raise SessionError {
  if self.phase != Draining {
    raise SessionError::WrongPhase(expected=Draining, actual=self.phase)
  }
  guard self.drain_request is Some((request_id, request)) else {
    raise SessionError::Frame(ProtocolError::InvalidFrameField)
  }
  let ack = DrainAck::from_payload(frame.payload()) catch {
    error => raise SessionError::Payload(error)
  }
  if ack.request_id != request_id ||
    ack.partition != request.partition ||
    ack.epoch != request.epoch ||
    frame.assignment_epoch() != request.epoch ||
    ack.persisted_offset != request.target_offset ||
    ack.applied_offset != request.target_offset {
    raise SessionError::Frame(ProtocolError::InvalidFrameField)
  }
  self.drain_ack = Some(ack)
}
