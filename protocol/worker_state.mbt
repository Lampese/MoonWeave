///|
/// Message-independent state transitions for one worker assignment.
///
/// This type is deliberately synchronous and deterministic. An async adapter
/// may call these methods from socket tasks, but cannot bypass their checks.
pub struct WorkerState {
  worker_id : String
  incarnation : Incarnation
  mut assignment : Assignment?
}

///|
pub fn WorkerState::new(
  worker_id : String,
  incarnation : Incarnation,
) -> WorkerState raise ProtocolError {
  if worker_id.is_empty() {
    raise ProtocolError::EmptyWorkerId
  }
  { worker_id, incarnation, assignment: None, }
}

///|
pub fn WorkerState::worker_id(self : WorkerState) -> String {
  self.worker_id
}

///|
pub fn WorkerState::incarnation(self : WorkerState) -> Incarnation {
  self.incarnation
}

///|
pub fn WorkerState::assignment(self : WorkerState) -> Assignment? {
  self.assignment
}

///|
/// Install or replace an assignment. A higher epoch fences the old assignment.
pub fn WorkerState::assign(
  self : WorkerState,
  assignment : Assignment,
) -> Unit raise ProtocolError {
  if assignment.incarnation() != self.incarnation {
    raise ProtocolError::StaleIncarnation
  }
  match self.assignment {
    None => self.assignment = Some(assignment)
    Some(current) => {
      if assignment.partition() != current.partition() {
        raise ProtocolError::UnknownAssignment
      }
      if assignment.epoch() < current.epoch() {
        raise ProtocolError::StaleEpoch(
          expected=current.epoch(),
          actual=assignment.epoch(),
        )
      }
      self.assignment = Some(assignment)
    }
  }
}

///|
fn WorkerState::checked_assignment(
  self : WorkerState,
  epoch : Int,
) -> Assignment raise ProtocolError {
  match self.assignment {
    None => raise ProtocolError::UnknownAssignment
    Some(assignment) => {
      if epoch != assignment.epoch() {
        raise ProtocolError::StaleEpoch(
          expected=assignment.epoch(),
          actual=epoch,
        )
      }
      assignment
    }
  }
}

///|
/// Grant additional event and byte credit to the worker.
pub fn WorkerState::grant_credit(
  self : WorkerState,
  epoch : Int,
  events : Int64,
  bytes : Int64,
) -> Unit raise ProtocolError {
  if events < 0L || bytes < 0L || (events == 0L && bytes == 0L) {
    raise ProtocolError::InvalidCredit
  }
  let assignment = self.checked_assignment(epoch)
  assignment.credit_events += events
  assignment.credit_bytes += bytes
}

///|
/// Reserve credit for an outgoing batch. Reservation advances the send offset.
pub fn WorkerState::reserve_batch(
  self : WorkerState,
  epoch : Int,
  events : Int64,
  bytes : Int64,
) -> Unit raise ProtocolError {
  if events <= 0L || bytes < 0L {
    raise ProtocolError::InvalidCredit
  }
  let assignment = self.checked_assignment(epoch)
  if events > assignment.credit_events || bytes > assignment.credit_bytes {
    raise ProtocolError::CreditExceeded(
      remaining=assignment.credit_events,
      requested=events,
    )
  }
  assignment.credit_events -= events
  assignment.credit_bytes -= bytes
  assignment.next_offset += events
}

///|
/// Reserve only target-side credit for a derived delta transfer. Exchange
/// frames do not advance the target's primary source offset.
pub fn WorkerState::reserve_delta(
  self : WorkerState,
  epoch : Int,
  events : Int64,
  bytes : Int64,
) -> Unit raise ProtocolError {
  if events <= 0L || bytes < 0L {
    raise ProtocolError::InvalidCredit
  }
  let assignment = self.checked_assignment(epoch)
  if events > assignment.credit_events || bytes > assignment.credit_bytes {
    raise ProtocolError::CreditExceeded(
      remaining=assignment.credit_events,
      requested=events,
    )
  }
  assignment.credit_events -= events
  assignment.credit_bytes -= bytes
}

///|
/// Accept a persisted ACK. ACK offsets are monotone and cannot exceed sent data.
pub fn WorkerState::ack_persisted(
  self : WorkerState,
  epoch : Int,
  offset : Int64,
) -> Unit raise ProtocolError {
  let assignment = self.checked_assignment(epoch)
  if offset < assignment.acked_offset {
    raise ProtocolError::AckRegression(
      expected_at_least=assignment.acked_offset,
      actual=offset,
    )
  }
  if offset >= assignment.next_offset {
    raise ProtocolError::InvalidOffset(offset)
  }
  assignment.acked_offset = offset
}

///|
/// Accept a persisted offset during resume. It must refer to data already
/// sent on this assignment and rewinds the unconfirmed suffix.
pub fn WorkerState::resume_from(
  self : WorkerState,
  epoch : Int,
  offset : Int64,
) -> Unit raise ProtocolError {
  let assignment = self.checked_assignment(epoch)
  if offset < assignment.acked_offset || offset >= assignment.next_offset {
    raise ProtocolError::InvalidOffset(offset)
  }
  assignment.acked_offset = offset
  assignment.next_offset = offset + 1L
}

///|
/// Restore a Coordinator's send cursor from durable control-plane state when a
/// new transport session is opened. This does not fabricate a worker ACK; it
/// only reconstructs how far the Coordinator's source log had reserved data.
pub fn WorkerState::restore_send_cursor(
  self : WorkerState,
  epoch : Int,
  next_offset : Int64,
) -> Unit raise ProtocolError {
  let assignment = self.checked_assignment(epoch)
  if next_offset < assignment.next_offset ||
    next_offset < assignment.acked_offset + 1L {
    raise ProtocolError::InvalidOffset(next_offset)
  }
  assignment.next_offset = next_offset
}
