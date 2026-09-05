///|
/// Errors raised by the pure worker protocol state machine.
pub(all) suberror ProtocolError {
  EmptyWorkerId
  EmptyIncarnation
  InvalidEpoch(Int)
  InvalidOffset(Int64)
  InvalidCredit
  StaleIncarnation
  StaleEpoch(expected~ : Int, actual~ : Int)
  AckRegression(expected_at_least~ : Int64, actual~ : Int64)
  CreditExceeded(remaining~ : Int64, requested~ : Int64)
  UnknownAssignment
  InvalidProtocolVersion(Int)
  InvalidFrameField
  FrameTooLarge(actual~ : Int, limit~ : Int)
  MalformedFrame
  EmptyBatch
  InvalidBatchOffset
  InvalidQueryId
  InvalidDeltaId
  InvalidDigest
  Causal(@causal.CausalError)
} derive(Debug, Eq)

///|
/// A monotonically increasing worker incarnation, changed on every process start.
pub struct Incarnation {
  value : String
} derive(Debug, Eq, Compare, Hash)

///|
pub fn Incarnation::new(value : String) -> Incarnation raise ProtocolError {
  if value.is_empty() {
    raise ProtocolError::EmptyIncarnation
  }
  { value, }
}

///|
pub fn Incarnation::value(self : Incarnation) -> String {
  self.value
}

///|
/// A worker's fenced assignment for one partition.
pub struct Assignment {
  partition : Int
  epoch : Int
  incarnation : Incarnation
  mut next_offset : Int64
  mut acked_offset : Int64
  mut credit_events : Int64
  mut credit_bytes : Int64
} derive(Debug)

///|
pub fn Assignment::new(
  partition : Int,
  epoch : Int,
  incarnation : Incarnation,
  start_offset? : Int64 = 0L,
) -> Assignment raise ProtocolError {
  if partition < 0 {
    raise ProtocolError::UnknownAssignment
  }
  if epoch < 0 {
    raise ProtocolError::InvalidEpoch(epoch)
  }
  if start_offset < 0L {
    raise ProtocolError::InvalidOffset(start_offset)
  }
  {
    partition,
    epoch,
    incarnation,
    next_offset: start_offset,
    acked_offset: start_offset - 1L,
    credit_events: 0L,
    credit_bytes: 0L,
  }
}

///|
pub fn Assignment::partition(self : Assignment) -> Int {
  self.partition
}

///|
pub fn Assignment::epoch(self : Assignment) -> Int {
  self.epoch
}

///|
pub fn Assignment::incarnation(self : Assignment) -> Incarnation {
  self.incarnation
}

///|
pub fn Assignment::next_offset(self : Assignment) -> Int64 {
  self.next_offset
}

///|
pub fn Assignment::acked_offset(self : Assignment) -> Int64 {
  self.acked_offset
}

///|
pub fn Assignment::credit_events(self : Assignment) -> Int64 {
  self.credit_events
}

///|
pub fn Assignment::credit_bytes(self : Assignment) -> Int64 {
  self.credit_bytes
}
