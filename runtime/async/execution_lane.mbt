///|
/// One serial execution slot shared by several fixed key shards. Network
/// sessions can wait concurrently; durable admission and operator evaluation
/// enter this slot before advancing a shard. Different slots run independently.
pub struct ExecutionLane {
  id : Int
  mutex : @async.Mutex
  mut admissions : Int64
}

///|
pub struct ExecutionPermit {
  lane : ExecutionLane
  mut held : Bool
}

///|
pub fn ExecutionLane::new(id : Int) -> ExecutionLane raise FrameIoError {
  if id < 0 {
    raise FrameIoError::Handler("negative execution slot")
  }
  { id, mutex: @async.Mutex::Mutex(), admissions: 0L, }
}

///|
pub fn ExecutionLane::id(self : ExecutionLane) -> Int {
  self.id
}

///|
pub fn ExecutionLane::admissions(self : ExecutionLane) -> Int64 {
  self.admissions
}

///|
pub async fn ExecutionLane::acquire(
  self : ExecutionLane,
) -> ExecutionPermit raise FrameIoError {
  self.mutex.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  self.admissions += 1L
  { lane: self, held: true, }
}

///|
/// Release before awaiting downstream delivery: a downstream checkpoint may
/// need progress from a sibling shard assigned to the same execution slot.
pub fn ExecutionPermit::release(self : ExecutionPermit) -> Unit {
  if self.held {
    self.held = false
    self.lane.mutex.release()
  }
}
