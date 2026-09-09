///|
/// Pure application state captured at a durable Worker checkpoint. The event
/// set contains both admitted and causally pending inputs; replaying it into a
/// fresh operator reconstructs the exact monotone state without any IO.
pub(all) struct TaskSnapshot {
  task_id : String
  operator_id : String
  operator : OperatorKind
  partition : Int
  epoch : Int
  offset : Int64
  frontier_digest : String
  emitted_outputs : Int
  events : Array[@protocol.EventRecord]
  operator_capability : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn TaskSnapshot::task_id(self : TaskSnapshot) -> String {
  self.task_id
}

///|
pub fn TaskSnapshot::operator_id(self : TaskSnapshot) -> String {
  self.operator_id
}

///|
pub fn TaskSnapshot::operator(self : TaskSnapshot) -> OperatorKind {
  self.operator
}

///|
pub fn TaskSnapshot::state_schema(self : TaskSnapshot) -> String {
  self.operator.state_schema()
}

///|
pub fn TaskSnapshot::operator_capability(self : TaskSnapshot) -> String {
  self.operator_capability
}

///|
pub fn TaskSnapshot::partition(self : TaskSnapshot) -> Int {
  self.partition
}

///|
pub fn TaskSnapshot::epoch(self : TaskSnapshot) -> Int {
  self.epoch
}

///|
pub fn TaskSnapshot::offset(self : TaskSnapshot) -> Int64 {
  self.offset
}

///|
pub fn TaskSnapshot::frontier_digest(self : TaskSnapshot) -> String {
  self.frontier_digest
}

///|
pub fn TaskSnapshot::emitted_outputs(self : TaskSnapshot) -> Int {
  self.emitted_outputs
}

///|
pub fn TaskSnapshot::events(
  self : TaskSnapshot,
) -> ArrayView[@protocol.EventRecord] {
  self.events[:]
}

///|
/// Rebind a verified snapshot to a new Task attempt. The event set and its
/// causal frontier remain unchanged; only the durable execution identity is
/// replaced. Worker migration is the only caller that should use this method.
pub fn TaskSnapshot::rebind(
  self : TaskSnapshot,
  task_id : String,
  epoch : Int,
) -> TaskSnapshot raise TaskError {
  if task_id.is_empty() {
    raise TaskError::EmptyTaskId
  }
  if epoch < 0 {
    raise TaskError::InvalidEpoch(epoch)
  }
  { ..self, task_id, epoch, }
}
