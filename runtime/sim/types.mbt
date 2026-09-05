///|
pub(all) suberror SimulationError {
  EmptyReplicaId
  DuplicateReplica(String)
  UnknownReplica(String)
  InvalidQueueIndex(Int)
  Causal(@causal.CausalError)
} derive(Debug, Eq)

///|
/// One deterministic, in-memory delivery unit.
pub struct Envelope {
  sender : String
  receiver : String
  event : @causal.Event[@reachability.Edge]
} derive(Debug, Eq)

///|
pub fn Envelope::sender(self : Envelope) -> String {
  self.sender
}

///|
pub fn Envelope::receiver(self : Envelope) -> String {
  self.receiver
}

///|
pub fn Envelope::event(self : Envelope) -> @causal.Event[@reachability.Edge] {
  self.event
}

///|
/// Observable effects of one replica receiving one envelope.
pub struct ReceiveReport {
  status : @causal.SubmitStatus
  admitted_ids : ReadOnlyArray[@causal.EventId]
  missing_ids : ReadOnlyArray[@causal.EventId]
  new_facts : ReadOnlyArray[@reachability.Reachable]
} derive(Debug)

///|
pub fn ReceiveReport::status(self : ReceiveReport) -> @causal.SubmitStatus {
  self.status
}

///|
pub fn ReceiveReport::admitted_ids(
  self : ReceiveReport,
) -> ArrayView[@causal.EventId] {
  self.admitted_ids[:]
}

///|
pub fn ReceiveReport::missing_ids(
  self : ReceiveReport,
) -> ArrayView[@causal.EventId] {
  self.missing_ids[:]
}

///|
pub fn ReceiveReport::new_facts(
  self : ReceiveReport,
) -> ArrayView[@reachability.Reachable] {
  self.new_facts[:]
}
