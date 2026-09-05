///|
/// Deterministic network model. Nothing is delivered until a test explicitly
/// selects a queued envelope, making reorder/drop/duplicate traces repeatable.
pub struct Network {
  replicas : Map[String, Replica]
  queue : Array[Envelope]
  mut delivered : Int
  mut dropped : Int
}

///|
pub fn Network::new() -> Network {
  { replicas: {}, queue: [], delivered: 0, dropped: 0, }
}

///|
pub fn Network::add_replica(
  self : Network,
  replica : Replica,
) -> Unit raise SimulationError {
  if self.replicas.contains(replica.id()) {
    raise SimulationError::DuplicateReplica(replica.id())
  }
  self.replicas[replica.id()] = replica
}

///|
pub fn Network::replica(self : Network, id : String) -> Replica? {
  self.replicas.get(id)
}

///|
pub fn Network::queued_count(self : Network) -> Int {
  self.queue.length()
}

///|
pub fn Network::delivered_count(self : Network) -> Int {
  self.delivered
}

///|
pub fn Network::dropped_count(self : Network) -> Int {
  self.dropped
}

///|
pub fn Network::enqueue(
  self : Network,
  sender : String,
  receiver : String,
  event : @causal.Event[@reachability.Edge],
) -> Unit raise SimulationError {
  if !self.replicas.contains(sender) {
    raise SimulationError::UnknownReplica(sender)
  }
  if !self.replicas.contains(receiver) {
    raise SimulationError::UnknownReplica(receiver)
  }
  self.queue.push({ sender, receiver, event, })
}

///|
fn Network::checked_envelope(
  self : Network,
  index : Int,
) -> Envelope raise SimulationError {
  match self.queue.get(index) {
    Some(envelope) => envelope
    None => raise SimulationError::InvalidQueueIndex(index)
  }
}

///|
/// Queue one additional copy without delivering either copy.
pub fn Network::duplicate(
  self : Network,
  index : Int,
) -> Unit raise SimulationError {
  self.queue.push(self.checked_envelope(index))
}

///|
/// Remove an envelope without delivering it.
pub fn Network::drop(
  self : Network,
  index : Int,
) -> Envelope raise SimulationError {
  ignore(self.checked_envelope(index))
  self.dropped += 1
  self.queue.remove(index)
}

///|
/// Deliver an explicitly selected envelope.
pub fn Network::deliver(
  self : Network,
  index : Int,
) -> ReceiveReport raise SimulationError {
  let envelope = self.checked_envelope(index)
  let receiver = match self.replicas.get(envelope.receiver()) {
    Some(replica) => replica
    None => raise SimulationError::UnknownReplica(envelope.receiver())
  }
  // Remove only after validating the receiver, but before applying the event.
  // A protocol error consumes this simulated delivery attempt.
  ignore(self.queue.remove(index))
  self.delivered += 1
  receiver.receive(envelope.event())
}

///|
pub fn Network::deliver_all(
  self : Network,
) -> Array[ReceiveReport] raise SimulationError {
  let reports = []
  while !self.queue.is_empty() {
    reports.push(self.deliver(0))
  }
  reports
}
