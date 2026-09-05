///|
/// A pure model of one worker replica running the reachability workload.
pub struct Replica {
  id : String
  log : @causal.CausalLog[@reachability.Edge]
  reachability : @reachability.State
}

///|
pub fn Replica::new(
  id : String,
  maximum_pending? : Int = 10000,
) -> Replica raise SimulationError {
  if id.is_empty() {
    raise SimulationError::EmptyReplicaId
  }
  let log = @causal.CausalLog::new(maximum_pending~) catch {
    error => raise SimulationError::Causal(error)
  }
  { id, log, reachability: @reachability.State::new(), }
}

///|
pub fn Replica::id(self : Replica) -> String {
  self.id
}

///|
pub fn Replica::event_count(self : Replica) -> Int {
  self.log.admitted_count()
}

///|
pub fn Replica::pending_count(self : Replica) -> Int {
  self.log.pending_count()
}

///|
pub fn Replica::frontier(self : Replica) -> @causal.Frontier {
  self.log.frontier()
}

///|
pub fn Replica::facts(self : Replica) -> Array[@reachability.Reachable] {
  self.reachability.facts()
}

///|
pub fn Replica::is_reachable(
  self : Replica,
  source : String,
  target : String,
) -> Bool {
  self.reachability.is_reachable(source, target)
}

///|
pub fn Replica::receive(
  self : Replica,
  event : @causal.Event[@reachability.Edge],
) -> ReceiveReport raise SimulationError {
  let admission = self.log.submit(event) catch {
    error => raise SimulationError::Causal(error)
  }
  let admitted_ids = []
  let new_facts = []
  for admitted in admission.admitted() {
    admitted_ids.push(admitted.id())
    new_facts.push_iter(self.reachability.apply(admitted.payload()).iter())
  }
  new_facts.sort()
  {
    status: admission.status(),
    admitted_ids: ReadOnlyArray::from_array(admitted_ids),
    missing_ids: ReadOnlyArray::from_array(admission.missing()),
    new_facts: ReadOnlyArray::from_array(new_facts),
  }
}
