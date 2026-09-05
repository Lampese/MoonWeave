///|
/// One local causal-frontier reachability engine.
///
/// The protocol delivers events in batches, but the causal log decides which
/// events are ready. A batch can therefore produce no fact yet, or can release
/// descendants that arrived earlier and were waiting on a missing parent.
pub struct Engine {
  log : @causal.CausalLog[Edge]
  state : State
  mut query : Query?
}

///|
pub(all) suberror EngineError {
  Causal(@causal.CausalError)
  Reachability(ReachabilityError)
} derive(Debug, Eq)

///|
pub fn Engine::new(
  maximum_pending? : Int = 10000,
) -> Engine raise @causal.CausalError {
  {
    log: @causal.CausalLog::new(maximum_pending~),
    state: State::new(),
    query: None,
  }
}

///|
pub fn Engine::set_query(self : Engine, query : Query) -> Unit {
  self.query = Some(query)
}

///|
pub fn Engine::query(self : Engine) -> Query? {
  self.query
}

///|
/// Build a certificate from the current query and a runtime-supplied proof
/// digest, without coupling the engine to a particular transport.
pub fn Engine::certificate(
  self : Engine,
  frontier_digest : String,
  supporting_events : ArrayView[@causal.EventId],
) -> ResultCertificate raise QueryError {
  match self.query {
    Some(query) => query.certificate(frontier_digest, supporting_events)
    None => raise QueryError::MissingQuery
  }
}

///|
/// Verify a certificate against the engine's current causal log and frontier.
/// This checks event existence and query identity; path completeness remains an
/// operator-specific proof obligation for future certified operators.
pub fn Engine::verify_certificate(
  self : Engine,
  certificate : ResultCertificate,
) -> Bool {
  let query = match self.query {
    Some(value) => value
    None => return false
  }
  certificate.verify(query, self.frontier().digest()) &&
  certificate.supporting_events().all(id => self.log.contains(id))
}

///|
pub fn Engine::event_count(self : Engine) -> Int {
  self.log.admitted_count()
}

///|
pub fn Engine::pending_count(self : Engine) -> Int {
  self.log.pending_count()
}

///|
pub fn Engine::facts(self : Engine) -> Array[Reachable] {
  self.state.facts()
}

///|
pub fn Engine::frontier(self : Engine) -> @causal.Frontier {
  self.log.frontier()
}

///|
/// Apply one causal edge event and return only facts newly derived by this
/// call. Admission happens before the monotone operator step.
pub fn Engine::apply(
  self : Engine,
  event : @causal.Event[Edge],
) -> Array[Reachable] raise @causal.CausalError {
  let new_facts = []
  let admission = self.log.submit(event)
  for admitted in admission.admitted() {
    new_facts.push_iter(self.state.apply(admitted.payload()).iter())
  }
  new_facts.sort()
  match self.query {
    None => ()
    Some(query) =>
      for fact in new_facts {
        ignore(query.observe(fact))
      }
  }
  new_facts
}

///|
/// Convert a protocol-shaped event payload at the application boundary and
/// feed it through causal admission. The protocol package stays independent
/// of this operator while callers still get one checked entry point.
pub fn Engine::apply_json_event(
  self : Engine,
  id : @causal.EventId,
  parents : ArrayView[@causal.EventId],
  key : String,
  payload : Json,
) -> Array[Reachable] raise EngineError {
  let edge = Edge::from_json(payload) catch {
    error => raise EngineError::Reachability(error)
  }
  let event = @causal.Event::new(id, parents, key, edge) catch {
    error => raise EngineError::Causal(error)
  }
  self.apply(event) catch {
    error => raise EngineError::Causal(error)
  }
}
