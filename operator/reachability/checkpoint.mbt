///|
pub(all) struct QuerySnapshot {
  query_id : String
  source : String
  target : String
  reason : String
  matched : Bool
  obligations : Array[Int]
  open : Array[Int]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct EngineSnapshot {
  causal : @causal.CausalLogSnapshot[Edge]
  edges : Array[Edge]
  query : QuerySnapshot?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Engine::snapshot(self : Engine) -> EngineSnapshot {
  let query = self.query.map(query => {
    let obligations = query.obligations.to_array()
    let open = query.open.to_array()
    obligations.sort()
    open.sort()
    QuerySnapshot::{
      query_id: query.query_id,
      source: query.source,
      target: query.target,
      reason: query.reason,
      matched: query.matched,
      obligations,
      open,
    }
  })
  { causal: self.log.snapshot(), edges: self.state.edges(), query, }
}

///|
pub fn Engine::restore(snapshot : EngineSnapshot) -> Engine raise EngineError {
  let log = @causal.CausalLog::restore(snapshot.causal) catch {
    error => raise EngineError::Causal(error)
  }
  let state = State::new()
  for edge in snapshot.edges {
    ignore(
      Edge::new(edge.source(), edge.target()) catch {
        error => raise EngineError::Reachability(error)
      },
    )
    if state.edges.contains((edge.source(), edge.target())) {
      raise EngineError::Reachability(ReachabilityError::MalformedPayload)
    }
    ignore(state.apply(edge))
  }
  for event in log.events() {
    if !state.edges.contains(
        (event.payload().source(), event.payload().target()),
      ) {
      raise EngineError::Reachability(ReachabilityError::MalformedPayload)
    }
  }
  let query = snapshot.query.map(saved => {
    let query = Query::new(
      saved.query_id,
      saved.source,
      saved.target,
      saved.obligations,
      reason=saved.reason,
    ) catch {
      _ => raise EngineError::Reachability(ReachabilityError::MalformedPayload)
    }
    if query.obligations.length() != saved.obligations.length() ||
      Set(saved.open).length() != saved.open.length() ||
      saved.open.any(partition => !query.obligations.contains(partition)) {
      raise EngineError::Reachability(ReachabilityError::MalformedPayload)
    }
    query.open.clear()
    for partition in saved.open {
      query.open.add(partition)
    }
    query.matched = saved.matched
    query
  })
  { log, state, query, }
}

///|
pub fn Engine::compact(
  self : Engine,
  certificate : @causal.CausalRetirement,
) -> Unit raise @causal.CausalError {
  self.log.compact(certificate)
}

///|
pub fn Engine::state_count(self : Engine) -> Int {
  self.log.state_count() + self.state.edge_count() + self.state.fact_count()
}
