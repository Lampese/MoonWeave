///|
/// Mutable keyed totals reconstructed from the causal contribution log.
pub struct State {
  totals : Map[String, Int64]
} derive(Debug, Eq)

///|
pub fn State::new() -> State {
  { totals: Map([]), }
}

///|
pub fn State::copy(self : State) -> State {
  { totals: Map(self.totals.to_array()), }
}

///|
pub fn State::total(self : State, key : String) -> Int64? {
  self.totals.get(key)
}

///|
pub fn State::totals(self : State) -> Array[KeyedSum] {
  let result = self.totals.to_array().map(pair => KeyedSum::new(pair.0, pair.1))
  result.sort()
  result
}

///|
fn checked_add(
  left : Int64,
  right : Int64,
  key : String,
) -> Int64 raise AggregationError {
  let max_value = 9223372036854775807L
  let min_value = -9223372036854775808L
  if right > 0L && left > max_value - right {
    raise AggregationError::Overflow(key)
  }
  if right < 0L && left < min_value - right {
    raise AggregationError::Overflow(key)
  }
  left + right
}

///|
pub fn State::apply(
  self : State,
  contribution : Contribution,
) -> KeyedSum raise AggregationError {
  let previous = self.totals.get(contribution.key()).unwrap_or(0L)
  let total = checked_add(previous, contribution.value(), contribution.key())
  self.totals[contribution.key()] = total
  KeyedSum::new(contribution.key(), total)
}

///|
/// Causal keyed aggregation. The event log provides duplicate suppression,
/// deterministic release of pending descendants, and replayable state.
pub struct Engine {
  mut log : @causal.CausalLog[Contribution]
  mut state : State
  mut history : Array[KeyedSum]
}

///|
pub fn Engine::new(
  maximum_pending? : Int = 10000,
) -> Engine raise @causal.CausalError {
  {
    log: @causal.CausalLog::new(maximum_pending~),
    state: State::new(),
    history: [],
  }
}

///|
pub fn Engine::copy(self : Engine) -> Engine {
  {
    log: self.log.copy(),
    state: self.state.copy(),
    history: self.history.copy(),
  }
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
pub fn Engine::frontier(self : Engine) -> @causal.Frontier {
  self.log.frontier()
}

///|
pub fn Engine::events(self : Engine) -> Array[@causal.Event[Contribution]] {
  self.log.events()
}

///|
pub fn Engine::pending_events(
  self : Engine,
) -> Array[@causal.Event[Contribution]] {
  self.log.pending_events()
}

///|
/// Return the complete emitted update history in deterministic order.
pub fn Engine::outputs(self : Engine) -> Array[KeyedSum] {
  let result = self.history.copy()
  result.sort()
  result
}

///|
pub fn Engine::totals(self : Engine) -> Array[KeyedSum] {
  self.state.totals()
}

///|
pub fn Engine::total(self : Engine, key : String) -> Int64? {
  self.state.total(key)
}

///|
pub fn Engine::apply(
  self : Engine,
  event : @causal.Event[Contribution],
) -> Array[KeyedSum] raise AggregationError {
  let candidate_log = self.log.copy()
  let candidate_state = self.state.copy()
  let candidate_history = self.history.copy()
  let admission = candidate_log.submit(event) catch {
    error => raise AggregationError::Causal(error)
  }
  let outputs : Array[KeyedSum] = []
  for admitted in admission.admitted() {
    let output = candidate_state.apply(admitted.payload())
    candidate_history.push(output)
    outputs.push(output)
  }
  self.log = candidate_log
  self.state = candidate_state
  self.history = candidate_history
  outputs.sort()
  outputs
}

///|
pub fn Engine::apply_json_event(
  self : Engine,
  id : @causal.EventId,
  parents : ArrayView[@causal.EventId],
  key : String,
  payload : Json,
) -> Array[KeyedSum] raise AggregationError {
  let contribution = Contribution::from_json(key, payload)
  let event = @causal.Event::new(id, parents, contribution.key(), contribution) catch {
    error => raise AggregationError::Causal(error)
  }
  self.apply(event)
}
