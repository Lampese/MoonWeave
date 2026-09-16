///|
/// Materialized keyed totals are independent of retired input history.
pub(all) struct EngineSnapshot {
  causal : @causal.CausalLogSnapshot[Contribution]
  totals : Array[KeyedSum]
  outputs : Array[KeyedSum]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Engine::snapshot(self : Engine) -> EngineSnapshot {
  {
    causal: self.log.snapshot(),
    totals: self.state.totals(),
    outputs: self.history.copy(),
  }
}

///|
pub fn Engine::restore(
  snapshot : EngineSnapshot,
) -> Engine raise AggregationError {
  let log = @causal.CausalLog::restore(snapshot.causal) catch {
    error => raise AggregationError::Causal(error)
  }
  let state = State::new()
  for total in snapshot.totals {
    if total.key().is_empty() || state.totals.contains(total.key()) {
      raise AggregationError::MalformedPayload
    }
    state.totals[total.key()] = total.total()
  }
  for record in snapshot.causal.admitted {
    if record.key != record.payload.key() || !state.totals.contains(record.key) {
      raise AggregationError::MalformedPayload
    }
  }
  for record in snapshot.causal.pending {
    if record.key != record.payload.key() {
      raise AggregationError::MalformedPayload
    }
  }
  if snapshot.outputs.any(output => !state.totals.contains(output.key())) {
    raise AggregationError::MalformedPayload
  }
  { log, state, history: snapshot.outputs.copy(), }
}

///|
/// Called only after the runtime has durably checkpointed these totals and
/// acknowledged emitted updates. The next update starts from the saved total;
/// it never rebuilds that total by reapplying retired contributions.
pub fn Engine::compact(
  self : Engine,
  certificate : @causal.CausalRetirement,
) -> Unit raise @causal.CausalError {
  self.log.compact(certificate)
  self.history.clear()
}

///|
pub fn Engine::causal_state_count(self : Engine) -> Int {
  self.log.state_count()
}
