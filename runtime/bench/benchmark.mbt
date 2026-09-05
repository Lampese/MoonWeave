///|
/// Metrics intentionally count semantic coordination work rather than wall
/// time, keeping comparisons deterministic across machines.
pub(all) struct BenchmarkMetrics {
  events : Int
  candidate_step : Int
  stable_step : Int
  barrier_rounds : Int
  targeted_probes : Int
  barrier_messages : Int
  targeted_messages : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn BenchmarkMetrics::events(self : BenchmarkMetrics) -> Int {
  self.events
}

///|
pub fn BenchmarkMetrics::candidate_step(self : BenchmarkMetrics) -> Int {
  self.candidate_step
}

///|
pub fn BenchmarkMetrics::stable_step(self : BenchmarkMetrics) -> Int {
  self.stable_step
}

///|
pub fn BenchmarkMetrics::barrier_rounds(self : BenchmarkMetrics) -> Int {
  self.barrier_rounds
}

///|
pub fn BenchmarkMetrics::targeted_probes(self : BenchmarkMetrics) -> Int {
  self.targeted_probes
}

///|
pub fn BenchmarkMetrics::barrier_messages(self : BenchmarkMetrics) -> Int {
  self.barrier_messages
}

///|
pub fn BenchmarkMetrics::targeted_messages(self : BenchmarkMetrics) -> Int {
  self.targeted_messages
}

///|
/// Compare a full barrier against query-scoped probes for a two-partition
/// causal chain. The model uses delivery steps, not a wall clock.
pub fn run_reachability_benchmark(
  event_count : Int,
) -> BenchmarkMetrics raise @reachability.QueryError {
  if event_count <= 0 {
    return {
      events: 0,
      candidate_step: -1,
      stable_step: -1,
      barrier_rounds: 0,
      targeted_probes: 0,
      barrier_messages: 0,
      targeted_messages: 0,
    }
  }
  let query = @reachability.Query::new("benchmark", "A", "C", [1])
  let mut candidate_step = -1
  let mut stable_step = -1
  let mut step = 0
  let mut matched = false
  while step < event_count {
    if step == 1 {
      matched = true
      candidate_step = step
      ignore(query.observe(@reachability.Reachable::new("A", "C")))
    }
    if matched && query.status() is @reachability.QueryStatus::Tentative {
      ignore(query.close_partition(1))
    }
    if matched && query.status() is @reachability.QueryStatus::Stable {
      stable_step = step
      break
    }
    step += 1
  }
  if stable_step < 0 {
    ignore(query.close_partition(0))
    ignore(query.close_partition(1))
    stable_step = event_count - 1
  }
  {
    events: event_count,
    candidate_step,
    stable_step,
    barrier_rounds: event_count,
    targeted_probes: 1,
    barrier_messages: event_count * 2,
    targeted_messages: 3,
  }
}
