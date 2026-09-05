///|
/// A deterministic liveness decision for one registered Worker.
pub(all) enum LivenessDecision {
  Healthy
  Suspect
  Fenced
} derive(Debug, Eq)

///|
pub fn CoordinatorState::liveness(
  self : CoordinatorState,
  worker_id : String,
  now_tick : Int64,
  timeout : Int64,
) -> LivenessDecision raise CoordinatorError {
  if now_tick < 0L {
    raise CoordinatorError::InvalidTick(now_tick)
  }
  if timeout <= 0L {
    raise CoordinatorError::InvalidHeartbeatTimeout(timeout)
  }
  let worker = match self.worker(worker_id) {
    Some(value) => value
    None => raise CoordinatorError::UnknownWorker(worker_id)
  }
  let stale = match worker.last_heartbeat_tick() {
    None => false
    Some(last) => now_tick >= last && now_tick - last >= timeout
  }
  if worker.lifecycle() is WorkerLifecycle::Fenced {
    LivenessDecision::Fenced
  } else if stale {
    LivenessDecision::Suspect
  } else {
    LivenessDecision::Healthy
  }
}
