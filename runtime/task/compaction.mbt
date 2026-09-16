///|
struct CustomStateSnapshot {
  causal : @causal.CausalLogSnapshot[TaskEventPayload]
  outputs : Array[Json]
} derive(ToJson, FromJson)

///|
fn Task::materialized_state(self : Task) -> Json? {
  if self.event_time_log is Some(log) {
    return Some(@json.to_json(log.snapshot()))
  }
  match self.operator {
    KeyedSum => Some(@json.to_json(self.aggregation.unwrap().snapshot()))
    Reachability => Some(@json.to_json(self.reachability.unwrap().snapshot()))
    SecurityChain => Some(@json.to_json(self.security.unwrap().snapshot()))
    Custom(_, _, _, _, _, _, _, _, _, _) =>
      Some(
        @json.to_json(CustomStateSnapshot::{
          causal: self.custom_log.unwrap().snapshot(),
          outputs: self.custom_outputs.copy(),
        }),
      )
  }
}

///|
fn Task::restore_materialized_state(
  self : Task,
  snapshot : TaskSnapshot,
) -> Unit raise TaskError {
  match self.operator {
    SecurityChain => {
      guard snapshot.materialized_state is Some(state) else {
        raise TaskError::InvalidTransition
      }
      let saved : @security.EngineSnapshot = @json.from_json(state) catch {
        _ => raise TaskError::InvalidTransition
      }
      self.security = Some(
        @security.Engine::restore(saved) catch {
          error => raise TaskError::Security(error)
        },
      )
    }
    Reachability => {
      guard snapshot.materialized_state is Some(state) else {
        raise TaskError::InvalidTransition
      }
      let saved : @reachability.EngineSnapshot = @json.from_json(state) catch {
        _ => raise TaskError::InvalidTransition
      }
      self.reachability = Some(
        @reachability.Engine::restore(saved) catch {
          error => raise TaskError::Reachability(error)
        },
      )
    }
    KeyedSum => {
      guard snapshot.materialized_state is Some(state) else {
        raise TaskError::InvalidTransition
      }
      let saved : @aggregation.EngineSnapshot = @json.from_json(state) catch {
        _ => raise TaskError::InvalidTransition
      }
      self.aggregation = Some(
        @aggregation.Engine::restore(saved) catch {
          error => raise TaskError::Aggregation(error)
        },
      )
    }
    Custom(_, _, _, _, _, _, _, _, _, _) => {
      guard snapshot.materialized_state is Some(state) else {
        raise TaskError::InvalidTransition
      }
      let saved : CustomStateSnapshot = @json.from_json(state) catch {
        _ => raise TaskError::InvalidTransition
      }
      self.custom_log = Some(
        @causal.CausalLog::restore(saved.causal) catch {
          error => raise TaskError::Causal(error)
        },
      )
      for event in self.custom_log.unwrap().events() {
        self.custom_operator.unwrap().validate_payload(event.payload().payload)
      }
      for event in self.custom_log.unwrap().pending_events() {
        self.custom_operator.unwrap().validate_payload(event.payload().payload)
      }
      self.custom_outputs = saved.outputs.copy()
    }
  }
}

///|
/// Retire checkpointed input identities without stopping a Task. The runtime
/// must persist the resulting snapshot before truncating its input/output WALs.
/// Security retains compact parent witnesses so later transitions still
/// validate the referenced user's stage after the full event is retired.
pub fn Task::compact(
  self : Task,
  certificate : @causal.CausalRetirement,
) -> Unit raise TaskError {
  if self.phase != TaskPhase::Running {
    raise TaskError::InvalidTransition
  }
  let candidate = self.copy()
  if candidate.graph_log is Some(log) {
    log.compact(certificate) catch {
      error => raise TaskError::Causal(error)
    }
  }
  if candidate.event_time_log is Some(log) {
    log.compact(certificate) catch {
      error => raise TaskError::Causal(error)
    }
  } else {
    match candidate.operator {
      SecurityChain =>
        candidate.security.unwrap().compact(certificate) catch {
          error => raise TaskError::Causal(error)
        }
      Reachability =>
        candidate.reachability.unwrap().compact(certificate) catch {
          error => raise TaskError::Causal(error)
        }
      KeyedSum =>
        candidate.aggregation.unwrap().compact(certificate) catch {
          error => raise TaskError::Causal(error)
        }
      Custom(_, _, _, _, _, _, _, _, _, _) => {
        candidate.custom_log.unwrap().compact(certificate) catch {
          error => raise TaskError::Causal(error)
        }
        candidate.custom_outputs.clear()
      }
    }
  }
  candidate.refresh_frontier()
  candidate.check_state_limit()
  self.commit(candidate)
}
