///|
/// Retired parent payloads are grouped by their user and stage. Identity
/// ranges preserve holes: a foreign user's login cannot justify a grant.
pub(all) struct SecurityWitness {
  user_id : String
  kind : SecurityEventKind
  identities : Array[@causal.EventRange]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct SecurityStageSnapshot {
  user_id : String
  kinds : Array[SecurityEventKind]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct EngineSnapshot {
  causal : @causal.CausalLogSnapshot[SecurityEvent]
  stages : Array[SecurityStageSnapshot]
  alerts : Array[SecurityAlert]
  witnesses : Array[SecurityWitness]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Engine::snapshot(self : Engine) -> EngineSnapshot {
  let stages = self.state.stages
    .to_array()
    .map(entry => {
      let kinds = entry.1.to_array()
      kinds.sort()
      SecurityStageSnapshot::{ user_id: entry.0, kinds, }
    })
  stages.sort_by((left, right) => left.user_id.compare(right.user_id))
  {
    causal: self.log.snapshot(),
    stages,
    alerts: self.state.alerts(),
    witnesses: self.witnesses.copy(),
  }
}

///|
pub fn Engine::restore(snapshot : EngineSnapshot) -> Engine raise SecurityError {
  let log = @causal.CausalLog::restore(snapshot.causal) catch {
    error => raise SecurityError::Causal(error)
  }
  let state = SecurityState::new()
  for row in snapshot.stages {
    if row.user_id.is_empty() ||
      state.stages.contains(row.user_id) ||
      Set(row.kinds).length() != row.kinds.length() {
      raise SecurityError::InvalidKind("invalid materialized stage")
    }
    for kind in row.kinds {
      ignore(state.apply(SecurityEvent::new(row.user_id, kind)))
    }
  }
  if state.alerts() != snapshot.alerts {
    raise SecurityError::InvalidKind("invalid materialized alerts")
  }
  let witnesses : Array[SecurityWitness] = []
  for witness in snapshot.witnesses {
    if witness.user_id.is_empty() ||
      witnesses.any(previous => {
        previous.user_id == witness.user_id && previous.kind == witness.kind
      }) {
      raise SecurityError::InvalidKind("duplicate parent witness")
    }
    let identities = @causal.CausalRetirement::new(
        "security-witness",
        witness.identities,
      )
      .ranges()
      .to_owned() catch {
        error => raise SecurityError::Causal(error)
      }
    for range in identities {
      if !log
        .retired_ranges()
        .any(retired => {
          retired.origin() == range.origin() &&
          retired.first() <= range.first() &&
          retired.last() >= range.last()
        }) {
        raise SecurityError::InvalidKind(
          "parent witness lacks retired membership",
        )
      }
      if witnesses.any(previous => {
          previous.identities.any(other => {
            other.origin() == range.origin() &&
            other.first() <= range.last() &&
            range.first() <= other.last()
          })
        }) {
        raise SecurityError::InvalidKind("conflicting parent witness")
      }
    }
    witnesses.push({ ..witness, identities, })
  }
  for event in log.events() {
    if !state.stages
      .get(event.payload().user_id())
      .map(kinds => kinds.contains(event.payload().kind()))
      .unwrap_or(false) {
      raise SecurityError::InvalidKind(
        "admitted event lacks materialized state",
      )
    }
    validate_transition(log, witnesses, event)
  }
  { log, state, witnesses, }
}

///|
pub fn Engine::compact(
  self : Engine,
  certificate : @causal.CausalRetirement,
) -> Unit raise @causal.CausalError {
  let candidate = self.log.copy()
  candidate.compact(certificate)
  let grouped : Map[(String, SecurityEventKind), Array[@causal.EventRange]] = Map([],
  )
  for witness in self.witnesses {
    grouped[(witness.user_id, witness.kind)] = witness.identities.copy()
  }
  for event in self.log.events() {
    if certificate.ranges().any(range => range.contains(event.id())) {
      let key = (event.payload().user_id(), event.payload().kind())
      let ranges = grouped.get(key).unwrap_or([])
      ranges.push(
        @causal.EventRange::new(
          event.id().origin(),
          event.id().sequence(),
          event.id().sequence(),
        ),
      )
      grouped[key] = ranges
    }
  }
  let witnesses = grouped
    .to_array()
    .map(entry => SecurityWitness::{
      user_id: entry.0.0,
      kind: entry.0.1,
      identities: @causal.CausalRetirement::new(
        certificate.checkpoint_id(),
        entry.1,
      )
      .ranges()
      .to_owned(),
    })
  witnesses.sort_by((left, right) => {
    (left.user_id, left.kind).compare((right.user_id, right.kind))
  })
  self.log = candidate
  self.witnesses = witnesses
}

///|
pub fn Engine::state_count(self : Engine) -> Int {
  self.log.state_count() +
  self.state.stages.length() +
  self.state.alerts.length() +
  self.witnesses.fold(init=0, (count, witness) => {
    count + witness.identities.length()
  })
}
