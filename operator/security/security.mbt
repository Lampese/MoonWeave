///|
/// Security event stages form a monotone chain for one user.
pub(all) enum SecurityEventKind {
  LoginSuccess
  GrantAdmin
  BulkExport
} derive(Debug, Eq, Compare, Hash, ToJson, FromJson)

///|
pub(all) struct SecurityEvent {
  user_id : String
  kind : SecurityEventKind
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct SecurityPayload {
  user_id : String
  kind : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) suberror SecurityError {
  EmptyUser
  InvalidKind(String)
  InvalidTransition(SecurityEventKind)
  Causal(@causal.CausalError)
} derive(Debug, Eq)

///|
pub fn SecurityEvent::new(
  user_id : String,
  kind : SecurityEventKind,
) -> SecurityEvent raise SecurityError {
  if user_id.is_empty() {
    raise SecurityError::EmptyUser
  }
  { user_id, kind, }
}

///|
pub fn SecurityEvent::user_id(self : SecurityEvent) -> String {
  self.user_id
}

///|
pub fn SecurityEvent::kind(self : SecurityEvent) -> SecurityEventKind {
  self.kind
}

///|
pub fn SecurityEvent::from_json(
  value : Json,
) -> SecurityEvent raise SecurityError {
  let payload : SecurityPayload = @json.from_json(value) catch {
    _ => raise SecurityError::InvalidKind("malformed payload")
  }
  let kind = match payload.kind {
    "login_success" => SecurityEventKind::LoginSuccess
    "grant_admin" => SecurityEventKind::GrantAdmin
    "bulk_export" => SecurityEventKind::BulkExport
    other => raise SecurityError::InvalidKind(other)
  }
  SecurityEvent::new(payload.user_id, kind)
}

///|
pub fn SecurityEvent::to_json(self : SecurityEvent) -> Json {
  @json.to_json(SecurityPayload::{
    user_id: self.user_id,
    kind: match self.kind {
      SecurityEventKind::LoginSuccess => "login_success"
      SecurityEventKind::GrantAdmin => "grant_admin"
      SecurityEventKind::BulkExport => "bulk_export"
    },
  })
}

///|
pub(all) struct SecurityAlert {
  user_id : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn SecurityAlert::user_id(self : SecurityAlert) -> String {
  self.user_id
}

///|
pub struct SecurityState {
  stages : Map[String, Set[SecurityEventKind]]
  alerts : Set[String]
}

///|
pub fn SecurityState::new() -> SecurityState {
  { stages: Map([]), alerts: Set([]), }
}

///|
pub fn SecurityState::alerts(self : SecurityState) -> Array[SecurityAlert] {
  let users = self.alerts.to_array()
  users.sort()
  users.map(user_id => SecurityAlert::{ user_id, })
}

///|
pub fn SecurityState::apply(
  self : SecurityState,
  event : SecurityEvent,
) -> Array[SecurityAlert] {
  let stages = match self.stages.get(event.user_id()) {
    Some(value) => value
    None => {
      let value : Set[SecurityEventKind] = Set([])
      self.stages[event.user_id()] = value
      value
    }
  }
  stages.add(event.kind())
  if stages.contains(SecurityEventKind::LoginSuccess) &&
    stages.contains(SecurityEventKind::GrantAdmin) &&
    stages.contains(SecurityEventKind::BulkExport) &&
    self.alerts.add_and_check(event.user_id()) {
    [SecurityAlert::{ user_id: event.user_id(), }]
  } else {
    []
  }
}

///|
pub struct Engine {
  mut log : @causal.CausalLog[SecurityEvent]
  state : SecurityState
}

///|
pub fn Engine::new(
  maximum_pending? : Int = 10000,
) -> Engine raise @causal.CausalError {
  {
    log: @causal.CausalLog::new(maximum_pending~),
    state: SecurityState::new(),
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
pub fn Engine::alerts(self : Engine) -> Array[SecurityAlert] {
  self.state.alerts()
}

///|
pub fn Engine::apply(
  self : Engine,
  event : @causal.Event[SecurityEvent],
) -> Array[SecurityAlert] raise SecurityError {
  let candidate = self.log.copy()
  let alerts = []
  let admission = candidate.submit(event) catch {
    error => raise SecurityError::Causal(error)
  }
  for admitted in admission.admitted() {
    validate_transition(candidate, admitted)
  }
  for admitted in admission.admitted() {
    alerts.push_iter(self.state.apply(admitted.payload()).iter())
  }
  self.log = candidate
  alerts.sort_by((left, right) => left.user_id().compare(right.user_id()))
  alerts
}

///|
/// Require the security-chain edge to be represented in event parents. This
/// prevents concurrent but unrelated events from being interpreted as a
/// causal attack sequence merely because their kinds coexist in state.
fn validate_transition(
  log : @causal.CausalLog[SecurityEvent],
  event : @causal.Event[SecurityEvent],
) -> Unit raise SecurityError {
  let required_parent = match event.payload().kind() {
    SecurityEventKind::LoginSuccess => None
    SecurityEventKind::GrantAdmin => Some(SecurityEventKind::LoginSuccess)
    SecurityEventKind::BulkExport => Some(SecurityEventKind::GrantAdmin)
  }
  match required_parent {
    None => ()
    Some(required) => {
      let valid = event
        .parents()
        .any(parent_id => {
          match log.event(parent_id) {
            Some(parent) =>
              parent.payload().user_id() == event.payload().user_id() &&
              parent.payload().kind() == required
            None => false
          }
        })
      if !valid {
        raise SecurityError::InvalidTransition(event.payload().kind())
      }
    }
  }
}

///|
pub fn Engine::apply_json_event(
  self : Engine,
  id : @causal.EventId,
  parents : ArrayView[@causal.EventId],
  key : String,
  payload : Json,
) -> Array[SecurityAlert] raise SecurityError {
  let security_event = SecurityEvent::from_json(payload)
  let event = @causal.Event::new(id, parents, key, security_event) catch {
    error => raise SecurityError::Causal(error)
  }
  self.apply(event)
}
