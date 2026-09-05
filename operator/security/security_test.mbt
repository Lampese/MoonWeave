///|
fn id(origin : String, sequence : Int) -> @causal.EventId raise {
  @causal.EventId::new(origin, sequence)
}

///|
fn event(
  origin : String,
  sequence : Int,
  parents : Array[@causal.EventId],
  kind : String,
) -> @causal.Event[SecurityEvent] raise {
  @causal.Event::new(
    id(origin, sequence),
    parents,
    "user-42",
    SecurityEvent::new(
      "user-42",
      match kind {
        "login" => SecurityEventKind::LoginSuccess
        "admin" => SecurityEventKind::GrantAdmin
        _ => SecurityEventKind::BulkExport
      },
    ),
  )
}

///|
test "security chain waits for missing causal stages" {
  let engine = Engine::new()
  let login = event("auth", 0, [], "login")
  let admin = event("admin", 0, [login.id()], "admin")
  let export_event = event("storage", 0, [admin.id()], "export")
  assert_eq(engine.apply(export_event), [])
  assert_eq(engine.pending_count(), 1)
  assert_eq(engine.apply(admin), [])
  assert_eq(engine.pending_count(), 2)
  let alerts = engine.apply(login)
  assert_eq(alerts, [SecurityAlert::{ user_id: "user-42", }])
  assert_eq(engine.pending_count(), 0)
  assert_eq(engine.alerts(), alerts)
}

///|
test "security event JSON adapter validates stage names" {
  let engine = Engine::new()
  let login_id = id("auth", 0)
  let alerts = engine.apply_json_event(
    login_id,
    [],
    "user-42",
    Json::object({
      "user_id": Json::string("user-42"),
      "kind": Json::string("login_success"),
    }),
  )
  assert_eq(alerts, [])
  try
    engine.apply_json_event(
      id("admin", 0),
      [],
      "user-42",
      Json::object({
        "user_id": Json::string("user-42"),
        "kind": Json::string("unknown"),
      }),
    )
  catch {
    SecurityError::InvalidKind(kind) => assert_eq(kind, "unknown")
    _ => fail("expected invalid security stage")
  } noraise {
    _ => fail("expected invalid security stage")
  }
}

///|
test "security engine rejects a stage without a causal predecessor" {
  let engine = Engine::new()
  try engine.apply(event("admin", 0, [], "admin")) catch {
    SecurityError::InvalidTransition(SecurityEventKind::GrantAdmin) => ()
    _ => fail("expected invalid causal transition")
  } noraise {
    _ => fail("expected invalid causal transition")
  }
}

///|
test "invalid released transition does not partially commit the causal log" {
  let engine = Engine::new()
  let bad_admin = event("admin", 0, [], "admin")
  let login = event("auth", 0, [], "login")
  try engine.apply(bad_admin) catch {
    SecurityError::InvalidTransition(SecurityEventKind::GrantAdmin) => ()
    _ => fail("expected invalid transition")
  } noraise {
    _ => fail("expected invalid transition")
  }
  assert_eq(engine.event_count(), 0)
  assert_eq(engine.pending_count(), 0)
  ignore(engine.apply(login))
  assert_eq(engine.event_count(), 1)
}
