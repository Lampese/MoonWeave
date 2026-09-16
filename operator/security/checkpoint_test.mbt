///|
test "security retirement preserves parent kinds and users across recovery" {
  let mut engine = Engine::new()
  for cycle in 0..<100 {
    let login = @causal.EventId::new("login", cycle.to_int64())
    let grant = @causal.EventId::new("grant", cycle.to_int64())
    let export = @causal.EventId::new("export", cycle.to_int64())
    ignore(
      engine.apply(
        @causal.Event::new(
          login,
          [],
          "alice",
          SecurityEvent::new("alice", LoginSuccess),
        ),
      ),
    )
    engine.compact(
      @causal.CausalRetirement::from_ids("login-checkpoint", [login]),
    )
    engine = Engine::restore(
      @json.from_json(@json.parse(@json.to_json(engine.snapshot()).stringify())),
    )
    ignore(
      engine.apply(
        @causal.Event::new(
          grant,
          [login],
          "alice",
          SecurityEvent::new("alice", GrantAdmin),
        ),
      ),
    )
    engine.compact(
      @causal.CausalRetirement::from_ids("grant-checkpoint", [grant]),
    )
    engine = Engine::restore(engine.snapshot())
    let alerts = engine.apply(
      @causal.Event::new(
        export,
        [grant],
        "alice",
        SecurityEvent::new("alice", BulkExport),
      ),
    )
    assert_eq(alerts.length(), if cycle == 0 { 1 } else { 0 })
    engine.compact(
      @causal.CausalRetirement::from_ids("export-checkpoint", [export]),
    )
    assert_eq(engine.state_count(), 8)
  }
  let wrong_parent = @causal.EventId::new("export", 0L)
  try {
    ignore(
      engine.apply(
        @causal.Event::new(
          @causal.EventId::new("bad", 0L),
          [wrong_parent],
          "alice",
          SecurityEvent::new("alice", GrantAdmin),
        ),
      ),
    )
    fail("retired export incorrectly justified a grant")
  } catch {
    SecurityError::InvalidTransition(GrantAdmin) => ()
    error => raise error
  }
  try {
    ignore(
      engine.apply(
        @causal.Event::new(
          @causal.EventId::new("bad", 1L),
          [@causal.EventId::new("login", 0L)],
          "bob",
          SecurityEvent::new("bob", GrantAdmin),
        ),
      ),
    )
    fail("another user's retired login incorrectly justified a grant")
  } catch {
    SecurityError::InvalidTransition(GrantAdmin) => ()
    error => raise error
  }
}
