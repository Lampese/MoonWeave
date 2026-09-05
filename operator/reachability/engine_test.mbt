///|
fn edge_event(
  origin : String,
  sequence : Int,
  parents : ArrayView[@causal.EventId],
  source : String,
  target : String,
) -> @causal.Event[Edge] raise @causal.CausalError {
  @causal.Event::new(
    @causal.EventId::new(origin, sequence),
    parents,
    "graph",
    Edge::new(source, target) catch {
      _ => raise @causal.CausalError::EmptyKey
    },
  )
}

///|
test "engine admits out of order edges and updates the query" {
  let engine = Engine::new()
  let query = Query::new("reach-a-c", "A", "C", [0])
  engine.set_query(query)
  let root = edge_event("source", 0, [], "A", "B")
  let child = edge_event("source", 1, [root.id()], "B", "C")
  assert_eq(engine.apply(child), [])
  assert_eq(engine.pending_count(), 1)
  let facts = engine.apply(root)
  assert_eq(facts, [
    Reachable::new("A", "B"),
    Reachable::new("A", "C"),
    Reachable::new("B", "C"),
  ])
  assert_eq(engine.event_count(), 2)
  guard engine.query() is Some(current) else { fail("query missing") }
  assert_eq(current.status(), QueryStatus::Tentative)
  assert_true(current.close_partition(0))
  assert_eq(current.status(), QueryStatus::Stable)
}

///|
test "engine duplicate events do not duplicate facts" {
  let engine = Engine::new()
  let event = edge_event("source", 0, [], "A", "B")
  assert_eq(engine.apply(event), [Reachable::new("A", "B")])
  assert_eq(engine.apply(event), [])
  assert_eq(engine.event_count(), 1)
}

///|
test "engine validates JSON edge payloads at the application boundary" {
  let engine = Engine::new()
  let id = @causal.EventId::new("source", 0)
  let facts = engine.apply_json_event(
    id,
    [],
    "graph",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  assert_eq(facts, [Reachable::new("A", "B")])
  try
    engine.apply_json_event(
      @causal.EventId::new("source", 1),
      [],
      "graph",
      Json::object({ "source": Json::string("A") }),
    )
  catch {
    EngineError::Reachability(ReachabilityError::MalformedPayload) => ()
    _ => fail("expected malformed edge payload")
  } noraise {
    _ => fail("expected malformed edge payload")
  }
}

///|
test "engine verifies certificate support events against its causal log" {
  let engine = Engine::new()
  let root_id = @causal.EventId::new("source", 0)
  let root = @causal.Event::new(root_id, [], "graph", Edge::new("A", "B"))
  ignore(engine.apply(root))
  let query = Query::new("cert-engine", "A", "B", [0])
  engine.set_query(query)
  ignore(query.observe(Reachable::new("A", "B")))
  ignore(query.close_partition(0))
  let certificate = engine.certificate(engine.frontier().digest(), [root_id])
  assert_true(engine.verify_certificate(certificate))
  let missing = @causal.EventId::new("missing", 0)
  let invalid = query.certificate(engine.frontier().digest(), [missing])
  assert_false(engine.verify_certificate(invalid))
}
