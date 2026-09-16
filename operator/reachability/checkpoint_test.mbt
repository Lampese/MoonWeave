///|
test "materialized graph and pending descendants survive historical input retirement" {
  let mut engine = Engine::new()
  for cycle in 0..<100 {
    let id = @causal.EventId::new("edges", cycle.to_int64())
    let edge = Edge::new("a", "b")
    let outputs = engine.apply(@causal.Event::new(id, [], "graph", edge))
    assert_eq(outputs.length(), if cycle == 0 { 1 } else { 0 })
    engine.compact(
      @causal.CausalRetirement::from_ids("cut-" + cycle.to_string(), [id]),
    )
    let snapshot : EngineSnapshot = @json.from_json(
      @json.parse(@json.to_json(engine.snapshot()).stringify()),
    )
    engine = Engine::restore(snapshot)
    assert_eq(engine.state_count(), 3)
  }
  let parent = @causal.EventId::new("edges", 100L)
  let child = @causal.EventId::new("edges", 101L)
  assert_eq(
    engine.apply(
      @causal.Event::new(child, [parent], "graph", Edge::new("c", "d")),
    ),
    [],
  )
  engine = Engine::restore(engine.snapshot())
  let result = engine.apply(
    @causal.Event::new(
      parent,
      [@causal.EventId::new("edges", 0L)],
      "graph",
      Edge::new("b", "c"),
    ),
  )
  assert_eq(result.length(), 5)
  assert_eq(engine.pending_count(), 0)
  assert_eq(engine.facts().length(), 6)
}
