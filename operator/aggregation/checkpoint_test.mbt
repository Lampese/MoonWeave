///|
test "materialized sums recover after repeated retirement without reapplying contributions" {
  let mut engine = @aggregation.Engine::new()
  for round in 0..<100 {
    let ids = []
    for item in 0..<20 {
      let id = @causal.EventId::new("sales", (round * 20 + item).to_int64())
      ids.push(id)
      ignore(
        engine.apply(
          @causal.Event::new(
            id,
            [],
            "store",
            @aggregation.Contribution::new("store", 3L),
          ),
        ),
      )
    }
    engine.compact(
      @causal.CausalRetirement::from_ids("checkpoint-" + round.to_string(), ids),
    )
    assert_eq(engine.causal_state_count(), 1)
    assert_eq(engine.output_count(), 0)
    let snapshot : @aggregation.EngineSnapshot = @json.from_json(
      @json.to_json(engine.snapshot()),
    )
    engine = @aggregation.Engine::restore(snapshot)
    assert_eq(engine.total("store"), Some((round + 1).to_int64() * 60L))
    assert_eq(
      engine.apply(
        @causal.Event::new(
          ids[0],
          [],
          "store",
          @aggregation.Contribution::new("store", 3L),
        ),
      ),
      [],
    )
  }
  let parent = @causal.EventId::new("sales", 1999L)
  let child = @causal.EventId::new("sales", 2000L)
  assert_eq(
    engine.apply(
      @causal.Event::new(
        child,
        [parent],
        "store",
        @aggregation.Contribution::new("store", 7L),
      ),
    ),
    [@aggregation.KeyedSum::new("store", 6007L)],
  )
}
