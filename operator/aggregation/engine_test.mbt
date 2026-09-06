///|
fn sum_event(
  origin : String,
  sequence : Int,
  parents : ArrayView[@causal.EventId],
  key : String,
  value : Int64,
) -> @causal.Event[Contribution] raise {
  let contribution = Contribution::new(key, value)
  @causal.Event::new(
    @causal.EventId::new(origin, sequence),
    parents,
    contribution.key(),
    contribution,
  )
}

///|
test "keyed sum emits replayable running totals" {
  let engine = Engine::new()
  let first = sum_event("orders", 0, [], "store-a", 3L)
  let second = sum_event("orders", 1, [], "store-a", 5L)
  let other = sum_event("orders", 2, [], "store-b", 7L)
  assert_eq(engine.apply(first), [KeyedSum::new("store-a", 3L)])
  assert_eq(engine.apply(second), [KeyedSum::new("store-a", 8L)])
  assert_eq(engine.apply(other), [KeyedSum::new("store-b", 7L)])
  assert_eq(engine.apply(first), [])
  assert_eq(engine.totals(), [
    KeyedSum::new("store-a", 8L),
    KeyedSum::new("store-b", 7L),
  ])
  assert_eq(engine.outputs(), [
    KeyedSum::new("store-a", 3L),
    KeyedSum::new("store-a", 8L),
    KeyedSum::new("store-b", 7L),
  ])
}

///|
test "keyed sum releases pending descendants deterministically" {
  let engine = Engine::new()
  let root = sum_event("orders", 0, [], "store-a", 10L)
  let child = sum_event("orders", 1, [root.id()], "store-a", 2L)
  assert_eq(engine.apply(child), [])
  assert_eq(engine.pending_count(), 1)
  assert_eq(engine.apply(root), [
    KeyedSum::new("store-a", 10L),
    KeyedSum::new("store-a", 12L),
  ])
  assert_eq(engine.pending_count(), 0)
}

///|
test "keyed sum rejects overflow without committing the input" {
  let engine = Engine::new()
  let maximum = sum_event("orders", 0, [], "store-a", 9223372036854775807L)
  let overflow = sum_event("orders", 1, [], "store-a", 1L)
  ignore(engine.apply(maximum))
  try engine.apply(overflow) catch {
    AggregationError::Overflow(key) => assert_eq(key, "store-a")
    _ => fail("expected keyed sum overflow")
  } noraise {
    _ => fail("expected keyed sum overflow")
  }
  assert_eq(engine.event_count(), 1)
  assert_eq(engine.total("store-a"), Some(9223372036854775807L))
}

///|
test "keyed sum JSON adapter validates the numeric payload" {
  let engine = Engine::new()
  let output = engine.apply_json_event(
    @causal.EventId::new("orders", 0),
    [],
    "store-a",
    Json::object({ "value": Json::string("4") }),
  )
  assert_eq(output, [KeyedSum::new("store-a", 4L)])
  try
    engine.apply_json_event(
      @causal.EventId::new("orders", 1),
      [],
      "store-a",
      Json::object({ "value": Json::string("four") }),
    )
  catch {
    AggregationError::MalformedPayload => ()
    _ => fail("expected malformed keyed sum payload")
  } noraise {
    _ => fail("expected malformed keyed sum payload")
  }
}
