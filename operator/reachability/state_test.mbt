///|
fn edge(
  source : String,
  target : String,
) -> @reachability.Edge raise @reachability.ReachabilityError {
  @reachability.Edge::new(source, target)
}

///|
test "incremental closure derives paths through a new edge" {
  let state = @reachability.State::new()
  assert_eq(state.apply(edge("a", "b")), [
    @reachability.Reachable::new("a", "b"),
  ])
  assert_eq(state.apply(edge("b", "c")), [
    @reachability.Reachable::new("a", "c"),
    @reachability.Reachable::new("b", "c"),
  ])
  assert_true(state.is_reachable("a", "c"))
  assert_eq(state.edge_count(), 2)
  assert_eq(state.fact_count(), 3)
}

///|
test "duplicate edges do not emit duplicate facts" {
  let state = @reachability.State::new()
  let input = edge("a", "b")
  ignore(state.apply(input))
  assert_eq(state.apply(input), [])
  assert_eq(state.edge_count(), 1)
  assert_eq(state.fact_count(), 1)
}

///|
test "delivery order does not change the fixed point" {
  let forward = @reachability.State::new()
  ignore(forward.apply(edge("a", "b")))
  ignore(forward.apply(edge("b", "c")))
  ignore(forward.apply(edge("c", "d")))
  let reverse = @reachability.State::new()
  ignore(reverse.apply(edge("c", "d")))
  ignore(reverse.apply(edge("b", "c")))
  ignore(reverse.apply(edge("a", "b")))
  assert_eq(forward.facts(), reverse.facts())
  assert_true(reverse.is_reachable("a", "d"))
}

///|
test "state merge is commutative and idempotent at the fixed point" {
  let left = @reachability.State::new()
  ignore(left.apply(edge("a", "b")))
  let right = @reachability.State::new()
  ignore(right.apply(edge("b", "c")))
  let left_join = @reachability.State::new()
  ignore(left_join.merge(left))
  ignore(left_join.merge(right))
  let right_join = @reachability.State::new()
  ignore(right_join.merge(right))
  ignore(right_join.merge(left))
  assert_eq(left_join.facts(), right_join.facts())
  assert_eq(left_join.merge(right), [])
}

///|
test "cycle derives non-empty self paths" {
  let state = @reachability.State::new()
  ignore(state.apply(edge("a", "b")))
  ignore(state.apply(edge("b", "a")))
  assert_true(state.is_reachable("a", "a"))
  assert_true(state.is_reachable("b", "b"))
}
