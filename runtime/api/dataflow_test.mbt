///|
struct Sale {
  store : String
  amount : Int64
} derive(Debug, Eq)

///|
test "generic local dataflow composes map filter keyBy and aggregate" {
  let source = Source::from_array([
    Sale::{ store: "north", amount: 10L, },
    Sale::{ store: "north", amount: 5L, },
    Sale::{ store: "south", amount: 7L, },
    Sale::{ store: "south", amount: -1L, },
  ])
  let aggregate : Aggregate[Sale, Int64, KeyedSumUpdate] = Aggregate::new(
    _ => 0L,
    (state, sale) => state + sale.amount,
    (key, total) => KeyedSumUpdate::new(key, total),
  )
  let stream = source
    .stream()
    .filter(sale => sale.amount >= 0L)
    .map(sale => sale)
    .key_by_fn(sale => sale.store)
    .aggregate(aggregate)
  let handle = stream.collect()
  assert_eq(handle.status(), RunStatus::Succeeded)
  assert_eq(handle.await_completion(), RunStatus::Succeeded)
  assert_eq(handle.outputs(), [
    KeyedSumUpdate::new("north", 10L),
    KeyedSumUpdate::new("north", 15L),
    KeyedSumUpdate::new("south", 7L),
  ])
  handle.close()
  assert_eq(handle.status(), RunStatus::Closed)
}

///|
test "generic local dataflow flushes bounded count windows" {
  let source = Source::from_array([
    Sale::{ store: "north", amount: 3L, },
    Sale::{ store: "north", amount: 4L, },
    Sale::{ store: "north", amount: 5L, },
    Sale::{ store: "south", amount: 2L, },
  ])
  let aggregate : Aggregate[Sale, Int64, KeyedSumUpdate] = Aggregate::new(
    _ => 0L,
    (state, sale) => state + sale.amount,
    (key, total) => KeyedSumUpdate::new(key, total),
  )
  let stream = source
    .stream()
    .key_by(KeyBy::new(sale => sale.store))
    .window(Window::count(2))
    .aggregate(aggregate)
  let handle = stream.run(Sink::collect())
  assert_eq(handle.outputs(), [
    KeyedSumUpdate::new("north", 7L),
    KeyedSumUpdate::new("north", 5L),
    KeyedSumUpdate::new("south", 2L),
  ])
}

///|
test "generic local dataflow validates keys and windows" {
  let source = Source::from_array([1, 2, 3])
  try source.stream().key_by(KeyBy::new(_ => "")) catch {
    ApiError::EmptyKey => ()
    _ => fail("expected empty key rejection")
  } noraise {
    _ => fail("expected empty key rejection")
  }
  try Window::count(0) catch {
    ApiError::InvalidWindowSize(size) => assert_eq(size, 0)
    _ => fail("expected invalid window size rejection")
  } noraise {
    _ => fail("expected invalid window size rejection")
  }
}

///|
test "generic local dataflow discard sink does not retain outputs" {
  let handle = Source::from_array([1, 2, 3]).run(Sink::discard())
  assert_eq(handle.outputs(), [])
  assert_eq(handle.status(), RunStatus::Succeeded)
}
