///|
struct TimedSale {
  store : String
  amount : Int64
  timestamp : Int64
} derive(Debug, Eq)

///|
test "closing windows whose event time order differs from insertion order" {
  let result = Source::from_array([
      TimedSale::{ store: "north", amount: 2L, timestamp: 12L, },
      TimedSale::{ store: "north", amount: 3L, timestamp: 1L, },
      TimedSale::{ store: "north", amount: 5L, timestamp: 40L, },
    ])
    .stream()
    .key_by_fn(sale => sale.store)
    .tumbling_event_time(10L, 20L, sale => sale.timestamp)
    .aggregate(
      Aggregate::new(_ => 0L, (state, sale) => state + sale.amount, (key, total) => {
        KeyedSumUpdate::new(key, total)
      }),
    )
  assert_eq(result.records(), [
    KeyedSumUpdate::new("north", 3L),
    KeyedSumUpdate::new("north", 2L),
    KeyedSumUpdate::new("north", 5L),
  ])
}

///|
test "event-time windows use watermark and account for late records" {
  let source = Source::from_array([
    TimedSale::{ store: "north", amount: 2L, timestamp: 1L, },
    TimedSale::{ store: "north", amount: 3L, timestamp: 12L, },
    // This belongs to [0, 10), but watermark is already 10 here.
    TimedSale::{ store: "north", amount: 100L, timestamp: 2L, },
    TimedSale::{ store: "north", amount: 5L, timestamp: 23L, },
  ])
  let aggregate : Aggregate[TimedSale, Int64, KeyedSumUpdate] = Aggregate::new(
    _ => 0L,
    (state, sale) => state + sale.amount,
    (key, total) => KeyedSumUpdate::new(key, total),
  )
  let result = source
    .stream()
    .key_by_fn(sale => sale.store)
    .tumbling_event_time(10L, 2L, sale => sale.timestamp)
    .aggregate_with_metrics(aggregate)
  assert_eq(result.late_events(), 1)
  assert_eq(result.final_watermark(), 21L)
  assert_eq(result.output().records(), [
    KeyedSumUpdate::new("north", 2L),
    KeyedSumUpdate::new("north", 3L),
    KeyedSumUpdate::new("north", 5L),
  ])
}

///|
test "event-time windows accept bounded out of order input" {
  let values = Source::from_array([
      TimedSale::{ store: "north", amount: 3L, timestamp: 12L, },
      TimedSale::{ store: "north", amount: 2L, timestamp: 11L, },
    ])
    .stream()
    .key_by_fn(sale => sale.store)
    .tumbling_event_time(10L, 2L, sale => sale.timestamp)
    .aggregate(
      Aggregate::new(_ => 0L, (state, sale) => state + sale.amount, (key, total) => {
        KeyedSumUpdate::new(key, total)
      }),
    )
  assert_eq(values.records(), [KeyedSumUpdate::new("north", 5L)])
}

///|
test "interval join matches key and event-time bounds" {
  let left = Source::from_array([
    TimedSale::{ store: "north", amount: 1L, timestamp: 100L, },
    TimedSale::{ store: "south", amount: 2L, timestamp: 100L, },
  ]).stream()
  let right = Source::from_array([
    TimedSale::{ store: "north", amount: 10L, timestamp: 95L, },
    TimedSale::{ store: "north", amount: 20L, timestamp: 110L, },
    TimedSale::{ store: "north", amount: 30L, timestamp: 130L, },
    TimedSale::{ store: "south", amount: 40L, timestamp: 90L, },
  ]).stream()
  let joined = left.interval_join(
    right,
    sale => sale.store,
    sale => sale.store,
    sale => sale.timestamp,
    sale => sale.timestamp,
    IntervalJoinWindow::new(5L, 10L),
    (left, right) => left.amount + right.amount,
  )
  assert_eq(joined.records(), [11L, 21L])
}

///|
test "event-time and join windows reject invalid bounds" {
  try EventTimeWindow::tumbling(0L, 0L) catch {
    ApiError::InvalidEventTimeWindow(size_ms=0L, allowed_lateness_ms=0L) => ()
    _ => fail("expected invalid event-time window")
  } noraise {
    _ => fail("expected invalid event-time window")
  }
  try IntervalJoinWindow::new(-1L, 0L) catch {
    ApiError::InvalidJoinWindow(before_ms=-1L, after_ms=0L) => ()
    _ => fail("expected invalid interval join window")
  } noraise {
    _ => fail("expected invalid interval join window")
  }
}
