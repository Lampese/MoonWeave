///|
/// A tumbling event-time window for finite local streams.
///
/// The watermark is `max_seen_timestamp - allowed_lateness_ms`. A bucket is
/// emitted once its end is at or before that watermark. Records arriving after
/// their bucket has been emitted are late and are excluded from the result.
pub(all) struct EventTimeWindow {
  size_ms : Int64
  allowed_lateness_ms : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn EventTimeWindow::tumbling(
  size_ms : Int64,
  allowed_lateness_ms : Int64,
) -> EventTimeWindow raise ApiError {
  if size_ms <= 0L || allowed_lateness_ms < 0L {
    raise ApiError::InvalidEventTimeWindow(size_ms~, allowed_lateness_ms~)
  }
  { size_ms, allowed_lateness_ms, }
}

///|
pub fn EventTimeWindow::size_ms(self : EventTimeWindow) -> Int64 {
  self.size_ms
}

///|
pub fn EventTimeWindow::allowed_lateness_ms(self : EventTimeWindow) -> Int64 {
  self.allowed_lateness_ms
}

///|
/// A bounded event-time interval for a stream join. For a left record at time
/// `t`, a right record is accepted in `[t - before_ms, t + after_ms]`.
pub(all) struct IntervalJoinWindow {
  before_ms : Int64
  after_ms : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn IntervalJoinWindow::new(
  before_ms : Int64,
  after_ms : Int64,
) -> IntervalJoinWindow raise ApiError {
  if before_ms < 0L || after_ms < 0L {
    raise ApiError::InvalidJoinWindow(before_ms~, after_ms~)
  }
  { before_ms, after_ms, }
}

///|
pub fn IntervalJoinWindow::before_ms(self : IntervalJoinWindow) -> Int64 {
  self.before_ms
}

///|
pub fn IntervalJoinWindow::after_ms(self : IntervalJoinWindow) -> Int64 {
  self.after_ms
}

///|
/// Metrics returned by an event-time aggregation. The stream contains only
/// on-time window results; late records are counted for observability.
pub struct EventTimeAggregateResult[U] {
  output : Stream[U]
  late_events : Int
  final_watermark : Int64
}

///|
pub fn[U] EventTimeAggregateResult::output(
  self : EventTimeAggregateResult[U],
) -> Stream[U] {
  self.output
}

///|
pub fn[U] EventTimeAggregateResult::late_events(
  self : EventTimeAggregateResult[U],
) -> Int {
  self.late_events
}

///|
pub fn[U] EventTimeAggregateResult::final_watermark(
  self : EventTimeAggregateResult[U],
) -> Int64 {
  self.final_watermark
}

///|
pub struct EventTimeWindowedKeyedStream[T] {
  records : Array[(String, T)]
  window : EventTimeWindow
  timestamp : (T) -> Int64
}

///|
pub fn[T] KeyedStream::window_event_time(
  self : KeyedStream[T],
  window : EventTimeWindow,
  timestamp : (T) -> Int64,
) -> EventTimeWindowedKeyedStream[T] {
  { records: self.records.copy(), window, timestamp, }
}

///|
pub fn[T] KeyedStream::tumbling_event_time(
  self : KeyedStream[T],
  size_ms : Int64,
  allowed_lateness_ms : Int64,
  timestamp : (T) -> Int64,
) -> EventTimeWindowedKeyedStream[T] raise ApiError {
  self.window_event_time(
    EventTimeWindow::tumbling(size_ms, allowed_lateness_ms),
    timestamp,
  )
}

///|
priv struct EventTimeBucket[S] {
  key : String
  start_ms : Int64
  mut state : S
}

///|
fn[S] event_time_bucket_index(
  buckets : Array[EventTimeBucket[S]],
  key : String,
  start_ms : Int64,
) -> Int? {
  for index in 0..<buckets.length() {
    let bucket = buckets[index]
    if bucket.key == key && bucket.start_ms == start_ms {
      return Some(index)
    }
  }
  None
}

///|
fn[S] event_time_ready_indices(
  buckets : Array[EventTimeBucket[S]],
  window_size_ms : Int64,
  watermark : Int64,
) -> Array[Int] {
  let ready : Array[Int] = []
  for index in 0..<buckets.length() {
    if buckets[index].start_ms + window_size_ms <= watermark {
      ready.push(index)
    }
  }
  // Stable insertion order is deterministic for equal timestamps and keys.
  ready.sort_by((left, right) => {
    let left_bucket = buckets[left]
    let right_bucket = buckets[right]
    if left_bucket.start_ms < right_bucket.start_ms {
      -1
    } else if left_bucket.start_ms > right_bucket.start_ms {
      1
    } else {
      left_bucket.key.compare(right_bucket.key)
    }
  })
  ready
}

///|
fn[T, S, U] event_time_aggregate_records(
  records : ArrayView[(String, T)],
  window : EventTimeWindow,
  timestamp : (T) -> Int64,
  aggregate : Aggregate[T, S, U],
) -> EventTimeAggregateResult[U] raise ApiError {
  let buckets : Array[EventTimeBucket[S]] = []
  let outputs : Array[U] = []
  let mut max_seen : Int64? = None
  let mut late_events = 0
  for item in records {
    let (key, value) = item
    let event_timestamp = timestamp(value)
    if event_timestamp < 0L {
      raise ApiError::InvalidEventTimestamp(event_timestamp)
    }
    let next_max = match max_seen {
      Some(previous) =>
        if event_timestamp > previous {
          event_timestamp
        } else {
          previous
        }
      None => event_timestamp
    }
    max_seen = Some(next_max)
    let watermark = next_max - window.allowed_lateness_ms
    let start_ms = event_timestamp / window.size_ms * window.size_ms
    if start_ms + window.size_ms <= watermark {
      late_events += 1
      continue
    }
    let bucket_index = match event_time_bucket_index(buckets, key, start_ms) {
      Some(index) => index
      None => {
        buckets.push({ key, start_ms, state: (aggregate.initial)(key), })
        buckets.length() - 1
      }
    }
    let state = (aggregate.add)(buckets[bucket_index].state, value)
    buckets[bucket_index].state = state
    let ready = event_time_ready_indices(buckets, window.size_ms, watermark)
    let ready_values : Array[(String, S)] = []
    for ready_index in ready {
      let bucket = buckets[ready_index]
      ready_values.push((bucket.key, bucket.state))
    }
    // Remove from the end so all indices remain valid while emitting in the
    // deterministic event-time order selected above.
    let removals = ready.copy()
    removals.sort()
    for position in 0..<removals.length() {
      let ready_index = removals[removals.length() - 1 - position]
      ignore(buckets.remove(ready_index))
    }
    for (key, state) in ready_values {
      outputs.push((aggregate.emit)(key, state))
    }
  }
  let final_watermark = match max_seen {
    Some(value) => value - window.allowed_lateness_ms
    None => 0L
  }
  // End-of-input is an explicit finite-stream watermark. It flushes every
  // retained bucket, including the final partial event-time windows.
  let final_ready = event_time_ready_indices(
    buckets,
    window.size_ms,
    9223372036854775807L,
  )
  let final_values : Array[(String, S)] = []
  for ready_index in final_ready {
    let bucket = buckets[ready_index]
    final_values.push((bucket.key, bucket.state))
  }
  for (key, state) in final_values {
    outputs.push((aggregate.emit)(key, state))
  }
  { output: { records: outputs, }, late_events, final_watermark, }
}

///|
pub fn[T, S, U] EventTimeWindowedKeyedStream::aggregate(
  self : EventTimeWindowedKeyedStream[T],
  aggregate : Aggregate[T, S, U],
) -> Stream[U] raise ApiError {
  event_time_aggregate_records(
    self.records[:],
    self.window,
    self.timestamp,
    aggregate,
  ).output()
}

///|
pub fn[T, S, U] EventTimeWindowedKeyedStream::aggregate_with_metrics(
  self : EventTimeWindowedKeyedStream[T],
  aggregate : Aggregate[T, S, U],
) -> EventTimeAggregateResult[U] raise ApiError {
  event_time_aggregate_records(
    self.records[:],
    self.window,
    self.timestamp,
    aggregate,
  )
}

///|
fn interval_join_match(
  left_timestamp : Int64,
  right_timestamp : Int64,
  window : IntervalJoinWindow,
) -> Bool {
  if right_timestamp >= left_timestamp {
    right_timestamp - left_timestamp <= window.after_ms
  } else {
    left_timestamp - right_timestamp <= window.before_ms
  }
}

///|
/// Join two finite local streams by key and event-time interval. The result is
/// deterministic in left input order, then right input order. The interval is
/// bounded, so an asynchronous adapter can use it as the state-retention
/// contract when wiring this operation into a live runner.
pub fn[T, U, V] Stream::interval_join(
  self : Stream[T],
  other : Stream[U],
  left_key : (T) -> String,
  right_key : (U) -> String,
  left_timestamp : (T) -> Int64,
  right_timestamp : (U) -> Int64,
  window : IntervalJoinWindow,
  combine : (T, U) -> V,
) -> Stream[V] raise ApiError {
  let outputs : Array[V] = []
  for left in self.records {
    let left_join_key = left_key(left)
    if left_join_key.is_empty() {
      raise ApiError::EmptyKey
    }
    let left_time = left_timestamp(left)
    if left_time < 0L {
      raise ApiError::InvalidEventTimestamp(left_time)
    }
    for right in other.records {
      let right_join_key = right_key(right)
      if right_join_key.is_empty() {
        raise ApiError::EmptyKey
      }
      let right_time = right_timestamp(right)
      if right_time < 0L {
        raise ApiError::InvalidEventTimestamp(right_time)
      }
      if left_join_key == right_join_key &&
        interval_join_match(left_time, right_time, window) {
        outputs.push(combine(left, right))
      }
    }
  }
  { records: outputs, }
}
