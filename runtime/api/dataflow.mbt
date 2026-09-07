///|
/// A finite local source. Source construction is pure; external connectors
/// will be added as asynchronous adapters around this type.
pub struct Source[T] {
  records : Array[T]
}

///|
pub fn[T] Source::from_array(records : ArrayView[T]) -> Source[T] {
  { records: records.to_owned(), }
}

///|
pub fn[T] Source::records(self : Source[T]) -> ArrayView[T] {
  self.records[:]
}

///|
pub fn[T] Source::stream(self : Source[T]) -> Stream[T] {
  { records: self.records.copy(), }
}

///|
pub fn[T] Source::run(self : Source[T], sink : Sink[T]) -> RunHandle[T] {
  self.stream().run(sink)
}

///|
/// A local finite stream. Transformations are ordinary local functions and are
/// deliberately not treated as deployable cluster artifacts.
pub struct Stream[T] {
  records : Array[T]
}

///|
pub fn[T] Stream::records(self : Stream[T]) -> ArrayView[T] {
  self.records[:]
}

///|
pub fn[T, U] Stream::map(self : Stream[T], transform : (T) -> U) -> Stream[U] {
  { records: self.records.map(transform), }
}

///|
pub fn[T] Stream::filter(
  self : Stream[T],
  predicate : (T) -> Bool,
) -> Stream[T] {
  { records: self.records.filter(predicate), }
}

///|
pub struct KeyBy[T] {
  selector : (T) -> String
}

///|
pub fn[T] KeyBy::new(selector : (T) -> String) -> KeyBy[T] {
  { selector, }
}

///|
pub fn[T] Stream::key_by(
  self : Stream[T],
  key_by : KeyBy[T],
) -> KeyedStream[T] raise ApiError {
  let records : Array[(String, T)] = []
  for record in self.records {
    let key = (key_by.selector)(record)
    if key.is_empty() {
      raise ApiError::EmptyKey
    }
    records.push((key, record))
  }
  { records, }
}

///|
pub fn[T] Stream::key_by_fn(
  self : Stream[T],
  selector : (T) -> String,
) -> KeyedStream[T] raise ApiError {
  self.key_by(KeyBy::new(selector))
}

///|
pub struct KeyedStream[T] {
  records : Array[(String, T)]
}

///|
pub fn[T] KeyedStream::records(self : KeyedStream[T]) -> ArrayView[(String, T)] {
  self.records[:]
}

///|
pub struct Aggregate[T, S, U] {
  initial : (String) -> S
  add : (S, T) -> S
  emit : (String, S) -> U
}

///|
pub fn[T, S, U] Aggregate::new(
  initial : (String) -> S,
  add : (S, T) -> S,
  emit : (String, S) -> U,
) -> Aggregate[T, S, U] {
  { initial, add, emit, }
}

///|
fn[T, S, U] aggregate_records(
  records : ArrayView[(String, T)],
  aggregate : Aggregate[T, S, U],
) -> Array[U] {
  let states : Map[String, S] = Map([])
  let outputs : Array[U] = []
  for item in records {
    let (key, value) = item
    let state = match states.get(key) {
      Some(previous) => (aggregate.add)(previous, value)
      None => (aggregate.add)((aggregate.initial)(key), value)
    }
    states[key] = state
    outputs.push((aggregate.emit)(key, state))
  }
  outputs
}

///|
pub fn[T, S, U] KeyedStream::aggregate(
  self : KeyedStream[T],
  aggregate : Aggregate[T, S, U],
) -> Stream[U] {
  { records: aggregate_records(self.records[:], aggregate), }
}

///|
pub(all) enum Window {
  Global
  Count(Int)
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// Create a global or count-based local window. Event-time windows and
/// watermarks belong to the future streaming source layer.
pub fn Window::global() -> Window {
  Window::Global
}

///|
/// Count windows are keyed and flush a final partial window for finite local
/// sources after all input records have been processed.
pub fn Window::count(size : Int) -> Window raise ApiError {
  if size <= 0 {
    raise ApiError::InvalidWindowSize(size)
  }
  Window::Count(size)
}

///|
pub struct WindowedKeyedStream[T] {
  records : Array[(String, T)]
  window : Window
}

///|
pub fn[T] KeyedStream::window(
  self : KeyedStream[T],
  window : Window,
) -> WindowedKeyedStream[T] {
  { records: self.records.copy(), window, }
}

///|
fn[T, S, U] windowed_aggregate_records(
  records : ArrayView[(String, T)],
  window : Window,
  aggregate : Aggregate[T, S, U],
) -> Array[U] {
  match window {
    Window::Global => aggregate_records(records, aggregate)
    Window::Count(size) => {
      let states : Map[String, S] = Map([])
      let counts : Map[String, Int] = Map([])
      let seen_keys : Set[String] = Set([])
      let key_order : Array[String] = []
      let outputs : Array[U] = []
      for item in records {
        let (key, value) = item
        if !states.contains(key) {
          states[key] = (aggregate.initial)(key)
          counts[key] = 0
          if seen_keys.add_and_check(key) {
            key_order.push(key)
          }
        }
        let state = (aggregate.add)(states[key], value)
        let count = counts[key] + 1
        states[key] = state
        counts[key] = count
        if count == size {
          outputs.push((aggregate.emit)(key, state))
          states.remove(key)
          counts.remove(key)
        }
      }
      // Bounded local streams flush a final partial window for each key.
      for key in key_order {
        match states.get(key) {
          Some(state) => outputs.push((aggregate.emit)(key, state))
          None => ()
        }
      }
      outputs
    }
  }
}

///|
pub fn[T, S, U] WindowedKeyedStream::aggregate(
  self : WindowedKeyedStream[T],
  aggregate : Aggregate[T, S, U],
) -> Stream[U] {
  {
    records: windowed_aggregate_records(self.records[:], self.window, aggregate),
  }
}

///|
pub(all) enum SinkMode {
  Collect
  Discard
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub struct Sink[T] {
  mode : SinkMode
  phantom : T?
}

///|
pub fn[T] Sink::collect() -> Sink[T] {
  { mode: SinkMode::Collect, phantom: None, }
}

///|
pub fn[T] Sink::discard() -> Sink[T] {
  { mode: SinkMode::Discard, phantom: None, }
}

///|
pub(all) enum RunStatus {
  Created
  Running
  Succeeded
  Closed
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// A completed local run handle. This is intentionally synchronous because the
/// local transforms and in-memory sinks perform no I/O.
pub struct RunHandle[T] {
  mut status : RunStatus
  outputs : Array[T]
}

///|
pub fn[T] Stream::run(self : Stream[T], sink : Sink[T]) -> RunHandle[T] {
  let outputs = match sink.mode {
    SinkMode::Collect => self.records.copy()
    SinkMode::Discard => []
  }
  { status: RunStatus::Succeeded, outputs, }
}

///|
pub fn[T] Stream::collect(self : Stream[T]) -> RunHandle[T] {
  self.run(Sink::collect())
}

///|
pub fn[T] Stream::discard(self : Stream[T]) -> RunHandle[T] {
  self.run(Sink::discard())
}

///|
pub fn[T] RunHandle::status(self : RunHandle[T]) -> RunStatus {
  self.status
}

///|
pub fn[T] RunHandle::await_completion(self : RunHandle[T]) -> RunStatus {
  self.status
}

///|
pub fn[T] RunHandle::outputs(self : RunHandle[T]) -> ArrayView[T] {
  self.outputs[:]
}

///|
pub fn[T] RunHandle::close(self : RunHandle[T]) -> Unit {
  self.status = RunStatus::Closed
}
