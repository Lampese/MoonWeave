///|
pub using @task {type WindowedSum}

///|
/// Attach event time and a logical input identity before submitting an event.
/// Window inputs use `main`; interval joins use `left` and `right`.
///
/// ```mbt check
/// test {
///   let event = Event::keyed_sum(@causal.EventId::new("sales", 0L), "a", 2L).with_event_time(
///     100L,
///   )
///   assert_eq(event.event_time(), Some(100L))
/// }
/// ```
pub fn Event::with_event_time(
  self : Event,
  timestamp : Int64,
  input_id? : String = "main",
) -> Event raise ApiError {
  let record = @protocol.EventRecord::new(
    self.record.id(),
    self.record.parents(),
    self.record.key(),
    self.record.payload(),
    event_time=timestamp,
    input_id~,
  ) catch {
    error => raise ApiError::InvalidEvent(@debug.to_string(error))
  }
  { record, }
}

///|
pub fn Event::event_time(self : Event) -> Int64? {
  self.record.event_time()
}

///|
pub fn Event::input_id(self : Event) -> String {
  self.record.input_id()
}

///|
pub fn Output::as_windowed_sum(self : Output) -> WindowedSum? {
  match self {
    WindowedSum(value) => Some(value)
    _ => None
  }
}

///|
/// Exclude an input from watermark coordination after it has been idle this
/// long. Zero keeps all declared inputs active. A reactivated input follows
/// the same late-event policy as every other input.
pub fn JobDefinition::with_watermark_idle_timeout(
  self : JobDefinition,
  timeout_ms : Int64,
) -> JobDefinition raise ApiError {
  if timeout_ms < 0L {
    raise ApiError::InvalidEvent("negative idle timeout")
  }
  { ..self, watermark_idle_timeout_ms: Some(timeout_ms), }
}

///|
fn CompiledJob::validate_event(
  self : CompiledJob,
  event : Event,
) -> Unit raise ApiError {
  if self.is_graph() {
    let root = self.graph_root(event.input_id())
    let stage = self.graph_stages.filter(item => item.0 == root.id)[0].1
    stage.validate_event(event.bind_input("main"))
    return
  }
  if self.definition.graph_input is Some(_) &&
    @task.is_graph_skip(event.payload()) {
    return
  }
  match self.definition.event_time_config {
    None => self.descriptor.validate_event(event)
    Some(config) => {
      guard event.event_time() is Some(timestamp) else {
        raise ApiError::InvalidEvent("event-time input requires a timestamp")
      }
      if timestamp < 0L || timestamp == 9223372036854775807L {
        raise ApiError::InvalidEventTimestamp(timestamp)
      }
      if config.window_size_ms() > 0L {
        if event.input_id() != "main" {
          raise ApiError::InvalidEvent("window input must be main")
        }
        self.descriptor.validate_event(event)
      } else if event.input_id() != "left" && event.input_id() != "right" {
        raise ApiError::InvalidEvent("join input must be left or right")
      }
    }
  }
}

///|
/// Advance the event-time frontier of all local partitions and return newly
/// completed results. Causally pending records may hold a partition back.
pub fn LocalRun::advance_watermark(
  self : LocalRun,
  timestamp : Int64,
) -> Array[Output] raise ApiError {
  if self.closed {
    raise ApiError::Closed
  }
  let result : Array[Output] = []
  let dispatches = self.task_set.advance_watermark(timestamp) catch {
    error => raise ApiError::TaskFailed(@debug.to_string(error))
  }
  for dispatch in dispatches {
    for output in dispatch.outputs() {
      result.push(local_output(output))
    }
  }
  result
}
