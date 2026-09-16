///|
/// A separately polled source bound to one logical operator input.
pub(all) struct NamedSource {
  input_id : String
  source : SourceSpec
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn NamedSource::new(
  input_id : String,
  source : SourceSpec,
) -> NamedSource raise ApiError {
  if input_id.is_empty() || source is SourceSpec::Inputs(_) {
    raise ApiError::Source(
      "named inputs require a nonempty identity and a leaf source",
    )
  }
  validate_source(source)
  { input_id, source, }
}

///|
/// Bind independently consumed inputs, for example left and right Kafka
/// topics for an interval join. Each source retains its own commit cursor.
pub fn SourceSpec::inputs(
  inputs : ArrayView[NamedSource],
) -> SourceSpec raise ApiError {
  let source = SourceSpec::Inputs(inputs.to_owned())
  validate_source(source)
  source
}

///|
fn SourceSpec::has_kafka(self : SourceSpec) -> Bool {
  match self {
    Kafka(_) => true
    Inputs(inputs) => inputs.any(input => input.source.has_kafka())
    _ => false
  }
}

///|
fn SourceSpec::has_external(self : SourceSpec) -> Bool {
  match self {
    Events => false
    Inputs(inputs) => inputs.any(input => input.source.has_external())
    _ => true
  }
}

///|
fn SourceSpec::json_lines_inputs(self : SourceSpec) -> Array[(String?, String)] {
  match self {
    JsonLines(path) => [(None, path)]
    Inputs(inputs) => {
      let result = []
      for input in inputs {
        if input.source is JsonLines(path) {
          result.push((Some(input.input_id), path))
        }
      }
      result
    }
    _ => []
  }
}

///|
fn SourceSpec::watermark_input_ids(self : SourceSpec) -> Array[String] {
  match self {
    Inputs(inputs) => inputs.map(input => input.input_id)
    _ => ["main"]
  }
}

///|
fn SourceSpec::accepts_driver_input(
  self : SourceSpec,
  input_id : String,
) -> Bool {
  match self {
    Kafka(_) => false
    Inputs(inputs) =>
      inputs.any(input => input.input_id == input_id && input.source is Events)
    _ => true
  }
}

///|
fn SourceSpec::kafka_inputs(
  self : SourceSpec,
) -> Array[(String?, KafkaSourceSpec)] {
  match self {
    Kafka(spec) => [(None, spec)]
    Inputs(inputs) => {
      let result = []
      for input in inputs {
        if input.source is Kafka(spec) {
          result.push((Some(input.input_id), spec))
        }
      }
      result
    }
    _ => []
  }
}

///|
fn Event::bind_input(self : Event, input_id : String) -> Event raise ApiError {
  let record = @protocol.EventRecord::new(
    self.id(),
    self.parents(),
    self.key(),
    self.payload(),
    event_time?=self.event_time(),
    input_id~,
  ) catch {
    error => raise ApiError::InvalidEvent(@debug.to_string(error))
  }
  { record, }
}
