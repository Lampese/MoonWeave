///|
/// Errors raised while decoding or applying a keyed numeric contribution.
pub(all) suberror AggregationError {
  EmptyKey
  MalformedPayload
  Overflow(String)
  Causal(@causal.CausalError)
} derive(Debug, Eq)

///|
/// JSON input accepted by the keyed-sum operator.
pub(all) struct SumPayload {
  value : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
/// A contribution after the event key has been attached to its numeric value.
pub(all) struct Contribution {
  key : String
  value : Int64
} derive(Debug, Eq)

///|
pub fn Contribution::new(
  key : String,
  value : Int64,
) -> Contribution raise AggregationError {
  if key.is_empty() {
    raise AggregationError::EmptyKey
  }
  { key, value, }
}

///|
pub fn Contribution::key(self : Contribution) -> String {
  self.key
}

///|
pub fn Contribution::value(self : Contribution) -> Int64 {
  self.value
}

///|
pub fn Contribution::to_json(self : Contribution) -> Json {
  @json.to_json(SumPayload::{ value: self.value, })
}

///|
pub fn Contribution::from_json(
  key : String,
  payload : Json,
) -> Contribution raise AggregationError {
  let value : SumPayload = @json.from_json(payload) catch {
    _ => raise AggregationError::MalformedPayload
  }
  Contribution::new(key, value.value)
}

///|
/// One emitted running total. A new value is emitted for every admitted input
/// so downstream consumers can reconstruct the exact update sequence.
pub(all) struct KeyedSum {
  key : String
  total : Int64
} derive(Debug, Eq, Compare, Hash, ToJson, FromJson)

///|
pub fn KeyedSum::new(key : String, total : Int64) -> KeyedSum {
  { key, total, }
}

///|
pub fn KeyedSum::key(self : KeyedSum) -> String {
  self.key
}

///|
pub fn KeyedSum::total(self : KeyedSum) -> Int64 {
  self.total
}
