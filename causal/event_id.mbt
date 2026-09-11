///|
/// Stable identity of an event within one origin's append-only sequence.
/// Sequences use Int64 throughout the engine. JSON encodes them as decimal
/// strings, preserving exact identities even above the 2^53 numeric boundary.
pub struct EventId {
  origin : String
  sequence : Int64
} derive(Debug, Eq, Compare, Hash, ToJson, FromJson)

///|
/// Construct a validated event identity.
///
/// ```mbt check
/// test {
///   let id = EventId::new("kafka/orders/0", 9007199254740993L)
///   assert_eq(id.sequence(), 9007199254740993L)
/// }
/// ```
pub fn EventId::new(
  origin : String,
  sequence : Int64,
) -> EventId raise CausalError {
  if origin.is_empty() {
    raise CausalError::EmptyOrigin
  }
  if sequence < 0L {
    raise CausalError::NegativeSequence(sequence)
  }
  { origin, sequence, }
}

///|
/// Return the origin which allocated this identity.
pub fn EventId::origin(self : EventId) -> String {
  self.origin
}

///|
/// Return the per-origin sequence number.
pub fn EventId::sequence(self : EventId) -> Int64 {
  self.sequence
}
