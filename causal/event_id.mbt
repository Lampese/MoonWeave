///|
/// Stable identity of an event within one origin's append-only sequence.
pub struct EventId {
  origin : String
  sequence : Int
} derive(Debug, Eq, Compare, Hash, ToJson, FromJson)

///|
/// Construct a validated event identity.
pub fn EventId::new(
  origin : String,
  sequence : Int,
) -> EventId raise CausalError {
  if origin.is_empty() {
    raise CausalError::EmptyOrigin
  }
  if sequence < 0 {
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
pub fn EventId::sequence(self : EventId) -> Int {
  self.sequence
}
