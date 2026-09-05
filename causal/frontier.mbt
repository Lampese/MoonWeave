///|
/// Maximal events in one causally admitted history.
///
/// The contained set is an antichain. Advancing a frontier may replace parent
/// IDs with descendants, so advancement is a causal relation rather than raw
/// set inclusion.
pub struct Frontier {
  ids : ReadOnlyArray[EventId]
} derive(Debug, Eq)

///|
pub fn Frontier::empty() -> Frontier {
  { ids: ReadOnlyArray::from_array([]), }
}

///|
fn Frontier::from_set(ids : Set[EventId]) -> Frontier {
  let sorted = ids.to_array()
  sorted.sort()
  { ids: ReadOnlyArray::from_array(sorted), }
}

///|
pub fn Frontier::ids(self : Frontier) -> ArrayView[EventId] {
  self.ids[:]
}

///|
pub fn Frontier::length(self : Frontier) -> Int {
  self.ids.length()
}

///|
pub fn Frontier::is_empty(self : Frontier) -> Bool {
  self.ids.is_empty()
}

///|
/// Return a deterministic semantic fingerprint of this causal frontier. It is
/// suitable for replay and certificates, but is not a cryptographic signature.
pub fn Frontier::digest(self : Frontier) -> String {
  let material = self.ids
    .map(id => id.origin() + "#" + id.sequence().to_string())
    .join(",")
  let mut hash : UInt64 = 14695981039346656037UL
  for byte in @utf8.encode(material) {
    hash = (hash ^ byte.to_uint64()) * 1099511628211UL
  }
  "frontier-" + hash.to_string(radix=16)
}
