///|
/// An immutable causal event. Payload values are treated as immutable by the
/// causal core; mutable application state must not be stored directly here.
pub struct Event[T] {
  id : EventId
  parents : ReadOnlyArray[EventId]
  key : String
  payload : T
} derive(Debug, Eq)

///|
/// Construct an event and defensively copy its parent identities.
pub fn[T] Event::new(
  id : EventId,
  parents : ArrayView[EventId],
  key : String,
  payload : T,
) -> Event[T] raise CausalError {
  if key.is_empty() {
    raise CausalError::EmptyKey
  }
  let canonical_id = EventId::new(id.origin(), id.sequence())
  let seen : Set[EventId] = Set([])
  let canonical_parents = []
  for parent in parents {
    let canonical_parent = EventId::new(parent.origin(), parent.sequence())
    if canonical_parent == canonical_id {
      raise CausalError::SelfDependency(canonical_id)
    }
    if !seen.add_and_check(canonical_parent) {
      raise CausalError::DuplicateParent(canonical_parent)
    }
    canonical_parents.push(canonical_parent)
  }
  canonical_parents.sort()
  {
    id: canonical_id,
    parents: ReadOnlyArray::from_array(canonical_parents),
    key,
    payload,
  }
}

///|
pub fn[T] Event::id(self : Event[T]) -> EventId {
  self.id
}

///|
pub fn[T] Event::parents(self : Event[T]) -> ArrayView[EventId] {
  self.parents[:]
}

///|
pub fn[T] Event::key(self : Event[T]) -> String {
  self.key
}

///|
pub fn[T] Event::payload(self : Event[T]) -> T {
  self.payload
}
