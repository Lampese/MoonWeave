///|
/// Immediate disposition of the event passed to `CausalLog::submit`.
pub(all) enum SubmitStatus {
  Admitted
  Pending
  Duplicate
} derive(Debug, Eq)

///|
/// Result of submitting one event. `admitted` also includes pending descendants
/// which became dependency-ready because of this submission.
pub struct Admission[T] {
  status : SubmitStatus
  admitted : ReadOnlyArray[Event[T]]
  missing : ReadOnlyArray[EventId]
} derive(Debug)

///|
pub fn[T] Admission::status(self : Admission[T]) -> SubmitStatus {
  self.status
}

///|
pub fn[T] Admission::admitted(self : Admission[T]) -> ArrayView[Event[T]] {
  self.admitted[:]
}

///|
pub fn[T] Admission::missing(self : Admission[T]) -> ArrayView[EventId] {
  self.missing[:]
}

///|
/// In-memory causal admission state. It deliberately contains no transport,
/// clock, filesystem, or process concerns.
pub struct CausalLog[T] {
  admitted : Map[EventId, Event[T]]
  pending : Map[EventId, Event[T]]
  frontier : Set[EventId]
  maximum_pending : Int
}

///|
pub fn[T] CausalLog::new(
  maximum_pending? : Int = 10000,
) -> CausalLog[T] raise CausalError {
  if maximum_pending < 0 {
    raise CausalError::InvalidPendingLimit(maximum_pending)
  }
  { admitted: {}, pending: {}, frontier: Set([]), maximum_pending, }
}

///|
pub fn[T] CausalLog::admitted_count(self : CausalLog[T]) -> Int {
  self.admitted.length()
}

///|
pub fn[T] CausalLog::pending_count(self : CausalLog[T]) -> Int {
  self.pending.length()
}

///|
pub fn[T] CausalLog::contains(self : CausalLog[T], id : EventId) -> Bool {
  self.admitted.contains(id)
}

///|
pub fn[T] CausalLog::event(self : CausalLog[T], id : EventId) -> Event[T]? {
  self.admitted.get(id)
}

///|
pub fn[T] CausalLog::frontier(self : CausalLog[T]) -> Frontier {
  Frontier::from_set(self.frontier.copy())
}

///|
pub fn[T] CausalLog::events(self : CausalLog[T]) -> Array[Event[T]] {
  let events = self.admitted.values().to_array()
  events.sort_by((left, right) => Compare::compare(left.id(), right.id()))
  events
}

///|
/// Return events which have been received but are still waiting for one or
/// more causal parents. Snapshot consumers use this alongside `events()` to
/// preserve the complete admission state across a restart.
pub fn[T] CausalLog::pending_events(self : CausalLog[T]) -> Array[Event[T]] {
  let events = self.pending.values().to_array()
  events.sort_by((left, right) => Compare::compare(left.id(), right.id()))
  events
}

///|
pub fn[T] CausalLog::pending_ids(self : CausalLog[T]) -> Array[EventId] {
  let ids = self.pending.keys().to_array()
  ids.sort()
  ids
}

///|
/// Copy admission state for a transactional operator preview. Events are
/// immutable values, while maps and the frontier set are copied so a failed
/// validation cannot mutate the original log.
pub fn[T] CausalLog::copy(self : CausalLog[T]) -> CausalLog[T] {
  {
    admitted: Map(self.admitted.to_array()),
    pending: Map(self.pending.to_array()),
    frontier: self.frontier.copy(),
    maximum_pending: self.maximum_pending,
  }
}

///|
fn[T] CausalLog::missing_parents(
  self : CausalLog[T],
  event : Event[T],
) -> Array[EventId] {
  let missing = event
    .parents()
    .filter(parent => !self.admitted.contains(parent))
  missing.sort()
  missing
}

///|
fn[T] CausalLog::admit_one(
  self : CausalLog[T],
  event : Event[T],
  newly_admitted : Array[Event[T]],
) -> Unit {
  for parent in event.parents() {
    self.frontier.remove(parent)
  }
  self.frontier.add(event.id())
  self.admitted[event.id()] = event
  newly_admitted.push(event)
}

///|
fn[T] CausalLog::drain_ready(
  self : CausalLog[T],
  newly_admitted : Array[Event[T]],
) -> Unit {
  for ;; {
    let ready = self.pending
      .values()
      .filter(event => {
        event.parents().all(parent => self.admitted.contains(parent))
      })
      .to_array()
    ready.sort_by((left, right) => Compare::compare(left.id(), right.id()))
    if ready.is_empty() {
      break
    }
    for event in ready {
      self.pending.remove(event.id())
      self.admit_one(event, newly_admitted)
    }
  }
}

///|
/// Submit an event using at-least-once-friendly semantics.
///
/// Equal retransmissions are duplicates. Reusing an identity for different
/// content is a protocol error. Events with missing parents remain pending and
/// are admitted automatically once their causal dependencies arrive.
pub fn[T : Eq] CausalLog::submit(
  self : CausalLog[T],
  event : Event[T],
) -> Admission[T] raise CausalError {
  let id = event.id()
  match self.admitted.get(id) {
    Some(existing) =>
      if existing == event {
        return {
          status: Duplicate,
          admitted: ReadOnlyArray::from_array([]),
          missing: ReadOnlyArray::from_array([]),
        }
      } else {
        raise CausalError::IdentityConflict(id)
      }
    None => ()
  }
  match self.pending.get(id) {
    Some(existing) =>
      if existing == event {
        return {
          status: Duplicate,
          admitted: ReadOnlyArray::from_array([]),
          missing: ReadOnlyArray::from_array(self.missing_parents(event)),
        }
      } else {
        raise CausalError::IdentityConflict(id)
      }
    None => ()
  }
  let missing = self.missing_parents(event)
  if !missing.is_empty() {
    if self.pending.length() >= self.maximum_pending {
      raise CausalError::PendingLimitExceeded(limit=self.maximum_pending)
    }
    self.pending[id] = event
    return {
      status: Pending,
      admitted: ReadOnlyArray::from_array([]),
      missing: ReadOnlyArray::from_array(missing),
    }
  }
  let newly_admitted = []
  self.admit_one(event, newly_admitted)
  self.drain_ready(newly_admitted)
  {
    status: Admitted,
    admitted: ReadOnlyArray::from_array(newly_admitted),
    missing: ReadOnlyArray::from_array([]),
  }
}

///|
fn[T] CausalLog::is_ancestor_or_equal(
  self : CausalLog[T],
  ancestor : EventId,
  descendant : EventId,
) -> Bool {
  if ancestor == descendant {
    return self.admitted.contains(ancestor)
  }
  let stack = [descendant]
  let visited : Set[EventId] = Set([])
  while stack.pop() is Some(current) {
    if !visited.add_and_check(current) {
      continue
    }
    match self.admitted.get(current) {
      None => ()
      Some(event) =>
        for parent in event.parents() {
          if parent == ancestor {
            return true
          }
          stack.push(parent)
        }
    }
  }
  false
}

///|
/// Return whether `newer` is causally at or after every event in `older`.
pub fn[T] CausalLog::frontier_covers(
  self : CausalLog[T],
  newer : Frontier,
  older : Frontier,
) -> Bool {
  older
  .ids()
  .all(old_id => {
    newer.ids().any(new_id => self.is_ancestor_or_equal(old_id, new_id))
  })
}
