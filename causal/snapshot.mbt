///|
pub(all) struct CausalEventSnapshot[T] {
  id : EventId
  parents : Array[EventId]
  key : String
  payload : T
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct CausalLogSnapshot[T] {
  admitted : Array[CausalEventSnapshot[T]]
  pending : Array[CausalEventSnapshot[T]]
  retired : Array[EventRange]
  maximum_pending : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
fn[T] event_snapshot(event : Event[T]) -> CausalEventSnapshot[T] {
  {
    id: event.id(),
    parents: event.parents().to_owned(),
    key: event.key(),
    payload: event.payload(),
  }
}

///|
fn[T] CausalEventSnapshot::event(
  self : CausalEventSnapshot[T],
) -> Event[T] raise CausalError {
  Event::new(self.id, self.parents, self.key, self.payload)
}

///|
pub fn[T] CausalLog::snapshot(self : CausalLog[T]) -> CausalLogSnapshot[T] {
  {
    admitted: self.events().map(event_snapshot),
    pending: self.pending_events().map(event_snapshot),
    retired: self.retired.copy(),
    maximum_pending: self.maximum_pending,
  }
}

///|
/// Restore admitted and pending histories separately. Sorting by identity is
/// not topological sorting, so restoration does not apply the live pending
/// budget to temporary reorderings of already-admitted records.
pub fn[T : Eq] CausalLog::restore(
  snapshot : CausalLogSnapshot[T],
) -> CausalLog[T] raise CausalError {
  if snapshot.maximum_pending < 0 {
    raise CausalError::InvalidPendingLimit(snapshot.maximum_pending)
  }
  let temporary_limit = if snapshot.maximum_pending > snapshot.admitted.length() {
    snapshot.maximum_pending
  } else {
    snapshot.admitted.length()
  }
  let log : CausalLog[T] = CausalLog::new(maximum_pending=temporary_limit)
  log.compact(CausalRetirement::new("restore", snapshot.retired))
  for record in snapshot.admitted {
    if log.submit(record.event()).status() == Duplicate {
      raise CausalError::InvalidRetirement(
        "duplicate admitted snapshot identity",
      )
    }
  }
  if log.pending_count() != 0 {
    raise CausalError::InvalidRetirement(
      "snapshot contains an unproved admission",
    )
  }
  let log = { ..log, maximum_pending: snapshot.maximum_pending, }
  for record in snapshot.pending {
    if log.submit(record.event()).status() != Pending {
      raise CausalError::InvalidRetirement(
        "snapshot pending identity is not pending",
      )
    }
  }
  log
}
