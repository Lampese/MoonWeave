///|
/// An inclusive interval of immutable identities from one origin. Holes are
/// represented explicitly; a high sequence alone never proves a prefix.
pub struct EventRange {
  origin : String
  first : Int64
  last : Int64
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn EventRange::new(
  origin : String,
  first : Int64,
  last : Int64,
) -> EventRange raise CausalError {
  ignore(EventId::new(origin, first))
  if last < first {
    raise CausalError::InvalidRetirement("inverted event interval")
  }
  { origin, first, last, }
}

///|
pub fn EventRange::origin(self : EventRange) -> String {
  self.origin
}

///|
pub fn EventRange::first(self : EventRange) -> Int64 {
  self.first
}

///|
pub fn EventRange::last(self : EventRange) -> Int64 {
  self.last
}

///|
pub fn EventRange::contains(self : EventRange, id : EventId) -> Bool {
  self.origin == id.origin() &&
  self.first <= id.sequence() &&
  id.sequence() <= self.last
}

///|
fn normalize_event_ranges(
  ranges : ArrayView[EventRange],
) -> Array[EventRange] raise CausalError {
  let sorted = ranges.map(range => {
    EventRange::new(range.origin, range.first, range.last)
  })
  sorted.sort()
  let result : Array[EventRange] = []
  for range in sorted {
    if result.length() > 0 {
      let index = result.length() - 1
      let previous = result[index]
      if previous.origin == range.origin &&
        (
          range.first <= previous.last ||
          (
            previous.last < 9223372036854775807L &&
            range.first == previous.last + 1L
          )
        ) {
        if range.last > previous.last {
          result[index] = { ..previous, last: range.last, }
        }
        continue
      }
    }
    result.push(range)
  }
  result
}

///|
fn range_is_covered(ranges : ArrayView[EventRange], range : EventRange) -> Bool {
  ranges.any(cover => {
    cover.origin == range.origin &&
    cover.first <= range.first &&
    cover.last >= range.last
  })
}

///|
/// A runtime-issued checkpoint certificate. Before issuing it, the runtime
/// must durably cover the named identities, operator state and downstream
/// outputs. The causal core validates shape and pending dependencies; it does
/// not perform IO or establish the runtime's durability proof itself.
pub struct CausalRetirement {
  checkpoint_id : String
  ranges : Array[EventRange]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn CausalRetirement::new(
  checkpoint_id : String,
  ranges : ArrayView[EventRange],
) -> CausalRetirement raise CausalError {
  if checkpoint_id.is_empty() {
    raise CausalError::InvalidRetirement("empty checkpoint identity")
  }
  { checkpoint_id, ranges: normalize_event_ranges(ranges), }
}

///|
pub fn CausalRetirement::checkpoint_id(self : CausalRetirement) -> String {
  self.checkpoint_id
}

///|
pub fn CausalRetirement::ranges(
  self : CausalRetirement,
) -> ArrayView[EventRange] {
  self.ranges[:]
}

///|
/// Keep pending identities and unresolved parents outside an otherwise safe
/// global cut. Removing points splits exact intervals without closing holes.
pub fn CausalRetirement::excluding(
  self : CausalRetirement,
  ids : ArrayView[EventId],
) -> CausalRetirement raise CausalError {
  let mut ranges = self.ranges.copy()
  for id in ids {
    let next : Array[EventRange] = []
    for range in ranges {
      if !range.contains(id) {
        next.push(range)
        continue
      }
      if range.first < id.sequence() {
        next.push({ ..range, last: id.sequence() - 1L, })
      }
      if range.last > id.sequence() {
        next.push({ ..range, first: id.sequence() + 1L, })
      }
    }
    ranges = next
  }
  CausalRetirement::new(self.checkpoint_id, ranges)
}

///|
/// Construct exact intervals from a checkpoint's admitted identities. This
/// merges adjacent IDs but cannot accidentally acknowledge gaps or a missing
/// prefix when an origin starts at a nonzero offset.
pub fn CausalRetirement::from_ids(
  checkpoint_id : String,
  ids : ArrayView[EventId],
) -> CausalRetirement raise CausalError {
  CausalRetirement::new(
    checkpoint_id,
    ids.map(id => EventRange::new(id.origin(), id.sequence(), id.sequence())),
  )
}

///|
pub fn[T] CausalLog::retired_ranges(
  self : CausalLog[T],
) -> ArrayView[EventRange] {
  self.retired[:]
}

///|
/// Retire certified payload history while preserving causal membership and
/// duplicate suppression. Identities remain immutable after retirement; their
/// old payloads are no longer retained for content-conflict diagnostics.
/// Pending inputs and missing parents cannot be silently promoted by a seal.
pub fn[T] CausalLog::compact(
  self : CausalLog[T],
  certificate : CausalRetirement,
) -> Unit raise CausalError {
  let certificate = CausalRetirement::new(
    certificate.checkpoint_id,
    certificate.ranges,
  )
  let covered = (id : EventId) => {
    certificate.ranges.any(range => range.contains(id))
  }
  for pending in self.pending.values() {
    if covered(pending.id()) || self.missing_parents(pending).any(covered) {
      raise CausalError::InvalidRetirement(
        "checkpoint crosses a pending dependency",
      )
    }
  }
  let combined = self.retired.copy()
  combined.push_iter(certificate.ranges.iter())
  let retired = normalize_event_ranges(combined)
  for id in self.admitted.keys().to_array() {
    if covered(id) {
      self.admitted.remove(id)
      self.frontier.remove(id)
    }
  }
  self.retired = retired
}
