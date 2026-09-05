///|
/// Errors raised by the WAL-backed exchange outbox.
pub(all) suberror DurableOutboxError {
  EmptyPath
  InvalidCapacity(Int)
  Closed
  Full(Int)
  UnknownDelta(String)
  IdentityConflict(String)
  Malformed
  UnsupportedVersion(Int)
  Wal(WalError)
  Io(String)
} derive(Debug, Eq)

///|
/// Durable, bounded exchange queue. `pending` contains all unacknowledged
/// Deltas, while `inflight` identifies entries currently handed to a sender.
pub struct DurableExchangeOutbox {
  wal : Wal
  capacity : Int
  pending : Array[@protocol.DeltaBatch]
  known : Map[String, @protocol.DeltaBatch]
  acked : Set[String]
  mut inflight : Set[String]
  write_lock : @async.Mutex
  mut closed : Bool
  mut disposed : Bool
  known_digests : Map[String, String]
}

///|
async fn durable_outbox_sleep() -> Unit raise DurableOutboxError {
  @async.sleep(1) catch {
    error => raise DurableOutboxError::Io(@debug.to_string(error))
  }
}

///|
pub async fn DurableExchangeOutbox::open(
  path : String,
  capacity : Int,
) -> DurableExchangeOutbox raise DurableOutboxError {
  if path.is_empty() {
    raise DurableOutboxError::EmptyPath
  }
  if capacity <= 0 {
    raise DurableOutboxError::InvalidCapacity(capacity)
  }
  let (wal, replay) = Wal::open(path) catch {
    error => raise DurableOutboxError::Wal(error)
  }
  errdefer wal.close()
  let pending : Array[@protocol.DeltaBatch] = []
  let known : Map[String, @protocol.DeltaBatch] = Map([])
  let known_digests : Map[String, String] = Map([])
  let acked : Set[String] = Set([])
  for encoded in replay.records() {
    let text = @utf8.decode(encoded) catch {
      error => raise DurableOutboxError::Io(@debug.to_string(error))
    }
    let json = @json.parse(text) catch {
      _ => raise DurableOutboxError::Malformed
    }
    let entry : DurableOutboxEntry = @json.from_json(json) catch {
      _ => raise DurableOutboxError::Malformed
    }
    apply_durable_outbox_entry(pending, known, known_digests, acked, entry)
  }
  {
    wal,
    capacity,
    pending,
    known,
    known_digests,
    acked,
    inflight: Set([]),
    write_lock: @async.Mutex::Mutex(),
    closed: false,
    disposed: false,
  }
}

///|
pub fn DurableExchangeOutbox::capacity(self : DurableExchangeOutbox) -> Int {
  self.capacity
}

///|
pub fn DurableExchangeOutbox::path(self : DurableExchangeOutbox) -> String {
  self.wal.path()
}

///|
pub fn DurableExchangeOutbox::is_closed(self : DurableExchangeOutbox) -> Bool {
  self.closed
}

///|
fn DurableExchangeOutbox::is_drained(
  self : DurableExchangeOutbox,
) -> Bool raise DurableOutboxError {
  if self.disposed {
    raise DurableOutboxError::Closed
  }
  if !self.write_lock.try_acquire() {
    return false
  }
  defer self.write_lock.release()
  self.closed && self.pending.is_empty()
}

///|
pub fn DurableExchangeOutbox::pending_count(
  self : DurableExchangeOutbox,
) -> Int {
  self.pending.length()
}

///|
pub fn DurableExchangeOutbox::inflight_count(
  self : DurableExchangeOutbox,
) -> Int {
  self.inflight.length()
}

///|
pub fn DurableExchangeOutbox::acked_count(self : DurableExchangeOutbox) -> Int {
  self.acked.length()
}

///|
pub fn DurableExchangeOutbox::pending(
  self : DurableExchangeOutbox,
) -> Array[@protocol.DeltaBatch] {
  self.pending.map(copy_durable_delta)
}

///|
pub fn DurableExchangeOutbox::close(self : DurableExchangeOutbox) -> Unit {
  self.closed = true
  self.disposed = true
  self.wal.close()
}

///|
/// Mark the producer side complete while keeping the WAL open for final ACKs.
/// Call `close` after the exchange driver has drained and acknowledged the
/// remaining entries.
pub fn DurableExchangeOutbox::seal(self : DurableExchangeOutbox) -> Unit {
  self.closed = true
}

///|
async fn DurableExchangeOutbox::append_entry(
  self : DurableExchangeOutbox,
  entry : DurableOutboxEntry,
) -> Unit raise DurableOutboxError {
  ignore(
    self.wal.append(@utf8.encode(@json.to_json(entry).stringify())) catch {
      error => raise DurableOutboxError::Wal(error)
    },
  )
}

///|
fn DurableExchangeOutbox::check_delta(
  self : DurableExchangeOutbox,
  delta : @protocol.DeltaBatch,
) -> Unit raise DurableOutboxError {
  if delta.delta_id().is_empty() {
    raise DurableOutboxError::Malformed
  }
  match self.known.get(delta.delta_id()) {
    Some(previous) if previous != delta =>
      raise DurableOutboxError::IdentityConflict(
        durable_outbox_key(delta.delta_id()),
      )
    _ => ()
  }
  match self.known_digests.get(delta.delta_id()) {
    Some(expected) if expected != durable_delta_digest(delta) =>
      raise DurableOutboxError::IdentityConflict(
        durable_outbox_key(delta.delta_id()),
      )
    _ => ()
  }
}

///|
/// Enqueue without waiting for capacity. It still awaits the WAL durability
/// barrier; `false` means the unacknowledged window is full.
pub async fn DurableExchangeOutbox::try_push(
  self : DurableExchangeOutbox,
  delta : @protocol.DeltaBatch,
) -> Bool raise DurableOutboxError {
  let delta = validate_durable_delta(delta)
  self.try_push_validated(delta)
}

///|
async fn DurableExchangeOutbox::try_push_validated(
  self : DurableExchangeOutbox,
  delta : @protocol.DeltaBatch,
) -> Bool raise DurableOutboxError {
  self.write_lock.acquire() catch {
    error => raise DurableOutboxError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  if self.closed {
    raise DurableOutboxError::Closed
  }
  self.check_delta(delta)
  match self.known.get(delta.delta_id()) {
    Some(_) => return true
    None => ()
  }
  if self.acked.contains(delta.delta_id()) {
    return true
  }
  if self.pending.length() >= self.capacity {
    return false
  }
  self.append_entry(durable_put_entry(delta))
  self.known[delta.delta_id()] = delta
  self.known_digests[delta.delta_id()] = durable_delta_digest(delta)
  self.pending.push(delta)
  true
}

///|
/// Enqueue with async backpressure. Waiting never blocks a native thread.
pub async fn DurableExchangeOutbox::push(
  self : DurableExchangeOutbox,
  delta : @protocol.DeltaBatch,
) -> Unit raise DurableOutboxError {
  let delta = validate_durable_delta(delta)
  for ;; {
    if self.try_push_validated(delta) {
      return
    }
    durable_outbox_sleep()
  }
}

///|
/// Pop the next ready Delta and mark it inflight. An unacked inflight entry is
/// requeued automatically when the outbox is reopened after a crash.
pub fn DurableExchangeOutbox::try_pop(
  self : DurableExchangeOutbox,
) -> @protocol.DeltaBatch? {
  if self.disposed || !self.write_lock.try_acquire() {
    return None
  }
  defer self.write_lock.release()
  for delta in self.pending {
    if !self.inflight.contains(delta.delta_id()) {
      self.inflight.add(delta.delta_id())
      return Some(copy_durable_delta(delta))
    }
  }
  None
}

///|
pub async fn DurableExchangeOutbox::pop(
  self : DurableExchangeOutbox,
) -> @protocol.DeltaBatch raise DurableOutboxError {
  for ;; {
    if self.is_drained() {
      raise DurableOutboxError::Closed
    }
    match self.try_pop() {
      Some(delta) => return delta
      None => durable_outbox_sleep()
    }
  }
}

///|
/// Acknowledge one Delta after the target Worker has durably admitted it.
pub async fn DurableExchangeOutbox::ack(
  self : DurableExchangeOutbox,
  delta_id : String,
) -> Unit raise DurableOutboxError {
  self.write_lock.acquire() catch {
    error => raise DurableOutboxError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  if self.disposed {
    raise DurableOutboxError::Closed
  }
  if self.acked.contains(delta_id) {
    self.inflight.remove(delta_id)
    return
  }
  guard self.known.contains(delta_id) else {
    raise DurableOutboxError::UnknownDelta(delta_id)
  }
  self.append_entry(durable_ack_entry(delta_id))
  self.acked.add(delta_id)
  self.known.remove(delta_id)
  self.inflight.remove(delta_id)
  let mut index = 0
  while index < self.pending.length() {
    if self.pending[index].delta_id() == delta_id {
      ignore(self.pending.remove(index))
      break
    }
    index += 1
  }
}

///|
/// Return an inflight Delta to the ready set after a send-side transport error.
pub fn DurableExchangeOutbox::requeue(
  self : DurableExchangeOutbox,
  delta_id : String,
) -> Unit {
  self.inflight.remove(delta_id)
}

///|
/// Requeue all unacknowledged sends, useful in a driver defer after a broken
/// connection so the same outbox can be retried without reopening its WAL.
pub fn DurableExchangeOutbox::requeue_all(self : DurableExchangeOutbox) -> Unit {
  self.inflight = Set([])
}

///|
/// Compact the outbox without losing duplicate suppression. Pending payloads
/// remain as `put` records; acknowledged identities become small tombstones.
/// Tombstones are retained indefinitely; this does not bound identity history.
pub async fn DurableExchangeOutbox::compact(
  self : DurableExchangeOutbox,
) -> Unit raise DurableOutboxError {
  self.write_lock.acquire() catch {
    error => raise DurableOutboxError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  if self.disposed {
    raise DurableOutboxError::Closed
  }
  let entries : Array[Bytes] = []
  for delta in self.pending {
    if !self.acked.contains(delta.delta_id()) {
      entries.push(
        @utf8.encode(@json.to_json(durable_put_entry(delta)).stringify()),
      )
    }
  }
  let acked_ids = self.acked.to_array()
  acked_ids.sort()
  for delta_id in acked_ids {
    guard self.known_digests.get(delta_id) is Some(digest) else {
      raise DurableOutboxError::Malformed
    }
    validate_durable_digest(digest)
    entries.push(
      @utf8.encode(
        @json.to_json(durable_tombstone_entry(delta_id, digest)).stringify(),
      ),
    )
  }
  self.wal.rewrite(entries[:]) catch {
    error => raise DurableOutboxError::Wal(error)
  }
}
