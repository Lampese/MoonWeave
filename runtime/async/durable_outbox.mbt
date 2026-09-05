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
/// One append-only outbox command. A `put` records the Delta payload; an `ack`
/// records that the target Worker durably admitted it.
struct DurableOutboxEntry {
  version : Int
  kind : String
  delta_id : String
  delta : @protocol.DeltaBatch?
} derive(Debug, Eq, ToJson, FromJson)

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
}

///|
fn durable_outbox_key(delta_id : String) -> String {
  delta_id.length().to_string() + ":" + delta_id
}

///|
fn durable_put_entry(delta : @protocol.DeltaBatch) -> DurableOutboxEntry {
  { version: 1, kind: "put", delta_id: delta.delta_id(), delta: Some(delta), }
}

///|
fn durable_ack_entry(delta_id : String) -> DurableOutboxEntry {
  { version: 1, kind: "ack", delta_id, delta: None, }
}

///|
async fn durable_outbox_sleep() -> Unit raise DurableOutboxError {
  @async.sleep(1) catch {
    error => raise DurableOutboxError::Io(@debug.to_string(error))
  }
}

///|
fn apply_durable_outbox_entry(
  pending : Array[@protocol.DeltaBatch],
  known : Map[String, @protocol.DeltaBatch],
  acked : Set[String],
  entry : DurableOutboxEntry,
) -> Unit raise DurableOutboxError {
  if entry.version != 1 {
    raise DurableOutboxError::UnsupportedVersion(entry.version)
  }
  if entry.delta_id.is_empty() {
    raise DurableOutboxError::Malformed
  }
  let key = durable_outbox_key(entry.delta_id)
  match entry.kind {
    "put" => {
      let delta = match entry.delta {
        Some(value) => value
        None => raise DurableOutboxError::Malformed
      }
      if delta.delta_id() != entry.delta_id {
        raise DurableOutboxError::IdentityConflict(key)
      }
      match known.get(entry.delta_id) {
        Some(previous) =>
          if previous != delta {
            raise DurableOutboxError::IdentityConflict(key)
          }
        None => {
          known[entry.delta_id] = delta
          if !acked.contains(entry.delta_id) {
            pending.push(delta)
          }
        }
      }
    }
    "ack" => {
      guard known.contains(entry.delta_id) else {
        raise DurableOutboxError::UnknownDelta(entry.delta_id)
      }
      acked.add(entry.delta_id)
      let mut index = 0
      while index < pending.length() {
        if pending[index].delta_id() == entry.delta_id {
          ignore(pending.remove(index))
          break
        }
        index += 1
      }
    }
    _ => raise DurableOutboxError::Malformed
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
    apply_durable_outbox_entry(pending, known, acked, entry)
  }
  {
    wal,
    capacity,
    pending,
    known,
    acked,
    inflight: Set([]),
    write_lock: @async.Mutex::Mutex(),
    closed: false,
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
  self.pending.copy()
}

///|
pub fn DurableExchangeOutbox::close(self : DurableExchangeOutbox) -> Unit {
  self.closed = true
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
}

///|
/// Nonblocking enqueue. `false` means the durable unacknowledged window is
/// full; no payload is dropped.
pub async fn DurableExchangeOutbox::try_push(
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
  if self.pending.length() >= self.capacity {
    return false
  }
  self.append_entry(durable_put_entry(delta))
  self.known[delta.delta_id()] = delta
  self.pending.push(delta)
  true
}

///|
/// Enqueue with async backpressure. Waiting never blocks a native thread.
pub async fn DurableExchangeOutbox::push(
  self : DurableExchangeOutbox,
  delta : @protocol.DeltaBatch,
) -> Unit raise DurableOutboxError {
  for ;; {
    self.write_lock.acquire() catch {
      error => raise DurableOutboxError::Io(@debug.to_string(error))
    }
    if self.closed {
      self.write_lock.release()
      raise DurableOutboxError::Closed
    }
    self.check_delta(delta)
    match self.known.get(delta.delta_id()) {
      Some(_) => {
        self.write_lock.release()
        return
      }
      None => ()
    }
    if self.pending.length() < self.capacity {
      self.append_entry(durable_put_entry(delta))
      self.known[delta.delta_id()] = delta
      self.pending.push(delta)
      self.write_lock.release()
      return
    }
    self.write_lock.release()
    durable_outbox_sleep()
  }
}

///|
/// Pop the next ready Delta and mark it inflight. An unacked inflight entry is
/// requeued automatically when the outbox is reopened after a crash.
pub fn DurableExchangeOutbox::try_pop(
  self : DurableExchangeOutbox,
) -> @protocol.DeltaBatch? {
  if !self.write_lock.try_acquire() {
    return None
  }
  defer self.write_lock.release()
  for delta in self.pending {
    if !self.inflight.contains(delta.delta_id()) {
      self.inflight.add(delta.delta_id())
      return Some(delta)
    }
  }
  None
}

///|
pub async fn DurableExchangeOutbox::pop(
  self : DurableExchangeOutbox,
) -> @protocol.DeltaBatch raise DurableOutboxError {
  for ;; {
    if self.closed && self.pending.is_empty() {
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
  guard self.known.contains(delta_id) else {
    raise DurableOutboxError::UnknownDelta(delta_id)
  }
  if self.acked.contains(delta_id) {
    self.inflight.remove(delta_id)
    return
  }
  self.append_entry(durable_ack_entry(delta_id))
  self.acked.add(delta_id)
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
