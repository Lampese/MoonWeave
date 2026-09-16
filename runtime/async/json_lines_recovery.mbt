///|
async fn JsonLinesSink::lock_file(
  self : JsonLinesSink,
) -> Unit raise JsonLinesSinkError {
  try {
    while !self.owner.try_lock(@fs.Exclusive) {
      @async.sleep(5)
    }
  } catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
}

///|
fn JsonLinesSink::check_recent(
  self : JsonLinesSink,
  record : TaskOutputRecord,
) -> Unit raise JsonLinesSinkError {
  if self.known.get(record.idempotency_key()) is Some(previous) &&
    (
      previous.output != record.output ||
      previous.partition != record.partition ||
      previous.version != record.version
    ) {
    raise JsonLinesSinkError::IdentityConflict(record.idempotency_key())
  }
}

///|
fn JsonLinesSink::index_with(
  self : JsonLinesSink,
  record : TaskOutputRecord,
) -> Array[@causal.EventRange] raise JsonLinesSinkError {
  let ranges = self.ranges.copy()
  let combined = try {
    ranges.push(
      @causal.EventRange::new(record.task_id, record.sequence, record.sequence),
    )
    @causal.CausalRetirement::new("json-lines", ranges).ranges().to_owned()
  } catch {
    _ => raise JsonLinesSinkError::Malformed
  }
  if combined.length() > self.maximum_index_ranges {
    raise JsonLinesSinkError::IndexLimit(self.maximum_index_ranges)
  }
  combined
}

///|
fn JsonLinesSink::remember(
  self : JsonLinesSink,
  record : TaskOutputRecord,
  bytes : Int,
) -> Unit {
  while !self.known.is_empty() &&
        (self.known.length() >= 256 || self.known_bytes > 1048576 - bytes) {
    let oldest = self.known.keys().next().unwrap()
    self.known_bytes -= self.sizes[oldest]
    self.known.remove(oldest)
    self.sizes.remove(oldest)
  }
  if bytes <= 1048576 {
    self.known[record.idempotency_key()] = record
    self.sizes[record.idempotency_key()] = bytes
    self.known_bytes += bytes
  }
  self.count += 1L
}

///|
fn JsonLinesSink::restore_line(
  self : JsonLinesSink,
  bytes : BytesView,
) -> Unit raise JsonLinesSinkError {
  let text = @utf8.decode(bytes) catch {
    _ => raise JsonLinesSinkError::Malformed
  }
  if text.trim().is_empty() {
    return
  }
  let record : TaskOutputRecord = @json.from_json(@json.parse(text)) catch {
    _ => raise JsonLinesSinkError::Malformed
  }
  self.check_recent(record)
  if self.contains(record.idempotency_key()) {
    return
  }
  self.ranges = self.index_with(record)
  self.remember(record, bytes.length())
}

///|
/// Only scan the suffix added by another writer since the previous operation.
/// Positional reads and line-size limits bound both startup and refresh memory.
async fn JsonLinesSink::refresh_file(
  self : JsonLinesSink,
) -> Unit raise JsonLinesSinkError {
  try {
    let file = @fs.open(self.path, mode=@fs.ReadOnly)
    defer file.close()
    let size = file.size()
    if size < self.next_offset {
      raise JsonLinesSinkError::Malformed
    }
    let mut position = self.next_offset
    let mut pending : Bytes = b""
    while position < size {
      let count = Int64::min(size - position, 65536L).to_int()
      pending = pending + file.read_exactly_at(count, position~)
      position += count.to_int64()
      while pending.find(b"\n") is Some(end) {
        if end > self.maximum_record_bytes {
          raise JsonLinesSinkError::RecordTooLarge(self.maximum_record_bytes)
        }
        self.restore_line(pending[:end])
        self.next_offset += (end + 1).to_int64()
        pending = pending[end + 1:].to_owned()
      }
      if pending.length() > self.maximum_record_bytes {
        raise JsonLinesSinkError::RecordTooLarge(self.maximum_record_bytes)
      }
    }
    if !pending.is_empty() {
      self.repair_tail(file)
    } else if size > 0L {
      // A complete line from a previously interrupted write may have reached
      // the page cache without its durability barrier. Sync before accepting it.
      file.sync()
    }
  } catch {
    JsonLinesSinkError::Malformed => raise JsonLinesSinkError::Malformed
    JsonLinesSinkError::IndexLimit(limit) =>
      raise JsonLinesSinkError::IndexLimit(limit)
    JsonLinesSinkError::RecordTooLarge(limit) =>
      raise JsonLinesSinkError::RecordTooLarge(limit)
    JsonLinesSinkError::IdentityConflict(key) =>
      raise JsonLinesSinkError::IdentityConflict(key)
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
}

///|
/// No ACK covers a partial final line. Preserve the complete prefix under the
/// writer lock and replace atomically; the pending Worker record is retried.
async fn JsonLinesSink::repair_tail(
  self : JsonLinesSink,
  source : @fs.File,
) -> Unit {
  let temporary = self.path + ".repair"
  let target = @fs.open(
    temporary,
    mode=@fs.ReadWrite,
    create_mode=@fs.CreateOrTruncate,
  )
  defer target.close()
  let mut position = 0L
  while position < self.next_offset {
    let count = Int64::min(self.next_offset - position, 65536L).to_int()
    target.write_at(source.read_exactly_at(count, position~), position~)
    position += count.to_int64()
  }
  target.sync()
  @async.protect_from_cancel(() => {
    @fs.rename(temporary, self.path, replace=true)
  })
}
