///|
pub(all) suberror JsonLinesSinkError {
  EmptyPath
  Malformed
  IdentityConflict(String)
  Io(String)
  OutputLog(String)
  Consumer(String)
  IndexLimit(Int)
  RecordTooLarge(Int)
  Closed
} derive(Debug, Eq)

///|
/// Append-only output with exact identity intervals and a bounded recent
/// payload cache. Old identities remain immutable after cache eviction.
/// Engine writers coordinate through a separate lock inode across processes.
pub struct JsonLinesSink {
  path : String
  known : Map[String, TaskOutputRecord]
  sizes : Map[String, Int]
  mut known_bytes : Int
  mut ranges : Array[@causal.EventRange]
  mut count : Int64
  maximum_index_ranges : Int
  maximum_record_bytes : Int
  owner : @fs.File
  mut next_offset : Int64
  mut closed : Bool
  write_lock : @async.Mutex
}

///|
pub async fn JsonLinesSink::open(
  path : String,
  maximum_index_ranges? : Int = 4096,
  maximum_record_bytes? : Int = 1048576,
) -> JsonLinesSink raise JsonLinesSinkError {
  if path.is_empty() {
    raise JsonLinesSinkError::EmptyPath
  }
  if maximum_index_ranges <= 0 || maximum_record_bytes <= 0 {
    raise JsonLinesSinkError::Malformed
  }
  let file = @fs.open(path, mode=@fs.ReadWrite, create_mode=@fs.OpenOrCreate) catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
  defer file.close()
  let owner = @fs.open(
    path + ".lock",
    mode=@fs.ReadWrite,
    create_mode=@fs.OpenOrCreate,
  ) catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
  errdefer owner.close()
  let sink = {
    path,
    owner,
    known: Map([]),
    sizes: Map([]),
    known_bytes: 0,
    ranges: [],
    count: 0L,
    maximum_index_ranges,
    maximum_record_bytes,
    next_offset: 0L,
    closed: false,
    write_lock: @async.Mutex::Mutex(),
  }
  sink.lock_file()
  defer owner.unlock()
  sink.refresh_file()
  sink
}

///|
pub fn JsonLinesSink::path(self : JsonLinesSink) -> String {
  self.path
}

///|
pub fn JsonLinesSink::close(self : JsonLinesSink) -> Unit {
  self.closed = true
  if self.write_lock.try_acquire() {
    self.release_write()
  }
}

///|
fn JsonLinesSink::release_write(self : JsonLinesSink) -> Unit {
  if self.closed {
    self.owner.close()
  }
  self.write_lock.release()
}

///|
pub fn JsonLinesSink::written_count(self : JsonLinesSink) -> Int64 {
  self.count
}

///|
pub fn JsonLinesSink::contains(
  self : JsonLinesSink,
  idempotency_key : String,
) -> Bool {
  let key = idempotency_key
  guard key.find(":") is Some(first) &&
    key.rev_find(":") is Some(last) &&
    last > first else {
    return false
  }
  let (length, task, sequence) = (
    @string.parse_int(key[:first]),
    key[first + 1:last].to_owned(),
    @string.parse_int64(key[last + 1:]),
  ) catch {
    _ => return false
  }
  if task.length() != length || sequence < 0L {
    return false
  }
  self.ranges.any(range => {
    range.origin() == task &&
    range.first() <= sequence &&
    sequence <= range.last()
  })
}

///|
pub async fn JsonLinesSink::write(
  self : JsonLinesSink,
  record : TaskOutputRecord,
) -> Unit raise JsonLinesSinkError {
  self.write_lock.acquire() catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
  defer self.release_write()
  if self.closed {
    raise JsonLinesSinkError::Closed
  }
  self.lock_file()
  defer self.owner.unlock()
  self.refresh_file()
  let key = record.idempotency_key()
  self.check_recent(record)
  if self.contains(key) {
    return
  }
  let ranges = self.index_with(record)
  let bytes = @utf8.encode(@json.to_json(record).stringify() + "\n")
  if bytes.length() - 1 > self.maximum_record_bytes {
    raise JsonLinesSinkError::RecordTooLarge(self.maximum_record_bytes)
  }
  @async.protect_from_cancel(() => {
    @fs.write_file(
      self.path,
      bytes,
      append=true,
      create_mode=@fs.OpenOrCreate,
      sync=@fs.Full,
    )
    self.ranges = ranges
    self.remember(record, bytes.length() - 1)
    self.next_offset += bytes.length().to_int64()
  }) catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
}

///|
pub async fn JsonLinesSink::consume(
  self : JsonLinesSink,
  consumer : TaskOutputConsumer,
  maximum_records? : Int = 128,
) -> Int raise JsonLinesSinkError {
  consumer.consume(record => self.write(record), maximum_records~) catch {
    error => raise JsonLinesSinkError::Consumer(@debug.to_string(error))
  }
}
