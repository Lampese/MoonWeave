///|
/// Errors raised by the idempotent JSONL output connector.
pub(all) suberror JsonLinesSinkError {
  EmptyPath
  Malformed
  IdentityConflict(String)
  Io(String)
  OutputLog(String)
  Consumer(String)
} derive(Debug, Eq)

///|
/// An append-only JSONL sink. Existing records are indexed at startup so a
/// retry after a lost ACK does not append the same `TaskOutputRecord` twice.
pub struct JsonLinesSink {
  path : String
  known : Map[String, TaskOutputRecord]
  write_lock : @async.Mutex
}

///|
pub async fn JsonLinesSink::open(
  path : String,
) -> JsonLinesSink raise JsonLinesSinkError {
  if path.is_empty() {
    raise JsonLinesSinkError::EmptyPath
  }
  let exists = @fs.exists(path) catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
  if !exists {
    @fs.write_file(path, "", create_mode=@fs.CreateOrTruncate, sync=@fs.Full) catch {
      error => raise JsonLinesSinkError::Io(@debug.to_string(error))
    }
  }
  let text = @fs.read_file(path).text() catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
  let known : Map[String, TaskOutputRecord] = Map([])
  for line in text.split("\n") {
    let line = line.trim()
    if line.is_empty() {
      continue
    }
    let json = @json.parse(line) catch {
      _ => raise JsonLinesSinkError::Malformed
    }
    let record : TaskOutputRecord = @json.from_json(json) catch {
      _ => raise JsonLinesSinkError::Malformed
    }
    let key = record.idempotency_key()
    match known.get(key) {
      Some(previous) if previous != record =>
        raise JsonLinesSinkError::IdentityConflict(key)
      _ => known[key] = record
    }
  }
  { path, known, write_lock: @async.Mutex::Mutex(), }
}

///|
pub fn JsonLinesSink::path(self : JsonLinesSink) -> String {
  self.path
}

///|
pub fn JsonLinesSink::close(_self : JsonLinesSink) -> Unit {
  // The connector writes through the filesystem API per record; there is no
  // open data handle to release.
}

///|
pub fn JsonLinesSink::written_count(self : JsonLinesSink) -> Int {
  self.known.length()
}

///|
pub fn JsonLinesSink::contains(
  self : JsonLinesSink,
  idempotency_key : String,
) -> Bool {
  self.known.contains(idempotency_key)
}

///|
/// Append one record, treating an identical idempotency key as a successful
/// retry and rejecting a conflicting payload.
pub async fn JsonLinesSink::write(
  self : JsonLinesSink,
  record : TaskOutputRecord,
) -> Unit raise JsonLinesSinkError {
  self.write_lock.acquire() catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let key = record.idempotency_key()
  match self.known.get(key) {
    Some(previous) if previous == record => return
    Some(_) => raise JsonLinesSinkError::IdentityConflict(key)
    None => ()
  }
  @fs.write_file(
    self.path,
    @json.to_json(record).stringify() + "\n",
    append=true,
    create_mode=@fs.OpenOrCreate,
    sync=@fs.Full,
  ) catch {
    error => raise JsonLinesSinkError::Io(@debug.to_string(error))
  }
  self.known[key] = record
}

///|
/// Consume a Task output stream with the existing at-least-once ACK protocol.
/// A crash after `write` and before the ACK is safe because `write` is keyed.
pub async fn JsonLinesSink::consume(
  self : JsonLinesSink,
  consumer : TaskOutputConsumer,
  maximum_records? : Int = 128,
) -> Int raise JsonLinesSinkError {
  consumer.consume(record => self.write(record), maximum_records~) catch {
    error => raise JsonLinesSinkError::Consumer(@debug.to_string(error))
  }
}
