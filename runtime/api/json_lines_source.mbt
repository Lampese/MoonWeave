///|
/// A restartable JSONL input connector. Each non-empty line contains one
/// serialized `Event`; the sidecar cursor stores the next physical line to
/// deliver and is only advanced by `commit`.
pub struct JsonLinesSource {
  path : String
  cursor_path : String
  owner : @fs.File
  operation_lock : @async_runtime.Mutex
  mut offset : Int64
  mut polled_offset : Int64
  mut prefix_digest : String
  mut closed : Bool
}

///|
struct JsonLinesSourceCursor {
  next_offset : Int64
  prefix_digest : String
} derive(ToJson, FromJson)

///|
fn source_error(stage : String, error : Error) -> ApiError {
  ApiError::Source(stage + ": " + @debug.to_string(error))
}

///|
async fn read_source_offset(
  path : String,
) -> JsonLinesSourceCursor raise ApiError {
  let exists = @fs.exists(path) catch {
    error => raise source_error("check cursor", error)
  }
  if !exists {
    return { next_offset: 0L, prefix_digest: api_digest(""), }
  }
  let text = @fs.read_file(path).text() catch {
    error => raise source_error("read cursor", error)
  }
  @json.from_json(@json.parse(text)) catch {
    error =>
      raise ApiError::Source(
        "invalid source cursor: " + @debug.to_string(error),
      )
  }
}

///|
pub async fn JsonLinesSource::open(
  path : String,
) -> JsonLinesSource raise ApiError {
  if path.is_empty() {
    raise ApiError::Source("source path is required")
  }
  let exists = @fs.exists(path) catch {
    error => raise source_error("check source", error)
  }
  if !exists {
    raise ApiError::Source("source file does not exist: " + path)
  }
  let cursor_path = path + ".offset"
  let owner = @fs.open(
    cursor_path + ".lock",
    mode=@fs.ReadWrite,
    create_mode=@fs.OpenOrCreate,
  ) catch {
    error => raise source_error("open cursor lock", error)
  }
  errdefer owner.close()
  let locked = owner.try_lock(@fs.Exclusive) catch {
    error => raise source_error("lock cursor", error)
  }
  if !locked {
    raise ApiError::Source("source cursor already has an owner")
  }
  let cursor = read_source_offset(cursor_path)
  if cursor.next_offset < 0L {
    raise ApiError::Source("source cursor cannot be negative")
  }
  let source = {
    path,
    cursor_path,
    owner,
    operation_lock: @async_runtime.Mutex::Mutex(),
    offset: cursor.next_offset,
    polled_offset: cursor.next_offset,
    prefix_digest: cursor.prefix_digest,
    closed: false,
  }
  ignore(source.read_lines())
  source
}

///|
pub fn JsonLinesSource::close(self : JsonLinesSource) -> Unit {
  self.closed = true
  if self.operation_lock.try_acquire() {
    self.release_operation()
  }
}

///|
fn JsonLinesSource::release_operation(self : JsonLinesSource) -> Unit {
  if self.closed {
    self.owner.close()
  }
  self.operation_lock.release()
}

///|
pub fn JsonLinesSource::path(self : JsonLinesSource) -> String {
  self.path
}

///|
pub fn JsonLinesSource::cursor_path(self : JsonLinesSource) -> String {
  self.cursor_path
}

///|
pub fn JsonLinesSource::offset(self : JsonLinesSource) -> Int64 {
  self.offset
}

///|
/// Return the end of the most recent uncommitted poll. This is the value to
/// pass to `commit` after the job has durably processed the returned events.
pub fn JsonLinesSource::polled_offset(self : JsonLinesSource) -> Int64 {
  self.polled_offset
}

///|
async fn JsonLinesSource::read_lines(
  self : JsonLinesSource,
) -> Array[String] raise ApiError {
  if self.closed {
    raise ApiError::Closed
  }
  let text = @fs.read_file(self.path).text() catch {
    error => raise source_error("read source", error)
  }
  let lines = text.split("\n").map(line => line.to_owned()).to_array()
  if lines is [.., ""] {
    ignore(lines.pop())
  }
  if self.offset > lines.length().to_int64() ||
    api_digest(lines[:self.offset.to_int()].to_owned().join("\n")) !=
    self.prefix_digest {
    raise ApiError::Source("source file no longer matches its committed prefix")
  }
  lines
}

///|
/// Poll at most `maximum_events` records and return the physical cursor that
/// would be committed after this batch. The source itself is not advanced.
pub async fn JsonLinesSource::poll_with_offset(
  self : JsonLinesSource,
  maximum_events? : Int = 1024,
) -> (Array[Event], Int64) raise ApiError {
  if !self.operation_lock.try_acquire() {
    raise ApiError::Source("source is busy")
  }
  defer self.release_operation()
  if maximum_events <= 0 {
    raise ApiError::Source("maximum_events must be positive")
  }
  let lines = self.read_lines()
  if self.offset >= lines.length().to_int64() {
    return ([], self.polled_offset)
  }
  let result : Array[Event] = []
  let mut physical = self.polled_offset.to_int()
  while physical < lines.length() && result.length() < maximum_events {
    let line = lines[physical].trim()
    if !line.is_empty() {
      let json = @json.parse(line) catch {
        error =>
          raise ApiError::Source(
            "malformed JSONL event at line " +
            physical.to_string() +
            ": " +
            @debug.to_string(error),
          )
      }
      let event : Event = @json.from_json(json) catch {
        error =>
          raise ApiError::Source(
            "invalid Event at line " +
            physical.to_string() +
            ": " +
            @debug.to_string(error),
          )
      }
      result.push(
        Event::new(
          event.id(),
          event.record.parents(),
          event.key(),
          event.payload(),
        ),
      )
    }
    physical += 1
  }
  self.polled_offset = physical.to_int64()
  (result, self.polled_offset)
}

///|
/// Poll at most `maximum_events` records without moving the committed cursor.
pub async fn JsonLinesSource::poll(
  self : JsonLinesSource,
  maximum_events? : Int = 1024,
) -> Array[Event] raise ApiError {
  self.poll_with_offset(maximum_events~).0
}

///|
/// Persist the next physical line to read. Commits are monotone and cannot
/// skip beyond the current file, which makes accidental data loss explicit.
pub async fn JsonLinesSource::commit(
  self : JsonLinesSource,
  next_offset : Int64,
) -> Unit raise ApiError {
  if !self.operation_lock.try_acquire() {
    raise ApiError::Source("source is busy")
  }
  defer self.release_operation()
  if next_offset < self.offset || next_offset > self.polled_offset {
    raise ApiError::Source("source commit must cover a polled prefix")
  }
  self.persist_offset(next_offset)
}

///|
async fn JsonLinesSource::persist_offset(
  self : JsonLinesSource,
  next_offset : Int64,
) -> Unit raise ApiError {
  let lines = self.read_lines()
  if next_offset > lines.length().to_int64() {
    raise ApiError::Source("source cursor is beyond the input file")
  }
  let prefix_digest = api_digest(
    lines[:next_offset.to_int()].to_owned().join("\n"),
  )
  let temporary = self.cursor_path + ".tmp"
  @fs.write_file(
    temporary,
    @json.to_json(JsonLinesSourceCursor::{ next_offset, prefix_digest, }).stringify(),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise source_error("write cursor", error)
  }
  @async_runtime.protect_from_cancel(resume_on_cancel=true, () => {
    @fs.rename(temporary, self.cursor_path, replace=true)
    self.offset = next_offset
    self.prefix_digest = prefix_digest
  }) catch {
    error => raise source_error("commit cursor", error)
  }
}

///|
pub async fn JsonLinesSource::rewind(
  self : JsonLinesSource,
  next_offset? : Int64 = 0L,
) -> Unit raise ApiError {
  if !self.operation_lock.try_acquire() {
    raise ApiError::Source("source is busy")
  }
  defer self.release_operation()
  if next_offset < 0L || next_offset > self.offset {
    raise ApiError::Source("rewind requires an already committed offset")
  }
  self.persist_offset(next_offset)
  self.polled_offset = next_offset
}
