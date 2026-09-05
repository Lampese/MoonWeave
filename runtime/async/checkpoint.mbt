///|
/// Errors raised while loading or atomically replacing a worker checkpoint.
pub(all) suberror CheckpointError {
  EmptyPath
  InvalidField
  Malformed
  Io(String)
} derive(Debug, Eq)

///|
/// Durable worker progress manifest. It describes a verified WAL prefix and
/// logical input/application offsets; it is not a replacement for the WAL.
pub(all) struct Checkpoint {
  version : Int
  partition : Int
  epoch : Int
  wal_offset : Int64
  delta_wal_offset : Int64
  persisted_offset : Int64
  applied_offset : Int64
  frontier_digest : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Checkpoint::new(
  partition : Int,
  epoch : Int,
  wal_offset : Int64,
  persisted_offset : Int64,
  applied_offset : Int64,
  frontier_digest : String,
  delta_wal_offset? : Int64 = 0L,
) -> Checkpoint raise CheckpointError {
  if partition < 0 ||
    epoch < 0 ||
    wal_offset < 0L ||
    delta_wal_offset < 0L ||
    persisted_offset < -1L ||
    applied_offset < -1L ||
    applied_offset > persisted_offset ||
    frontier_digest.is_empty() {
    raise CheckpointError::InvalidField
  }
  {
    version: 1,
    partition,
    epoch,
    wal_offset,
    delta_wal_offset,
    persisted_offset,
    applied_offset,
    frontier_digest,
  }
}

///|
pub fn Checkpoint::version(self : Checkpoint) -> Int {
  self.version
}

///|
pub fn Checkpoint::partition(self : Checkpoint) -> Int {
  self.partition
}

///|
pub fn Checkpoint::epoch(self : Checkpoint) -> Int {
  self.epoch
}

///|
pub fn Checkpoint::wal_offset(self : Checkpoint) -> Int64 {
  self.wal_offset
}

///|
pub fn Checkpoint::delta_wal_offset(self : Checkpoint) -> Int64 {
  self.delta_wal_offset
}

///|
pub fn Checkpoint::persisted_offset(self : Checkpoint) -> Int64 {
  self.persisted_offset
}

///|
pub fn Checkpoint::applied_offset(self : Checkpoint) -> Int64 {
  self.applied_offset
}

///|
pub fn Checkpoint::frontier_digest(self : Checkpoint) -> String {
  self.frontier_digest
}

///|
fn Checkpoint::validate(self : Checkpoint) -> Checkpoint raise CheckpointError {
  if self.version != 1 {
    raise CheckpointError::Malformed
  }
  Checkpoint::new(
    self.partition,
    self.epoch,
    self.wal_offset,
    self.persisted_offset,
    self.applied_offset,
    self.frontier_digest,
    delta_wal_offset=self.delta_wal_offset,
  )
}

///|
pub async fn Checkpoint::load(
  path : String,
) -> Checkpoint? raise CheckpointError {
  if path.is_empty() {
    raise CheckpointError::EmptyPath
  }
  let exists = @fs.exists(path) catch {
    error => raise CheckpointError::Io(@debug.to_string(error))
  }
  if !exists {
    return None
  }
  let text = @fs.read_file(path).text() catch {
    error => raise CheckpointError::Io(@debug.to_string(error))
  }
  let json = @json.parse(text) catch { _ => raise CheckpointError::Malformed }
  let checkpoint : Checkpoint = @json.from_json(json) catch {
    _ => raise CheckpointError::Malformed
  }
  Some(checkpoint.validate())
}

///|
pub async fn Checkpoint::save(
  self : Checkpoint,
  path : String,
) -> Unit raise CheckpointError {
  if path.is_empty() {
    raise CheckpointError::EmptyPath
  }
  let encoded = @utf8.encode(@json.to_json(self.validate()).stringify())
  let temporary = path + ".tmp"
  @fs.write_file(
    temporary,
    encoded,
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise CheckpointError::Io(@debug.to_string(error))
  }
  @fs.rename(temporary, path, replace=true) catch {
    error => raise CheckpointError::Io(@debug.to_string(error))
  }
}
