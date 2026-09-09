///|
/// Failures at the explicit, offline Worker state transfer boundary.
pub(all) suberror WorkerMigrationError {
  InvalidIdentity
  PathConflict
  Busy(String)
  Malformed
  DigestMismatch
  CheckpointMismatch
  OutputConflict
  TargetConflict
  MissingCheckpoint
  InvalidMaximumBundleBytes(Int)
  BundleTooLarge(actual~ : Int64, limit~ : Int)
  Io(String)
  Wal(WalError)
  Task(@task.TaskError)
  CoordinatorStore(CoordinatorStoreError)
  JobStore(JobStoreError)
  Reconciliation(ReconciliationError)
} derive(Debug, Eq)

///|
/// A transfer is bound to one fenced lease transition in one Job. Incarnations
/// identify the source and replacement processes; the Task/output identity is
/// retained from the checkpoint so sink idempotency keys survive takeover.
pub(all) struct WorkerMigrationIdentity {
  cluster_id : String
  coordinator_id : String
  job_id : String
  partition : Int
  start_offset : Int64
  source_worker_id : String
  source_incarnation : String
  source_epoch : Int
  target_worker_id : String
  target_incarnation : String
  target_epoch : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Bind a source lease to its next-epoch replacement.
pub fn WorkerMigrationIdentity::new(
  cluster_id : String,
  coordinator_id : String,
  job_id : String,
  partition : Int,
  start_offset : Int64,
  source_worker_id : String,
  source_incarnation : String,
  source_epoch : Int,
  target_worker_id : String,
  target_incarnation : String,
  target_epoch : Int,
) -> WorkerMigrationIdentity raise WorkerMigrationError {
  let identity = WorkerMigrationIdentity::{
    cluster_id,
    coordinator_id,
    job_id,
    partition,
    start_offset,
    source_worker_id,
    source_incarnation,
    source_epoch,
    target_worker_id,
    target_incarnation,
    target_epoch,
  }
  identity.validate()
  identity
}

///|
pub fn WorkerMigrationIdentity::cluster_id(
  self : WorkerMigrationIdentity,
) -> String {
  self.cluster_id
}

///|
pub fn WorkerMigrationIdentity::coordinator_id(
  self : WorkerMigrationIdentity,
) -> String {
  self.coordinator_id
}

///|
pub fn WorkerMigrationIdentity::job_id(
  self : WorkerMigrationIdentity,
) -> String {
  self.job_id
}

///|
pub fn WorkerMigrationIdentity::partition(
  self : WorkerMigrationIdentity,
) -> Int {
  self.partition
}

///|
pub fn WorkerMigrationIdentity::start_offset(
  self : WorkerMigrationIdentity,
) -> Int64 {
  self.start_offset
}

///|
pub fn WorkerMigrationIdentity::source_worker_id(
  self : WorkerMigrationIdentity,
) -> String {
  self.source_worker_id
}

///|
pub fn WorkerMigrationIdentity::source_incarnation(
  self : WorkerMigrationIdentity,
) -> String {
  self.source_incarnation
}

///|
pub fn WorkerMigrationIdentity::source_epoch(
  self : WorkerMigrationIdentity,
) -> Int {
  self.source_epoch
}

///|
pub fn WorkerMigrationIdentity::target_worker_id(
  self : WorkerMigrationIdentity,
) -> String {
  self.target_worker_id
}

///|
pub fn WorkerMigrationIdentity::target_incarnation(
  self : WorkerMigrationIdentity,
) -> String {
  self.target_incarnation
}

///|
pub fn WorkerMigrationIdentity::target_epoch(
  self : WorkerMigrationIdentity,
) -> Int {
  self.target_epoch
}

///|
fn WorkerMigrationIdentity::validate(
  self : WorkerMigrationIdentity,
) -> Unit raise WorkerMigrationError {
  if self.cluster_id.is_empty() ||
    self.coordinator_id.is_empty() ||
    self.job_id.is_empty() ||
    self.partition < 0 ||
    self.start_offset < 0L ||
    self.source_worker_id.is_empty() ||
    self.source_incarnation.is_empty() ||
    self.target_worker_id.is_empty() ||
    self.target_incarnation.is_empty() ||
    self.source_worker_id == self.target_worker_id ||
    self.source_epoch < 0 ||
    self.source_epoch == 2147483647 ||
    self.target_epoch != self.source_epoch + 1 {
    raise WorkerMigrationError::InvalidIdentity
  }
}

///|
/// The canonical paths of one Task. An installed migration atomically selects
/// a complete generation through `wal_path + ".migration"`; original files
/// remain available for inspection and are never overwritten by an import.
pub(all) struct WorkerStoragePaths {
  wal_path : String
  checkpoint_path : String
  delta_wal_path : String?
  output_log_path : String?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn WorkerStoragePaths::new(
  wal_path : String,
  checkpoint_path : String,
  delta_wal_path : String?,
  output_log_path : String?,
) -> WorkerStoragePaths raise WorkerMigrationError {
  let paths = WorkerStoragePaths::{
    wal_path,
    checkpoint_path,
    delta_wal_path,
    output_log_path,
  }
  paths.validate()
  paths
}

///|
pub fn WorkerStoragePaths::wal_path(self : WorkerStoragePaths) -> String {
  self.wal_path
}

///|
pub fn WorkerStoragePaths::checkpoint_path(self : WorkerStoragePaths) -> String {
  self.checkpoint_path
}

///|
pub fn WorkerStoragePaths::delta_wal_path(self : WorkerStoragePaths) -> String? {
  self.delta_wal_path
}

///|
pub fn WorkerStoragePaths::output_log_path(
  self : WorkerStoragePaths,
) -> String? {
  self.output_log_path
}

///|
fn WorkerStoragePaths::validate(
  self : WorkerStoragePaths,
) -> Unit raise WorkerMigrationError {
  let paths = [self.wal_path, self.checkpoint_path]
  for path in [self.delta_wal_path, self.output_log_path] {
    if path is Some(path) {
      paths.push(path)
    }
  }
  for index, path in paths {
    if path.is_empty() {
      raise WorkerMigrationError::PathConflict
    }
    for other in paths[index + 1:] {
      if path == other ||
        path.has_prefix(other + ".") ||
        other.has_prefix(path + ".") {
        raise WorkerMigrationError::PathConflict
      }
    }
  }
}

///|
/// Portable logical WAL/checkpoint bundle. Its digest detects accidental
/// corruption, not malicious replacement; transfer it over a trusted channel.
/// Output records retain their Task IDs and sequences. Their epoch metadata
/// is rebound on installation, so consumers reopen a subscription for the new
/// epoch and may redeliver using the same idempotency keys.
pub struct WorkerMigration {
  version : Int
  identity : WorkerMigrationIdentity
  snapshot : @task.TaskSnapshot
  primary_records : Int
  delta_records : Int
  batches : Array[@protocol.EventBatch]
  deltas : Array[@protocol.DeltaBatch]
  outputs : Array[TaskOutputRecord]?
  digest : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn WorkerMigration::identity(
  self : WorkerMigration,
) -> WorkerMigrationIdentity {
  self.identity
}

///|
pub fn WorkerMigration::task_id(self : WorkerMigration) -> String {
  self.snapshot.task_id()
}

///|
pub fn WorkerMigration::digest(self : WorkerMigration) -> String {
  self.digest
}

///|
pub fn WorkerMigration::persisted_offset(self : WorkerMigration) -> Int64 {
  self.batches
  .last()
  .map(batch => batch.end_offset())
  .unwrap_or(self.identity.start_offset - 1L)
}

///|
fn WorkerMigration::migration_digest(self : WorkerMigration) -> String {
  manifest_json_digest(@json.to_json({ ..self, digest: "", }))
}

///|
fn WorkerMigration::validate(
  self : WorkerMigration,
) -> Unit raise WorkerMigrationError {
  self.identity.validate()
  if self.version != 1 || self.digest != self.migration_digest() {
    raise WorkerMigrationError::DigestMismatch
  }
  let identity = self.identity
  let snapshot = self.snapshot
  if snapshot.partition() != identity.partition ||
    snapshot.epoch() != identity.source_epoch ||
    snapshot.operator_capability() != snapshot.operator().capability() ||
    self.primary_records < 0 ||
    self.primary_records > self.batches.length() ||
    self.delta_records < 0 ||
    self.delta_records > self.deltas.length() {
    raise WorkerMigrationError::CheckpointMismatch
  }
  let mut next = identity.start_offset
  let prefix_events : Map[@causal.EventId, @protocol.EventRecord] = Map([])
  let add_event = (event : @protocol.EventRecord) => {
    match prefix_events.get(event.id()) {
      Some(previous) if previous != event =>
        raise WorkerMigrationError::CheckpointMismatch
      _ => prefix_events[event.id()] = event
    }
  }
  for index, raw in self.batches {
    let batch = @protocol.EventBatch::from_payload(raw.to_payload()) catch {
      _ => raise WorkerMigrationError::Malformed
    }
    if batch.partition() != identity.partition ||
      batch.epoch() != identity.source_epoch ||
      batch.first_offset() != next ||
      batch.end_offset() < next ||
      batch.end_offset() == 9223372036854775807L {
      raise WorkerMigrationError::CheckpointMismatch
    }
    next = batch.end_offset() + 1L
    if index < self.primary_records {
      for event in batch.events() {
        add_event(event)
      }
    }
  }
  let expected_offset = if self.primary_records == 0 {
    identity.start_offset - 1L
  } else {
    self.batches[self.primary_records - 1].end_offset()
  }
  if snapshot.offset() != expected_offset {
    raise WorkerMigrationError::CheckpointMismatch
  }
  let ids : Set[String] = Set([])
  for index, raw in self.deltas {
    let delta = @protocol.DeltaBatch::from_payload(raw.to_payload()) catch {
      _ => raise WorkerMigrationError::Malformed
    }
    if delta.target_partition() != identity.partition ||
      delta.target_epoch() != identity.source_epoch ||
      !ids.add_and_check(delta.delta_id()) {
      raise WorkerMigrationError::CheckpointMismatch
    }
    if index < self.delta_records {
      for event in delta.events() {
        add_event(event)
      }
    }
  }
  let events = prefix_events.values().to_array()
  events.sort_by((a, b) => a.id().compare(b.id()))
  if events != snapshot.events().to_owned() {
    raise WorkerMigrationError::CheckpointMismatch
  }
  let task = @task.Task::new(
    snapshot.task_id(),
    snapshot.operator_id(),
    snapshot.operator(),
    identity.partition,
    identity.source_epoch,
    start_offset=identity.start_offset,
  ) catch {
    error => raise WorkerMigrationError::Task(error)
  }
  task.restore(snapshot) catch {
    error => raise WorkerMigrationError::Task(error)
  }
  if self.outputs is Some(outputs) {
    if outputs.length() < snapshot.emitted_outputs() {
      raise WorkerMigrationError::OutputConflict
    }
    for index, raw in outputs {
      let record = validate_output_record(raw) catch {
        _ => raise WorkerMigrationError::OutputConflict
      }
      if record.task_id != snapshot.task_id() ||
        record.partition != identity.partition ||
        record.epoch != identity.source_epoch ||
        record.sequence != index.to_int64() {
        raise WorkerMigrationError::OutputConflict
      }
    }
  }
}

///|
async fn migration_lock(path : String) -> @fs.File raise WorkerMigrationError {
  let lock = @fs.open(
    path + ".lock",
    mode=@fs.ReadWrite,
    create_mode=@fs.OpenOrCreate,
  ) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  errdefer lock.close()
  let acquired = lock.try_lock(@fs.Exclusive) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  if !acquired {
    raise WorkerMigrationError::Busy(path)
  }
  lock
}

///|
async fn migration_replay(
  path : String,
) -> WalReplay raise WorkerMigrationError {
  let bytes = @fs.read_file(path).binary() catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  parse_records(bytes, 1024 * 1024) catch {
    error => raise WorkerMigrationError::Wal(error)
  }
}

///|
async fn optional_migration_replay(
  path : String,
) -> WalReplay? raise WorkerMigrationError {
  let exists = @fs.exists(path) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  if !exists {
    return None
  }
  Some(migration_replay(path))
}

///|
fn migration_json(encoded : Bytes) -> Json raise WorkerMigrationError {
  @json.parse(@utf8.decode(encoded)) catch {
    _ => raise WorkerMigrationError::Malformed
  }
}

///|
/// Capture a stopped Worker's verified state without modifying its files.
/// All WAL locks are held together, and a live owner returns Busy. A checkpoint
/// may lag its WAL: the suffix travels with the snapshot and replays at startup.
/// Completed/compacted output streams are deliberately ineligible for takeover.
pub async fn WorkerMigration::capture(
  identity : WorkerMigrationIdentity,
  paths : WorkerStoragePaths,
) -> WorkerMigration raise WorkerMigrationError {
  identity.validate()
  paths.validate()
  // Installation publishes through this same pointer. Hold it while resolving
  // the active generation so a capture cannot mix files from two generations.
  let pointer_lock = migration_lock(paths.wal_path + ".migration")
  defer pointer_lock.close()
  let active = paths.resolve()
  let locks : Array[@fs.File] = []
  defer (for lock in locks {
    lock.close()
  })
  for
    path in [
      active.output_log_path,
      Some(active.wal_path),
      active.delta_wal_path,
      Some(active.checkpoint_path),
    ] {
    if path is Some(path) {
      locks.push(migration_lock(path))
    }
  }
  let replay = migration_replay(active.wal_path)
  let delta_replay = match active.delta_wal_path {
    Some(path) => optional_migration_replay(path)
    None => None
  }
  let saved = Checkpoint::load(active.checkpoint_path) catch {
    _ => raise WorkerMigrationError::CheckpointMismatch
  }
  guard saved is Some(saved) else {
    raise WorkerMigrationError::MissingCheckpoint
  }
  guard saved.operator_state() is Some(state) else {
    raise WorkerMigrationError::MissingCheckpoint
  }
  let snapshot : @task.TaskSnapshot = @json.from_json(state) catch {
    _ => raise WorkerMigrationError::Malformed
  }
  if saved.partition() != identity.partition ||
    saved.epoch() != identity.source_epoch ||
    saved.state_schema() != snapshot.state_schema() ||
    saved.persisted_offset() != snapshot.offset() ||
    saved.applied_offset() != snapshot.offset() ||
    saved.frontier_digest() != snapshot.frontier_digest() {
    raise WorkerMigrationError::CheckpointMismatch
  }
  let primary_records = replay.record_count_before(saved.wal_offset()) catch {
    _ => raise WorkerMigrationError::CheckpointMismatch
  }
  let delta_records = match delta_replay {
    Some(replay) =>
      replay.record_count_before(saved.delta_wal_offset()) catch {
        _ => raise WorkerMigrationError::CheckpointMismatch
      }
    None => {
      if saved.delta_wal_offset() != 0L {
        raise WorkerMigrationError::CheckpointMismatch
      }
      0
    }
  }
  let batches = replay
    .records()
    .map(encoded => {
      let raw : @protocol.EventBatch = @json.from_json(migration_json(encoded)) catch {
        _ => raise WorkerMigrationError::Malformed
      }
      raw
    })
  let deltas = match delta_replay {
    None => []
    Some(replay) =>
      replay
      .records()
      .map(encoded => {
        let record : DeltaWalRecord = @json.from_json(migration_json(encoded)) catch {
          _ => raise WorkerMigrationError::Malformed
        }
        if record.delta_id != record.delta.delta_id() {
          raise WorkerMigrationError::Malformed
        }
        record.delta
      })
  }
  let outputs = match active.output_log_path {
    None => None
    Some(path) =>
      match optional_migration_replay(path) {
        None => Some([])
        Some(replay) =>
          Some(
            replay
            .records()
            .map(encoded => {
              let record : TaskOutputRecord = @json.from_json(
                migration_json(encoded),
              ) catch {
                _ => raise WorkerMigrationError::OutputConflict
              }
              record
            }),
          )
      }
  }
  let bundle = WorkerMigration::{
    version: 1,
    identity,
    snapshot,
    primary_records,
    delta_records,
    batches,
    deltas,
    outputs,
    digest: "",
  }
  let bundle = { ..bundle, digest: bundle.migration_digest(), }
  bundle.validate()
  bundle
}

///|
/// Write a portable bundle using an atomic, synchronized replacement.
pub async fn WorkerMigration::save(
  self : WorkerMigration,
  path : String,
) -> Unit raise WorkerMigrationError {
  self.validate()
  if path.is_empty() {
    raise WorkerMigrationError::PathConflict
  }
  let lock = migration_lock(path)
  defer lock.close()
  migration_write_json(path, @json.to_json(self))
}

///|
/// Encode a verified bundle for transport over a caller-owned channel. The
/// returned bytes are self-contained and include the integrity digest; a
/// receiver must call `WorkerMigration::from_bytes` before installing them.
pub fn WorkerMigration::to_bytes(
  self : WorkerMigration,
) -> Bytes raise WorkerMigrationError {
  self.validate()
  @utf8.encode(@json.to_json(self).stringify())
}

///|
/// Decode and verify a bundle received through a caller-owned transport.
pub fn WorkerMigration::from_bytes(
  encoded : Bytes,
) -> WorkerMigration raise WorkerMigrationError {
  let bundle : WorkerMigration = @json.from_json(migration_json(encoded)) catch {
    _ => raise WorkerMigrationError::Malformed
  }
  bundle.validate()
  bundle
}

///|
const MIGRATION_WIRE_MAGIC : Bytes = b"MWM1"

///|
const MIGRATION_WIRE_HEADER_BYTES : Int = 13

///|
fn migration_wire_header(length : Int) -> Bytes {
  let buffer = Buffer(size_hint=MIGRATION_WIRE_HEADER_BYTES)
  buffer.write_bytes(MIGRATION_WIRE_MAGIC[:])
  buffer.write_byte(1)
  buffer.write_bytes(length.to_int64().reinterpret_as_uint64().to_be_bytes()[:])
  buffer.to_bytes()
}

///|
fn migration_wire_length(
  header : Bytes,
  maximum_bundle_bytes : Int,
) -> Int raise WorkerMigrationError {
  if header.length() != MIGRATION_WIRE_HEADER_BYTES ||
    !header[0:4].equal_to_bytes(MIGRATION_WIRE_MAGIC) ||
    header[4] != 1 {
    raise WorkerMigrationError::Malformed
  }
  let mut value : UInt64 = 0UL
  for byte in header[5:] {
    value = (value << 8) | byte.to_uint64()
  }
  let limit = maximum_bundle_bytes.to_int64().reinterpret_as_uint64()
  if value > limit {
    raise WorkerMigrationError::BundleTooLarge(
      actual=value.reinterpret_as_int64(),
      limit=maximum_bundle_bytes,
    )
  }
  value.to_int()
}

///|
/// Send one verified migration bundle over a caller-owned stream. The fixed
/// header bounds the receiver before it allocates the JSON body, and the body
/// digest is checked again by the receiver.
pub async fn[W : @io.Writer] WorkerMigration::send(
  self : WorkerMigration,
  writer : W,
  maximum_bundle_bytes? : Int = 256 * 1024 * 1024,
) -> Unit raise WorkerMigrationError {
  if maximum_bundle_bytes <= 0 {
    raise WorkerMigrationError::InvalidMaximumBundleBytes(maximum_bundle_bytes)
  }
  let encoded = self.to_bytes()
  if encoded.length() > maximum_bundle_bytes {
    raise WorkerMigrationError::BundleTooLarge(
      actual=encoded.length().to_int64(),
      limit=maximum_bundle_bytes,
    )
  }
  writer.write(migration_wire_header(encoded.length())) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  writer.write(encoded) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
}

///|
/// Receive and verify one migration bundle from a caller-owned stream.
pub async fn[R : @io.Reader] WorkerMigration::receive(
  reader : R,
  maximum_bundle_bytes? : Int = 256 * 1024 * 1024,
) -> WorkerMigration raise WorkerMigrationError {
  if maximum_bundle_bytes <= 0 {
    raise WorkerMigrationError::InvalidMaximumBundleBytes(maximum_bundle_bytes)
  }
  let header = reader.read_exactly(MIGRATION_WIRE_HEADER_BYTES) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  let length = migration_wire_length(header, maximum_bundle_bytes)
  let encoded = reader.read_exactly(length) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  WorkerMigration::from_bytes(encoded)
}

///|
pub async fn WorkerMigration::load(
  path : String,
  maximum_bundle_bytes? : Int = 256 * 1024 * 1024,
) -> WorkerMigration raise WorkerMigrationError {
  if path.is_empty() {
    raise WorkerMigrationError::PathConflict
  }
  if maximum_bundle_bytes <= 0 {
    raise WorkerMigrationError::InvalidMaximumBundleBytes(maximum_bundle_bytes)
  }
  let lock = migration_lock(path)
  defer lock.close()
  let file = @fs.open(path, mode=@fs.ReadOnly) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  defer file.close()
  let size = file.size() catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  if size < 0L || size > maximum_bundle_bytes.to_int64() {
    raise WorkerMigrationError::BundleTooLarge(
      actual=size,
      limit=maximum_bundle_bytes,
    )
  }
  let encoded = file.read_all().binary() catch {
      error => raise WorkerMigrationError::Io(@debug.to_string(error))
    }
  WorkerMigration::from_bytes(encoded)
}

///|
async fn migration_write_json(
  path : String,
  value : Json,
) -> Unit raise WorkerMigrationError {
  @fs.write_file(
    path + ".tmp",
    @utf8.encode(value.stringify()),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  @fs.rename(path + ".tmp", path, replace=true) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
}

///|
struct WorkerMigrationInstallation {
  version : Int
  identity : WorkerMigrationIdentity
  task_id : String
  digest : String
  canonical : WorkerStoragePaths
  active : WorkerStoragePaths
} derive(Debug, Eq, ToJson, FromJson)

///|
async fn load_worker_installation(
  wal_path : String,
) -> WorkerMigrationInstallation? raise WorkerMigrationError {
  let path = wal_path + ".migration"
  let exists = @fs.exists(path) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  if !exists {
    return None
  }
  let json = migration_json(
    @fs.read_file(path).binary() catch {
      error => raise WorkerMigrationError::Io(@debug.to_string(error))
    },
  )
  let installed : WorkerMigrationInstallation = @json.from_json(json) catch {
    _ => raise WorkerMigrationError::Malformed
  }
  installed.identity.validate()
  installed.canonical.validate()
  installed.active.validate()
  if installed.version != 1 ||
    installed.canonical.wal_path != wal_path ||
    installed.task_id.is_empty() ||
    installed.digest.is_empty() ||
    installed.active !=
    migration_generation(installed.canonical, installed.digest) {
    raise WorkerMigrationError::TargetConflict
  }
  let required = [
    Some(installed.active.wal_path),
    Some(installed.active.checkpoint_path),
    installed.active.delta_wal_path,
    installed.active.output_log_path,
  ]
  for path in required {
    if path is Some(path) {
      let exists = @fs.exists(path) catch {
        error => raise WorkerMigrationError::Io(@debug.to_string(error))
      }
      if !exists {
        raise WorkerMigrationError::TargetConflict
      }
    }
  }
  Some(installed)
}

///|
fn migration_generation(
  paths : WorkerStoragePaths,
  digest : String,
) -> WorkerStoragePaths {
  let suffix = ".migration-" + digest
  {
    wal_path: paths.wal_path + suffix,
    checkpoint_path: paths.checkpoint_path + suffix,
    delta_wal_path: paths.delta_wal_path.map(path => path + suffix),
    output_log_path: paths.output_log_path.map(path => path + suffix),
  }
}

///|
/// Resolve the atomically selected storage generation. Call this when reading
/// Worker checkpoints or outputs outside the running Task adapter as well.
pub async fn WorkerStoragePaths::resolve(
  self : WorkerStoragePaths,
) -> WorkerStoragePaths raise WorkerMigrationError {
  self.validate()
  match load_worker_installation(self.wal_path) {
    None => self
    Some(installed) => {
      if installed.canonical != self {
        raise WorkerMigrationError::TargetConflict
      }
      installed.active
    }
  }
}

///|
/// Resolve a Task runtime's optional files through the published generation
/// pointer. A marker without a checkpoint is rejected instead of silently
/// falling back to the old generation.
pub async fn resolve_worker_storage_paths(
  wal_path : String,
  checkpoint_path : String?,
  delta_wal_path : String?,
  output_log_path : String?,
) -> WorkerStoragePaths raise WorkerMigrationError {
  guard checkpoint_path is Some(checkpoint) else {
    match load_worker_installation(wal_path) {
      Some(_) => raise WorkerMigrationError::PathConflict
      None =>
        return {
          wal_path,
          checkpoint_path: wal_path + ".checkpoint-unused",
          delta_wal_path,
          output_log_path,
        }
    }
  }
  let paths = WorkerStoragePaths::{
    wal_path,
    checkpoint_path: checkpoint,
    delta_wal_path,
    output_log_path,
  }
  paths.resolve()
}

///|
/// Recover the stable Task ID retained by a previously installed transfer.
pub async fn migrated_task_id(
  wal_path : String,
) -> String? raise WorkerMigrationError {
  load_worker_installation(wal_path).map(value => value.task_id)
}

///|
/// Validate the target process before any imported file is opened. The
/// handshake remains authoritative for epoch/start offset and will reject a
/// pointer that was prepared for a different lease.
pub async fn validate_migration_target(
  wal_path : String,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  incarnation : String,
  partition : Int,
  task_id : String,
) -> Unit raise WorkerMigrationError {
  match load_worker_installation(wal_path) {
    None => ()
    Some(installed) => {
      let identity = installed.identity
      if identity.cluster_id != cluster_id ||
        identity.coordinator_id != coordinator_id ||
        identity.target_worker_id != worker_id ||
        identity.target_incarnation != incarnation ||
        identity.partition != partition ||
        installed.task_id != task_id {
        raise WorkerMigrationError::InvalidIdentity
      }
    }
  }
}

///|
/// Stage every file, then publish one atomic pointer. Repeating an identical
/// installation is a no-op, including after the target has processed a suffix.
/// A different bundle for an already selected epoch is rejected. The caller
/// must fence the old lease and commit migration cursors before starting the
/// replacement; its ordinary handshake still validates the authoritative epoch.
pub async fn WorkerMigration::install(
  self : WorkerMigration,
  expected : WorkerMigrationIdentity,
  paths : WorkerStoragePaths,
) -> Unit raise WorkerMigrationError {
  self.validate()
  paths.validate()
  if self.identity != expected {
    raise WorkerMigrationError::InvalidIdentity
  }
  if (!self.deltas.is_empty() && paths.delta_wal_path is None) ||
    (self.outputs is Some(_)) != (paths.output_log_path is Some(_)) {
    raise WorkerMigrationError::PathConflict
  }
  let owner = migration_lock(paths.wal_path + ".migration")
  defer owner.close()
  match load_worker_installation(paths.wal_path) {
    Some(previous) => {
      if previous.digest == self.digest &&
        previous.canonical == paths &&
        previous.identity == expected {
        return
      }
      raise WorkerMigrationError::TargetConflict
    }
    None => ()
  }
  let current = paths.resolve()
  let locks : Array[@fs.File] = []
  defer (for lock in locks {
    lock.close()
  })
  for
    path in [
      current.output_log_path,
      Some(current.wal_path),
      current.delta_wal_path,
      Some(current.checkpoint_path),
    ] {
    if path is Some(path) {
      locks.push(migration_lock(path))
    }
  }
  let active = migration_generation(paths, self.digest)
  let batches = self.batches.map(batch => @protocol.EventBatch::{
    ..batch,
    epoch: expected.target_epoch,
  })
  let deltas = self.deltas.map(delta => @protocol.DeltaBatch::{
    ..delta,
    target_epoch: expected.target_epoch,
  })
  let primary_payloads = batches.map(batch => @utf8.encode(batch.to_payload()))
  let delta_payloads = deltas.map(delta => {
    @utf8.encode(
      @json.to_json(DeltaWalRecord::{ delta_id: delta.delta_id(), delta, }).stringify(),
    )
  })
  let wal_offset = migration_stage_wal(
    active.wal_path,
    primary_payloads,
    self.primary_records,
  )
  let delta_wal_offset = match active.delta_wal_path {
    None => 0L
    Some(path) => migration_stage_wal(path, delta_payloads, self.delta_records)
  }
  if active.output_log_path is Some(path) && self.outputs is Some(outputs) {
    ignore(
      migration_stage_wal(
        path,
        outputs.map(record => {
          @utf8.encode(
            @json.to_json(TaskOutputRecord::{
              ..record,
              epoch: expected.target_epoch,
            }).stringify(),
          )
        }),
        0,
      ),
    )
  }
  let snapshot = self.snapshot.rebind(self.task_id(), expected.target_epoch) catch {
    error => raise WorkerMigrationError::Task(error)
  }
  let checkpoint = Checkpoint::new(
    expected.partition,
    expected.target_epoch,
    wal_offset,
    snapshot.offset(),
    snapshot.offset(),
    snapshot.frontier_digest(),
    delta_wal_offset~,
    state_schema=snapshot.state_schema(),
    operator_state=@json.to_json(snapshot),
  ) catch {
    _ => raise WorkerMigrationError::CheckpointMismatch
  }
  checkpoint.save(active.checkpoint_path) catch {
    _ => raise WorkerMigrationError::CheckpointMismatch
  }
  migration_write_json(
    paths.wal_path + ".migration",
    @json.to_json(WorkerMigrationInstallation::{
      version: 1,
      identity: expected,
      task_id: self.task_id(),
      digest: self.digest,
      canonical: paths,
      active,
    }),
  )
}

///|
async fn migration_stage_wal(
  path : String,
  payloads : Array[Bytes],
  prefix : Int,
) -> Int64 raise WorkerMigrationError {
  let bytes = encode_records(payloads[:], 1024 * 1024) catch {
    error => raise WorkerMigrationError::Wal(error)
  }
  let prefix_bytes = encode_records(payloads[:prefix], 1024 * 1024) catch {
    error => raise WorkerMigrationError::Wal(error)
  }
  @fs.write_file(path, bytes, create_mode=@fs.CreateOrTruncate, sync=@fs.Full) catch {
    error => raise WorkerMigrationError::Io(@debug.to_string(error))
  }
  prefix_bytes.length().to_int64()
}

///|
/// A replayable cursor commit. The sidecar is separate from state files: if a
/// process dies after one store is advanced, the next call observes desired
/// values and advances only the lagging store.
struct WorkerMigrationCursorEntry {
  version : Int
  kind : String
  bundle : WorkerMigration?
  digest : String?
  identity : WorkerMigrationIdentity?
} derive(Debug, Eq, ToJson, FromJson)

///|
fn migration_cursor_entry(
  kind : String,
  bundle : WorkerMigration?,
) -> WorkerMigrationCursorEntry {
  {
    version: 1,
    kind,
    bundle,
    digest: bundle.map(value => value.digest),
    identity: bundle.map(value => value.identity),
  }
}

///|
fn migration_cursor_commit_entry(
  bundle : WorkerMigration,
) -> WorkerMigrationCursorEntry {
  {
    version: 1,
    kind: "commit",
    bundle: None,
    digest: Some(bundle.digest),
    identity: Some(bundle.identity),
  }
}

///|
async fn apply_migration_cursors(
  coordinator : CoordinatorStore,
  job : JobStore,
  bundle : WorkerMigration,
) -> Unit raise WorkerMigrationError {
  let identity = bundle.identity
  if identity.cluster_id != coordinator.state.cluster_id() ||
    identity.coordinator_id != coordinator.state.coordinator_id() ||
    identity.job_id != job.job_id() {
    raise WorkerMigrationError::InvalidIdentity
  }
  let target_incarnation = @protocol.Incarnation::new(
    identity.target_incarnation,
  ) catch {
    _ => raise WorkerMigrationError::InvalidIdentity
  }
  guard coordinator.state.assignment(identity.partition) is Some(lease) else {
    raise WorkerMigrationError::InvalidIdentity
  }
  guard job.state.partition(identity.partition) is Some(job_partition) else {
    raise WorkerMigrationError::InvalidIdentity
  }
  if lease.worker_id() != identity.target_worker_id ||
    lease.incarnation() != target_incarnation ||
    lease.epoch() != identity.target_epoch ||
    lease.start_offset() != identity.start_offset ||
    job_partition.worker_id() != identity.target_worker_id ||
    job_partition.epoch() != identity.target_epoch ||
    job_partition.start_offset() != identity.start_offset {
    raise WorkerMigrationError::InvalidIdentity
  }
  let sent_end = bundle.persisted_offset()
  let desired_end = bundle.snapshot.offset()
  let desired_next = if sent_end < identity.start_offset {
    identity.start_offset
  } else if sent_end == 9223372036854775807L {
    raise WorkerMigrationError::CheckpointMismatch
  } else {
    sent_end + 1L
  }
  if lease.next_offset() > desired_next ||
    job_partition.next_offset() > desired_next ||
    lease.persisted_offset() > sent_end ||
    lease.applied_offset() > sent_end ||
    job_partition.persisted_offset() > sent_end ||
    job_partition.applied_offset() > sent_end {
    raise WorkerMigrationError::TargetConflict
  }
  if lease.next_offset() < desired_next {
    let entry = CoordinatorJournalEntry::{
      ..empty_entry("batch_sent"),
      worker_id: identity.target_worker_id,
      incarnation: identity.target_incarnation,
      partition: identity.partition,
      epoch: identity.target_epoch,
      first_offset: lease.next_offset(),
      event_count: desired_next - lease.next_offset(),
    }
    let preview = coordinator.preview_state() catch {
      error => raise WorkerMigrationError::CoordinatorStore(error)
    }
    ignore(
      preview.record_batch_sent(
        identity.partition,
        identity.target_worker_id,
        target_incarnation,
        identity.target_epoch,
        lease.next_offset(),
        entry.event_count,
      ) catch {
        error =>
          raise WorkerMigrationError::CoordinatorStore(
            CoordinatorStoreError::Coordinator(error),
          )
      },
    )
    coordinator.append_entry(entry) catch {
      error => raise WorkerMigrationError::CoordinatorStore(error)
    }
    ignore(
      coordinator.state.record_batch_sent(
        identity.partition,
        identity.target_worker_id,
        target_incarnation,
        identity.target_epoch,
        lease.next_offset(),
        desired_next - lease.next_offset(),
      ) catch {
        error =>
          raise WorkerMigrationError::CoordinatorStore(
            CoordinatorStoreError::Coordinator(error),
          )
      },
    )
  }
  guard coordinator.state.assignment(identity.partition) is Some(updated_lease) else {
    raise WorkerMigrationError::InvalidIdentity
  }
  if updated_lease.persisted_offset() < desired_end ||
    updated_lease.applied_offset() < desired_end {
    let persisted_offset = if updated_lease.persisted_offset() > desired_end {
      updated_lease.persisted_offset()
    } else {
      desired_end
    }
    let applied_offset = if updated_lease.applied_offset() > desired_end {
      updated_lease.applied_offset()
    } else {
      desired_end
    }
    let entry = CoordinatorJournalEntry::{
      ..empty_entry("progress"),
      worker_id: identity.target_worker_id,
      incarnation: identity.target_incarnation,
      partition: identity.partition,
      epoch: identity.target_epoch,
      persisted_offset,
      applied_offset,
    }
    let preview = coordinator.preview_state() catch {
      error => raise WorkerMigrationError::CoordinatorStore(error)
    }
    preview.record_progress(
      identity.partition,
      identity.target_worker_id,
      target_incarnation,
      identity.target_epoch,
      persisted_offset,
      applied_offset,
    ) catch {
      error =>
        raise WorkerMigrationError::CoordinatorStore(
          CoordinatorStoreError::Coordinator(error),
        )
    }
    coordinator.append_entry(entry) catch {
      error => raise WorkerMigrationError::CoordinatorStore(error)
    }
    coordinator.state.record_progress(
      identity.partition,
      identity.target_worker_id,
      target_incarnation,
      identity.target_epoch,
      persisted_offset,
      applied_offset,
    ) catch {
      error =>
        raise WorkerMigrationError::CoordinatorStore(
          CoordinatorStoreError::Coordinator(error),
        )
    }
  }
  guard job.state.partition(identity.partition) is Some(updated_job) else {
    raise WorkerMigrationError::InvalidIdentity
  }
  if updated_job.next_offset() < desired_next {
    ignore(
      job.reconcile_record_batch_unlocked(
        identity.partition,
        identity.target_worker_id,
        identity.target_epoch,
        updated_job.next_offset(),
        desired_next - updated_job.next_offset(),
      ) catch {
        error => raise WorkerMigrationError::JobStore(error)
      },
    )
  }
  guard job.state.partition(identity.partition) is Some(updated_job) else {
    raise WorkerMigrationError::InvalidIdentity
  }
  if updated_job.persisted_offset() < desired_end ||
    updated_job.applied_offset() < desired_end {
    let persisted_offset = if updated_job.persisted_offset() > desired_end {
      updated_job.persisted_offset()
    } else {
      desired_end
    }
    let applied_offset = if updated_job.applied_offset() > desired_end {
      updated_job.applied_offset()
    } else {
      desired_end
    }
    job.reconcile_record_progress_unlocked(
      identity.partition,
      identity.target_worker_id,
      identity.target_epoch,
      persisted_offset,
      applied_offset,
    ) catch {
      error => raise WorkerMigrationError::JobStore(error)
    }
  }
}

///|
/// Commit the advanced source cursor after installation and ordinary recovery
/// have succeeded. The target must remain stopped until this returns. It is
/// safe to retry after a crash.
pub async fn commit_worker_migration(
  coordinator : CoordinatorStore,
  job : JobStore,
  bundle : WorkerMigration,
  reconcile_path? : String,
) -> Unit raise WorkerMigrationError {
  bundle.validate()
  if !job.supervisor_lock.try_acquire() {
    raise WorkerMigrationError::JobStore(JobStoreError::AlreadyRunning)
  }
  defer job.supervisor_lock.release()
  let path = match reconcile_path {
    Some(path) if !path.is_empty() => path
    Some(_) => raise WorkerMigrationError::PathConflict
    None => job.journal_path() + ".migration-cursors"
  }
  if path == job.journal_path() ||
    path == job.checkpoint_path() ||
    path == coordinator.journal_path() ||
    path == coordinator.checkpoint_path() {
    raise WorkerMigrationError::PathConflict
  }
  let (journal, replay) = Wal::open(path) catch {
    error => raise WorkerMigrationError::Wal(error)
  }
  defer journal.close()
  let mut pending : WorkerMigration? = None
  let mut completed_digest : String? = None
  let mut completed_identity : WorkerMigrationIdentity? = None
  for encoded in replay.records() {
    let entry : WorkerMigrationCursorEntry = @json.from_json(
      migration_json(encoded),
    ) catch {
      _ => raise WorkerMigrationError::Malformed
    }
    if entry.version != 1 {
      raise WorkerMigrationError::Malformed
    }
    match entry.kind {
      "prepare" => {
        guard entry.bundle is Some(value) else {
          raise WorkerMigrationError::Malformed
        }
        value.validate()
        completed_digest = None
        completed_identity = None
        match pending {
          Some(previous) if previous != value =>
            raise WorkerMigrationError::TargetConflict
          _ => pending = Some(value)
        }
      }
      "commit" => {
        match entry.bundle {
          None => ()
          Some(value) => {
            value.validate()
            completed_digest = Some(value.digest)
            completed_identity = Some(value.identity)
          }
        }
        match (entry.bundle, entry.digest, entry.identity) {
          (None, Some(digest), Some(identity)) => {
            if digest.is_empty() {
              raise WorkerMigrationError::Malformed
            }
            identity.validate()
            completed_digest = Some(digest)
            completed_identity = Some(identity)
          }
          (None, Some(_), None) | (None, None, Some(_)) =>
            raise WorkerMigrationError::Malformed
          _ => ()
        }
        pending = None
      }
      _ => raise WorkerMigrationError::Malformed
    }
  }
  match (completed_digest, completed_identity) {
    (Some(digest), Some(identity)) if digest == bundle.digest &&
      identity == bundle.identity => return
    // A later epoch may reuse the same sidecar. Its prepare record below
    // starts a new transaction and supersedes this completed one.
    (Some(_), Some(_)) => ()
    (None, None) => ()
    _ => raise WorkerMigrationError::Malformed
  }
  job.write_lock.acquire() catch {
    error =>
      raise WorkerMigrationError::JobStore(
        JobStoreError::Io(@debug.to_string(error)),
      )
  }
  defer job.write_lock.release()
  coordinator.write_lock.acquire() catch {
    error =>
      raise WorkerMigrationError::CoordinatorStore(
        CoordinatorStoreError::Io(@debug.to_string(error)),
      )
  }
  defer coordinator.write_lock.release()
  let pending = match pending {
    Some(value) => {
      if value != bundle {
        raise WorkerMigrationError::TargetConflict
      }
      value
    }
    None => {
      ignore(
        journal.append(
          @utf8.encode(
            @json.to_json(migration_cursor_entry("prepare", Some(bundle))).stringify(),
          ),
        ) catch {
          error => raise WorkerMigrationError::Wal(error)
        },
      )
      bundle
    }
  }
  apply_migration_cursors(coordinator, job, pending)
  ignore(
    journal.append(
      @utf8.encode(
        @json.to_json(migration_cursor_commit_entry(pending)).stringify(),
      ),
    ) catch {
      error => raise WorkerMigrationError::Wal(error)
    },
  )
  journal.rewrite([
    @utf8.encode(
      @json.to_json(migration_cursor_commit_entry(pending)).stringify(),
    ),
  ]) catch {
    error => raise WorkerMigrationError::Wal(error)
  }
}
