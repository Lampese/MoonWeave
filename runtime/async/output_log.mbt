///|
/// Errors raised by the durable Task output log.
pub(all) suberror OutputLogError {
  EmptyPath
  EmptyTaskId
  InvalidPartition(Int)
  InvalidEpoch(Int)
  InvalidSequence(Int64)
  SequenceGap(expected~ : Int64, actual~ : Int64)
  Malformed
  UnsupportedVersion(Int)
  IdentityConflict(String)
  InvalidRetention(Int64)
  RetentionConflict(String)
  Wal(WalError)
  Io(String)
} derive(Debug, Eq)

///|
/// A retention barrier issued by a completed JobOutputManifest. The barrier
/// carries the full historical count and digest so an output WAL can discard
/// its physical prefix without changing the Job's durable certificate.
pub struct TaskOutputRetentionBarrier {
  task_id : String
  partition : Int
  epoch : Int
  retain_from : Int64
  output_count : Int64
  output_digest : String
} derive(Debug, Eq)

///|
pub fn TaskOutputRetentionBarrier::task_id(
  self : TaskOutputRetentionBarrier,
) -> String {
  self.task_id
}

///|
pub fn TaskOutputRetentionBarrier::partition(
  self : TaskOutputRetentionBarrier,
) -> Int {
  self.partition
}

///|
pub fn TaskOutputRetentionBarrier::epoch(
  self : TaskOutputRetentionBarrier,
) -> Int {
  self.epoch
}

///|
pub fn TaskOutputRetentionBarrier::retain_from(
  self : TaskOutputRetentionBarrier,
) -> Int64 {
  self.retain_from
}

///|
pub fn TaskOutputRetentionBarrier::output_count(
  self : TaskOutputRetentionBarrier,
) -> Int64 {
  self.output_count
}

///|
pub fn TaskOutputRetentionBarrier::output_digest(
  self : TaskOutputRetentionBarrier,
) -> String {
  self.output_digest
}

///|
/// One durable Task output. Sequence numbers are allocated by the Task
/// adapter, while the output log makes `(task_id, sequence)` idempotent.
pub(all) struct TaskOutputRecord {
  version : Int
  task_id : String
  partition : Int
  epoch : Int
  sequence : Int64
  output : @task.TaskOutput
} derive(Debug, Eq, ToJson, FromJson)

///|
struct OutputRetentionMarker {
  version : Int
  kind : String
  task_id : String
  partition : Int
  epoch : Int
  retain_from : Int64
  output_count : Int64
  output_digest : String
} derive(Debug, Eq, ToJson, FromJson)

///|
struct OutputTaskIdentity {
  partition : Int
  epoch : Int
} derive(Debug, Eq)

///|
struct OutputRetentionMetadata {
  task_id : String
  partition : Int
  epoch : Int
  retain_from : Int64
  output_count : Int64
  output_digest : String
} derive(Debug, Eq)

///|
pub fn TaskOutputRecord::version(self : TaskOutputRecord) -> Int {
  self.version
}

///|
pub fn TaskOutputRecord::task_id(self : TaskOutputRecord) -> String {
  self.task_id
}

///|
pub fn TaskOutputRecord::partition(self : TaskOutputRecord) -> Int {
  self.partition
}

///|
pub fn TaskOutputRecord::epoch(self : TaskOutputRecord) -> Int {
  self.epoch
}

///|
pub fn TaskOutputRecord::sequence(self : TaskOutputRecord) -> Int64 {
  self.sequence
}

///|
pub fn TaskOutputRecord::output(self : TaskOutputRecord) -> @task.TaskOutput {
  self.output
}

///|
/// Stable identity for an idempotent sink. Task IDs must be unique across jobs
/// and attempts, as with TaskSpec::from_job.
pub fn TaskOutputRecord::idempotency_key(self : TaskOutputRecord) -> String {
  output_key(self.task_id, self.sequence)
}

///|
/// Durable, append-only output storage. It is intentionally separate from the
/// input WAL: consumers may acknowledge or compact outputs independently of
/// input replay.
pub struct TaskOutputLog {
  wal : Wal
  mut records : Array[TaskOutputRecord]
  known : Map[String, TaskOutputRecord]
  identities : Map[String, OutputTaskIdentity]
  retention : Map[String, OutputRetentionMetadata]
  next_sequences : Map[String, Int64]
  write_lock : @async.Mutex
}

///|
fn output_key(task_id : String, sequence : Int64) -> String {
  task_id.length().to_string() + ":" + task_id + ":" + sequence.to_string()
}

///|
fn output_retention_marker(metadata : OutputRetentionMetadata) -> Bytes {
  @utf8.encode(
    @json.to_json(OutputRetentionMarker::{
      version: 1,
      kind: "retention",
      task_id: metadata.task_id,
      partition: metadata.partition,
      epoch: metadata.epoch,
      retain_from: metadata.retain_from,
      output_count: metadata.output_count,
      output_digest: metadata.output_digest,
    }).stringify(),
  )
}

///|
fn validate_output_record(
  record : TaskOutputRecord,
) -> TaskOutputRecord raise OutputLogError {
  if record.version != 1 {
    raise OutputLogError::UnsupportedVersion(record.version)
  }
  if record.task_id.is_empty() {
    raise OutputLogError::EmptyTaskId
  }
  if record.partition < 0 {
    raise OutputLogError::InvalidPartition(record.partition)
  }
  if record.epoch < 0 {
    raise OutputLogError::InvalidEpoch(record.epoch)
  }
  if record.sequence < 0L || record.sequence == 9223372036854775807L {
    raise OutputLogError::InvalidSequence(record.sequence)
  }
  {
    version: 1,
    task_id: record.task_id,
    partition: record.partition,
    epoch: record.epoch,
    sequence: record.sequence,
    output: record.output,
  }
}

///|
fn validate_retention_metadata(
  metadata : OutputRetentionMetadata,
) -> OutputRetentionMetadata raise OutputLogError {
  if metadata.task_id.is_empty() {
    raise OutputLogError::EmptyTaskId
  }
  if metadata.partition < 0 {
    raise OutputLogError::InvalidPartition(metadata.partition)
  }
  if metadata.epoch < 0 {
    raise OutputLogError::InvalidEpoch(metadata.epoch)
  }
  if metadata.retain_from < 0L ||
    metadata.output_count < metadata.retain_from ||
    metadata.output_digest.is_empty() {
    raise OutputLogError::InvalidRetention(metadata.retain_from)
  }
  metadata
}

///|
fn retention_metadata_from_barrier(
  barrier : TaskOutputRetentionBarrier,
) -> OutputRetentionMetadata raise OutputLogError {
  validate_retention_metadata(OutputRetentionMetadata::{
    task_id: barrier.task_id,
    partition: barrier.partition,
    epoch: barrier.epoch,
    retain_from: barrier.retain_from,
    output_count: barrier.output_count,
    output_digest: barrier.output_digest,
  })
}

///|
fn output_retention_marker_from_json(
  json : Json,
) -> OutputRetentionMarker? raise OutputLogError {
  guard json is Object(fields) else { return None }
  guard fields.get("kind") is Some(String(kind)) else { return None }
  if kind != "retention" {
    return None
  }
  let marker : OutputRetentionMarker = @json.from_json(json) catch {
    _ => raise OutputLogError::Malformed
  }
  let metadata = validate_retention_metadata(OutputRetentionMetadata::{
    task_id: marker.task_id,
    partition: marker.partition,
    epoch: marker.epoch,
    retain_from: marker.retain_from,
    output_count: marker.output_count,
    output_digest: marker.output_digest,
  })
  if marker.version != 1 {
    raise OutputLogError::UnsupportedVersion(marker.version)
  }
  Some(OutputRetentionMarker::{
    version: marker.version,
    kind: marker.kind,
    task_id: metadata.task_id,
    partition: metadata.partition,
    epoch: metadata.epoch,
    retain_from: metadata.retain_from,
    output_count: metadata.output_count,
    output_digest: metadata.output_digest,
  })
}

///|
pub async fn TaskOutputLog::open(
  path : String,
) -> TaskOutputLog raise OutputLogError {
  if path.is_empty() {
    raise OutputLogError::EmptyPath
  }
  let (wal, replay) = Wal::open(path) catch {
    error => raise OutputLogError::Wal(error)
  }
  errdefer wal.close()
  let records : Array[TaskOutputRecord] = []
  let known : Map[String, TaskOutputRecord] = Map([])
  let identities : Map[String, OutputTaskIdentity] = Map([])
  let retention : Map[String, OutputRetentionMetadata] = Map([])
  let next_sequences : Map[String, Int64] = Map([])
  for encoded in replay.records() {
    let text = @utf8.decode(encoded) catch {
      error => raise OutputLogError::Io(@debug.to_string(error))
    }
    let json = @json.parse(text) catch { _ => raise OutputLogError::Malformed }
    match output_retention_marker_from_json(json) {
      Some(marker) => {
        let metadata = OutputRetentionMetadata::{
          task_id: marker.task_id,
          partition: marker.partition,
          epoch: marker.epoch,
          retain_from: marker.retain_from,
          output_count: marker.output_count,
          output_digest: marker.output_digest,
        }
        match identities.get(marker.task_id) {
          Some(previous) if previous.partition != marker.partition ||
            previous.epoch != marker.epoch =>
            raise OutputLogError::IdentityConflict(marker.task_id)
          _ => ()
        }
        identities[marker.task_id] = OutputTaskIdentity::{
          partition: marker.partition,
          epoch: marker.epoch,
        }
        match retention.get(marker.task_id) {
          Some(previous) if previous != metadata =>
            raise OutputLogError::RetentionConflict(marker.task_id)
          _ => retention[marker.task_id] = metadata
        }
        let expected = next_sequences.get(marker.task_id).unwrap_or(0L)
        if expected < marker.retain_from {
          next_sequences[marker.task_id] = marker.retain_from
        }
      }
      None => {
        let raw : TaskOutputRecord = @json.from_json(json) catch {
          _ => raise OutputLogError::Malformed
        }
        let record = validate_output_record(raw)
        let key = output_key(record.task_id, record.sequence)
        match known.get(key) {
          Some(previous) =>
            if previous != record {
              raise OutputLogError::IdentityConflict(key)
            }
          None => {
            let base = retention
              .get(record.task_id)
              .map(value => value.retain_from)
              .unwrap_or(0L)
            let expected = next_sequences.get(record.task_id).unwrap_or(base)
            if record.sequence != expected {
              raise OutputLogError::SequenceGap(
                expected~,
                actual=record.sequence,
              )
            }
            match identities.get(record.task_id) {
              Some(previous) if previous.partition != record.partition ||
                previous.epoch != record.epoch =>
                raise OutputLogError::IdentityConflict(record.task_id)
              _ => ()
            }
            identities[record.task_id] = OutputTaskIdentity::{
              partition: record.partition,
              epoch: record.epoch,
            }
            records.push(record)
            known[key] = record
            next_sequences[record.task_id] = expected + 1L
          }
        }
      }
    }
  }
  for metadata in retention.values() {
    let next = next_sequences.get(metadata.task_id).unwrap_or(0L)
    if next != metadata.output_count {
      raise OutputLogError::SequenceGap(
        expected=next,
        actual=metadata.output_count,
      )
    }
  }
  records.sort_by((left, right) => {
    let task_order = left.task_id.compare(right.task_id)
    if task_order == 0 {
      left.sequence.compare(right.sequence)
    } else {
      task_order
    }
  })
  {
    wal,
    records,
    known,
    identities,
    retention,
    next_sequences,
    write_lock: @async.Mutex::Mutex(),
  }
}

///|
fn check_output_task_identity(
  known : Map[String, TaskOutputRecord],
  identities : Map[String, OutputTaskIdentity],
  task_id : String,
  partition : Int,
  epoch : Int,
) -> Unit raise OutputLogError {
  match identities.get(task_id) {
    Some(previous) if previous.partition != partition || previous.epoch != epoch =>
      raise OutputLogError::IdentityConflict(task_id)
    _ => ()
  }
  match known.get(output_key(task_id, 0L)) {
    Some(first) if first.partition != partition || first.epoch != epoch =>
      raise OutputLogError::IdentityConflict(task_id)
    _ => ()
  }
}

///|
fn TaskOutputLog::check_identity(
  self : TaskOutputLog,
  task_id : String,
  partition : Int,
  epoch : Int,
) -> Unit raise OutputLogError {
  check_output_task_identity(
    self.known,
    self.identities,
    task_id,
    partition,
    epoch,
  )
}

///|
pub fn TaskOutputLog::close(self : TaskOutputLog) -> Unit {
  self.wal.close()
}

///|
pub fn TaskOutputLog::path(self : TaskOutputLog) -> String {
  self.wal.path()
}

///|
/// Return the source WAL's current lifecycle statistics. This is an in-memory
/// snapshot; it does not acquire an additional filesystem handle.
pub fn TaskOutputLog::stats(self : TaskOutputLog) -> WalStats {
  self.wal.stats()
}

///|
/// Return the first sequence still physically retained for a Task. A log that
/// has not crossed a retention barrier returns zero.
pub fn TaskOutputLog::retained_from(
  self : TaskOutputLog,
  task_id : String,
) -> Int64 {
  self.retention.get(task_id).map(value => value.retain_from).unwrap_or(0L)
}

///|
/// Return whether a sequence was removed by a committed retention barrier.
/// Consumers may use this only for already-acknowledged records; new delivery
/// always starts at `next_sequence`.
pub fn TaskOutputLog::is_trimmed(
  self : TaskOutputLog,
  task_id : String,
  sequence : Int64,
) -> Bool {
  sequence >= 0L && sequence < self.retained_from(task_id)
}

///|
pub fn TaskOutputLog::records(self : TaskOutputLog) -> Array[TaskOutputRecord] {
  self.records.copy()
}

///|
pub fn TaskOutputLog::records_for(
  self : TaskOutputLog,
  task_id : String,
) -> Array[TaskOutputRecord] {
  let result = self.records.filter(record => record.task_id == task_id)
  result.sort_by((left, right) => left.sequence.compare(right.sequence))
  result
}

///|
/// Deterministic digest of one Task's historical output stream. After a
/// retention barrier this returns the persisted full-stream digest even though
/// the physical prefix is no longer present. It is a local consistency
/// fingerprint, not an authentication tag.
pub fn TaskOutputLog::output_digest(
  self : TaskOutputLog,
  task_id : String,
) -> String {
  match self.retention.get(task_id) {
    Some(metadata) => metadata.output_digest
    None => manifest_records_digest(self.records_for(task_id))
  }
}

///|
/// Return the next output sequence expected for one Task.
pub fn TaskOutputLog::next_sequence(
  self : TaskOutputLog,
  task_id : String,
) -> Int64 {
  self.next_sequences.get(task_id).unwrap_or(0L)
}

///|
/// Read durable outputs strictly after a consumer checkpoint. Consumer
/// checkpoint compaction does not delete source records; the output WAL keeps
/// its zero-based proof prefix until a manifest retention barrier is committed.
pub fn TaskOutputLog::records_after(
  self : TaskOutputLog,
  task_id : String,
  sequence : Int64,
) -> Array[TaskOutputRecord] {
  let result = self.records.filter(record => {
    record.task_id == task_id && record.sequence > sequence
  })
  result.sort_by((left, right) => left.sequence.compare(right.sequence))
  result
}

///|
/// Atomically trim a Task's physical output prefix after its Job manifest has
/// issued a retention barrier. The barrier must cover the complete historical
/// count and digest, and the Task must already be terminal and fully consumed;
/// callers obtain it from `JobOutputManifest::seal_partition_for_retention`.
/// Other Tasks sharing this WAL remain intact.
pub async fn TaskOutputLog::compact(
  self : TaskOutputLog,
  barrier : TaskOutputRetentionBarrier,
) -> Unit raise OutputLogError {
  let metadata = retention_metadata_from_barrier(barrier)
  self.write_lock.acquire() catch {
    error => raise OutputLogError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.wal.check_writable() catch {
    error => raise OutputLogError::Wal(error)
  }
  self.check_identity(metadata.task_id, metadata.partition, metadata.epoch)
  let expected_count = self.next_sequence(metadata.task_id)
  if metadata.output_count != expected_count {
    raise OutputLogError::RetentionConflict(metadata.task_id)
  }
  if self.output_digest(metadata.task_id) != metadata.output_digest {
    raise OutputLogError::RetentionConflict(metadata.task_id)
  }
  match self.retention.get(metadata.task_id) {
    Some(previous) if previous != metadata =>
      raise OutputLogError::RetentionConflict(metadata.task_id)
    _ => ()
  }
  let retained_records = self.records.filter(record => {
    record.task_id != metadata.task_id ||
    record.sequence >= metadata.retain_from
  })
  let marker_map = self.retention.copy()
  marker_map[metadata.task_id] = metadata
  let marker_tasks = marker_map.keys().to_array()
  marker_tasks.sort()
  let payloads : Array[Bytes] = []
  for task_id in marker_tasks {
    guard marker_map.get(task_id) is Some(value) else {
      raise OutputLogError::Malformed
    }
    payloads.push(output_retention_marker(value))
  }
  for record in retained_records {
    payloads.push(@utf8.encode(@json.to_json(record).stringify()))
  }
  self.wal.rewrite(payloads[:]) catch {
    error => raise OutputLogError::Wal(error)
  }
  self.records = retained_records
  let keys = self.known.keys().to_array()
  for key in keys {
    guard self.known.get(key) is Some(record) else { continue }
    if record.task_id == metadata.task_id &&
      record.sequence < metadata.retain_from {
      self.known.remove(key)
    }
  }
  // Rebuild known entries for the retained Task in case the key format ever
  // changes; records from other Tasks were never removed.
  for record in self.records {
    self.known[output_key(record.task_id, record.sequence)] = record
  }
  self.retention[metadata.task_id] = metadata
  self.identities[metadata.task_id] = OutputTaskIdentity::{
    partition: metadata.partition,
    epoch: metadata.epoch,
  }
  self.next_sequences[metadata.task_id] = metadata.output_count
}

///|
/// Append a contiguous output range. Equal records are silently treated as
/// retries; a reused sequence with different content fails closed.
pub async fn TaskOutputLog::append(
  self : TaskOutputLog,
  task_id : String,
  partition : Int,
  epoch : Int,
  first_sequence : Int64,
  outputs : ArrayView[@task.TaskOutput],
) -> Unit raise OutputLogError {
  if task_id.is_empty() {
    raise OutputLogError::EmptyTaskId
  }
  if partition < 0 {
    raise OutputLogError::InvalidPartition(partition)
  }
  if epoch < 0 {
    raise OutputLogError::InvalidEpoch(epoch)
  }
  if first_sequence < 0L {
    raise OutputLogError::InvalidSequence(first_sequence)
  }
  self.write_lock.acquire() catch {
    error => raise OutputLogError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.wal.check_writable() catch {
    error => raise OutputLogError::Wal(error)
  }
  if self.retention.contains(task_id) {
    raise OutputLogError::RetentionConflict(task_id)
  }
  self.check_identity(task_id, partition, epoch)
  if outputs.length().to_int64() > 9223372036854775807L - first_sequence {
    raise OutputLogError::InvalidSequence(first_sequence)
  }
  let mut expected = self.next_sequence(task_id)
  let canonical_records : Array[TaskOutputRecord] = []
  for index, output in outputs {
    let sequence = first_sequence + index.to_int64()
    let record = TaskOutputRecord::{
      version: 1,
      task_id,
      partition,
      epoch,
      sequence,
      output,
    }
    let canonical = validate_output_record(record)
    let key = output_key(task_id, sequence)
    match self.known.get(key) {
      Some(previous) =>
        if previous != canonical {
          raise OutputLogError::IdentityConflict(key)
        }
      None => {
        if sequence != expected {
          raise OutputLogError::SequenceGap(expected~, actual=sequence)
        }
        canonical_records.push(canonical)
        expected += 1L
      }
    }
  }
  for canonical in canonical_records {
    let key = output_key(canonical.task_id, canonical.sequence)
    let encoded = @utf8.encode(@json.to_json(canonical).stringify())
    ignore(
      self.wal.append(encoded) catch {
        error => raise OutputLogError::Wal(error)
      },
    )
    self.records.push(canonical)
    self.known[key] = canonical
    self.next_sequences[task_id] = canonical.sequence + 1L
  }
}
