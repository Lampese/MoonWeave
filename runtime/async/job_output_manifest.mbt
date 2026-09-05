///|
/// Durable Job-level output aggregation. The manifest binds each completed
/// Task output stream to its assignment identity, frontier digest, and output
/// digest. Consumer cursors are monotone acknowledgements; delivery remains
/// at-least-once and sinks must use TaskOutputRecord::idempotency_key.
pub(all) suberror JobOutputManifestError {
  EmptyPath
  EmptyJobId
  EmptyOperatorId
  EmptyTaskId
  EmptyWorkerId
  EmptyConsumerId
  InvalidPartition(Int)
  InvalidPartitionCount(Int)
  InvalidEpoch(Int)
  InvalidSequence(Int64)
  InvalidDigest
  InvalidCertificate
  NotComplete
  FrontierMismatch
  UnknownPartition(Int)
  IdentityConflict(String)
  Job(@control.JobError)
  CursorRegression(String)
  CursorAhead(String)
  DuplicatePartition(Int)
  Malformed
  UnsupportedVersion(Int)
  Wal(WalError)
  OutputLog(OutputLogError)
} derive(Debug, Eq)

///|
pub(all) struct JobOutputPartitionSummary {
  partition : Int
  task_id : String
  worker_id : String
  epoch : Int
  output_log_path : String
  output_count : Int64
  output_digest : String
  frontier_digest : String
  complete : Bool
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobOutputPartitionSummary::partition(
  self : JobOutputPartitionSummary,
) -> Int {
  self.partition
}

///|
pub fn JobOutputPartitionSummary::task_id(
  self : JobOutputPartitionSummary,
) -> String {
  self.task_id
}

///|
pub fn JobOutputPartitionSummary::worker_id(
  self : JobOutputPartitionSummary,
) -> String {
  self.worker_id
}

///|
pub fn JobOutputPartitionSummary::epoch(
  self : JobOutputPartitionSummary,
) -> Int {
  self.epoch
}

///|
pub fn JobOutputPartitionSummary::output_log_path(
  self : JobOutputPartitionSummary,
) -> String {
  self.output_log_path
}

///|
pub fn JobOutputPartitionSummary::output_count(
  self : JobOutputPartitionSummary,
) -> Int64 {
  self.output_count
}

///|
pub fn JobOutputPartitionSummary::output_digest(
  self : JobOutputPartitionSummary,
) -> String {
  self.output_digest
}

///|
pub fn JobOutputPartitionSummary::frontier_digest(
  self : JobOutputPartitionSummary,
) -> String {
  self.frontier_digest
}

///|
pub fn JobOutputPartitionSummary::complete(
  self : JobOutputPartitionSummary,
) -> Bool {
  self.complete
}

///|
pub(all) struct JobOutputConsumerSummary {
  consumer_id : String
  task_id : String
  partition : Int
  epoch : Int
  checkpoint_path : String
  acked_sequence : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobOutputConsumerSummary::consumer_id(
  self : JobOutputConsumerSummary,
) -> String {
  self.consumer_id
}

///|
pub fn JobOutputConsumerSummary::task_id(
  self : JobOutputConsumerSummary,
) -> String {
  self.task_id
}

///|
pub fn JobOutputConsumerSummary::partition(
  self : JobOutputConsumerSummary,
) -> Int {
  self.partition
}

///|
pub fn JobOutputConsumerSummary::epoch(self : JobOutputConsumerSummary) -> Int {
  self.epoch
}

///|
pub fn JobOutputConsumerSummary::checkpoint_path(
  self : JobOutputConsumerSummary,
) -> String {
  self.checkpoint_path
}

///|
pub fn JobOutputConsumerSummary::acked_sequence(
  self : JobOutputConsumerSummary,
) -> Int64 {
  self.acked_sequence
}

///|
fn manifest_consumer_key(consumer_id : String, task_id : String) -> String {
  consumer_id.length().to_string() +
  ":" +
  consumer_id +
  ":" +
  task_id.length().to_string() +
  ":" +
  task_id
}

///|
fn valid_certificate_status(status : String) -> Bool {
  status == "Stable" || status == "ResolvedNoMatch"
}

///|
/// An operator-independent certificate envelope. `payload` may contain the
/// concrete reachability/security certificate; the manifest authenticates its
/// identity and frontier binding, while the operator validates its semantics.
pub(all) struct JobOutputCertificate {
  query_id : String
  operator_id : String
  status : String
  result_digest : String
  frontier_digest : String
  payload : Json
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobOutputCertificate::new(
  query_id : String,
  operator_id : String,
  status : String,
  result_digest : String,
  frontier_digest : String,
  payload : Json,
) -> JobOutputCertificate raise JobOutputManifestError {
  if query_id.is_empty() ||
    operator_id.is_empty() ||
    !valid_certificate_status(status) ||
    result_digest.is_empty() ||
    frontier_digest.is_empty() ||
    status == "Tentative" {
    raise JobOutputManifestError::InvalidCertificate
  }
  { query_id, operator_id, status, result_digest, frontier_digest, payload, }
}

///|
pub fn JobOutputCertificate::query_id(self : JobOutputCertificate) -> String {
  self.query_id
}

///|
pub fn JobOutputCertificate::operator_id(self : JobOutputCertificate) -> String {
  self.operator_id
}

///|
pub fn JobOutputCertificate::status(self : JobOutputCertificate) -> String {
  self.status
}

///|
pub fn JobOutputCertificate::result_digest(
  self : JobOutputCertificate,
) -> String {
  self.result_digest
}

///|
pub fn JobOutputCertificate::frontier_digest(
  self : JobOutputCertificate,
) -> String {
  self.frontier_digest
}

///|
pub fn JobOutputCertificate::payload(self : JobOutputCertificate) -> Json {
  self.payload
}

///|
pub fn JobOutputCertificate::digest(self : JobOutputCertificate) -> String {
  manifest_json_digest(@json.to_json(self))
}

///|
pub(all) struct JobOutputManifestSnapshot {
  version : Int
  job_id : String
  operator_id : String
  partition_count : Int
  partitions : Array[JobOutputPartitionSummary]
  consumers : Array[JobOutputConsumerSummary]
  certificate : JobOutputCertificate?
  final_digest : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobOutputManifestSnapshot::version(
  self : JobOutputManifestSnapshot,
) -> Int {
  self.version
}

///|
pub fn JobOutputManifestSnapshot::job_id(
  self : JobOutputManifestSnapshot,
) -> String {
  self.job_id
}

///|
pub fn JobOutputManifestSnapshot::operator_id(
  self : JobOutputManifestSnapshot,
) -> String {
  self.operator_id
}

///|
pub fn JobOutputManifestSnapshot::partition_count(
  self : JobOutputManifestSnapshot,
) -> Int {
  self.partition_count
}

///|
pub fn JobOutputManifestSnapshot::partitions(
  self : JobOutputManifestSnapshot,
) -> ArrayView[JobOutputPartitionSummary] {
  self.partitions[:]
}

///|
pub fn JobOutputManifestSnapshot::consumers(
  self : JobOutputManifestSnapshot,
) -> ArrayView[JobOutputConsumerSummary] {
  self.consumers[:]
}

///|
pub fn JobOutputManifestSnapshot::certificate(
  self : JobOutputManifestSnapshot,
) -> JobOutputCertificate? {
  self.certificate
}

///|
pub fn JobOutputManifestSnapshot::final_digest(
  self : JobOutputManifestSnapshot,
) -> String {
  self.final_digest
}

///|
fn manifest_copy_json(value : Json) -> Json {
  match value {
    Object(fields) => {
      let keys = fields.keys().to_array()
      keys.sort()
      let canonical : Map[String, Json] = Map([])
      for key in keys {
        canonical[key] = manifest_copy_json(fields[key])
      }
      Json::object(canonical)
    }
    Array(items) => Json::array(items.map(item => manifest_copy_json(item)))
    _ => value
  }
}

///|
fn manifest_hash_bytes(bytes : Bytes) -> String {
  let mut hash : UInt64 = 14695981039346656037UL
  for byte in bytes {
    hash = (hash ^ byte.to_uint64()) * 1099511628211UL
  }
  "manifest-" + hash.to_string(radix=16)
}

///|
fn manifest_json_digest(value : Json) -> String {
  manifest_hash_bytes(@utf8.encode(manifest_copy_json(value).stringify()))
}

///|
fn manifest_records_digest(records : Array[TaskOutputRecord]) -> String {
  manifest_json_digest(
    Json::array(records.map(record => @json.to_json(record))),
  )
}

///|
fn validate_partition_summary(
  value : JobOutputPartitionSummary,
  partition_count : Int,
) -> Unit raise JobOutputManifestError {
  if value.partition < 0 || value.partition >= partition_count {
    raise JobOutputManifestError::InvalidPartition(value.partition)
  }
  if value.task_id.is_empty() {
    raise JobOutputManifestError::EmptyTaskId
  }
  if value.worker_id.is_empty() {
    raise JobOutputManifestError::EmptyWorkerId
  }
  if value.epoch < 0 {
    raise JobOutputManifestError::InvalidEpoch(value.epoch)
  }
  if value.output_log_path.is_empty() ||
    value.output_count < 0L ||
    value.output_digest.is_empty() ||
    value.frontier_digest.is_empty() {
    raise JobOutputManifestError::InvalidDigest
  }
}

///|
fn validate_consumer_summary(
  value : JobOutputConsumerSummary,
  partition_count : Int,
) -> Unit raise JobOutputManifestError {
  if value.consumer_id.is_empty() {
    raise JobOutputManifestError::EmptyConsumerId
  }
  if value.task_id.is_empty() {
    raise JobOutputManifestError::EmptyTaskId
  }
  if value.partition < 0 || value.partition >= partition_count {
    raise JobOutputManifestError::InvalidPartition(value.partition)
  }
  if value.epoch < 0 ||
    value.checkpoint_path.is_empty() ||
    value.acked_sequence < -1L {
    raise JobOutputManifestError::InvalidSequence(value.acked_sequence)
  }
}

///|
struct JobOutputManifestEntry {
  version : Int
  kind : String
  job_id : String
  operator_id : String
  partition_count : Int
  partition : JobOutputPartitionSummary?
  consumer : JobOutputConsumerSummary?
  certificate : JobOutputCertificate?
} derive(Debug, Eq, ToJson, FromJson)

///|
fn empty_manifest_entry(
  job_id : String,
  operator_id : String,
  partition_count : Int,
  kind : String,
) -> JobOutputManifestEntry {
  {
    version: 1,
    kind,
    job_id,
    operator_id,
    partition_count,
    partition: None,
    consumer: None,
    certificate: None,
  }
}

///|
pub struct JobOutputManifest {
  wal : Wal
  job_id : String
  operator_id : String
  partition_count : Int
  partitions : Map[Int, JobOutputPartitionSummary]
  consumers : Map[String, JobOutputConsumerSummary]
  mut certificate : JobOutputCertificate?
  write_lock : @async.Mutex
}

///|
fn manifest_entry_matches(
  manifest : JobOutputManifest,
  entry : JobOutputManifestEntry,
) -> Unit raise JobOutputManifestError {
  if entry.version != 1 {
    raise JobOutputManifestError::UnsupportedVersion(entry.version)
  }
  if entry.job_id != manifest.job_id ||
    entry.operator_id != manifest.operator_id ||
    entry.partition_count != manifest.partition_count {
    raise JobOutputManifestError::IdentityConflict(entry.job_id)
  }
}

///|
fn apply_manifest_partition(
  manifest : JobOutputManifest,
  value : JobOutputPartitionSummary,
) -> Unit raise JobOutputManifestError {
  validate_partition_summary(value, manifest.partition_count)
  match manifest.partitions.get(value.partition) {
    None => manifest.partitions[value.partition] = value
    Some(previous) => {
      if previous.task_id != value.task_id ||
        previous.worker_id != value.worker_id ||
        previous.epoch != value.epoch ||
        previous.output_log_path != value.output_log_path {
        raise JobOutputManifestError::IdentityConflict(
          value.partition.to_string(),
        )
      }
      if previous.complete && (!value.complete || previous != value) {
        raise JobOutputManifestError::IdentityConflict(
          value.partition.to_string(),
        )
      }
      if value.output_count < previous.output_count {
        raise JobOutputManifestError::CursorRegression(
          value.partition.to_string(),
        )
      }
      if value.output_count == previous.output_count &&
        value.output_digest != previous.output_digest {
        raise JobOutputManifestError::IdentityConflict(
          value.partition.to_string(),
        )
      }
      manifest.partitions[value.partition] = value
    }
  }
}

///|
fn apply_manifest_consumer(
  manifest : JobOutputManifest,
  value : JobOutputConsumerSummary,
) -> Unit raise JobOutputManifestError {
  validate_consumer_summary(value, manifest.partition_count)
  match manifest.partitions.get(value.partition) {
    Some(partition) => {
      if partition.task_id != value.task_id || partition.epoch != value.epoch {
        raise JobOutputManifestError::IdentityConflict(value.consumer_id)
      }
      if value.acked_sequence >= partition.output_count {
        raise JobOutputManifestError::CursorAhead(value.consumer_id)
      }
    }
    None => raise JobOutputManifestError::UnknownPartition(value.partition)
  }
  let key = manifest_consumer_key(value.consumer_id, value.task_id)
  match manifest.consumers.get(key) {
    None => manifest.consumers[key] = value
    Some(previous) => {
      if previous.task_id != value.task_id ||
        previous.partition != value.partition ||
        previous.epoch != value.epoch ||
        previous.checkpoint_path != value.checkpoint_path {
        raise JobOutputManifestError::IdentityConflict(key)
      }
      if value.acked_sequence < previous.acked_sequence {
        raise JobOutputManifestError::CursorRegression(key)
      }
      manifest.consumers[key] = value
    }
  }
}

///|
fn apply_manifest_entry(
  manifest : JobOutputManifest,
  entry : JobOutputManifestEntry,
) -> Unit raise JobOutputManifestError {
  manifest_entry_matches(manifest, entry)
  match entry.kind {
    "header" =>
      if entry.partition is Some(_) ||
        entry.consumer is Some(_) ||
        entry.certificate is Some(_) {
        raise JobOutputManifestError::Malformed
      }
    "partition" => {
      guard entry.partition is Some(value) &&
        entry.consumer is None &&
        entry.certificate is None else {
        raise JobOutputManifestError::Malformed
      }
      apply_manifest_partition(manifest, value)
    }
    "consumer" => {
      guard entry.consumer is Some(value) &&
        entry.partition is None &&
        entry.certificate is None else {
        raise JobOutputManifestError::Malformed
      }
      apply_manifest_consumer(manifest, value)
    }
    "certificate" => {
      guard entry.certificate is Some(value) &&
        entry.partition is None &&
        entry.consumer is None else {
        raise JobOutputManifestError::Malformed
      }
      if value.operator_id != manifest.operator_id ||
        !valid_certificate_status(value.status) ||
        value.query_id.is_empty() ||
        value.result_digest.is_empty() ||
        value.frontier_digest.is_empty() {
        raise JobOutputManifestError::InvalidCertificate
      }
      if manifest.certificate is Some(previous) && previous != value {
        raise JobOutputManifestError::IdentityConflict("certificate")
      }
      manifest.certificate = Some(value)
    }
    _ => raise JobOutputManifestError::Malformed
  }
}

///|
pub async fn JobOutputManifest::open(
  path : String,
  job_id : String,
  operator_id : String,
  partition_count : Int,
) -> JobOutputManifest raise JobOutputManifestError {
  if path.is_empty() {
    raise JobOutputManifestError::EmptyPath
  }
  if job_id.is_empty() {
    raise JobOutputManifestError::EmptyJobId
  }
  if operator_id.is_empty() {
    raise JobOutputManifestError::EmptyOperatorId
  }
  if partition_count <= 0 {
    raise JobOutputManifestError::InvalidPartitionCount(partition_count)
  }
  let (wal, replay) = Wal::open(path) catch {
    error => raise JobOutputManifestError::Wal(error)
  }
  errdefer wal.close()
  let manifest = JobOutputManifest::{
    wal,
    job_id,
    operator_id,
    partition_count,
    partitions: Map([]),
    consumers: Map([]),
    certificate: None,
    write_lock: @async.Mutex::Mutex(),
  }
  if replay.records().is_empty() {
    let header = empty_manifest_entry(
      manifest.job_id,
      manifest.operator_id,
      manifest.partition_count,
      "header",
    )
    ignore(
      manifest.wal.append(@utf8.encode(@json.to_json(header).stringify())) catch {
        error => raise JobOutputManifestError::Wal(error)
      },
    )
  } else {
    let mut first = true
    for encoded in replay.records() {
      let text = @utf8.decode(encoded) catch {
        _ => raise JobOutputManifestError::Malformed
      }
      let json = @json.parse(text) catch {
        _ => raise JobOutputManifestError::Malformed
      }
      let entry : JobOutputManifestEntry = @json.from_json(json) catch {
        _ => raise JobOutputManifestError::Malformed
      }
      if first && entry.kind != "header" {
        raise JobOutputManifestError::Malformed
      }
      first = false
      apply_manifest_entry(manifest, entry)
    }
  }
  manifest
}

///|
pub async fn JobOutputManifest::open_for_job(
  path : String,
  job : JobStore,
) -> JobOutputManifest raise JobOutputManifestError {
  let snapshot = job.snapshot()
  JobOutputManifest::open(
    path,
    snapshot.job_id(),
    snapshot.operator_id(),
    snapshot.partition_count(),
  )
}

///|
pub fn JobOutputManifest::close(self : JobOutputManifest) -> Unit {
  self.wal.close()
}

///|
pub fn JobOutputManifest::path(self : JobOutputManifest) -> String {
  self.wal.path()
}

///|
pub fn JobOutputManifest::job_id(self : JobOutputManifest) -> String {
  self.job_id
}

///|
pub fn JobOutputManifest::operator_id(self : JobOutputManifest) -> String {
  self.operator_id
}

///|
pub fn JobOutputManifest::partition_count(self : JobOutputManifest) -> Int {
  self.partition_count
}

///|
pub fn JobOutputManifest::partition(
  self : JobOutputManifest,
  partition : Int,
) -> JobOutputPartitionSummary? {
  self.partitions.get(partition)
}

///|
pub fn JobOutputManifest::partitions(
  self : JobOutputManifest,
) -> Array[JobOutputPartitionSummary] {
  let values = self.partitions.values().to_array()
  values.sort_by((left, right) => left.partition.compare(right.partition))
  values
}

///|
pub fn JobOutputManifest::consumers(
  self : JobOutputManifest,
) -> Array[JobOutputConsumerSummary] {
  let values = self.consumers.values().to_array()
  values.sort_by((left, right) => {
    let by_consumer = left.consumer_id.compare(right.consumer_id)
    if by_consumer == 0 {
      left.task_id.compare(right.task_id)
    } else {
      by_consumer
    }
  })
  values
}

///|
pub fn JobOutputManifest::certificate(
  self : JobOutputManifest,
) -> JobOutputCertificate? {
  self.certificate
}

///|
pub fn JobOutputManifest::is_complete(self : JobOutputManifest) -> Bool {
  self.partitions.length() == self.partition_count &&
  self.partitions.values().all(value => value.complete)
}

///|
pub fn JobOutputManifest::frontier_digest(self : JobOutputManifest) -> String {
  let partitions = self.partitions()
  manifest_json_digest(
    Json::array(
      partitions.map(value => {
        Json::object({
          "partition": Json::number(value.partition.to_double()),
          "frontier_digest": Json::string(value.frontier_digest),
        })
      }),
    ),
  )
}

///|
pub fn JobOutputManifest::output_digest(self : JobOutputManifest) -> String {
  let partitions = self.partitions()
  manifest_json_digest(
    Json::array(
      partitions.map(value => {
        Json::object({
          "partition": Json::number(value.partition.to_double()),
          "output_count": Json::number(value.output_count.to_double()),
          "output_digest": Json::string(value.output_digest),
        })
      }),
    ),
  )
}

///|
pub fn JobOutputManifest::final_digest(self : JobOutputManifest) -> String {
  manifest_json_digest(
    Json::object({
      "job_id": Json::string(self.job_id),
      "operator_id": Json::string(self.operator_id),
      "frontier_digest": Json::string(self.frontier_digest()),
      "output_digest": Json::string(self.output_digest()),
      "certificate": match self.certificate {
        None => Json::null()
        Some(value) => @json.to_json(value)
      },
    }),
  )
}

///|
pub fn JobOutputManifest::is_fully_consumed(self : JobOutputManifest) -> Bool {
  if !self.is_complete() {
    return false
  }
  for partition in self.partitions.values() {
    if partition.output_count == 0L {
      continue
    }
    if !self.consumers
      .values()
      .any(consumer => {
        consumer.partition == partition.partition &&
        consumer.task_id == partition.task_id &&
        consumer.epoch == partition.epoch &&
        consumer.acked_sequence >= partition.output_count - 1L
      }) {
      return false
    }
  }
  true
}

///|
pub fn JobOutputManifest::snapshot(
  self : JobOutputManifest,
) -> JobOutputManifestSnapshot {
  {
    version: 1,
    job_id: self.job_id,
    operator_id: self.operator_id,
    partition_count: self.partition_count,
    partitions: self.partitions(),
    consumers: self.consumers(),
    certificate: self.certificate,
    final_digest: self.final_digest(),
  }
}

///|
async fn JobOutputManifest::append_entry(
  self : JobOutputManifest,
  entry : JobOutputManifestEntry,
) -> Unit raise JobOutputManifestError {
  // Validate and apply against a copy before appending. If the WAL append
  // succeeds, applying the same already-validated entry cannot fail; if it
  // fails, the in-memory manifest remains unchanged.
  let preview = JobOutputManifest::{
    wal: self.wal,
    job_id: self.job_id,
    operator_id: self.operator_id,
    partition_count: self.partition_count,
    partitions: self.partitions.copy(),
    consumers: self.consumers.copy(),
    certificate: self.certificate,
    write_lock: @async.Mutex::Mutex(),
  }
  apply_manifest_entry(preview, entry)
  ignore(
    self.wal.append(@utf8.encode(@json.to_json(entry).stringify())) catch {
      error => raise JobOutputManifestError::Wal(error)
    },
  )
  apply_manifest_entry(self, entry)
}

///|
fn output_partition_from_log(
  partition : Int,
  task_id : String,
  worker_id : String,
  epoch : Int,
  log : TaskOutputLog,
  frontier_digest : String,
  complete : Bool,
) -> JobOutputPartitionSummary raise JobOutputManifestError {
  if task_id.is_empty() {
    raise JobOutputManifestError::EmptyTaskId
  }
  if worker_id.is_empty() {
    raise JobOutputManifestError::EmptyWorkerId
  }
  if partition < 0 {
    raise JobOutputManifestError::InvalidPartition(partition)
  }
  if epoch < 0 {
    raise JobOutputManifestError::InvalidEpoch(epoch)
  }
  if frontier_digest.is_empty() {
    raise JobOutputManifestError::InvalidDigest
  }
  let records = log.records_for(task_id)
  for index, record in records {
    if record.partition() != partition ||
      record.epoch() != epoch ||
      record.sequence() != index.to_int64() {
      raise JobOutputManifestError::IdentityConflict(task_id)
    }
  }
  {
    partition,
    task_id,
    worker_id,
    epoch,
    output_log_path: log.path(),
    output_count: records.length().to_int64(),
    output_digest: manifest_records_digest(records),
    frontier_digest,
    complete,
  }
}

///|
pub async fn JobOutputManifest::record_partition(
  self : JobOutputManifest,
  partition : Int,
  task_id : String,
  worker_id : String,
  epoch : Int,
  log : TaskOutputLog,
  frontier_digest : String,
  complete : Bool,
) -> Unit raise JobOutputManifestError {
  let value = output_partition_from_log(
    partition, task_id, worker_id, epoch, log, frontier_digest, complete,
  )
  self.write_lock.acquire() catch {
    error =>
      raise JobOutputManifestError::Wal(WalError::Io(@debug.to_string(error)))
  }
  defer self.write_lock.release()
  self.append_entry(JobOutputManifestEntry::{
    ..empty_manifest_entry(
      self.job_id,
      self.operator_id,
      self.partition_count,
      "partition",
    ),
    partition: Some(value),
  })
}

///|
/// Record one partition only after the durable Job state says that its lease
/// succeeded. This prevents a caller from publishing an output summary for a
/// merely caught-up or failed partition.
pub async fn JobOutputManifest::record_job_partition(
  self : JobOutputManifest,
  job : JobStore,
  partition : Int,
  task_id : String,
  log : TaskOutputLog,
  frontier_digest : String,
  complete : Bool,
) -> Unit raise JobOutputManifestError {
  let snapshot = job.snapshot()
  if snapshot.job_id() != self.job_id ||
    snapshot.operator_id() != self.operator_id ||
    snapshot.partition_count() != self.partition_count {
    raise JobOutputManifestError::Job(
      @control.JobError::AssignmentConflict(partition),
    )
  }
  guard snapshot.partitions().get(partition) is Some(job_partition) else {
    raise JobOutputManifestError::UnknownPartition(partition)
  }
  if complete && job_partition.phase() != @control.JobPartitionPhase::Succeeded {
    raise JobOutputManifestError::NotComplete
  }
  self.record_partition(
    partition,
    task_id,
    job_partition.worker_id(),
    job_partition.epoch(),
    log,
    frontier_digest,
    complete,
  )
}

///|
pub async fn JobOutputManifest::sync_consumer(
  self : JobOutputManifest,
  consumer : TaskOutputConsumer,
) -> Unit raise JobOutputManifestError {
  let value = JobOutputConsumerSummary::{
    consumer_id: consumer.consumer_id(),
    task_id: consumer.task_id(),
    partition: consumer.partition(),
    epoch: consumer.epoch(),
    checkpoint_path: consumer.checkpoint_path(),
    acked_sequence: consumer.next_sequence() - 1L,
  }
  self.write_lock.acquire() catch {
    error =>
      raise JobOutputManifestError::Wal(WalError::Io(@debug.to_string(error)))
  }
  defer self.write_lock.release()
  self.append_entry(JobOutputManifestEntry::{
    ..empty_manifest_entry(
      self.job_id,
      self.operator_id,
      self.partition_count,
      "consumer",
    ),
    consumer: Some(value),
  })
}

///|
pub async fn JobOutputManifest::publish_certificate(
  self : JobOutputManifest,
  certificate : JobOutputCertificate,
) -> Unit raise JobOutputManifestError {
  if !self.is_complete() {
    raise JobOutputManifestError::NotComplete
  }
  if certificate.operator_id != self.operator_id ||
    !valid_certificate_status(certificate.status) ||
    certificate.query_id.is_empty() ||
    certificate.result_digest.is_empty() ||
    certificate.frontier_digest.is_empty() {
    raise JobOutputManifestError::InvalidCertificate
  }
  if certificate.frontier_digest != self.frontier_digest() {
    raise JobOutputManifestError::FrontierMismatch
  }
  self.write_lock.acquire() catch {
    error =>
      raise JobOutputManifestError::Wal(WalError::Io(@debug.to_string(error)))
  }
  defer self.write_lock.release()
  match self.certificate {
    Some(previous) if previous != certificate =>
      raise JobOutputManifestError::IdentityConflict("certificate")
    _ => ()
  }
  self.append_entry(JobOutputManifestEntry::{
    ..empty_manifest_entry(
      self.job_id,
      self.operator_id,
      self.partition_count,
      "certificate",
    ),
    certificate: Some(certificate),
  })
}
