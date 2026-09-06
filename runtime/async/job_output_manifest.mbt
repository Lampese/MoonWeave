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
  InvalidProof(Int)
  InvalidQuery
  DigestMismatch
  NotComplete
  FrontierMismatch
  UnknownPartition(Int)
  MissingSummary(Int)
  IdentityConflict(String)
  Job(@control.JobError)
  CursorRegression(String)
  CursorAhead(String)
  RetentionNotSafe(Int)
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
  task_state_digest : String
  applied_offset : Int64
  complete : Bool
  retained_from : Int64?
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
pub fn JobOutputPartitionSummary::task_state_digest(
  self : JobOutputPartitionSummary,
) -> String {
  self.task_state_digest
}

///|
pub fn JobOutputPartitionSummary::applied_offset(
  self : JobOutputPartitionSummary,
) -> Int64 {
  self.applied_offset
}

///|
/// First output sequence still retained in the source WAL. `None` means the
/// complete zero-based proof prefix remains available.
pub fn JobOutputPartitionSummary::retained_from(
  self : JobOutputPartitionSummary,
) -> Int64? {
  self.retained_from
}

///|

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
  job_id : String
  operator_id : String
  status : String
  result_digest : String
  frontier_digest : String
  output_digest : String
  task_state_digest : String
  payload : Json
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobOutputCertificate::new(
  query_id : String,
  job_id : String,
  operator_id : String,
  status : String,
  result_digest : String,
  frontier_digest : String,
  output_digest : String,
  task_state_digest : String,
  payload : Json,
) -> JobOutputCertificate raise JobOutputManifestError {
  if query_id.is_empty() ||
    job_id.is_empty() ||
    operator_id.is_empty() ||
    !valid_certificate_status(status) ||
    result_digest.is_empty() ||
    frontier_digest.is_empty() ||
    output_digest.is_empty() ||
    task_state_digest.is_empty() ||
    status == "Tentative" {
    raise JobOutputManifestError::InvalidCertificate
  }
  {
    query_id,
    job_id,
    operator_id,
    status,
    result_digest,
    frontier_digest,
    output_digest,
    task_state_digest,
    payload,
  }
}

///|
pub fn JobOutputCertificate::query_id(self : JobOutputCertificate) -> String {
  self.query_id
}

///|
pub fn JobOutputCertificate::job_id(self : JobOutputCertificate) -> String {
  self.job_id
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
pub fn JobOutputCertificate::output_digest(
  self : JobOutputCertificate,
) -> String {
  self.output_digest
}

///|
pub fn JobOutputCertificate::task_state_digest(
  self : JobOutputCertificate,
) -> String {
  self.task_state_digest
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
/// The v1 certificate query language is intentionally small and typed. A
/// certificate can therefore be derived from verified Task outputs instead of
/// accepting an arbitrary result string supplied by a caller.
pub(all) enum JobOutputQuery {
  Reachable(String, String)
  SecurityAlert(String)
} derive(Debug, Eq, ToJson, FromJson)

///|
fn validate_job_output_query(
  query : JobOutputQuery,
) -> Unit raise JobOutputManifestError {
  match query {
    JobOutputQuery::Reachable(source, target) if source.is_empty() ||
      target.is_empty() => raise JobOutputManifestError::InvalidQuery
    JobOutputQuery::SecurityAlert(user_id) if user_id.is_empty() =>
      raise JobOutputManifestError::InvalidQuery
    _ => ()
  }
}

///|
fn job_output_query_matches(
  query : JobOutputQuery,
  output : @task.TaskOutput,
) -> Bool {
  match (query, output) {
    (
      JobOutputQuery::Reachable(source, target),
      @task.TaskOutput::Reachable(value),
    ) => value.source() == source && value.target() == target
    (
      JobOutputQuery::SecurityAlert(user_id),
      @task.TaskOutput::SecurityAlert(value),
    ) => value.user_id() == user_id
    _ => false
  }
}

///|
fn job_output_query_result_digest(
  query : JobOutputQuery,
  matched : Bool,
) -> String {
  let prefix = if matched { "match:" } else { "no-match:" }
  match query {
    JobOutputQuery::Reachable(source, target) => prefix + source + "->" + target
    JobOutputQuery::SecurityAlert(user_id) =>
      prefix + "security-alert:" + user_id
  }
}

///|
pub(all) struct JobOutputTaskProof {
  partition : Int
  task_id : String
  snapshot : @task.TaskSnapshot
  outputs : Array[TaskOutputRecord]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobOutputTaskProof::new(
  partition : Int,
  task_id : String,
  snapshot : @task.TaskSnapshot,
  outputs : ArrayView[TaskOutputRecord],
) -> JobOutputTaskProof {
  { partition, task_id, snapshot, outputs: outputs.to_owned(), }
}

///|
pub fn JobOutputTaskProof::partition(self : JobOutputTaskProof) -> Int {
  self.partition
}

///|
pub fn JobOutputTaskProof::task_id(self : JobOutputTaskProof) -> String {
  self.task_id
}

///|
pub fn JobOutputTaskProof::snapshot(
  self : JobOutputTaskProof,
) -> @task.TaskSnapshot {
  self.snapshot
}

///|
pub fn JobOutputTaskProof::outputs(
  self : JobOutputTaskProof,
) -> ArrayView[TaskOutputRecord] {
  self.outputs[:]
}

///|
/// Verify one complete Task proof against the durable partition summary. The
/// proof must carry the exact snapshot and output prefix that produced the
/// Worker drain digest; reordering or omitting a record is rejected.
fn verify_job_output_task_proof(
  manifest : JobOutputManifest,
  query : JobOutputQuery,
  proof : JobOutputTaskProof,
  seen : Set[Int],
) -> (Bool, Json) raise JobOutputManifestError {
  let partition = proof.partition
  if partition < 0 || partition >= manifest.partition_count {
    raise JobOutputManifestError::InvalidPartition(partition)
  }
  if !seen.add_and_check(partition) {
    raise JobOutputManifestError::DuplicatePartition(partition)
  }
  guard manifest.partitions.get(partition) is Some(summary) else {
    raise JobOutputManifestError::UnknownPartition(partition)
  }
  let snapshot = proof.snapshot
  if proof.task_id != summary.task_id ||
    snapshot.task_id() != summary.task_id ||
    snapshot.operator_id() != manifest.operator_id ||
    snapshot.partition() != partition ||
    snapshot.epoch() != summary.epoch ||
    snapshot.frontier_digest() != summary.frontier_digest ||
    snapshot.emitted_outputs().to_int64() != summary.output_count {
    raise JobOutputManifestError::InvalidProof(partition)
  }
  let snapshot_digest = task_snapshot_digest(snapshot)
  if snapshot_digest != summary.task_state_digest {
    raise JobOutputManifestError::InvalidProof(partition)
  }
  if snapshot.offset() != summary.applied_offset {
    raise JobOutputManifestError::InvalidProof(partition)
  }
  let outputs = proof.outputs.copy()
  outputs.sort_by((left, right) => left.sequence().compare(right.sequence()))
  if outputs.length().to_int64() != summary.output_count {
    raise JobOutputManifestError::InvalidProof(partition)
  }
  for index, output in outputs {
    if output.task_id() != summary.task_id ||
      output.partition() != partition ||
      output.epoch() != summary.epoch ||
      output.sequence() != index.to_int64() {
      raise JobOutputManifestError::InvalidProof(partition)
    }
  }
  let output_digest = manifest_records_digest(outputs)
  if output_digest != summary.output_digest {
    raise JobOutputManifestError::InvalidProof(partition)
  }
  let replayed = @task.Task::new(
    snapshot.task_id(),
    snapshot.operator_id(),
    snapshot.operator(),
    snapshot.partition(),
    snapshot.epoch(),
    start_offset=0L,
  ) catch {
    _ => raise JobOutputManifestError::InvalidProof(partition)
  }
  replayed.start() catch {
    _ => raise JobOutputManifestError::InvalidProof(partition)
  }
  replayed.restore(snapshot) catch {
    _ => raise JobOutputManifestError::InvalidProof(partition)
  }
  let replayed_outputs = replayed.outputs()
  let proved_outputs = outputs.map(record => record.output())
  proved_outputs.sort()
  if replayed_outputs != proved_outputs {
    raise JobOutputManifestError::InvalidProof(partition)
  }
  let matched = outputs.any(record => {
    job_output_query_matches(query, record.output())
  })
  let proof_json = Json::object({
    "partition": Json::number(partition.to_double()),
    "task_id": Json::string(summary.task_id),
    "epoch": Json::number(summary.epoch.to_double()),
    "snapshot_digest": Json::string(snapshot_digest),
    "output_digest": Json::string(output_digest),
  })
  (matched, proof_json)
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
/// Canonical digest used in Worker DrainAck for a Task snapshot.
pub fn task_snapshot_digest(snapshot : @task.TaskSnapshot) -> String {
  manifest_json_digest(@json.to_json(snapshot))
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
    value.frontier_digest.is_empty() ||
    value.task_state_digest.is_empty() {
    raise JobOutputManifestError::InvalidDigest
  }
  if value.applied_offset < -1L {
    raise JobOutputManifestError::InvalidSequence(value.applied_offset)
  }
  match value.retained_from {
    Some(retained_from) if retained_from < 0L ||
      retained_from > value.output_count =>
      raise JobOutputManifestError::InvalidSequence(retained_from)
    _ => ()
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
      match (previous.retained_from, value.retained_from) {
        (Some(previous_from), Some(value_from)) if previous_from != value_from =>
          raise JobOutputManifestError::IdentityConflict(
            value.partition.to_string(),
          )
        (Some(_), None) =>
          raise JobOutputManifestError::IdentityConflict(
            value.partition.to_string(),
          )
        _ => ()
      }
      let retention_only = previous.retained_from is None &&
        value.retained_from is Some(_) &&
        { ..previous, retained_from: value.retained_from, } == value
      if previous.complete &&
        (!value.complete || (previous != value && !retention_only)) {
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
        value.job_id != manifest.job_id ||
        !valid_certificate_status(value.status) ||
        value.query_id.is_empty() ||
        value.result_digest.is_empty() ||
        value.frontier_digest.is_empty() ||
        value.output_digest != manifest.output_digest() ||
        value.frontier_digest != manifest.frontier_digest() ||
        value.task_state_digest != manifest.task_state_digest() {
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
/// Verify that a certificate is bound to this complete manifest. Operator
/// semantics for the certificate payload remain the responsibility of the
/// operator package; this method verifies the durable Job identity and all
/// aggregate evidence digests.
pub fn JobOutputManifest::verify_certificate(
  self : JobOutputManifest,
  certificate : JobOutputCertificate,
) -> Bool {
  self.is_complete() &&
  certificate.job_id == self.job_id &&
  certificate.operator_id == self.operator_id &&
  valid_certificate_status(certificate.status) &&
  !certificate.query_id.is_empty() &&
  !certificate.result_digest.is_empty() &&
  certificate.frontier_digest == self.frontier_digest() &&
  certificate.output_digest == self.output_digest() &&
  certificate.task_state_digest == self.task_state_digest()
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
/// Aggregate digest of the exact Task snapshots reported at drain. This is
/// distinct from frontier/output digests so certificate verifiers can tell
/// whether the operator state itself changed.
pub fn JobOutputManifest::task_state_digest(self : JobOutputManifest) -> String {
  let partitions = self.partitions()
  manifest_json_digest(
    Json::array(
      partitions.map(value => {
        Json::object({
          "partition": Json::number(value.partition.to_double()),
          "task_id": Json::string(value.task_id),
          "epoch": Json::number(value.epoch.to_double()),
          "task_state_digest": Json::string(value.task_state_digest),
          "applied_offset": Json::number(value.applied_offset.to_double()),
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
      "task_state_digest": Json::string(self.task_state_digest()),
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
fn JobOutputManifest::partition_fully_consumed(
  self : JobOutputManifest,
  partition : JobOutputPartitionSummary,
) -> Bool {
  if partition.output_count == 0L {
    return true
  }
  let mut found = false
  for consumer in self.consumers.values() {
    if consumer.partition == partition.partition &&
      consumer.task_id == partition.task_id &&
      consumer.epoch == partition.epoch {
      found = true
      if consumer.acked_sequence < partition.output_count - 1L {
        return false
      }
    }
  }
  found
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
  task_state_digest : String,
  complete : Bool,
  applied_offset? : Int64 = -1L,
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
    task_state_digest,
    applied_offset,
    complete,
    retained_from: None,
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
  task_state_digest : String,
  complete : Bool,
  applied_offset? : Int64 = -1L,
) -> Unit raise JobOutputManifestError {
  let value = output_partition_from_log(
    partition,
    task_id,
    worker_id,
    epoch,
    log,
    frontier_digest,
    task_state_digest,
    complete,
    applied_offset~,
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
  task_state_digest : String,
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
    task_state_digest,
    complete,
  )
}

///|
/// Record a summary received from a remote Worker during a drain handshake.
/// The logical output URI intentionally does not claim that the Coordinator
/// can open the Worker-local WAL; the digest/count are the durable evidence
/// transferred over the protocol.
pub async fn JobOutputManifest::record_partition_summary(
  self : JobOutputManifest,
  partition : Int,
  task_id : String,
  worker_id : String,
  epoch : Int,
  output_count : Int64,
  output_digest : String,
  frontier_digest : String,
  task_state_digest : String,
  complete : Bool,
  applied_offset? : Int64 = -1L,
) -> Unit raise JobOutputManifestError {
  let value = JobOutputPartitionSummary::{
    partition,
    task_id,
    worker_id,
    epoch,
    output_log_path: "worker://" + worker_id + "/" + task_id,
    output_count,
    output_digest,
    frontier_digest,
    task_state_digest,
    applied_offset,
    complete,
    retained_from: None,
  }
  validate_partition_summary(value, self.partition_count)
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
/// Seal one local output partition for physical retention. A durable
/// certificate and a fully acknowledged consumer barrier are required because
/// the source WAL may no longer contain the records needed to construct a new
/// proof after compaction. The summary keeps the historical count/digest, so
/// existing certificate verification remains valid.
pub async fn JobOutputManifest::seal_partition_for_retention(
  self : JobOutputManifest,
  partition : Int,
  log : TaskOutputLog,
) -> TaskOutputRetentionBarrier raise JobOutputManifestError {
  if partition < 0 || partition >= self.partition_count {
    raise JobOutputManifestError::InvalidPartition(partition)
  }
  self.write_lock.acquire() catch {
    error =>
      raise JobOutputManifestError::Wal(WalError::Io(@debug.to_string(error)))
  }
  defer self.write_lock.release()
  guard self.partitions.get(partition) is Some(summary) else {
    raise JobOutputManifestError::MissingSummary(partition)
  }
  if !summary.complete || self.certificate is None {
    raise JobOutputManifestError::RetentionNotSafe(partition)
  }
  if !self.partition_fully_consumed(summary) {
    raise JobOutputManifestError::RetentionNotSafe(partition)
  }
  if summary.output_log_path != log.path() ||
    log.next_sequence(summary.task_id) != summary.output_count ||
    log.output_digest(summary.task_id) != summary.output_digest {
    raise JobOutputManifestError::IdentityConflict(summary.task_id)
  }
  let retain_from = summary.retained_from.unwrap_or(summary.output_count)
  if retain_from < 0L || retain_from > summary.output_count {
    raise JobOutputManifestError::InvalidSequence(retain_from)
  }
  if summary.retained_from is Some(_) && retain_from != summary.output_count {
    raise JobOutputManifestError::RetentionNotSafe(partition)
  }
  let barrier = TaskOutputRetentionBarrier::{
    task_id: summary.task_id,
    partition: summary.partition,
    epoch: summary.epoch,
    retain_from,
    output_count: summary.output_count,
    output_digest: summary.output_digest,
  }
  if summary.retained_from is Some(_) {
    return barrier
  }
  let retained_summary = JobOutputPartitionSummary::{
    ..summary,
    retained_from: Some(summary.output_count),
  }
  self.append_entry(JobOutputManifestEntry::{
    ..empty_manifest_entry(
      self.job_id,
      self.operator_id,
      self.partition_count,
      "partition",
    ),
    partition: Some(retained_summary),
  })
  barrier
}

///|
/// Seal and physically compact one local output partition. The manifest seal
/// is committed before the source WAL rewrite; if the rewrite fails, a later
/// call observes the durable barrier and retries without losing evidence.
pub async fn JobOutputManifest::compact_partition_for_retention(
  self : JobOutputManifest,
  partition : Int,
  log : TaskOutputLog,
) -> Unit raise JobOutputManifestError {
  let barrier = self.seal_partition_for_retention(partition, log)
  log.compact(barrier) catch {
    error => raise JobOutputManifestError::OutputLog(error)
  }
}

///|
async fn JobOutputManifest::append_certificate(
  self : JobOutputManifest,
  certificate : JobOutputCertificate,
) -> Unit raise JobOutputManifestError {
  if !self.is_complete() {
    raise JobOutputManifestError::NotComplete
  }
  if certificate.job_id != self.job_id ||
    certificate.operator_id != self.operator_id ||
    !valid_certificate_status(certificate.status) ||
    certificate.query_id.is_empty() ||
    certificate.result_digest.is_empty() ||
    certificate.frontier_digest.is_empty() ||
    certificate.output_digest.is_empty() ||
    certificate.task_state_digest.is_empty() {
    raise JobOutputManifestError::InvalidCertificate
  }
  if certificate.frontier_digest != self.frontier_digest() {
    raise JobOutputManifestError::FrontierMismatch
  }
  if certificate.output_digest != self.output_digest() ||
    certificate.task_state_digest != self.task_state_digest() {
    raise JobOutputManifestError::DigestMismatch
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

///|
/// Publish a certificate after checking that it is bound to this Job's
/// durable aggregate digests. For typed, replayable evidence use
/// `publish_verified_certificate`.
pub async fn JobOutputManifest::publish_certificate(
  self : JobOutputManifest,
  certificate : JobOutputCertificate,
) -> Unit raise JobOutputManifestError {
  self.append_certificate(certificate)
}

///|
/// Verify every Task snapshot/output prefix, derive the result state from the
/// typed query, and persist the resulting certificate. This is the only API
/// that claims a certificate is backed by replayable Task evidence.
pub async fn JobOutputManifest::publish_verified_certificate(
  self : JobOutputManifest,
  query_id : String,
  query : JobOutputQuery,
  proofs : ArrayView[JobOutputTaskProof],
) -> JobOutputCertificate raise JobOutputManifestError {
  if query_id.is_empty() {
    raise JobOutputManifestError::InvalidCertificate
  }
  validate_job_output_query(query)
  if !self.is_complete() || proofs.length() != self.partition_count {
    raise JobOutputManifestError::NotComplete
  }
  let seen : Set[Int] = Set([])
  let proof_records : Array[Json] = []
  let mut matched = false
  for proof in proofs {
    let (proof_matched, proof_json) = verify_job_output_task_proof(
      self, query, proof, seen,
    )
    if proof_matched {
      matched = true
    }
    proof_records.push(proof_json)
  }
  if seen.length() != self.partition_count {
    raise JobOutputManifestError::InvalidProof(-1)
  }
  let status = if matched { "Stable" } else { "ResolvedNoMatch" }
  let certificate = JobOutputCertificate::new(
    query_id,
    self.job_id,
    self.operator_id,
    status,
    job_output_query_result_digest(query, matched),
    self.frontier_digest(),
    self.output_digest(),
    self.task_state_digest(),
    Json::object({
      "query": @json.to_json(query),
      "proofs": Json::array(proof_records),
    }),
  )
  self.append_certificate(certificate)
  certificate
}
