///|
/// Capture all partition and consumer updates at one durable WAL boundary.
pub async fn JobOutputManifest::authority_snapshot(
  self : JobOutputManifest,
) -> JobOutputManifestSnapshot raise JobOutputManifestError {
  self.write_lock.acquire() catch {
    error =>
      raise JobOutputManifestError::Wal(WalError::Io(@debug.to_string(error)))
  }
  defer self.write_lock.release()
  self.snapshot()
}

///|
/// Install a complete quorum-confirmed manifest. Validation uses an isolated
/// preview, and one atomic WAL rewrite precedes publishing the new indexes.
pub async fn JobOutputManifest::replace_snapshot(
  self : JobOutputManifest,
  snapshot : JobOutputManifestSnapshot,
) -> Unit raise JobOutputManifestError {
  self.write_lock.acquire() catch {
    error =>
      raise JobOutputManifestError::Wal(WalError::Io(@debug.to_string(error)))
  }
  defer self.write_lock.release()
  if snapshot.version != 1 {
    raise JobOutputManifestError::UnsupportedVersion(snapshot.version)
  }
  if snapshot.job_id != self.job_id ||
    snapshot.operator_id != self.operator_id ||
    snapshot.partition_count != self.partition_count {
    raise JobOutputManifestError::IdentityConflict(snapshot.job_id)
  }
  let preview = JobOutputManifest::{
    wal: self.wal,
    job_id: self.job_id,
    operator_id: self.operator_id,
    partition_count: self.partition_count,
    partitions: Map([]),
    consumers: Map([]),
    certificate: None,
    write_lock: @async.Mutex::Mutex(),
  }
  let header = empty_manifest_entry(
    self.job_id,
    self.operator_id,
    self.partition_count,
    "header",
  )
  let entries = [header]
  for partition in snapshot.partitions {
    if preview.partitions.contains(partition.partition) {
      raise JobOutputManifestError::DuplicatePartition(partition.partition)
    }
    let entry = { ..header, kind: "partition", partition: Some(partition), }
    apply_manifest_entry(preview, entry)
    entries.push(entry)
  }
  for consumer in snapshot.consumers {
    let key = manifest_consumer_key(consumer.consumer_id, consumer.task_id)
    if preview.consumers.contains(key) {
      raise JobOutputManifestError::IdentityConflict(key)
    }
    let entry = { ..header, kind: "consumer", consumer: Some(consumer), }
    apply_manifest_entry(preview, entry)
    entries.push(entry)
  }
  for partition in snapshot.partitions {
    if partition.retained_from is Some(floor) &&
      floor > 0L &&
      !preview.consumers
      .values()
      .any(consumer => {
        consumer.partition == partition.partition &&
        consumer.task_id == partition.task_id &&
        consumer.epoch == partition.epoch &&
        consumer.acked_sequence >= floor - 1L
      }) {
      raise JobOutputManifestError::RetentionNotSafe(partition.partition)
    }
  }
  if snapshot.certificate is Some(certificate) {
    if !preview.verify_certificate(certificate) {
      raise JobOutputManifestError::InvalidCertificate
    }
    let entry = {
      ..header,
      kind: "certificate",
      certificate: Some(certificate),
    }
    apply_manifest_entry(preview, entry)
    entries.push(entry)
  }
  if preview.final_digest() != snapshot.final_digest {
    raise JobOutputManifestError::DigestMismatch
  }
  self.wal.rewrite(
    entries.map(entry => @utf8.encode(@json.to_json(entry).stringify())),
  ) catch {
    error => raise JobOutputManifestError::Wal(error)
  }
  self.partitions = preview.partitions
  self.consumers = preview.consumers
  self.certificate = preview.certificate
}
