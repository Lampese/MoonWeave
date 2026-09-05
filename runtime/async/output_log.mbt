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
  Wal(WalError)
  Io(String)
} derive(Debug, Eq)

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
  records : Array[TaskOutputRecord]
  known : Map[String, TaskOutputRecord]
  next_sequences : Map[String, Int64]
  write_lock : @async.Mutex
}

///|
fn output_key(task_id : String, sequence : Int64) -> String {
  task_id.length().to_string() + ":" + task_id + ":" + sequence.to_string()
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
  let next_sequences : Map[String, Int64] = Map([])
  for encoded in replay.records() {
    let text = @utf8.decode(encoded) catch {
      error => raise OutputLogError::Io(@debug.to_string(error))
    }
    let json = @json.parse(text) catch { _ => raise OutputLogError::Malformed }
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
        let expected = next_sequences.get(record.task_id).unwrap_or(0L)
        if record.sequence != expected {
          raise OutputLogError::SequenceGap(expected~, actual=record.sequence)
        }
        check_output_task_identity(
          known,
          record.task_id,
          record.partition,
          record.epoch,
        )
        records.push(record)
        known[key] = record
        next_sequences[record.task_id] = expected + 1L
      }
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
  { wal, records, known, next_sequences, write_lock: @async.Mutex::Mutex(), }
}

///|
fn check_output_task_identity(
  known : Map[String, TaskOutputRecord],
  task_id : String,
  partition : Int,
  epoch : Int,
) -> Unit raise OutputLogError {
  match known.get(output_key(task_id, 0L)) {
    Some(first) if first.partition != partition || first.epoch != epoch =>
      raise OutputLogError::IdentityConflict(task_id)
    _ => ()
  }
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
/// Return the next output sequence expected for one Task.
pub fn TaskOutputLog::next_sequence(
  self : TaskOutputLog,
  task_id : String,
) -> Int64 {
  self.next_sequences.get(task_id).unwrap_or(0L)
}

///|
/// Read durable outputs strictly after a consumer checkpoint. Records remain
/// in the WAL until an explicit compaction policy is introduced.
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
  check_output_task_identity(self.known, task_id, partition, epoch)
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
