///|
pub(all) suberror OutputConsumerError {
  EmptyPath
  EmptyConsumerId
  EmptyTaskId
  InvalidPartition(Int)
  InvalidEpoch(Int)
  InvalidBatchSize(Int)
  PathConflict
  IdentityMismatch
  MissingOutput(Int64)
  OutputConflict(Int64)
  SequenceGap(expected~ : Int64, actual~ : Int64)
  Busy
  Malformed
  UnsupportedVersion(Int)
  Wal(WalError)
  Source(OutputLogError)
} derive(Debug, Eq)

///|
/// A durable, single-writer subscription to one Task's output stream. The
/// caller owns the source log and must keep it open. Each consumer needs its
/// own checkpoint path; the WAL process lock protects that checkpoint.
pub struct TaskOutputConsumer {
  log : TaskOutputLog
  journal : Wal
  binding : OutputConsumerEntry
  operation_lock : @async.Mutex
  mut next_sequence : Int64
}

///|
pub async fn TaskOutputConsumer::open(
  log : TaskOutputLog,
  checkpoint_path : String,
  consumer_id : String,
  task_id : String,
  partition : Int,
  epoch : Int,
) -> TaskOutputConsumer raise OutputConsumerError {
  if checkpoint_path.is_empty() {
    raise OutputConsumerError::EmptyPath
  }
  if checkpoint_path == log.path() {
    raise OutputConsumerError::PathConflict
  }
  if consumer_id.is_empty() {
    raise OutputConsumerError::EmptyConsumerId
  }
  if task_id.is_empty() {
    raise OutputConsumerError::EmptyTaskId
  }
  if partition < 0 {
    raise OutputConsumerError::InvalidPartition(partition)
  }
  if epoch < 0 {
    raise OutputConsumerError::InvalidEpoch(epoch)
  }
  log.wal.check_writable() catch {
    error => raise OutputConsumerError::Source(OutputLogError::Wal(error))
  }
  log.check_identity(task_id, partition, epoch) catch {
    _ => raise OutputConsumerError::IdentityMismatch
  }
  let binding = OutputConsumerEntry::{
    version: 1,
    consumer_id,
    task_id,
    partition,
    epoch,
    record: None,
    compacted: None,
  }
  let (journal, replay) = Wal::open(checkpoint_path) catch {
    error => raise OutputConsumerError::Wal(error)
  }
  errdefer journal.close()
  let next_sequence = replay_output_consumer(replay, log, binding)
  if replay.records().is_empty() {
    ignore(
      journal.append(@utf8.encode(@json.to_json(binding).stringify())) catch {
        error => raise OutputConsumerError::Wal(error)
      },
    )
  }
  {
    log,
    journal,
    binding,
    operation_lock: @async.Mutex::Mutex(),
    next_sequence,
  }
}

///|
pub fn TaskOutputConsumer::close(self : TaskOutputConsumer) -> Unit {
  self.journal.close()
}

///|
pub fn TaskOutputConsumer::consumer_id(self : TaskOutputConsumer) -> String {
  self.binding.consumer_id
}

///|
pub fn TaskOutputConsumer::task_id(self : TaskOutputConsumer) -> String {
  self.binding.task_id
}

///|
pub fn TaskOutputConsumer::partition(self : TaskOutputConsumer) -> Int {
  self.binding.partition
}

///|
pub fn TaskOutputConsumer::epoch(self : TaskOutputConsumer) -> Int {
  self.binding.epoch
}

///|
pub fn TaskOutputConsumer::checkpoint_path(self : TaskOutputConsumer) -> String {
  self.journal.path()
}

///|
/// Return the consumer checkpoint WAL's current lifecycle statistics.
pub fn TaskOutputConsumer::checkpoint_stats(
  self : TaskOutputConsumer,
) -> WalStats {
  self.journal.stats()
}

///|
/// The first output not yet durably acknowledged. This is a consumption cursor,
/// not a proof that the Task or its Job has finished producing results.
pub fn TaskOutputConsumer::next_sequence(self : TaskOutputConsumer) -> Int64 {
  self.next_sequence
}

///|
fn TaskOutputConsumer::check_open(
  self : TaskOutputConsumer,
) -> Unit raise OutputConsumerError {
  self.journal.check_writable() catch {
    error => raise OutputConsumerError::Wal(error)
  }
  self.log.wal.check_writable() catch {
    error => raise OutputConsumerError::Source(OutputLogError::Wal(error))
  }
}

///|
/// Read a bounded, ordered snapshot without acknowledging it. An empty result
/// means caught up for now, not end of stream. Repeated reads may repeat data.
pub fn TaskOutputConsumer::read(
  self : TaskOutputConsumer,
  maximum_records? : Int = 128,
) -> Array[TaskOutputRecord] raise OutputConsumerError {
  self.check_open()
  if maximum_records <= 0 {
    raise OutputConsumerError::InvalidBatchSize(maximum_records)
  }
  let end = self.log.next_sequence(self.binding.task_id)
  let records = []
  for sequence = self.next_sequence
      sequence < end && records.length() < maximum_records
      sequence = sequence + 1L {
    records.push(output_consumer_record(self.log, self.binding, sequence))
  }
  records
}

///|
async fn TaskOutputConsumer::commit_ack(
  self : TaskOutputConsumer,
  record : TaskOutputRecord,
) -> Unit raise OutputConsumerError {
  self.check_open()
  validate_consumer_record(
    self.log,
    self.binding,
    record,
    allow_trimmed=record.sequence < self.next_sequence,
  )
  if record.sequence < self.next_sequence {
    return
  }
  if record.sequence != self.next_sequence {
    raise OutputConsumerError::SequenceGap(
      expected=self.next_sequence,
      actual=record.sequence,
    )
  }
  let entry = OutputConsumerEntry::{ ..self.binding, record: Some(record), }
  ignore(
    self.journal.append(@utf8.encode(@json.to_json(entry).stringify())) catch {
      error => raise OutputConsumerError::Wal(error)
    },
  )
  self.next_sequence = record.sequence + 1L
}

///|
/// Acknowledge exactly the next output after the sink has accepted it. Equal
/// retries are idempotent; skipped, foreign, or altered records are rejected.
/// Concurrent ACK/consume operations fail with Busy instead of racing a sink.
pub async fn TaskOutputConsumer::ack(
  self : TaskOutputConsumer,
  record : TaskOutputRecord,
) -> Unit raise OutputConsumerError {
  if !self.operation_lock.try_acquire() {
    raise OutputConsumerError::Busy
  }
  defer self.operation_lock.release()
  self.commit_ack(record)
}

///|
/// Compact an acknowledged consumer checkpoint while preserving its binding
/// and latest output proof. Unacknowledged output is never discarded: the
/// source TaskOutputLog remains the authoritative stream and the next cursor
/// is recovered from the retained ACK.
pub async fn TaskOutputConsumer::compact(
  self : TaskOutputConsumer,
) -> Unit raise OutputConsumerError {
  if !self.operation_lock.try_acquire() {
    raise OutputConsumerError::Busy
  }
  defer self.operation_lock.release()
  self.check_open()
  let compacted_binding = OutputConsumerEntry::{
    ..self.binding,
    compacted: Some(true),
  }
  let entries : Array[Bytes] = [
    @utf8.encode(@json.to_json(compacted_binding).stringify()),
  ]
  if self.next_sequence > 0L {
    let latest = output_consumer_record(
      self.log,
      self.binding,
      self.next_sequence - 1L,
    )
    entries.push(
      @utf8.encode(
        @json.to_json(OutputConsumerEntry::{
          ..compacted_binding,
          record: Some(latest),
        }).stringify(),
      ),
    )
  }
  self.journal.rewrite(entries[:]) catch {
    error => raise OutputConsumerError::Wal(error)
  }
}

///|
/// Deliver a bounded snapshot serially, committing each ACK after its async
/// handler returns. Errors/cancellation leave that record unacknowledged.
/// A crash between a sink write and its ACK can repeat the write: use the
/// record's idempotency_key in the sink. This API provides at-least-once delivery.
pub async fn TaskOutputConsumer::consume(
  self : TaskOutputConsumer,
  handler : async (TaskOutputRecord) -> Unit,
  maximum_records? : Int = 128,
) -> Int {
  if !self.operation_lock.try_acquire() {
    raise OutputConsumerError::Busy
  }
  defer self.operation_lock.release()
  let records = self.read(maximum_records~)
  for record in records {
    self.check_open()
    handler(record)
    self.commit_ack(record)
  }
  records.length()
}
