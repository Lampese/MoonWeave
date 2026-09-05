///|
/// The header binds a checkpoint to one logical subscription. ACK entries
/// retain the exact output so recovery detects a replaced or truncated source.
struct OutputConsumerEntry {
  version : Int
  consumer_id : String
  task_id : String
  partition : Int
  epoch : Int
  record : TaskOutputRecord?
} derive(Debug, Eq, ToJson, FromJson)

///|
fn output_consumer_record(
  log : TaskOutputLog,
  binding : OutputConsumerEntry,
  sequence : Int64,
) -> TaskOutputRecord raise OutputConsumerError {
  guard log.known.get(output_key(binding.task_id, sequence)) is Some(record) else {
    raise OutputConsumerError::MissingOutput(sequence)
  }
  if record.partition != binding.partition || record.epoch != binding.epoch {
    raise OutputConsumerError::IdentityMismatch
  }
  record
}

///|
fn validate_consumer_record(
  log : TaskOutputLog,
  binding : OutputConsumerEntry,
  record : TaskOutputRecord,
) -> Unit raise OutputConsumerError {
  if record.task_id != binding.task_id ||
    record.partition != binding.partition ||
    record.epoch != binding.epoch {
    raise OutputConsumerError::IdentityMismatch
  }
  if record != output_consumer_record(log, binding, record.sequence) {
    raise OutputConsumerError::OutputConflict(record.sequence)
  }
}

///|
fn replay_output_consumer(
  replay : WalReplay,
  log : TaskOutputLog,
  binding : OutputConsumerEntry,
) -> Int64 raise OutputConsumerError {
  let mut next_sequence = 0L
  for index, encoded in replay.records() {
    let text = @utf8.decode(encoded) catch {
      _ => raise OutputConsumerError::Malformed
    }
    let json = @json.parse(text) catch {
      _ => raise OutputConsumerError::Malformed
    }
    let entry : OutputConsumerEntry = @json.from_json(json) catch {
      _ => raise OutputConsumerError::Malformed
    }
    if entry.version != 1 {
      raise OutputConsumerError::UnsupportedVersion(entry.version)
    }
    if { ..entry, record: None, } != binding {
      raise OutputConsumerError::IdentityMismatch
    }
    if index == 0 {
      if entry.record is Some(_) {
        raise OutputConsumerError::Malformed
      }
      continue
    }
    guard entry.record is Some(record) else {
      raise OutputConsumerError::Malformed
    }
    validate_consumer_record(log, binding, record)
    if record.sequence != next_sequence {
      raise OutputConsumerError::SequenceGap(
        expected=next_sequence,
        actual=record.sequence,
      )
    }
    next_sequence += 1L
  }
  next_sequence
}
