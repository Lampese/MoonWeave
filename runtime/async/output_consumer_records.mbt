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
  // Set only on a compacted header/ack pair. Keeping this optional preserves
  // replay of checkpoints written before consumer WAL compaction existed.
  compacted : Bool?
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
  allow_trimmed? : Bool = false,
) -> Unit raise OutputConsumerError {
  if record.task_id != binding.task_id ||
    record.partition != binding.partition ||
    record.epoch != binding.epoch {
    raise OutputConsumerError::IdentityMismatch
  }
  match log.known.get(output_key(record.task_id, record.sequence)) {
    Some(previous) if previous != record =>
      raise OutputConsumerError::OutputConflict(record.sequence)
    Some(_) => ()
    None if allow_trimmed && log.is_trimmed(record.task_id, record.sequence) =>
      ()
    None => raise OutputConsumerError::MissingOutput(record.sequence)
  }
}

///|
fn replay_output_consumer(
  replay : WalReplay,
  log : TaskOutputLog,
  binding : OutputConsumerEntry,
) -> Int64 raise OutputConsumerError {
  let mut next_sequence = 0L
  let mut compacted = false
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
    if { ..entry, record: None, compacted: None, } != binding {
      raise OutputConsumerError::IdentityMismatch
    }
    if index == 0 {
      if entry.record is Some(_) {
        raise OutputConsumerError::Malformed
      }
      compacted = match entry.compacted {
        None => false
        Some(value) => value
      }
      continue
    }
    if compacted {
      if index == 1 {
        match entry.compacted {
          Some(true) => ()
          None => ()
          Some(false) => raise OutputConsumerError::Malformed
        }
      } else if entry.compacted is Some(_) {
        raise OutputConsumerError::Malformed
      }
    } else if entry.compacted is Some(_) {
      raise OutputConsumerError::Malformed
    }
    guard entry.record is Some(record) else {
      raise OutputConsumerError::Malformed
    }
    validate_consumer_record(log, binding, record, allow_trimmed=compacted)
    if compacted && index == 1 && entry.compacted == Some(true) {
      // A compacted checkpoint retains only the latest ACK. Its sequence is
      // therefore the recovered cursor, rather than an expected zero-based
      // prefix. Later entries must continue from this cursor normally.
      next_sequence = record.sequence + 1L
      continue
    }
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
