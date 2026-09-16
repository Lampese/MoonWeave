///|
fn WorkerMigration::validate_reclaimed(
  self : WorkerMigration,
  base : WorkerJournalCheckpoint,
  records : Array[WorkerWalRecord],
) -> Unit raise WorkerMigrationError {
  if self.primary_records != 1 ||
    self.delta_records != 0 ||
    !self.deltas.is_empty() ||
    base.checkpoint.operator_state() != Some(@json.to_json(self.snapshot)) ||
    base.checkpoint.persisted_offset() != self.snapshot.offset() {
    raise WorkerMigrationError::CheckpointMismatch
  }
  let assignment = @protocol.Assignment::new(
    self.identity.partition,
    self.identity.source_epoch,
    @protocol.Incarnation::new(self.identity.source_incarnation),
    start_offset=self.identity.start_offset,
  ) catch {
    _ => raise WorkerMigrationError::InvalidIdentity
  }
  ignore(
    base.validate(assignment, self.snapshot.state_schema()) catch {
      _ => raise WorkerMigrationError::CheckpointMismatch
    },
  )
  self.snapshot.validate_transfer(self.identity.start_offset) catch {
    error => raise WorkerMigrationError::Task(error)
  }
  let mut next = self.snapshot.offset() + 1L
  let mut watermark = self.snapshot.watermark_requested()
  let batches : Array[@protocol.EventBatch] = []
  for record in records[1:] {
    match record {
      WorkerWalRecord::Batch(batch) => {
        if batch.partition() != assignment.partition() ||
          batch.epoch() != assignment.epoch() ||
          batch.first_offset() != next {
          raise WorkerMigrationError::CheckpointMismatch
        }
        next = batch.end_offset() + 1L
        batches.push(batch)
      }
      WorkerWalRecord::EmbeddedDelta(delta) =>
        if delta.target_partition() != assignment.partition() ||
          delta.target_epoch() != assignment.epoch() {
          raise WorkerMigrationError::CheckpointMismatch
        }
      WorkerWalRecord::Watermark(value) => {
        if value.partition() != assignment.partition() ||
          value.epoch() != assignment.epoch() ||
          value.timestamp() < watermark {
          raise WorkerMigrationError::CheckpointMismatch
        }
        watermark = value.timestamp()
      }
      _ => raise WorkerMigrationError::CheckpointMismatch
    }
  }
  if batches != self.batches {
    raise WorkerMigrationError::CheckpointMismatch
  }
  let mut output_start = 0L
  if self.output_retention is Some(marker) {
    if marker.task_id != self.snapshot.task_id() ||
      marker.partition != assignment.partition() ||
      marker.epoch != assignment.epoch() ||
      marker.continuing != Some(true) ||
      marker.retain_from != marker.output_count ||
      marker.output_count > self.snapshot.emitted_outputs().to_int64() {
      raise WorkerMigrationError::OutputConflict
    }
    ignore(
      output_retention_marker_from_json(@json.to_json(marker)) catch {
        _ => raise WorkerMigrationError::OutputConflict
      },
    )
    output_start = marker.retain_from
  }
  if self.outputs is Some(outputs) {
    if output_start + outputs.length().to_int64() <
      self.snapshot.emitted_outputs().to_int64() {
      raise WorkerMigrationError::OutputConflict
    }
    for index, record in outputs {
      ignore(
        validate_output_record(record) catch {
          _ => raise WorkerMigrationError::OutputConflict
        },
      )
      if record.task_id != self.snapshot.task_id() ||
        record.partition != assignment.partition() ||
        record.epoch != assignment.epoch() ||
        record.sequence != output_start + index.to_int64() {
        raise WorkerMigrationError::OutputConflict
      }
    }
  } else if self.output_retention is Some(_) {
    raise WorkerMigrationError::OutputConflict
  }
}

///|
fn rebound_journal_checkpoint(
  base : WorkerJournalCheckpoint,
  epoch : Int,
) -> WorkerJournalCheckpoint raise WorkerMigrationError {
  let snapshot : @task.TaskSnapshot = @json.from_json(
    base.checkpoint.operator_state().unwrap(),
  ) catch {
    _ => raise WorkerMigrationError::CheckpointMismatch
  }
  let snapshot = snapshot.rebind(snapshot.task_id(), epoch) catch {
    error => raise WorkerMigrationError::Task(error)
  }
  {
    ..base,
    checkpoint: {
      ..base.checkpoint,
      epoch,
      operator_state: Some(@json.to_json(snapshot)),
    },
  }
}
