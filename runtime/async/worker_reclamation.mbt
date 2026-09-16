///|
/// The first record of a reclaimed journal is its authoritative checkpoint.
/// State and the replay boundary share one atomic WAL replacement. Subsequent
/// exchanges carry their payload in this journal, so recovery can ignore the
/// obsolete Delta WAL even after a crash before its physical deletion.
struct WorkerJournalCheckpoint {
  kind : String
  checkpoint : Checkpoint
  causal : @causal.CausalLogSnapshot[Json]
} derive(Debug, Eq, ToJson, FromJson)

///|
struct EmbeddedDeltaRecord {
  kind : String
  delta : @protocol.DeltaBatch
} derive(ToJson, FromJson)

///|
fn WorkerJournalCheckpoint::validate(
  self : WorkerJournalCheckpoint,
  assignment : @protocol.Assignment,
  state_schema : String,
) -> @causal.CausalLog[Json] raise FrameIoError {
  let saved = self.checkpoint.validate() catch {
    error => raise FrameIoError::Checkpoint(error)
  }
  if self.kind != "checkpoint" ||
    saved.partition() != assignment.partition() ||
    saved.epoch() != assignment.epoch() ||
    saved.state_schema() != state_schema ||
    saved.applied_offset() != saved.persisted_offset() ||
    saved.operator_state() is None ||
    saved.wal_offset() != 0L ||
    saved.delta_wal_offset() != 0L {
    raise FrameIoError::Handler("invalid journal checkpoint")
  }
  let causal = @causal.CausalLog::restore(self.causal) catch {
    error => raise FrameIoError::Handler(@debug.to_string(error))
  }
  if causal.frontier().digest() != saved.frontier_digest() {
    raise FrameIoError::Handler("journal checkpoint frontier mismatch")
  }
  causal
}

///|
async fn reclaim_worker_journal(
  wal : Wal,
  delta_wal : Wal?,
  assignment : @protocol.Assignment,
  session : @protocol.WorkerSession,
  causal : @causal.CausalLog[Json],
  state_schema : String,
  state_compact : (@causal.CausalRetirement) -> Unit raise FrameIoError,
  state_snapshot : () -> Json raise FrameIoError,
  global_certificate? : @causal.CausalRetirement,
) -> @causal.CausalLog[Json] raise FrameIoError {
  let local_certificate = @causal.CausalRetirement::from_ids(
    "worker-" +
    assignment.partition().to_string() +
    "-" +
    session.last_applied_offset().to_string(),
    causal.events().map(event => event.id()),
  ) catch {
    error => raise FrameIoError::Handler(@debug.to_string(error))
  }
  let held : Array[@causal.EventId] = []
  for event in causal.pending_events() {
    held.push(event.id())
    for parent in event.parents() {
      if !causal.contains(parent) {
        held.push(parent)
      }
    }
  }
  let certificate = global_certificate
    .unwrap_or(local_certificate)
    .excluding(held) catch {
      error => raise FrameIoError::Handler(@debug.to_string(error))
    }
  let candidate = causal.copy()
  candidate.compact(certificate) catch {
    error => raise FrameIoError::Handler(@debug.to_string(error))
  }
  state_compact(certificate)
  let operator_state = state_snapshot()
  let checkpoint = Checkpoint::new(
    assignment.partition(),
    assignment.epoch(),
    0L,
    session.last_persisted_offset(),
    session.last_applied_offset(),
    candidate.frontier().digest(),
    state_schema~,
    operator_state~,
    watermark?=if session.watermark() < 0L {
      None
    } else {
      Some(session.watermark())
    },
  ) catch {
    error => raise FrameIoError::Checkpoint(error)
  }
  let record : WorkerJournalCheckpoint = {
    kind: "checkpoint",
    checkpoint,
    causal: candidate.snapshot(),
  }
  @crash_boundary.hit("worker.reclaim.prepared", wal.path()) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  wal.rewrite([@utf8.encode(@json.to_json(record).stringify())]) catch {
    error => raise FrameIoError::Wal(error)
  }
  @crash_boundary.hit("worker.reclaim.installed", wal.path()) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  match delta_wal {
    None => ()
    Some(log) =>
      log.rewrite([]) catch {
        error => raise FrameIoError::Wal(error)
      }
  }
  @crash_boundary.hit("worker.reclaim.delta_removed", wal.path()) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  candidate
}
