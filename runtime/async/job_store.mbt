///|
/// Errors raised by the durable async Job lifecycle store.
pub(all) suberror JobStoreError {
  EmptyPath
  InvalidJournalOffset(Int64)
  InvalidJournalGeneration(Int64)
  Malformed
  UnsupportedVersion(Int)
  Io(String)
  Job(@control.JobError)
  Wal(WalError)
  PlanMismatch(Int)
  AlreadyRunning
} derive(Debug, Eq)

///|
/// Versioned JSON command for the Job lifecycle journal. Sentinel values keep
/// every line on one stable schema and make replay independent of command kind.
struct JobJournalEntry {
  version : Int
  kind : String
  partition : Int
  worker_id : String
  epoch : Int
  start_offset : Int64
  first_offset : Int64
  event_count : Int64
  persisted_offset : Int64
  applied_offset : Int64
  pending_events : Int
  reason : String
  work_plan : JobExecutionPlan?
} derive(Debug, Eq, ToJson, FromJson)

///|
struct JobStoreCheckpoint {
  version : Int
  journal_offset : Int64
  snapshot : @control.JobSnapshot
  work_plans : Array[JobExecutionPlan]
  journal_generation : Int64?
} derive(Debug, Eq, ToJson, FromJson)

///|
/// A generation marker is the first record of a rotated journal. It is not a
/// state-machine command; the checkpoint snapshot carries the state covered by
/// the marker. Keeping the marker inside the checksummed WAL lets recovery
/// distinguish an old canonical file from a staged replacement.
struct JobJournalGenerationMarker {
  version : Int
  kind : String
  generation : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Async durable wrapper around the synchronous JobState machine.
pub struct JobStore {
  state : @control.JobState
  journal_path : String
  checkpoint_path : String
  journal : Wal
  write_lock : @async.Mutex
  mut journal_offset : Int64
  mut journal_generation : Int64
  work_plans : Map[Int, JobExecutionPlan]
  supervisor_lock : @async.Mutex
}

///|
pub fn JobStore::snapshot(self : JobStore) -> @control.JobSnapshot {
  self.state.snapshot()
}

///|
pub fn JobStore::phase(self : JobStore) -> @control.JobPhase {
  self.state.phase()
}

///|
pub fn JobStore::job_id(self : JobStore) -> String {
  self.state.job_id()
}

///|
pub fn JobStore::journal_path(self : JobStore) -> String {
  self.journal_path
}

///|
pub fn JobStore::checkpoint_path(self : JobStore) -> String {
  self.checkpoint_path
}

///|
pub fn JobStore::journal_offset(self : JobStore) -> Int64 {
  self.journal_offset
}

///|
pub fn JobStore::journal_generation(self : JobStore) -> Int64 {
  self.journal_generation
}

///|
pub fn JobStore::close(self : JobStore) -> Unit {
  self.journal.close()
}

///|
fn JobStore::preview_state(
  self : JobStore,
) -> @control.JobState raise JobStoreError {
  @control.JobState::from_snapshot(self.state.snapshot()) catch {
    error => raise JobStoreError::Job(error)
  }
}

///|
fn empty_job_entry(kind : String) -> JobJournalEntry {
  {
    version: 1,
    kind,
    partition: -1,
    worker_id: "",
    epoch: -1,
    start_offset: -1L,
    first_offset: -1L,
    event_count: -1L,
    persisted_offset: -1L,
    applied_offset: -1L,
    pending_events: -1,
    reason: "",
    work_plan: None,
  }
}

///|
fn apply_job_entry(
  state : @control.JobState,
  entry : JobJournalEntry,
) -> Unit raise JobStoreError {
  if entry.version != 1 {
    raise JobStoreError::UnsupportedVersion(entry.version)
  }
  match entry.kind {
    "assign" =>
      ignore(
        state.assign(
          entry.partition,
          entry.worker_id,
          entry.epoch,
          entry.start_offset,
        ) catch {
          error => raise JobStoreError::Job(error)
        },
      )
    "start" => state.start() catch { error => raise JobStoreError::Job(error) }
    "batch_sent" =>
      ignore(
        state.record_batch_sent(
          entry.partition,
          entry.worker_id,
          entry.epoch,
          entry.first_offset,
          entry.event_count,
        ) catch {
          error => raise JobStoreError::Job(error)
        },
      )
    "progress" =>
      state.record_progress(
        entry.partition,
        entry.worker_id,
        entry.epoch,
        entry.persisted_offset,
        entry.applied_offset,
      ) catch {
        error => raise JobStoreError::Job(error)
      }
    "drain" => state.drain() catch { error => raise JobStoreError::Job(error) }
    "drain_partition" =>
      state.drain_partition(entry.partition, entry.worker_id, entry.epoch) catch {
        error => raise JobStoreError::Job(error)
      }
    "stop_partition" =>
      state.stop_partition(
        entry.partition,
        entry.worker_id,
        entry.epoch,
        entry.pending_events,
      ) catch {
        error => raise JobStoreError::Job(error)
      }
    "fail_partition" =>
      state.fail_partition(
        entry.partition,
        entry.worker_id,
        entry.epoch,
        entry.reason,
      ) catch {
        error => raise JobStoreError::Job(error)
      }
    "reassign" =>
      ignore(
        state.reassign(
          entry.partition,
          entry.worker_id,
          entry.epoch,
          entry.start_offset,
        ) catch {
          error => raise JobStoreError::Job(error)
        },
      )
    "fail" =>
      state.fail(entry.reason) catch {
        error => raise JobStoreError::Job(error)
      }
    "cancel" =>
      state.cancel() catch {
        error => raise JobStoreError::Job(error)
      }
    _ => raise JobStoreError::Malformed
  }
}

///|
async fn load_job_checkpoint(
  path : String,
) -> JobStoreCheckpoint? raise JobStoreError {
  let exists = @fs.exists(path) catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  if !exists {
    return None
  }
  let text = @fs.read_file(path).text() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  let json = @json.parse(text) catch { _ => raise JobStoreError::Malformed }
  let checkpoint : JobStoreCheckpoint = @json.from_json(json) catch {
    _ => raise JobStoreError::Malformed
  }
  if checkpoint.version != 1 && checkpoint.version != 2 {
    raise JobStoreError::UnsupportedVersion(checkpoint.version)
  }
  if checkpoint.journal_offset < 0L {
    raise JobStoreError::InvalidJournalOffset(checkpoint.journal_offset)
  }
  match checkpoint.journal_generation {
    Some(value) if value < 0L =>
      raise JobStoreError::InvalidJournalGeneration(value)
    _ => ()
  }
  Some(checkpoint)
}

///|
fn job_checkpoint_generation(checkpoint : JobStoreCheckpoint) -> Int64 {
  checkpoint.journal_generation.unwrap_or(0L)
}

///|
fn job_generation_marker(generation : Int64) -> Bytes {
  @utf8.encode(
    @json.to_json(JobJournalGenerationMarker::{
      version: 1,
      kind: "generation",
      generation,
    }).stringify(),
  )
}

///|
fn decode_job_generation_marker(encoded : Bytes) -> Int64? raise JobStoreError {
  let text = @utf8.decode(encoded) catch { _ => return None }
  let json = @json.parse(text) catch { _ => return None }
  guard json is Object(fields) else { return None }
  guard fields.get("kind") is Some(String(kind)) else { return None }
  if kind != "generation" {
    return None
  }
  let marker : JobJournalGenerationMarker = @json.from_json(json) catch {
    _ => raise JobStoreError::Malformed
  }
  if marker.version != 1 || marker.generation < 0L {
    raise JobStoreError::InvalidJournalGeneration(marker.generation)
  }
  Some(marker.generation)
}

///|
fn job_replay_generation(replay : WalReplay) -> Int64 raise JobStoreError {
  if replay.records().is_empty() {
    return 0L
  }
  let generation = match decode_job_generation_marker(replay.records()[0]) {
    Some(value) => value
    None => 0L
  }
  for index, encoded in replay.records() {
    if index > 0 && decode_job_generation_marker(encoded) is Some(_) {
      raise JobStoreError::Malformed
    }
  }
  generation
}

///|
async fn cleanup_job_candidate(path : String) -> Unit raise JobStoreError {
  let exists = @fs.exists(path) catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  if exists {
    @fs.remove(path) catch {
      error => raise JobStoreError::Io(@debug.to_string(error))
    }
  }
  let lock_exists = @fs.exists(path + ".lock") catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  if lock_exists {
    @fs.remove(path + ".lock") catch {
      error => raise JobStoreError::Io(@debug.to_string(error))
    }
  }
}

///|
async fn recover_job_journal_candidate(
  journal : Wal,
  journal_path : String,
  replay : WalReplay,
  expected_generation : Int64,
) -> (WalReplay, Int64) raise JobStoreError {
  let canonical_generation = job_replay_generation(replay)
  if canonical_generation > expected_generation {
    raise JobStoreError::InvalidJournalGeneration(canonical_generation)
  }
  let candidate_path = journal_path + ".next"
  let candidate_exists = @fs.exists(candidate_path) catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  if canonical_generation == expected_generation {
    if candidate_exists {
      // The canonical marker already matches the checkpoint. Any staged file
      // is unreferenced debris, so do not let a corrupt stale candidate block
      // recovery of the authoritative journal.
      cleanup_job_candidate(candidate_path)
    }
    return (replay, canonical_generation)
  }
  if !candidate_exists {
    raise JobStoreError::InvalidJournalGeneration(expected_generation)
  }
  let (candidate, candidate_replay) = Wal::open(candidate_path) catch {
    error => raise JobStoreError::Wal(error)
  }
  let candidate_generation = job_replay_generation(candidate_replay)
  let candidate_records = candidate_replay.records().to_owned()
  candidate.close()
  if candidate_generation != expected_generation {
    raise JobStoreError::InvalidJournalGeneration(candidate_generation)
  }
  journal.rewrite(candidate_records[:]) catch {
    error => raise JobStoreError::Wal(error)
  }
  cleanup_job_candidate(candidate_path)
  (candidate_replay, candidate_generation)
}

///|
fn same_job_spec(spec : @plan.JobSpec, snapshot : @control.JobSnapshot) -> Bool {
  spec.job_id() == snapshot.job_id() &&
  spec.operator_id() == snapshot.operator_id() &&
  spec.partition_count() == snapshot.partition_count() &&
  spec.workers() == snapshot.workers() &&
  spec.routes() == snapshot.routes()
}

///|
/// Open a Job store, validate the checkpoint topology, and replay its journal
/// suffix. The supplied static JobSpec is the caller's expected identity.
pub async fn JobStore::open(
  spec : @plan.JobSpec,
  journal_path : String,
  checkpoint_path? : String,
) -> JobStore raise JobStoreError {
  if journal_path.is_empty() {
    raise JobStoreError::EmptyPath
  }
  let checkpoint_path = match checkpoint_path {
    Some(path) if !path.is_empty() => path
    Some(_) => raise JobStoreError::EmptyPath
    None => journal_path + ".checkpoint"
  }
  let checkpoint = load_job_checkpoint(checkpoint_path)
  let expected_generation = match checkpoint {
    Some(value) => job_checkpoint_generation(value)
    None => 0L
  }
  let (journal, initial_replay) = Wal::open(journal_path) catch {
    error => raise JobStoreError::Wal(error)
  }
  errdefer journal.close()
  let (replay, journal_generation) = recover_job_journal_candidate(
    journal, journal_path, initial_replay, expected_generation,
  )
  let work_plans : Map[Int, JobExecutionPlan] = Map([])
  let (state, checkpoint_offset) = match checkpoint {
    None => (@control.JobState::new(spec), 0L)
    Some(value) => {
      if !same_job_spec(spec, value.snapshot) {
        raise JobStoreError::Malformed
      }
      let state = @control.JobState::from_snapshot(value.snapshot) catch {
        error => raise JobStoreError::Job(error)
      }
      for plan in value.work_plans {
        let canonical = validate_job_work_plan(state, plan)
        if work_plans.contains(canonical.partition) {
          raise JobStoreError::PlanMismatch(canonical.partition)
        }
        work_plans[canonical.partition] = canonical
      }
      (state, value.journal_offset)
    }
  }
  if checkpoint_offset > replay.valid_bytes() ||
    !replay.is_valid_offset(checkpoint_offset) {
    raise JobStoreError::InvalidJournalOffset(checkpoint_offset)
  }
  let suffix = replay.records_after(checkpoint_offset) catch {
    _ => raise JobStoreError::InvalidJournalOffset(checkpoint_offset)
  }
  for encoded in suffix {
    let line = @utf8.decode(encoded) catch {
      error => raise JobStoreError::Io(@debug.to_string(error))
    }
    let json = @json.parse(line) catch { _ => raise JobStoreError::Malformed }
    let entry : JobJournalEntry = @json.from_json(json) catch {
      _ => raise JobStoreError::Malformed
    }
    if entry.kind == "work_plan" {
      if entry.version != 1 {
        raise JobStoreError::UnsupportedVersion(entry.version)
      }
      guard entry.work_plan is Some(plan) else {
        raise JobStoreError::Malformed
      }
      install_job_work_plan(state, work_plans, plan)
    } else {
      apply_job_entry(state, entry)
    }
  }
  {
    state,
    journal_path,
    checkpoint_path,
    journal,
    write_lock: @async.Mutex::Mutex(),
    journal_offset: replay.valid_bytes(),
    journal_generation,
    work_plans,
    supervisor_lock: @async.Mutex::Mutex(),
  }
}

///|
async fn JobStore::append_entry(
  self : JobStore,
  entry : JobJournalEntry,
) -> Unit raise JobStoreError {
  let line = @json.to_json(entry).stringify() + "\n"
  ignore(
    self.journal.append(@utf8.encode(line)) catch {
      error => raise JobStoreError::Wal(error)
    },
  )
  self.journal_offset = self.journal.write_offset()
}

///|
/// Unlocked mutations reserved for the cross-store reconciliation critical
/// section. `reconcile_job_coordinator` acquires `write_lock` first.
async fn JobStore::reconcile_assign_unlocked(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  start_offset : Int64,
) -> @control.JobPartition raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("assign"),
    partition,
    worker_id,
    epoch,
    start_offset,
  }
  let preview = self.preview_state()
  let assignment = preview.assign(partition, worker_id, epoch, start_offset) catch {
    error => raise JobStoreError::Job(error)
  }
  self.append_entry(entry)
  ignore(
    self.state.assign(partition, worker_id, epoch, start_offset) catch {
      error => raise JobStoreError::Job(error)
    },
  )
  assignment
}

///|
async fn JobStore::reconcile_record_batch_unlocked(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  first_offset : Int64,
  event_count : Int64,
) -> Int64 raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("batch_sent"),
    partition,
    worker_id,
    epoch,
    first_offset,
    event_count,
  }
  let preview = self.preview_state()
  let end_offset = preview.record_batch_sent(
    partition, worker_id, epoch, first_offset, event_count,
  ) catch {
    error => raise JobStoreError::Job(error)
  }
  self.append_entry(entry)
  ignore(
    self.state.record_batch_sent(
      partition, worker_id, epoch, first_offset, event_count,
    ) catch {
      error => raise JobStoreError::Job(error)
    },
  )
  end_offset
}

///|
async fn JobStore::reconcile_record_progress_unlocked(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("progress"),
    partition,
    worker_id,
    epoch,
    persisted_offset,
    applied_offset,
  }
  let preview = self.preview_state()
  preview.record_progress(
    partition, worker_id, epoch, persisted_offset, applied_offset,
  ) catch {
    error => raise JobStoreError::Job(error)
  }
  self.append_entry(entry)
  self.state.record_progress(
    partition, worker_id, epoch, persisted_offset, applied_offset,
  ) catch {
    error => raise JobStoreError::Job(error)
  }
}

///|
pub async fn JobStore::checkpoint(self : JobStore) -> Unit raise JobStoreError {
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let work_plans = self.work_plans.values().to_array()
  work_plans.sort_by((left, right) => left.partition.compare(right.partition))
  let checkpoint = JobStoreCheckpoint::{
    version: 2,
    journal_offset: self.journal_offset,
    snapshot: self.state.snapshot(),
    work_plans,
    journal_generation: Some(self.journal_generation),
  }
  let temporary = self.checkpoint_path + ".tmp"
  @fs.write_file(
    temporary,
    @utf8.encode(@json.to_json(checkpoint).stringify()),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  @fs.rename(temporary, self.checkpoint_path, replace=true) catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
}

///|
/// Persist a complete state snapshot and rotate the journal to a single
/// generation marker. The staged `.next` WAL is synced before the checkpoint
/// is renamed. Recovery adopts that candidate if the process stops before the
/// canonical journal is replaced, so no checkpoint offset is reused against a
/// different WAL generation.
pub async fn JobStore::checkpoint_and_compact(
  self : JobStore,
) -> Unit raise JobStoreError {
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.journal.check_writable() catch {
    error => raise JobStoreError::Wal(error)
  }
  if self.journal_generation == 9223372036854775807L {
    raise JobStoreError::InvalidJournalGeneration(self.journal_generation)
  }
  let generation = self.journal_generation + 1L
  let marker = job_generation_marker(generation)
  let candidate_path = self.journal_path + ".next"
  let (candidate, _) = Wal::open(candidate_path) catch {
    error => raise JobStoreError::Wal(error)
  }
  errdefer candidate.close()
  candidate.rewrite([marker]) catch {
    error => raise JobStoreError::Wal(error)
  }
  let candidate_offset = candidate.write_offset()
  candidate.close()
  let work_plans = self.work_plans.values().to_array()
  work_plans.sort_by((left, right) => left.partition.compare(right.partition))
  let checkpoint = JobStoreCheckpoint::{
    version: 2,
    journal_offset: candidate_offset,
    snapshot: self.state.snapshot(),
    work_plans,
    journal_generation: Some(generation),
  }
  let temporary = self.checkpoint_path + ".tmp"
  @fs.write_file(
    temporary,
    @utf8.encode(@json.to_json(checkpoint).stringify()),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  @fs.rename(temporary, self.checkpoint_path, replace=true) catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  self.journal.rewrite([marker]) catch {
    error => raise JobStoreError::Wal(error)
  }
  self.journal_generation = generation
  self.journal_offset = self.journal.write_offset()
  cleanup_job_candidate(candidate_path)
}

///|
async fn JobStore::commit(
  self : JobStore,
  entry : JobJournalEntry,
) -> Unit raise JobStoreError {
  let preview = self.preview_state()
  apply_job_entry(preview, entry)
  self.append_entry(entry)
  apply_job_entry(self.state, entry)
}

///|
pub async fn JobStore::assign(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  start_offset : Int64,
) -> @control.JobPartition raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("assign"),
    partition,
    worker_id,
    epoch,
    start_offset,
  }
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let assignment = preview.assign(partition, worker_id, epoch, start_offset) catch {
    error => raise JobStoreError::Job(error)
  }
  self.append_entry(entry)
  ignore(
    self.state.assign(partition, worker_id, epoch, start_offset) catch {
      error => raise JobStoreError::Job(error)
    },
  )
  assignment
}

///|
pub async fn JobStore::start(self : JobStore) -> Unit raise JobStoreError {
  let entry = empty_job_entry("start")
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit(entry)
}

///|
pub async fn JobStore::record_batch_sent(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  first_offset : Int64,
  event_count : Int64,
) -> Int64 raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("batch_sent"),
    partition,
    worker_id,
    epoch,
    first_offset,
    event_count,
  }
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let end = preview.record_batch_sent(
    partition, worker_id, epoch, first_offset, event_count,
  ) catch {
    error => raise JobStoreError::Job(error)
  }
  self.append_entry(entry)
  ignore(
    self.state.record_batch_sent(
      partition, worker_id, epoch, first_offset, event_count,
    ) catch {
      error => raise JobStoreError::Job(error)
    },
  )
  end
}

///|
pub async fn JobStore::record_progress(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("progress"),
    partition,
    worker_id,
    epoch,
    persisted_offset,
    applied_offset,
  }
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit(entry)
}

///|
pub async fn JobStore::drain(self : JobStore) -> Unit raise JobStoreError {
  let entry = empty_job_entry("drain")
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit(entry)
}

///|
pub async fn JobStore::stop_partition(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  pending_events : Int,
) -> Unit raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("stop_partition"),
    partition,
    worker_id,
    epoch,
    pending_events,
  }
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit(entry)
}

///|
pub async fn JobStore::fail_partition(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  reason : String,
) -> Unit raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("fail_partition"),
    partition,
    worker_id,
    epoch,
    reason,
  }
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit(entry)
}

///|
pub async fn JobStore::reassign(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
  start_offset : Int64,
) -> @control.JobPartition raise JobStoreError {
  let entry = JobJournalEntry::{
    ..empty_job_entry("reassign"),
    partition,
    worker_id,
    epoch,
    start_offset,
  }
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let assignment = preview.reassign(partition, worker_id, epoch, start_offset) catch {
    error => raise JobStoreError::Job(error)
  }
  self.append_entry(entry)
  ignore(
    self.state.reassign(partition, worker_id, epoch, start_offset) catch {
      error => raise JobStoreError::Job(error)
    },
  )
  assignment
}

///|
pub async fn JobStore::fail(
  self : JobStore,
  reason : String,
) -> Unit raise JobStoreError {
  let entry = JobJournalEntry::{ ..empty_job_entry("fail"), reason, }
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit(entry)
}

///|
pub async fn JobStore::cancel(self : JobStore) -> Unit raise JobStoreError {
  let entry = empty_job_entry("cancel")
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit(entry)
}
