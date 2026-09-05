///|
/// Errors raised by the durable Coordinator control store. The store is an
/// async adapter; all state transitions still execute in runtime/control.
pub(all) suberror CoordinatorStoreError {
  EmptyPath
  InvalidJournalOffset(Int64)
  Malformed
  UnsupportedVersion(Int)
  Io(String)
  Coordinator(@control.CoordinatorError)
  Protocol(@protocol.ProtocolError)
} derive(Debug, Eq)

///|
/// Versioned JSON-lines command. Unused fields carry sentinel values so every
/// journal line has one stable schema and can be decoded without ad-hoc maps.
struct CoordinatorJournalEntry {
  version : Int
  kind : String
  mut worker_id : String
  mut incarnation : String
  mut capabilities : Array[String]
  mut partition : Int
  mut epoch : Int
  mut start_offset : Int64
  mut first_offset : Int64
  mut event_count : Int64
  mut persisted_offset : Int64
  mut applied_offset : Int64
  mut tick : Int64
  mut replacement_worker_id : String
  mut partitions : Array[Int]
} derive(Debug, Eq, ToJson, FromJson)

///|
struct CoordinatorStoreCheckpoint {
  version : Int
  journal_offset : Int64
  snapshot : @control.CoordinatorSnapshot
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Async durable wrapper around the synchronous Coordinator state machine.
/// The lock file prevents two Coordinator processes from owning one journal.
pub struct CoordinatorStore {
  state : @control.CoordinatorState
  journal_path : String
  checkpoint_path : String
  journal : Wal
  write_lock : @async.Mutex
  mut journal_offset : Int64
}

///|
fn CoordinatorStore::state(
  self : CoordinatorStore,
) -> @control.CoordinatorState {
  self.state
}

///|
/// Return an immutable data snapshot. Mutating Coordinator state must go
/// through store commands so the journal remains authoritative.
pub fn CoordinatorStore::snapshot(
  self : CoordinatorStore,
) -> @control.CoordinatorSnapshot {
  self.state.snapshot()
}

///|
pub fn CoordinatorStore::journal_path(self : CoordinatorStore) -> String {
  self.journal_path
}

///|
pub fn CoordinatorStore::checkpoint_path(self : CoordinatorStore) -> String {
  self.checkpoint_path
}

///|
pub fn CoordinatorStore::journal_offset(self : CoordinatorStore) -> Int64 {
  self.journal_offset
}

///|
pub fn CoordinatorStore::close(self : CoordinatorStore) -> Unit {
  self.journal.close()
}

///|
/// Clone the pure control state for validation before a journal write. This is
/// intentionally a snapshot round-trip: it keeps the store independent from
/// private control-plane representation details.
fn CoordinatorStore::preview_state(
  self : CoordinatorStore,
) -> @control.CoordinatorState raise CoordinatorStoreError {
  @control.CoordinatorState::from_snapshot(self.state.snapshot()) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
fn empty_entry(kind : String) -> CoordinatorJournalEntry {
  {
    version: 1,
    kind,
    worker_id: "",
    incarnation: "",
    capabilities: [],
    partition: -1,
    epoch: -1,
    start_offset: -1L,
    first_offset: -1L,
    event_count: -1L,
    persisted_offset: -1L,
    applied_offset: -1L,
    tick: -1L,
    replacement_worker_id: "",
    partitions: [],
  }
}

///|
fn decode_incarnation(
  value : String,
) -> @protocol.Incarnation raise CoordinatorStoreError {
  @protocol.Incarnation::new(value) catch {
    error => raise CoordinatorStoreError::Protocol(error)
  }
}

///|
fn apply_journal_entry(
  state : @control.CoordinatorState,
  entry : CoordinatorJournalEntry,
) -> Unit raise CoordinatorStoreError {
  if entry.version != 1 {
    raise CoordinatorStoreError::UnsupportedVersion(entry.version)
  }
  match entry.kind {
    "register" => {
      let incarnation = decode_incarnation(entry.incarnation)
      ignore(
        state.register_worker(
          entry.worker_id,
          incarnation,
          entry.capabilities[:],
        ) catch {
          error => raise CoordinatorStoreError::Coordinator(error)
        },
      )
    }
    "set_suspect" =>
      state.set_suspect(entry.worker_id) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
    "fence" => {
      let incarnation = decode_incarnation(entry.incarnation)
      state.fence_worker(entry.worker_id, incarnation) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
    }
    "assign" =>
      ignore(
        state.assign(
          entry.partition,
          entry.worker_id,
          start_offset=entry.start_offset,
        ) catch {
          error => raise CoordinatorStoreError::Coordinator(error)
        },
      )
    "revoke" =>
      state.revoke(entry.partition) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
    "reassign" =>
      ignore(
        state.reassign(
          entry.partition,
          entry.worker_id,
          start_offset=entry.start_offset,
        ) catch {
          error => raise CoordinatorStoreError::Coordinator(error)
        },
      )
    "recover" => {
      let incarnation = decode_incarnation(entry.incarnation)
      ignore(
        state.fence_and_reassign(
          entry.worker_id,
          incarnation,
          entry.replacement_worker_id,
        ) catch {
          error => raise CoordinatorStoreError::Coordinator(error)
        },
      )
    }
    "batch_sent" => {
      let incarnation = decode_incarnation(entry.incarnation)
      ignore(
        state.record_batch_sent(
          entry.partition,
          entry.worker_id,
          incarnation,
          entry.epoch,
          entry.first_offset,
          entry.event_count,
        ) catch {
          error => raise CoordinatorStoreError::Coordinator(error)
        },
      )
    }
    "resume" => {
      let incarnation = decode_incarnation(entry.incarnation)
      state.record_resume(
        entry.partition,
        entry.worker_id,
        incarnation,
        entry.epoch,
        entry.persisted_offset,
      ) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
    }
    "progress" => {
      let incarnation = decode_incarnation(entry.incarnation)
      state.record_progress(
        entry.partition,
        entry.worker_id,
        incarnation,
        entry.epoch,
        entry.persisted_offset,
        entry.applied_offset,
      ) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
    }
    "heartbeat" => {
      let incarnation = decode_incarnation(entry.incarnation)
      state.record_heartbeat(entry.worker_id, incarnation, entry.tick) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
    }
    _ => raise CoordinatorStoreError::Malformed
  }
}

///|
async fn load_store_checkpoint(
  path : String,
) -> CoordinatorStoreCheckpoint? raise CoordinatorStoreError {
  let exists = @fs.exists(path) catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  if !exists {
    return None
  }
  let text = @fs.read_file(path).text() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  let json = @json.parse(text) catch {
    _ => raise CoordinatorStoreError::Malformed
  }
  let checkpoint : CoordinatorStoreCheckpoint = @json.from_json(json) catch {
    _ => raise CoordinatorStoreError::Malformed
  }
  if checkpoint.version != 1 {
    raise CoordinatorStoreError::UnsupportedVersion(checkpoint.version)
  }
  if checkpoint.journal_offset < 0L {
    raise CoordinatorStoreError::InvalidJournalOffset(checkpoint.journal_offset)
  }
  Some(checkpoint)
}

///|
/// Open a store, repair an incomplete final JSON line, and replay the journal
/// suffix after the latest checkpoint.
pub async fn CoordinatorStore::open(
  cluster_id : String,
  coordinator_id : String,
  journal_path : String,
  checkpoint_path? : String,
) -> CoordinatorStore raise CoordinatorStoreError {
  if journal_path.is_empty() {
    raise CoordinatorStoreError::EmptyPath
  }
  let checkpoint_path = match checkpoint_path {
    Some(path) if !path.is_empty() => path
    Some(_) => raise CoordinatorStoreError::EmptyPath
    None => journal_path + ".checkpoint"
  }
  let (journal, replay) = Wal::open(journal_path) catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  errdefer journal.close()
  let checkpoint = load_store_checkpoint(checkpoint_path)
  let (state, checkpoint_offset) = match checkpoint {
    None => {
      let state = @control.CoordinatorState::new(cluster_id, coordinator_id) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
      (state, 0L)
    }
    Some(value) => {
      if value.snapshot.cluster_id != cluster_id ||
        value.snapshot.coordinator_id != coordinator_id {
        raise CoordinatorStoreError::Malformed
      }
      let state = @control.CoordinatorState::from_snapshot(value.snapshot) catch {
        error => raise CoordinatorStoreError::Coordinator(error)
      }
      (state, value.journal_offset)
    }
  }
  if checkpoint_offset > replay.valid_bytes() ||
    !replay.is_valid_offset(checkpoint_offset) {
    raise CoordinatorStoreError::InvalidJournalOffset(checkpoint_offset)
  }
  let suffix = replay.records_after(checkpoint_offset) catch {
    _ => raise CoordinatorStoreError::InvalidJournalOffset(checkpoint_offset)
  }
  for encoded in suffix {
    let line = @utf8.decode(encoded) catch {
      error => raise CoordinatorStoreError::Io(@debug.to_string(error))
    }
    let json = @json.parse(line) catch {
      _ => raise CoordinatorStoreError::Malformed
    }
    let entry : CoordinatorJournalEntry = @json.from_json(json) catch {
      _ => raise CoordinatorStoreError::Malformed
    }
    apply_journal_entry(state, entry)
  }
  {
    state,
    journal_path,
    checkpoint_path,
    journal,
    write_lock: @async.Mutex::Mutex(),
    journal_offset: replay.valid_bytes(),
  }
}

///|
async fn CoordinatorStore::append_entry(
  self : CoordinatorStore,
  entry : CoordinatorJournalEntry,
) -> Unit raise CoordinatorStoreError {
  let line = @json.to_json(entry).stringify() + "\n"
  let encoded = @utf8.encode(line)
  ignore(
    self.journal.append(encoded) catch {
      error => raise CoordinatorStoreError::Io(@debug.to_string(error))
    },
  )
  self.journal_offset = self.journal.write_offset()
}

///|
/// Store mutations used by the cross-store reconciliation barrier. Callers
/// hold `write_lock`; keeping these helpers unlocked gives reconciliation a
/// stable critical section across both durable state machines.
async fn CoordinatorStore::reconcile_assign_unlocked(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  start_offset : Int64,
) -> @control.PartitionAssignment raise CoordinatorStoreError {
  let preview = self.preview_state()
  let assignment = preview.assign(partition, worker_id, start_offset~) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = CoordinatorJournalEntry::{
    ..empty_entry("assign"),
    partition,
    worker_id,
    start_offset,
  }
  self.append_entry(entry)
  ignore(
    self.state.assign(partition, worker_id, start_offset~) catch {
      error => raise CoordinatorStoreError::Coordinator(error)
    },
  )
  assignment
}

///|
async fn CoordinatorStore::reconcile_record_batch_unlocked(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  first_offset : Int64,
  event_count : Int64,
) -> Int64 raise CoordinatorStoreError {
  let preview = self.preview_state()
  let end_offset = preview.record_batch_sent(
    partition, worker_id, incarnation, epoch, first_offset, event_count,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = CoordinatorJournalEntry::{
    ..empty_entry("batch_sent"),
    partition,
    worker_id,
    incarnation: incarnation.value(),
    epoch,
    first_offset,
    event_count,
  }
  self.append_entry(entry)
  ignore(
    self.state.record_batch_sent(
      partition, worker_id, incarnation, epoch, first_offset, event_count,
    ) catch {
      error => raise CoordinatorStoreError::Coordinator(error)
    },
  )
  end_offset
}

///|
async fn CoordinatorStore::reconcile_record_progress_unlocked(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise CoordinatorStoreError {
  let preview = self.preview_state()
  preview.record_progress(
    partition, worker_id, incarnation, epoch, persisted_offset, applied_offset,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = CoordinatorJournalEntry::{
    ..empty_entry("progress"),
    partition,
    worker_id,
    incarnation: incarnation.value(),
    epoch,
    persisted_offset,
    applied_offset,
  }
  self.append_entry(entry)
  self.state.record_progress(
    partition, worker_id, incarnation, epoch, persisted_offset, applied_offset,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
pub async fn CoordinatorStore::checkpoint(
  self : CoordinatorStore,
) -> Unit raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let checkpoint = CoordinatorStoreCheckpoint::{
    version: 1,
    journal_offset: self.journal_offset,
    snapshot: self.state.snapshot(),
  }
  let encoded = @utf8.encode(@json.to_json(checkpoint).stringify())
  let temporary = self.checkpoint_path + ".tmp"
  @fs.write_file(
    temporary,
    encoded,
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  @fs.rename(temporary, self.checkpoint_path, replace=true) catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
}

///|
pub async fn CoordinatorStore::register_worker(
  self : CoordinatorStore,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  capabilities : ArrayView[String],
) -> @control.RegisterStatus raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let status = preview.register_worker(worker_id, incarnation, capabilities) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("register")
  entry.worker_id = worker_id
  entry.incarnation = incarnation.value()
  entry.capabilities = capabilities.to_owned()
  self.append_entry(entry)
  ignore(
    self.state.register_worker(worker_id, incarnation, capabilities) catch {
      error => raise CoordinatorStoreError::Coordinator(error)
    },
  )
  status
}

///|
pub async fn CoordinatorStore::set_suspect(
  self : CoordinatorStore,
  worker_id : String,
) -> Unit raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  preview.set_suspect(worker_id) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("set_suspect")
  entry.worker_id = worker_id
  self.append_entry(entry)
  self.state.set_suspect(worker_id) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
pub async fn CoordinatorStore::fence_worker(
  self : CoordinatorStore,
  worker_id : String,
  incarnation : @protocol.Incarnation,
) -> Unit raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  preview.fence_worker(worker_id, incarnation) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("fence")
  entry.worker_id = worker_id
  entry.incarnation = incarnation.value()
  self.append_entry(entry)
  self.state.fence_worker(worker_id, incarnation) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
pub async fn CoordinatorStore::assign(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  start_offset? : Int64 = 0L,
) -> @control.PartitionAssignment raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let assignment = preview.assign(partition, worker_id, start_offset~) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("assign")
  entry.partition = partition
  entry.worker_id = worker_id
  entry.start_offset = start_offset
  self.append_entry(entry)
  ignore(
    self.state.assign(partition, worker_id, start_offset~) catch {
      error => raise CoordinatorStoreError::Coordinator(error)
    },
  )
  assignment
}

///|
pub async fn CoordinatorStore::revoke(
  self : CoordinatorStore,
  partition : Int,
) -> Unit raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  preview.revoke(partition) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("revoke")
  entry.partition = partition
  self.append_entry(entry)
  self.state.revoke(partition) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
/// Install a new owner after the previous lease has been fenced/revoked.
pub async fn CoordinatorStore::reassign(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  start_offset? : Int64 = 0L,
) -> @control.PartitionAssignment raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let assignment = preview.reassign(partition, worker_id, start_offset~) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("reassign")
  entry.partition = partition
  entry.worker_id = worker_id
  entry.start_offset = start_offset
  self.append_entry(entry)
  ignore(
    self.state.reassign(partition, worker_id, start_offset~) catch {
      error => raise CoordinatorStoreError::Coordinator(error)
    },
  )
  assignment
}

///|
/// Persist a complete failed-worker recovery decision as one journal command.
pub async fn CoordinatorStore::fence_and_reassign(
  self : CoordinatorStore,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  replacement_worker_id : String,
) -> Array[@control.PartitionAssignment] raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let moved = preview.fence_and_reassign(
    worker_id, incarnation, replacement_worker_id,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("recover")
  entry.worker_id = worker_id
  entry.incarnation = incarnation.value()
  entry.replacement_worker_id = replacement_worker_id
  entry.partitions = moved.map(item => item.partition())
  self.append_entry(entry)
  ignore(
    self.state.fence_and_reassign(worker_id, incarnation, replacement_worker_id) catch {
      error => raise CoordinatorStoreError::Coordinator(error)
    },
  )
  moved
}

///|
/// Record send intent before bytes enter the socket. A crash after this line
/// is conservative: recovery may resend the suffix, but cannot skip it.
pub async fn CoordinatorStore::record_batch_sent(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  first_offset : Int64,
  event_count : Int64,
) -> Int64 raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  let end_offset = preview.record_batch_sent(
    partition, worker_id, incarnation, epoch, first_offset, event_count,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("batch_sent")
  entry.partition = partition
  entry.worker_id = worker_id
  entry.incarnation = incarnation.value()
  entry.epoch = epoch
  entry.first_offset = first_offset
  entry.event_count = event_count
  self.append_entry(entry)
  ignore(
    self.state.record_batch_sent(
      partition, worker_id, incarnation, epoch, first_offset, event_count,
    ) catch {
      error => raise CoordinatorStoreError::Coordinator(error)
    },
  )
  end_offset
}

///|
pub async fn CoordinatorStore::record_resume(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  persisted_offset : Int64,
) -> Unit raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  preview.record_resume(
    partition, worker_id, incarnation, epoch, persisted_offset,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("resume")
  entry.partition = partition
  entry.worker_id = worker_id
  entry.incarnation = incarnation.value()
  entry.epoch = epoch
  entry.persisted_offset = persisted_offset
  self.append_entry(entry)
  self.state.record_resume(
    partition, worker_id, incarnation, epoch, persisted_offset,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
pub async fn CoordinatorStore::record_progress(
  self : CoordinatorStore,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  preview.record_progress(
    partition, worker_id, incarnation, epoch, persisted_offset, applied_offset,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("progress")
  entry.partition = partition
  entry.worker_id = worker_id
  entry.incarnation = incarnation.value()
  entry.epoch = epoch
  entry.persisted_offset = persisted_offset
  entry.applied_offset = applied_offset
  self.append_entry(entry)
  self.state.record_progress(
    partition, worker_id, incarnation, epoch, persisted_offset, applied_offset,
  ) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
pub async fn CoordinatorStore::record_heartbeat(
  self : CoordinatorStore,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  tick : Int64,
) -> Unit raise CoordinatorStoreError {
  self.write_lock.acquire() catch {
    error => raise CoordinatorStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let preview = self.preview_state()
  preview.record_heartbeat(worker_id, incarnation, tick) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
  let entry = empty_entry("heartbeat")
  entry.worker_id = worker_id
  entry.incarnation = incarnation.value()
  entry.tick = tick
  self.append_entry(entry)
  self.state.record_heartbeat(worker_id, incarnation, tick) catch {
    error => raise CoordinatorStoreError::Coordinator(error)
  }
}

///|
/// Persist a liveness timeout observation without fencing the Worker. A later
/// recovery policy may either accept a fresh heartbeat or fence/reassign it.
pub async fn CoordinatorStore::mark_suspect(
  self : CoordinatorStore,
  worker_id : String,
) -> Unit raise CoordinatorStoreError {
  self.set_suspect(worker_id)
}
