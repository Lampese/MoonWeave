///|
/// Errors raised while reconciling the durable Job and Coordinator stores.
/// Reconciliation is deliberately fail-closed: an epoch, lease, or frozen
/// plan conflict is never repaired by guessing.
pub(all) suberror ReconciliationError {
  EmptyPath
  PathConflict
  EmptyTransactionId
  EmptyJobId
  Malformed
  UnsupportedVersion(Int)
  IdentityMismatch
  AssignmentMismatch(Int)
  MissingAssignment(Int)
  MissingPlan(Int)
  CursorRegression(Int)
  CursorConflict(Int)
  EpochMismatch(Int)
  PhaseConflict(Int)
  StaleIntent(String)
  UnknownTransaction(String)
  TransactionConflict(String)
  JobStore(JobStoreError)
  CoordinatorStore(CoordinatorStoreError)
  Wal(WalError)
} derive(Debug, Eq)

///|
/// One target state for a partition. The record contains only monotone
/// cursors and the fenced lease identity; it is safe to apply repeatedly.
struct ReconciliationPartition {
  partition : Int
  worker_id : String
  epoch : Int
  start_offset : Int64
  next_offset : Int64
  persisted_offset : Int64
  applied_offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
struct ReconciliationPlan {
  job_id : String
  partitions : Array[ReconciliationPartition]
} derive(Debug, Eq, ToJson, FromJson)

///|
/// The sidecar WAL is a tiny write-ahead intent log for operations that touch
/// both stores. It does not pretend to make two files atomic; an uncommitted
/// plan is replayed idempotently on the next startup.
struct ReconciliationEntry {
  version : Int
  kind : String
  transaction_id : String
  plan : ReconciliationPlan?
} derive(Debug, Eq, ToJson, FromJson)

///|
priv struct ReconciliationLog {
  wal : Wal
  pending : Map[String, ReconciliationPlan]
  mut committed : Set[String]
}

///|
pub(all) struct ReconciliationSummary {
  repaired_transactions : Int
  recovered_transactions : Int
  repaired_partitions : Int
} derive(Debug, Eq)

///|
pub fn ReconciliationSummary::repaired_transactions(
  self : ReconciliationSummary,
) -> Int {
  self.repaired_transactions
}

///|
pub fn ReconciliationSummary::recovered_transactions(
  self : ReconciliationSummary,
) -> Int {
  self.recovered_transactions
}

///|
pub fn ReconciliationSummary::repaired_partitions(
  self : ReconciliationSummary,
) -> Int {
  self.repaired_partitions
}

///|
fn reconciliation_key(transaction_id : String) -> String {
  transaction_id.length().to_string() + ":" + transaction_id
}

///|
fn validate_reconciliation_partition(
  value : ReconciliationPartition,
) -> Unit raise ReconciliationError {
  if value.partition < 0 ||
    value.worker_id.is_empty() ||
    value.epoch < 0 ||
    value.start_offset < 0L ||
    value.next_offset < value.start_offset ||
    value.persisted_offset < value.start_offset - 1L ||
    value.applied_offset < value.start_offset - 1L ||
    value.applied_offset > value.persisted_offset ||
    value.persisted_offset >= value.next_offset ||
    value.applied_offset >= value.next_offset {
    raise ReconciliationError::CursorConflict(value.partition)
  }
}

///|
fn validate_reconciliation_plan(
  plan : ReconciliationPlan,
) -> ReconciliationPlan raise ReconciliationError {
  if plan.job_id.is_empty() {
    raise ReconciliationError::EmptyJobId
  }
  let seen : Set[Int] = Set([])
  let partitions = plan.partitions.copy()
  partitions.sort_by((left, right) => left.partition.compare(right.partition))
  for partition in partitions {
    validate_reconciliation_partition(partition)
    if !seen.add_and_check(partition.partition) {
      raise ReconciliationError::TransactionConflict(
        reconciliation_key(partition.partition.to_string()),
      )
    }
  }
  { job_id: plan.job_id, partitions, }
}

///|
fn reconciliation_entry_prepare(
  transaction_id : String,
  plan : ReconciliationPlan,
) -> ReconciliationEntry {
  { version: 1, kind: "prepare", transaction_id, plan: Some(plan), }
}

///|
fn reconciliation_entry_commit(transaction_id : String) -> ReconciliationEntry {
  { version: 1, kind: "commit", transaction_id, plan: None, }
}

///|
fn install_reconciliation_entry(
  pending : Map[String, ReconciliationPlan],
  committed : Set[String],
  entry : ReconciliationEntry,
) -> Unit raise ReconciliationError {
  if entry.version != 1 {
    raise ReconciliationError::UnsupportedVersion(entry.version)
  }
  if entry.transaction_id.is_empty() {
    raise ReconciliationError::EmptyTransactionId
  }
  let key = reconciliation_key(entry.transaction_id)
  match entry.kind {
    "prepare" => {
      guard entry.plan is Some(raw_plan) else {
        raise ReconciliationError::Malformed
      }
      let plan = validate_reconciliation_plan(raw_plan)
      if committed.contains(entry.transaction_id) {
        raise ReconciliationError::TransactionConflict(key)
      }
      match pending.get(entry.transaction_id) {
        Some(previous) if previous != plan =>
          raise ReconciliationError::TransactionConflict(key)
        _ => pending[entry.transaction_id] = plan
      }
    }
    "commit" => {
      if entry.plan is Some(_) {
        raise ReconciliationError::Malformed
      }
      if committed.contains(entry.transaction_id) {
        return
      }
      guard pending.contains(entry.transaction_id) else {
        raise ReconciliationError::UnknownTransaction(key)
      }
      pending.remove(entry.transaction_id)
      committed.add(entry.transaction_id)
    }
    _ => raise ReconciliationError::Malformed
  }
}

///|
async fn ReconciliationLog::open(
  path : String,
) -> ReconciliationLog raise ReconciliationError {
  if path.is_empty() {
    raise ReconciliationError::EmptyPath
  }
  let (wal, replay) = Wal::open(path) catch {
    error => raise ReconciliationError::Wal(error)
  }
  errdefer wal.close()
  let pending : Map[String, ReconciliationPlan] = Map([])
  let committed : Set[String] = Set([])
  for encoded in replay.records() {
    let text = @utf8.decode(encoded) catch {
      _ => raise ReconciliationError::Malformed
    }
    let json = @json.parse(text) catch {
      _ => raise ReconciliationError::Malformed
    }
    let entry : ReconciliationEntry = @json.from_json(json) catch {
      _ => raise ReconciliationError::Malformed
    }
    install_reconciliation_entry(pending, committed, entry)
  }
  { wal, pending, committed, }
}

///|
fn ReconciliationLog::close(self : ReconciliationLog) -> Unit {
  self.wal.close()
}

///|
/// Remove completed transactions while retaining every prepared intent that
/// still needs replay. A completed transaction has already been applied to
/// both stores, so its commit marker carries no recovery value. The rewrite is
/// atomic under the sidecar WAL lock; in-memory committed state is cleared only
/// after the replacement is installed successfully.
async fn ReconciliationLog::compact(
  self : ReconciliationLog,
) -> Unit raise ReconciliationError {
  let entries : Array[Bytes] = []
  for transaction_id in self.pending_ids() {
    guard self.pending.get(transaction_id) is Some(plan) else {
      raise ReconciliationError::Malformed
    }
    entries.push(
      @utf8.encode(
        @json.to_json(reconciliation_entry_prepare(transaction_id, plan)).stringify(),
      ),
    )
  }
  self.wal.rewrite(entries[:]) catch {
    error => raise ReconciliationError::Wal(error)
  }
  self.committed = Set([])
}

///|
fn ReconciliationLog::pending_ids(self : ReconciliationLog) -> Array[String] {
  let ids = self.pending.keys().to_array()
  ids.sort()
  ids
}

///|
fn reconciliation_plan_id(plan : ReconciliationPlan) -> String {
  let material = @json.to_json(plan).stringify()
  let mut hash : UInt64 = 14695981039346656037UL
  for byte in @utf8.encode(material) {
    hash = (hash ^ byte.to_uint64()) * 1099511628211UL
  }
  "reconcile-" + hash.to_string(radix=16)
}

///|
/// Allocate a fresh transaction identity even when an identical repair plan
/// is needed again later. The WAL offset is monotone for this sidecar, while
/// the plan digest makes the intent auditable.
fn ReconciliationLog::transaction_id(
  self : ReconciliationLog,
  plan : ReconciliationPlan,
) -> String {
  reconciliation_plan_id(plan) + "-" + self.wal.write_offset().to_string()
}

///|
async fn ReconciliationLog::prepare(
  self : ReconciliationLog,
  transaction_id : String,
  plan : ReconciliationPlan,
) -> Unit raise ReconciliationError {
  let plan = validate_reconciliation_plan(plan)
  let entry = reconciliation_entry_prepare(transaction_id, plan)
  ignore(
    self.wal.append(@utf8.encode(@json.to_json(entry).stringify())) catch {
      error => raise ReconciliationError::Wal(error)
    },
  )
  install_reconciliation_entry(self.pending, self.committed, entry)
}

///|
async fn ReconciliationLog::commit(
  self : ReconciliationLog,
  transaction_id : String,
) -> Unit raise ReconciliationError {
  let entry = reconciliation_entry_commit(transaction_id)
  ignore(
    self.wal.append(@utf8.encode(@json.to_json(entry).stringify())) catch {
      error => raise ReconciliationError::Wal(error)
    },
  )
  install_reconciliation_entry(self.pending, self.committed, entry)
}
