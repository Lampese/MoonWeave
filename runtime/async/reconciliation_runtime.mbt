///|
fn reconciliation_job_partition(
  snapshot : @control.JobSnapshot,
  partition : Int,
) -> @control.JobPartition raise ReconciliationError {
  for value in snapshot.partitions() {
    if value.partition() == partition {
      return value
    }
  }
  raise ReconciliationError::AssignmentMismatch(partition)
}

///|
fn reconciliation_coordinator_assignment(
  snapshot : @control.CoordinatorSnapshot,
  partition : Int,
) -> @control.AssignmentSnapshot? {
  for value in snapshot.assignments() {
    if value.partition() == partition {
      return Some(value)
    }
  }
  None
}

///|
fn reconciliation_coordinator_epoch(
  snapshot : @control.CoordinatorSnapshot,
  partition : Int,
) -> Int {
  for value in snapshot.epochs() {
    if value.partition == partition {
      return value.epoch + 1
    }
  }
  0
}

///|
fn reconciliation_max(left : Int64, right : Int64) -> Int64 {
  if left >= right {
    left
  } else {
    right
  }
}

///|
fn reconciliation_identity_matches(
  workers : ArrayView[String],
  worker_id : String,
) -> Bool {
  workers.any(value => value == worker_id)
}

///|
fn reconciliation_target(
  partition : Int,
  worker_id : String,
  epoch : Int,
  start_offset : Int64,
  next_offset : Int64,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> ReconciliationPartition raise ReconciliationError {
  let result = ReconciliationPartition::{
    partition,
    worker_id,
    epoch,
    start_offset,
    next_offset,
    persisted_offset,
    applied_offset,
  }
  validate_reconciliation_partition(result)
  result
}

///|
fn reconciliation_needs_repair(
  target : ReconciliationPartition,
  assignment : @control.AssignmentSnapshot?,
  job_partition : @control.JobPartition,
) -> Bool {
  if assignment is None ||
    assignment.unwrap().worker_id() != target.worker_id ||
    assignment.unwrap().epoch() != target.epoch ||
    assignment.unwrap().start_offset() != target.start_offset {
    return true
  }
  let current = assignment.unwrap()
  current.next_offset() != target.next_offset ||
  current.persisted_offset() != target.persisted_offset ||
  current.applied_offset() != target.applied_offset ||
  job_partition.next_offset() != target.next_offset ||
  job_partition.persisted_offset() != target.persisted_offset ||
  job_partition.applied_offset() != target.applied_offset
}

///|
fn build_reconciliation_plan(
  coordinator : CoordinatorStore,
  job : JobStore,
) -> ReconciliationPlan raise ReconciliationError {
  let job_snapshot = job.snapshot()
  let coordinator_snapshot = coordinator.snapshot()
  if job_snapshot.job_id().is_empty() {
    raise ReconciliationError::EmptyJobId
  }
  for assignment in coordinator_snapshot.assignments() {
    if assignment.partition() >= job_snapshot.partition_count() {
      raise ReconciliationError::AssignmentMismatch(assignment.partition())
    }
  }
  let repairs : Array[ReconciliationPartition] = []
  for job_partition in job_snapshot.partitions() {
    let partition = job_partition.partition()
    let coordinator_assignment = reconciliation_coordinator_assignment(
      coordinator_snapshot, partition,
    )
    if job_snapshot.phase() == @control.JobPhase::Failed ||
      job_snapshot.phase() == @control.JobPhase::Cancelled ||
      job_partition.phase() == @control.JobPartitionPhase::Failed {
      continue
    }
    match job_partition.phase() {
      @control.JobPartitionPhase::Unassigned =>
        match coordinator_assignment {
          None => ()
          Some(assignment) => {
            if job_snapshot.phase() != @control.JobPhase::Created {
              raise ReconciliationError::PhaseConflict(partition)
            }
            if !reconciliation_identity_matches(
                job_snapshot.workers(),
                assignment.worker_id(),
              ) ||
              assignment.next_offset() != assignment.start_offset() ||
              assignment.persisted_offset() != assignment.start_offset() - 1L ||
              assignment.applied_offset() != assignment.start_offset() - 1L {
              raise ReconciliationError::PhaseConflict(partition)
            }
            repairs.push(
              reconciliation_target(
                partition,
                assignment.worker_id(),
                assignment.epoch(),
                assignment.start_offset(),
                assignment.next_offset(),
                assignment.persisted_offset(),
                assignment.applied_offset(),
              ),
            )
          }
        }
      _ => {
        if !reconciliation_identity_matches(
            job_snapshot.workers(),
            job_partition.worker_id(),
          ) {
          raise ReconciliationError::IdentityMismatch
        }
        let target = match coordinator_assignment {
          None =>
            reconciliation_target(
              partition,
              job_partition.worker_id(),
              job_partition.epoch(),
              job_partition.start_offset(),
              job_partition.next_offset(),
              job_partition.persisted_offset(),
              job_partition.applied_offset(),
            )
          Some(assignment) => {
            if assignment.worker_id() != job_partition.worker_id() ||
              assignment.epoch() != job_partition.epoch() ||
              assignment.start_offset() != job_partition.start_offset() {
              raise ReconciliationError::AssignmentMismatch(partition)
            }
            reconciliation_target(
              partition,
              job_partition.worker_id(),
              job_partition.epoch(),
              job_partition.start_offset(),
              reconciliation_max(
                job_partition.next_offset(),
                assignment.next_offset(),
              ),
              reconciliation_max(
                job_partition.persisted_offset(),
                assignment.persisted_offset(),
              ),
              reconciliation_max(
                job_partition.applied_offset(),
                assignment.applied_offset(),
              ),
            )
          }
        }
        if job_snapshot.phase() == @control.JobPhase::Created &&
          (
            target.next_offset != target.start_offset ||
            target.persisted_offset != target.start_offset - 1L ||
            target.applied_offset != target.start_offset - 1L
          ) {
          raise ReconciliationError::PhaseConflict(partition)
        }
        if reconciliation_needs_repair(
            target, coordinator_assignment, job_partition,
          ) {
          repairs.push(target)
        }
      }
    }
  }
  repairs.sort_by((left, right) => left.partition.compare(right.partition))
  { job_id: job_snapshot.job_id(), partitions: repairs, }
}

///|
fn reconciliation_plan_batches(
  plan : JobExecutionPlan,
  target : ReconciliationPartition,
  next_offset : Int64,
) -> Array[@protocol.EventBatch] {
  plan.batches.filter(batch => {
    batch.first_offset() >= next_offset &&
    batch.end_offset() < target.next_offset
  })
}

///|
async fn reconcile_job_cursor(
  job : JobStore,
  target : ReconciliationPartition,
) -> Unit raise ReconciliationError {
  let current = reconciliation_job_partition(job.snapshot(), target.partition)
  // A succeeded partition is already a terminal Job fact. It may still need
  // its Coordinator cursor repaired after a crash, but JobState deliberately
  // rejects further progress writes once the Job is terminal.
  if current.phase() == @control.JobPartitionPhase::Succeeded {
    if current.next_offset() > target.next_offset ||
      current.persisted_offset() > target.persisted_offset ||
      current.applied_offset() > target.applied_offset {
      raise ReconciliationError::CursorRegression(target.partition)
    }
    return
  }
  if current.next_offset() > target.next_offset {
    raise ReconciliationError::CursorRegression(target.partition)
  }
  if current.next_offset() < target.next_offset {
    guard job.work_plans.get(target.partition) is Some(plan) else {
      raise ReconciliationError::MissingPlan(target.partition)
    }
    if plan.worker_id != target.worker_id ||
      plan.epoch != target.epoch ||
      plan.start_offset != target.start_offset {
      raise ReconciliationError::StaleIntent(
        reconciliation_key(target.partition.to_string()),
      )
    }
    let mut cursor = current.next_offset()
    let batches = reconciliation_plan_batches(plan, target, cursor)
    for batch in batches {
      if batch.first_offset() != cursor {
        raise ReconciliationError::MissingPlan(target.partition)
      }
      ignore(
        job.reconcile_record_batch_unlocked(
          target.partition,
          target.worker_id,
          target.epoch,
          batch.first_offset(),
          batch.events().length().to_int64(),
        ) catch {
          error => raise ReconciliationError::JobStore(error)
        },
      )
      cursor = batch.end_offset() + 1L
    }
    if cursor != target.next_offset {
      raise ReconciliationError::MissingPlan(target.partition)
    }
  }
  let current = reconciliation_job_partition(job.snapshot(), target.partition)
  if current.persisted_offset() > target.persisted_offset ||
    current.applied_offset() > target.applied_offset {
    raise ReconciliationError::CursorRegression(target.partition)
  }
  if current.persisted_offset() < target.persisted_offset ||
    current.applied_offset() < target.applied_offset {
    job.reconcile_record_progress_unlocked(
      target.partition,
      target.worker_id,
      target.epoch,
      target.persisted_offset,
      target.applied_offset,
    ) catch {
      error => raise ReconciliationError::JobStore(error)
    }
  }
}

///|
async fn reconcile_coordinator_cursor(
  coordinator : CoordinatorStore,
  job : JobStore,
  target : ReconciliationPartition,
) -> Unit raise ReconciliationError {
  let state = coordinator.state()
  let assignment = state.assignment(target.partition)
  guard assignment is Some(current_assignment) else {
    raise ReconciliationError::MissingAssignment(target.partition)
  }
  if current_assignment.worker_id() != target.worker_id ||
    current_assignment.epoch() != target.epoch ||
    current_assignment.start_offset() != target.start_offset {
    raise ReconciliationError::StaleIntent(
      reconciliation_key(target.partition.to_string()),
    )
  }
  if current_assignment.next_offset() > target.next_offset {
    raise ReconciliationError::CursorRegression(target.partition)
  }
  if current_assignment.next_offset() < target.next_offset {
    guard job.work_plans.get(target.partition) is Some(plan) else {
      raise ReconciliationError::MissingPlan(target.partition)
    }
    if plan.worker_id != target.worker_id ||
      plan.epoch != target.epoch ||
      plan.start_offset != target.start_offset {
      raise ReconciliationError::StaleIntent(
        reconciliation_key(target.partition.to_string()),
      )
    }
    let mut cursor = current_assignment.next_offset()
    let batches = reconciliation_plan_batches(plan, target, cursor)
    for batch in batches {
      if batch.first_offset() != cursor {
        raise ReconciliationError::MissingPlan(target.partition)
      }
      ignore(
        coordinator.reconcile_record_batch_unlocked(
          target.partition,
          target.worker_id,
          current_assignment.incarnation(),
          target.epoch,
          batch.first_offset(),
          batch.events().length().to_int64(),
        ) catch {
          error => raise ReconciliationError::CoordinatorStore(error)
        },
      )
      cursor = batch.end_offset() + 1L
    }
    if cursor != target.next_offset {
      raise ReconciliationError::MissingPlan(target.partition)
    }
  }
  let current = coordinator.state().assignment(target.partition).unwrap()
  if current.persisted_offset() > target.persisted_offset ||
    current.applied_offset() > target.applied_offset {
    raise ReconciliationError::CursorRegression(target.partition)
  }
  if current.persisted_offset() < target.persisted_offset ||
    current.applied_offset() < target.applied_offset {
    coordinator.reconcile_record_progress_unlocked(
      target.partition,
      target.worker_id,
      current.incarnation(),
      target.epoch,
      target.persisted_offset,
      target.applied_offset,
    ) catch {
      error => raise ReconciliationError::CoordinatorStore(error)
    }
  }
}

///|
async fn apply_reconciliation_partition(
  coordinator : CoordinatorStore,
  job : JobStore,
  target : ReconciliationPartition,
) -> Unit raise ReconciliationError {
  let coordinator_snapshot = coordinator.snapshot()
  match
    reconciliation_coordinator_assignment(
      coordinator_snapshot,
      target.partition,
    ) {
    Some(assignment) =>
      if assignment.worker_id() != target.worker_id ||
        assignment.epoch() != target.epoch ||
        assignment.start_offset() != target.start_offset {
        raise ReconciliationError::StaleIntent(
          reconciliation_key(target.partition.to_string()),
        )
      }
    None => {
      if reconciliation_coordinator_epoch(
          coordinator_snapshot,
          target.partition,
        ) !=
        target.epoch {
        raise ReconciliationError::EpochMismatch(target.partition)
      }
      let assignment = coordinator.reconcile_assign_unlocked(
        target.partition,
        target.worker_id,
        target.start_offset,
      ) catch {
        error => raise ReconciliationError::CoordinatorStore(error)
      }
      if assignment.epoch() != target.epoch ||
        assignment.worker_id() != target.worker_id ||
        assignment.start_offset() != target.start_offset {
        raise ReconciliationError::EpochMismatch(target.partition)
      }
    }
  }
  let job_partition = reconciliation_job_partition(
    job.snapshot(),
    target.partition,
  )
  if job_partition.phase() == @control.JobPartitionPhase::Unassigned {
    if job.phase() != @control.JobPhase::Created {
      raise ReconciliationError::PhaseConflict(target.partition)
    }
    ignore(
      job.reconcile_assign_unlocked(
        target.partition,
        target.worker_id,
        target.epoch,
        target.start_offset,
      ) catch {
        error => raise ReconciliationError::JobStore(error)
      },
    )
  } else if job_partition.worker_id() != target.worker_id ||
    job_partition.epoch() != target.epoch ||
    job_partition.start_offset() != target.start_offset {
    raise ReconciliationError::StaleIntent(
      reconciliation_key(target.partition.to_string()),
    )
  }
  reconcile_job_cursor(job, target)
  reconcile_coordinator_cursor(coordinator, job, target)
}

///|
/// Reconcile a durable Job against the durable Coordinator state. This is a
/// startup barrier: callers must run it before accepting new driver writes.
/// A sidecar WAL records prepare/commit intent; a crash between the two store
/// writes leaves a replayable plan instead of an ambiguous silent divergence.
pub async fn reconcile_job_coordinator(
  coordinator : CoordinatorStore,
  job : JobStore,
  reconcile_path? : String,
) -> ReconciliationSummary raise ReconciliationError {
  let path = match reconcile_path {
    Some(value) if !value.is_empty() => value
    Some(_) => raise ReconciliationError::EmptyPath
    None => job.journal_path() + ".coordinator-reconcile"
  }
  if path == coordinator.journal_path() ||
    path == job.journal_path() ||
    path == coordinator.checkpoint_path() ||
    path == job.checkpoint_path() {
    raise ReconciliationError::PathConflict
  }
  let log = ReconciliationLog::open(path)
  defer log.close()
  job.write_lock.acquire() catch {
    error =>
      raise ReconciliationError::JobStore(
        JobStoreError::Io(@debug.to_string(error)),
      )
  }
  defer job.write_lock.release()
  coordinator.write_lock.acquire() catch {
    error =>
      raise ReconciliationError::CoordinatorStore(
        CoordinatorStoreError::Io(@debug.to_string(error)),
      )
  }
  defer coordinator.write_lock.release()
  let job_id = job.job_id()
  let mut recovered_transactions = 0
  let mut repaired_partitions = 0
  for transaction_id in log.pending_ids() {
    guard log.pending.get(transaction_id) is Some(plan) else {
      raise ReconciliationError::Malformed
    }
    if plan.job_id != job_id {
      raise ReconciliationError::StaleIntent(transaction_id)
    }
    ignore(validate_reconciliation_plan(plan))
    for target in plan.partitions {
      apply_reconciliation_partition(coordinator, job, target)
      repaired_partitions += 1
    }
    log.commit(transaction_id)
    recovered_transactions += 1
  }
  let plan = build_reconciliation_plan(coordinator, job)
  if plan.partitions.is_empty() {
    return {
      repaired_transactions: 0,
      recovered_transactions,
      repaired_partitions,
    }
  }
  let transaction_id = log.transaction_id(plan)
  log.prepare(transaction_id, plan)
  for target in plan.partitions {
    apply_reconciliation_partition(coordinator, job, target)
    repaired_partitions += 1
  }
  log.commit(transaction_id)
  { repaired_transactions: 1, recovered_transactions, repaired_partitions, }
}
