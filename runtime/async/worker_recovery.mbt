///|
fn worker_recovery_reassignments(
  plan : ReconciliationPlan,
) -> Array[@control.JobReassignment] {
  plan.partitions.map(target => {
    @control.JobReassignment::new(
      target.partition,
      target.worker_id,
      target.epoch,
      target.start_offset,
    )
  })
}

///|
fn build_worker_recovery_plan(
  coordinator : CoordinatorStore,
  job : JobStore,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  replacement_worker_id : String,
) -> ReconciliationPlan raise ReconciliationError {
  let preview = coordinator.preview_state() catch {
    error => raise ReconciliationError::CoordinatorStore(error)
  }
  let moved = preview.fence_and_reassign(
    worker_id, incarnation, replacement_worker_id,
  ) catch {
    error =>
      raise ReconciliationError::CoordinatorStore(
        CoordinatorStoreError::Coordinator(error),
      )
  }
  if moved.is_empty() {
    raise ReconciliationError::MissingAssignment(-1)
  }
  guard preview.worker(replacement_worker_id) is Some(replacement) else {
    raise ReconciliationError::IdentityMismatch
  }
  let plan = validate_reconciliation_plan({
    job_id: job.job_id(),
    recovery: Some({
      cluster_id: preview.cluster_id(),
      coordinator_id: preview.coordinator_id(),
      old_worker_id: worker_id,
      old_incarnation: incarnation.value(),
      replacement_worker_id,
      replacement_incarnation: replacement.incarnation().value(),
    }),
    partitions: moved.map(lease => {
      partition: lease.partition(),
      worker_id: lease.worker_id(),
      epoch: lease.epoch(),
      start_offset: lease.start_offset(),
      next_offset: lease.start_offset(),
      persisted_offset: lease.start_offset() - 1L,
      applied_offset: lease.start_offset() - 1L,
    }),
  })
  ignore(check_worker_recovery(coordinator, job, plan))
  plan
}

///|
fn reconciliation_sidecar_path(
  coordinator : CoordinatorStore,
  job : JobStore,
  reconcile_path : String?,
) -> String raise ReconciliationError {
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
  path
}

///|
async fn replay_reconciliation_unlocked(
  log : ReconciliationLog,
  coordinator : CoordinatorStore,
  job : JobStore,
) -> (Int, Int) raise ReconciliationError {
  let mut recovered = 0
  let mut repaired_partitions = 0
  for transaction_id in log.pending_ids() {
    guard log.pending.get(transaction_id) is Some(plan) else {
      raise ReconciliationError::Malformed
    }
    if plan.job_id != job.job_id() {
      raise ReconciliationError::StaleIntent(transaction_id)
    }
    ignore(validate_reconciliation_plan(plan))
    apply_reconciliation_plan(coordinator, job, plan)
    log.commit(transaction_id)
    recovered += 1
    repaired_partitions += plan.partitions.length()
  }
  (recovered, repaired_partitions)
}

///|
/// Validate both stores before either write. Each store must be wholly before
/// or wholly after this intent; a partial, stale, or advanced state is unsafe.
fn check_worker_recovery(
  coordinator : CoordinatorStore,
  job : JobStore,
  plan : ReconciliationPlan,
) -> (Bool, Bool) raise ReconciliationError {
  validate_worker_recovery_intent(plan)
  guard plan.recovery is Some(intent) else {
    raise ReconciliationError::Malformed
  }
  let state = coordinator.state()
  if plan.job_id != job.job_id() ||
    intent.cluster_id != state.cluster_id() ||
    intent.coordinator_id != state.coordinator_id() {
    raise ReconciliationError::IdentityMismatch
  }
  guard state.worker(intent.old_worker_id) is Some(old) &&
    state.worker(intent.replacement_worker_id) is Some(replacement) else {
    raise ReconciliationError::IdentityMismatch
  }
  if old.incarnation().value() != intent.old_incarnation ||
    replacement.incarnation().value() != intent.replacement_incarnation ||
    replacement.lifecycle() != @control.WorkerLifecycle::Active {
    raise ReconciliationError::IdentityMismatch
  }
  let coordinator_pending = old.lifecycle() != @control.WorkerLifecycle::Fenced
  let expected_partitions = plan.partitions.map(target => target.partition)
  let owned = state
    .assignments()
    .filter(lease => lease.worker_id() == intent.old_worker_id)
  if owned.map(lease => lease.partition()) !=
    (if coordinator_pending { expected_partitions } else { [] }) {
    raise ReconciliationError::AssignmentMismatch(-1)
  }
  let job_snapshot = job.snapshot()
  if job_snapshot.phase() != @control.JobPhase::Running &&
    job_snapshot.phase() != @control.JobPhase::Draining {
    raise ReconciliationError::PhaseConflict(-1)
  }
  let job_owned = job_snapshot
    .partitions()
    .filter(partition => partition.worker_id() == intent.old_worker_id)
  let job_pending = !job_owned.is_empty()
  if job_owned.map(partition => partition.partition()) !=
    (if job_pending { expected_partitions } else { [] }) {
    raise ReconciliationError::AssignmentMismatch(-1)
  }
  // The Coordinator fence is always durable before the Job replacement.
  if coordinator_pending && !job_pending {
    raise ReconciliationError::StaleIntent(reconciliation_plan_id(plan))
  }
  for target in plan.partitions {
    guard state.assignment(target.partition) is Some(lease) else {
      raise ReconciliationError::MissingAssignment(target.partition)
    }
    let expected_worker = if coordinator_pending {
      intent.old_worker_id
    } else {
      target.worker_id
    }
    let expected_incarnation = if coordinator_pending {
      intent.old_incarnation
    } else {
      intent.replacement_incarnation
    }
    let expected_epoch = if coordinator_pending {
      target.epoch - 1
    } else {
      target.epoch
    }
    if lease.worker_id() != expected_worker ||
      lease.incarnation().value() != expected_incarnation ||
      lease.epoch() != expected_epoch ||
      lease.start_offset() != target.start_offset {
      raise ReconciliationError::AssignmentMismatch(target.partition)
    }
    if !coordinator_pending &&
      (
        lease.next_offset() != target.next_offset ||
        lease.persisted_offset() != target.persisted_offset ||
        lease.applied_offset() != target.applied_offset
      ) {
      raise ReconciliationError::CursorConflict(target.partition)
    }
    let partition = reconciliation_job_partition(job_snapshot, target.partition)
    if job_pending {
      if partition.worker_id() != intent.old_worker_id ||
        partition.epoch() != target.epoch - 1 ||
        partition.start_offset() != target.start_offset {
        raise ReconciliationError::AssignmentMismatch(target.partition)
      }
      if partition.phase() != @control.JobPartitionPhase::Running &&
        partition.phase() != @control.JobPartitionPhase::Draining &&
        partition.phase() != @control.JobPartitionPhase::Failed {
        raise ReconciliationError::PhaseConflict(target.partition)
      }
    } else {
      let phase = if job_snapshot.phase() == @control.JobPhase::Running {
        @control.JobPartitionPhase::Running
      } else {
        @control.JobPartitionPhase::Draining
      }
      if partition.worker_id() != target.worker_id ||
        partition.epoch() != target.epoch ||
        partition.start_offset() != target.start_offset ||
        partition.phase() != phase ||
        partition.failure_reason() is Some(_) {
        raise ReconciliationError::AssignmentMismatch(target.partition)
      }
      if partition.next_offset() != target.next_offset ||
        partition.persisted_offset() != target.persisted_offset ||
        partition.applied_offset() != target.applied_offset {
        raise ReconciliationError::CursorConflict(target.partition)
      }
    }
    if job.work_plans.get(target.partition) is Some(work) &&
      work.required_capability is Some(capability) &&
      !replacement.capabilities().contains(capability) {
      raise ReconciliationError::IdentityMismatch
    }
  }
  (coordinator_pending, job_pending)
}

///|
async fn apply_worker_recovery(
  coordinator : CoordinatorStore,
  job : JobStore,
  plan : ReconciliationPlan,
) -> Unit raise ReconciliationError {
  let (coordinator_pending, job_pending) = check_worker_recovery(
    coordinator, job, plan,
  )
  guard plan.recovery is Some(intent) else {
    raise ReconciliationError::Malformed
  }
  if coordinator_pending {
    let incarnation = @protocol.Incarnation::new(intent.old_incarnation) catch {
      _ => raise ReconciliationError::Malformed
    }
    ignore(
      coordinator.reconcile_fence_and_reassign_unlocked(
        intent.old_worker_id,
        incarnation,
        intent.replacement_worker_id,
      ),
    ) catch {
      error => raise ReconciliationError::CoordinatorStore(error)
    }
  }
  if job_pending {
    ignore(
      job.reconcile_recover_many_unlocked(
        worker_recovery_reassignments(plan),
        "Worker recovery",
      ),
    ) catch {
      error => raise ReconciliationError::JobStore(error)
    }
  }
}

///|
async fn apply_reconciliation_plan(
  coordinator : CoordinatorStore,
  job : JobStore,
  plan : ReconciliationPlan,
) -> Unit raise ReconciliationError {
  match plan.recovery {
    Some(_) => apply_worker_recovery(coordinator, job, plan)
    None =>
      for target in plan.partitions {
        apply_reconciliation_partition(coordinator, job, target)
      }
  }
}

///|
/// Move every failed lease of one Worker using a durable cross-store intent.
/// Stop its supervisor first. If any write fails, run reconciliation before
/// accepting work again; the intent replays without advancing epochs twice.
pub async fn recover_worker_leases(
  coordinator : CoordinatorStore,
  job : JobStore,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  replacement_worker_id : String,
  reconcile_path? : String,
) -> Array[@control.PartitionAssignment] raise ReconciliationError {
  if !job.supervisor_lock.try_acquire() {
    raise ReconciliationError::JobStore(JobStoreError::AlreadyRunning)
  }
  defer job.supervisor_lock.release()
  let path = reconciliation_sidecar_path(coordinator, job, reconcile_path)
  let log = ReconciliationLog::open(
    path,
    maximum_bytes=coordinator.journal.maximum_bytes(),
  )
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
  let mut replayed : ReconciliationPlan? = None
  for plan in log.pending.values() {
    if plan.recovery is Some(intent) &&
      intent.old_worker_id == worker_id &&
      intent.old_incarnation == incarnation.value() &&
      intent.replacement_worker_id == replacement_worker_id {
      replayed = Some(plan)
    }
  }
  ignore(replay_reconciliation_unlocked(log, coordinator, job))
  let plan = match replayed {
    Some(plan) => plan
    None => {
      let plan = build_worker_recovery_plan(
        coordinator, job, worker_id, incarnation, replacement_worker_id,
      )
      let transaction_id = log.transaction_id(plan)
      log.prepare(transaction_id, plan)
      apply_worker_recovery(coordinator, job, plan)
      log.commit(transaction_id)
      plan
    }
  }
  log.compact()
  let result : Array[@control.PartitionAssignment] = []
  for target in plan.partitions {
    guard coordinator.state().assignment(target.partition) is Some(lease) else {
      raise ReconciliationError::MissingAssignment(target.partition)
    }
    result.push(lease)
  }
  result
}
