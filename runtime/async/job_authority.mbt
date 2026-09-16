///|
/// Replace lifecycle and delivery plans together using the same recoverable
/// checkpoint/WAL generation switch as compaction. Callers must stop the
/// supervisor before installing authority from another Coordinator.
pub async fn JobStore::replace_authority_snapshot(
  self : JobStore,
  authority : JobStoreAuthoritySnapshot,
) -> Unit raise JobStoreError {
  if authority.version != 1 {
    raise JobStoreError::UnsupportedVersion(authority.version)
  }
  if !self.supervisor_lock.try_acquire() {
    raise JobStoreError::AlreadyRunning
  }
  defer self.supervisor_lock.release()
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let before = self.state.snapshot()
  let snapshot = authority.snapshot
  if before.job_id() != snapshot.job_id() ||
    before.operator_id() != snapshot.operator_id() ||
    before.partition_count() != snapshot.partition_count() ||
    before.routes() != snapshot.routes() {
    raise JobStoreError::Malformed
  }
  let state = @control.JobState::from_snapshot(snapshot) catch {
    error => raise JobStoreError::Job(error)
  }
  let plans : Map[Int, JobExecutionPlan] = Map([])
  for encoded in authority.work_plans {
    let raw : JobExecutionPlan = @json.from_json(encoded) catch {
      _ => raise JobStoreError::Malformed
    }
    let plan = validate_job_work_plan(state, raw)
    if plans.contains(plan.partition) {
      raise JobStoreError::PlanMismatch(plan.partition)
    }
    let partition = snapshot.partitions()[plan.partition]
    if plan.epoch == partition.epoch() {
      let floor = plan.retained_from.unwrap_or(plan.start_offset)
      let end = plan.batches
        .last()
        .map(batch => batch.end_offset() + 1L)
        .unwrap_or(floor)
      if partition.next_offset() > end ||
        (
          partition.next_offset() != floor &&
          !plan.batches.any(batch => {
            batch.end_offset() + 1L == partition.next_offset()
          })
        ) {
        raise JobStoreError::PlanMismatch(plan.partition)
      }
    }
    plans[plan.partition] = plan
  }
  for partition in snapshot.partitions() {
    if !plans.contains(partition.partition()) &&
      partition.next_offset() > partition.start_offset() &&
      snapshot.phase() != @control.JobPhase::Succeeded &&
      snapshot.phase() != @control.JobPhase::Cancelled &&
      snapshot.phase() != @control.JobPhase::Failed {
      raise JobStoreError::PlanMismatch(partition.partition())
    }
  }
  self.checkpoint_plans_and_compact(plans, state~)
}
