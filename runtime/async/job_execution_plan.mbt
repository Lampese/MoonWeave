///|
/// Session IDs deliberately stay outside durable work identity: reconnects
/// must use fresh sessions without changing the finite input/exchange plan.
struct JobExecutionPlan {
  job_id : String
  partition : Int
  worker_id : String
  epoch : Int
  start_offset : Int64
  batches : Array[@protocol.EventBatch]
  deltas : Array[@protocol.DeltaBatch]
  probe : @protocol.Probe?
  required_capability : String?
  append_only : Bool?
} derive(Debug, Eq, ToJson, FromJson)

///|
fn validate_job_work_plan(
  state : @control.JobState,
  plan : JobExecutionPlan,
) -> JobExecutionPlan raise JobStoreError {
  if plan.required_capability is Some(capability) && capability.is_empty() {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  let expected_capability = @task.OperatorKind::from_id(state.operator_id()) catch {
    _ => raise JobStoreError::PlanMismatch(plan.partition)
  }
  match plan.required_capability {
    Some(value) if value != expected_capability.capability() =>
      raise JobStoreError::PlanMismatch(plan.partition)
    _ => ()
  }
  if plan.job_id != state.job_id() ||
    plan.partition < 0 ||
    plan.partition >= state.partition_count() ||
    plan.worker_id.is_empty() ||
    plan.epoch < 0 ||
    plan.start_offset < 0L {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  guard state.partition(plan.partition) is Some(current) else {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  if plan.epoch > current.epoch() ||
    (
      plan.epoch == current.epoch() &&
      (
        plan.worker_id != current.worker_id() ||
        plan.start_offset != current.start_offset()
      )
    ) {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  let batches = []
  let mut next_offset = plan.start_offset
  for raw in plan.batches {
    let batch = @protocol.EventBatch::from_payload(raw.to_payload()) catch {
      _ => raise JobStoreError::PlanMismatch(plan.partition)
    }
    if batch.partition() != plan.partition ||
      batch.epoch() != plan.epoch ||
      batch.first_offset() != next_offset ||
      batch.end_offset() < next_offset ||
      batch.end_offset() == 9223372036854775807L {
      raise JobStoreError::PlanMismatch(plan.partition)
    }
    next_offset = batch.end_offset() + 1L
    batches.push(batch)
  }
  let deltas = []
  let ids : Set[String] = Set([])
  for raw in plan.deltas {
    let delta = @protocol.DeltaBatch::from_payload(raw.to_payload()) catch {
      _ => raise JobStoreError::PlanMismatch(plan.partition)
    }
    if delta.target_partition() != plan.partition ||
      delta.target_epoch() != plan.epoch ||
      !ids.add_and_check(delta.delta_id()) {
      raise JobStoreError::PlanMismatch(plan.partition)
    }
    deltas.push(delta)
  }
  let probe = match plan.probe {
    None => None
    Some(raw) => {
      let value = @protocol.Probe::from_payload(raw.to_payload()) catch {
        _ => raise JobStoreError::PlanMismatch(plan.partition)
      }
      if value.partition() != plan.partition ||
        value.epoch() != plan.epoch ||
        value.target_offset() >= next_offset {
        raise JobStoreError::PlanMismatch(plan.partition)
      }
      Some(value)
    }
  }
  { ..plan, batches, deltas, probe, }
}

///|
fn install_job_work_plan(
  state : @control.JobState,
  plans : Map[Int, JobExecutionPlan],
  plan : JobExecutionPlan,
) -> Unit raise JobStoreError {
  let plan = validate_job_work_plan(state, plan)
  guard state.partition(plan.partition) is Some(partition) else {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  if partition.worker_id() != plan.worker_id ||
    partition.epoch() != plan.epoch ||
    partition.start_offset() != plan.start_offset {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  match plans.get(plan.partition) {
    Some(previous) if previous.required_capability != plan.required_capability ||
      previous.append_only != plan.append_only ||
      previous.epoch > plan.epoch ||
      (
        previous.epoch == plan.epoch &&
        previous != plan &&
        (
          previous.append_only != Some(true) ||
          partition.phase() == @control.JobPartitionPhase::Succeeded ||
          !previous.is_append_only_prefix_of(plan)
        )
      ) => raise JobStoreError::PlanMismatch(plan.partition)
    _ => ()
  }
  plans[plan.partition] = plan
}

///|
/// A source-backed live job may grow its durable work plan while retaining the
/// same lease. Every existing primary batch and exchange delta must remain in
/// the same order; the only permitted mutation is appending new work.
fn JobExecutionPlan::is_append_only_prefix_of(
  self : JobExecutionPlan,
  next : JobExecutionPlan,
) -> Bool {
  if self.job_id != next.job_id ||
    self.partition != next.partition ||
    self.worker_id != next.worker_id ||
    self.epoch != next.epoch ||
    self.start_offset != next.start_offset ||
    self.required_capability != next.required_capability ||
    self.probe != next.probe ||
    self.batches.length() > next.batches.length() ||
    self.deltas.length() > next.deltas.length() {
    return false
  }
  for index in 0..<self.batches.length() {
    if self.batches[index] != next.batches[index] {
      return false
    }
  }
  for index in 0..<self.deltas.length() {
    if self.deltas[index] != next.deltas[index] {
      return false
    }
  }
  true
}

///|
fn JobStore::check_work(
  self : JobStore,
  work : SupervisorWork,
  append_only? : Bool = false,
) -> JobExecutionPlan raise JobStoreError {
  guard self.state.partition(work.partition()) is Some(partition) else {
    raise JobStoreError::PlanMismatch(work.partition())
  }
  let candidate = JobExecutionPlan::{
    job_id: self.job_id(),
    partition: work.partition(),
    worker_id: work.worker_id(),
    epoch: partition.epoch(),
    start_offset: partition.start_offset(),
    batches: work.batches().to_owned(),
    deltas: work.deltas().to_owned(),
    probe: work.probe(),
    required_capability: work.required_capability(),
    append_only: if append_only {
      Some(true)
    } else {
      None
    },
  }
  let plan = validate_job_work_plan(self.state, candidate)
  let preview = self.work_plans.copy()
  install_job_work_plan(self.state, preview, plan)
  let next_offset = match plan.batches.last() {
    Some(batch) => batch.end_offset() + 1L
    None => plan.start_offset
  }
  if partition.next_offset() > next_offset {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  // A previously persisted reservation must end at a complete batch boundary.
  if partition.next_offset() != plan.start_offset &&
    !plan.batches.any(batch => {
      batch.end_offset() + 1L == partition.next_offset()
    }) {
    raise JobStoreError::PlanMismatch(plan.partition)
  }
  plan
}

///|
async fn JobStore::bind_work(
  self : JobStore,
  work : SupervisorWork,
  append_only? : Bool = false,
) -> Unit raise JobStoreError {
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  let plan = self.check_work(work, append_only~)
  if self.work_plans.get(plan.partition) == Some(plan) {
    return
  }
  self.append_entry({ ..empty_job_entry("work_plan"), work_plan: Some(plan), })
  install_job_work_plan(self.state, self.work_plans, plan)
}

///|
pub async fn JobStore::drain_partition(
  self : JobStore,
  partition : Int,
  worker_id : String,
  epoch : Int,
) -> Unit raise JobStoreError {
  self.write_lock.acquire() catch {
    error => raise JobStoreError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  self.commit({
    ..empty_job_entry("drain_partition"),
    partition,
    worker_id,
    epoch,
  })
}
