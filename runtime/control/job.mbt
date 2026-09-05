///|
/// Lifecycle of one logical Job in the Coordinator control plane.
pub(all) enum JobPhase {
  Created
  Running
  Draining
  Succeeded
  Failed
  Cancelled
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// Lifecycle of one partition lease inside a Job.
pub(all) enum JobPartitionPhase {
  Unassigned
  Assigned
  Running
  Draining
  Succeeded
  Failed
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// Errors raised by the pure Job lifecycle controller.
pub(all) suberror JobError {
  EmptyReason
  InvalidTransition(expected~ : JobPhase, actual~ : JobPhase)
  NotFullyAssigned
  UnknownPartition(Int)
  UnknownWorker(String)
  AssignmentConflict(Int)
  InvalidEpoch(Int)
  InvalidOffset(Int64)
  InvalidEventCount(Int64)
  WorkerMismatch(expected~ : String, actual~ : String)
  EpochMismatch(expected~ : Int, actual~ : Int)
  OffsetRegression(expected_at_least~ : Int64, actual~ : Int64)
  AppliedAheadOfPersisted
  ProgressAheadOfSent
  PartitionFailed(Int)
  PartitionNotRunning(Int)
  NotCaughtUp(expected~ : Int64, actual~ : Int64)
  PartitionNotDraining(Int)
  PendingEvents(Int, Int)
} derive(Debug, Eq)

///|
/// Coordinator-visible state for one Job partition. Offsets are source
/// offsets, while phase describes the lifecycle of the corresponding lease.
pub(all) struct JobPartition {
  partition : Int
  worker_id : String
  epoch : Int
  start_offset : Int64
  mut next_offset : Int64
  mut persisted_offset : Int64
  mut applied_offset : Int64
  mut phase : JobPartitionPhase
  mut failure_reason : String?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobPartition::partition(self : JobPartition) -> Int {
  self.partition
}

///|
pub fn JobPartition::worker_id(self : JobPartition) -> String {
  self.worker_id
}

///|
pub fn JobPartition::epoch(self : JobPartition) -> Int {
  self.epoch
}

///|
pub fn JobPartition::start_offset(self : JobPartition) -> Int64 {
  self.start_offset
}

///|
pub fn JobPartition::next_offset(self : JobPartition) -> Int64 {
  self.next_offset
}

///|
pub fn JobPartition::persisted_offset(self : JobPartition) -> Int64 {
  self.persisted_offset
}

///|
pub fn JobPartition::applied_offset(self : JobPartition) -> Int64 {
  self.applied_offset
}

///|
pub fn JobPartition::phase(self : JobPartition) -> JobPartitionPhase {
  self.phase
}

///|
pub fn JobPartition::failure_reason(self : JobPartition) -> String? {
  self.failure_reason
}

///|
/// Mutable Coordinator-side lifecycle state. All methods are synchronous and
/// deterministic; async adapters supply transport and durable barriers.
pub struct JobState {
  spec : @plan.JobSpec
  mut phase : JobPhase
  partitions : Map[Int, JobPartition]
  mut failure_reason : String?
}

///|
pub fn JobState::new(spec : @plan.JobSpec) -> JobState {
  let partitions : Map[Int, JobPartition] = Map([])
  for partition in 0..<spec.partition_count() {
    partitions[partition] = JobPartition::{
      partition,
      worker_id: "",
      epoch: -1,
      start_offset: 0L,
      next_offset: 0L,
      persisted_offset: -1L,
      applied_offset: -1L,
      phase: JobPartitionPhase::Unassigned,
      failure_reason: None,
    }
  }
  { spec, phase: JobPhase::Created, partitions, failure_reason: None, }
}

///|
pub fn JobState::spec(self : JobState) -> @plan.JobSpec {
  self.spec
}

///|
pub fn JobState::job_id(self : JobState) -> String {
  self.spec.job_id()
}

///|
pub fn JobState::operator_id(self : JobState) -> String {
  self.spec.operator_id()
}

///|
pub fn JobState::partition_count(self : JobState) -> Int {
  self.spec.partition_count()
}

///|
pub fn JobState::phase(self : JobState) -> JobPhase {
  self.phase
}

///|
pub fn JobState::failure_reason(self : JobState) -> String? {
  self.failure_reason
}

///|
pub fn JobState::partitions(self : JobState) -> Array[JobPartition] {
  let result = self.partitions.values().to_array()
  result.sort_by((left, right) => left.partition.compare(right.partition))
  result
}

///|
pub fn JobState::partition(self : JobState, partition : Int) -> JobPartition? {
  self.partitions.get(partition)
}

///|
fn JobState::checked_partition(
  self : JobState,
  partition : Int,
) -> JobPartition raise JobError {
  match self.partitions.get(partition) {
    Some(value) => value
    None => raise JobError::UnknownPartition(partition)
  }
}

///|
fn check_identity(
  current : JobPartition,
  worker_id : String,
  epoch : Int,
) -> Unit raise JobError {
  if current.worker_id != worker_id {
    raise JobError::WorkerMismatch(expected=current.worker_id, actual=worker_id)
  }
  if current.epoch != epoch {
    raise JobError::EpochMismatch(expected=current.epoch, actual=epoch)
  }
}

///|
/// Install one initial assignment. Repeating the exact same assignment is
/// idempotent; changing an existing lease requires an explicit reassign path.
pub fn JobState::assign(
  self : JobState,
  partition : Int,
  worker_id : String,
  epoch : Int,
  start_offset : Int64,
) -> JobPartition raise JobError {
  if self.phase != JobPhase::Created {
    raise JobError::InvalidTransition(
      expected=JobPhase::Created,
      actual=self.phase,
    )
  }
  if epoch < 0 {
    raise JobError::InvalidEpoch(epoch)
  }
  if start_offset < 0L {
    raise JobError::InvalidOffset(start_offset)
  }
  if !self.spec.workers().any(worker => worker == worker_id) {
    raise JobError::UnknownWorker(worker_id)
  }
  let current = self.checked_partition(partition)
  match current.phase {
    JobPartitionPhase::Unassigned => {
      let assigned = JobPartition::{
        partition,
        worker_id,
        epoch,
        start_offset,
        next_offset: start_offset,
        persisted_offset: start_offset - 1L,
        applied_offset: start_offset - 1L,
        phase: JobPartitionPhase::Assigned,
        failure_reason: None,
      }
      self.partitions[partition] = assigned
      assigned
    }
    _ =>
      if current.worker_id == worker_id &&
        current.epoch == epoch &&
        current.start_offset == start_offset {
        current
      } else {
        raise JobError::AssignmentConflict(partition)
      }
  }
}

///|
fn JobState::all_assigned(self : JobState) -> Bool {
  self.partitions
  .values()
  .all(partition => partition.phase != JobPartitionPhase::Unassigned)
}

///|
pub fn JobState::start(self : JobState) -> Unit raise JobError {
  if self.phase != JobPhase::Created {
    raise JobError::InvalidTransition(
      expected=JobPhase::Created,
      actual=self.phase,
    )
  }
  if !self.all_assigned() {
    raise JobError::NotFullyAssigned
  }
  self.phase = JobPhase::Running
  for partition in self.partitions.values() {
    if partition.phase is JobPartitionPhase::Assigned {
      partition.phase = JobPartitionPhase::Running
    }
  }
}

///|
/// Record a source reservation before its batch enters the network.
pub fn JobState::record_batch_sent(
  self : JobState,
  partition : Int,
  worker_id : String,
  epoch : Int,
  first_offset : Int64,
  event_count : Int64,
) -> Int64 raise JobError {
  if self.phase != JobPhase::Running && self.phase != JobPhase::Draining {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  if event_count <= 0L {
    raise JobError::InvalidEventCount(event_count)
  }
  let current = self.checked_partition(partition)
  check_identity(current, worker_id, epoch)
  if current.phase != JobPartitionPhase::Running &&
    current.phase != JobPartitionPhase::Draining {
    raise JobError::PartitionNotRunning(partition)
  }
  if first_offset != current.next_offset {
    raise JobError::InvalidOffset(first_offset)
  }
  let end_offset = first_offset + event_count - 1L
  current.next_offset += event_count
  end_offset
}

///|
/// Record monotone worker progress, bounded by the source cursor already sent.
pub fn JobState::record_progress(
  self : JobState,
  partition : Int,
  worker_id : String,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise JobError {
  if self.phase != JobPhase::Running && self.phase != JobPhase::Draining {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  let current = self.checked_partition(partition)
  check_identity(current, worker_id, epoch)
  if persisted_offset < current.persisted_offset {
    raise JobError::OffsetRegression(
      expected_at_least=current.persisted_offset,
      actual=persisted_offset,
    )
  }
  if applied_offset < current.applied_offset {
    raise JobError::OffsetRegression(
      expected_at_least=current.applied_offset,
      actual=applied_offset,
    )
  }
  if persisted_offset < current.start_offset - 1L ||
    applied_offset < current.start_offset - 1L {
    raise JobError::InvalidOffset(
      if persisted_offset < current.start_offset - 1L {
        persisted_offset
      } else {
        applied_offset
      },
    )
  }
  if applied_offset > persisted_offset {
    raise JobError::AppliedAheadOfPersisted
  }
  if persisted_offset >= current.next_offset ||
    applied_offset >= current.next_offset {
    raise JobError::ProgressAheadOfSent
  }
  current.persisted_offset = persisted_offset
  current.applied_offset = applied_offset
}

///|
pub fn JobState::drain(self : JobState) -> Unit raise JobError {
  if self.phase != JobPhase::Running {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  for partition in self.partitions.values() {
    if partition.phase is JobPartitionPhase::Failed {
      raise JobError::PartitionFailed(partition.partition)
    }
  }
  self.phase = JobPhase::Draining
  for partition in self.partitions.values() {
    if partition.phase is JobPartitionPhase::Running {
      partition.phase = JobPartitionPhase::Draining
    }
  }
}

///|
/// Mark one lease failed while keeping the Job recoverable. A scheduler may
/// fence its Worker and install a higher-epoch replacement with `reassign`.
pub fn JobState::fail_partition(
  self : JobState,
  partition : Int,
  worker_id : String,
  epoch : Int,
  reason : String,
) -> Unit raise JobError {
  if self.phase != JobPhase::Running && self.phase != JobPhase::Draining {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  if reason.is_empty() {
    raise JobError::EmptyReason
  }
  let current = self.checked_partition(partition)
  check_identity(current, worker_id, epoch)
  if current.phase != JobPartitionPhase::Running &&
    current.phase != JobPartitionPhase::Draining {
    raise JobError::PartitionFailed(partition)
  }
  current.phase = JobPartitionPhase::Failed
  current.failure_reason = Some(reason)
}

///|
/// Replace a failed partition lease with a strictly higher fencing epoch.
/// Progress restarts at the supplied source offset and inherits the Job's
/// current Running/Draining phase.
pub fn JobState::reassign(
  self : JobState,
  partition : Int,
  worker_id : String,
  epoch : Int,
  start_offset : Int64,
) -> JobPartition raise JobError {
  if self.phase != JobPhase::Running && self.phase != JobPhase::Draining {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  if epoch < 0 {
    raise JobError::InvalidEpoch(epoch)
  }
  if start_offset < 0L {
    raise JobError::InvalidOffset(start_offset)
  }
  if !self.spec.workers().any(worker => worker == worker_id) {
    raise JobError::UnknownWorker(worker_id)
  }
  let current = self.checked_partition(partition)
  if current.phase != JobPartitionPhase::Failed {
    raise JobError::AssignmentConflict(partition)
  }
  if epoch <= current.epoch {
    raise JobError::EpochMismatch(expected=current.epoch + 1, actual=epoch)
  }
  let phase = if self.phase is JobPhase::Running {
    JobPartitionPhase::Running
  } else {
    JobPartitionPhase::Draining
  }
  let replacement = JobPartition::{
    partition,
    worker_id,
    epoch,
    start_offset,
    next_offset: start_offset,
    persisted_offset: start_offset - 1L,
    applied_offset: start_offset - 1L,
    phase,
    failure_reason: None,
  }
  self.partitions[partition] = replacement
  replacement
}

///|
/// Mark one partition stopped after its pending causal work has drained.
pub fn JobState::stop_partition(
  self : JobState,
  partition : Int,
  worker_id : String,
  epoch : Int,
  pending_events : Int,
) -> Unit raise JobError {
  if self.phase != JobPhase::Draining {
    raise JobError::InvalidTransition(
      expected=JobPhase::Draining,
      actual=self.phase,
    )
  }
  if pending_events < 0 {
    raise JobError::PendingEvents(partition, pending_events)
  }
  let current = self.checked_partition(partition)
  check_identity(current, worker_id, epoch)
  if current.phase != JobPartitionPhase::Draining {
    raise JobError::PartitionNotDraining(partition)
  }
  if pending_events > 0 {
    raise JobError::PendingEvents(partition, pending_events)
  }
  if current.applied_offset < current.next_offset - 1L {
    raise JobError::NotCaughtUp(
      expected=current.next_offset - 1L,
      actual=current.applied_offset,
    )
  }
  current.phase = JobPartitionPhase::Succeeded
  if self.partitions
    .values()
    .all(partition => partition.phase is JobPartitionPhase::Succeeded) {
    self.phase = JobPhase::Succeeded
  }
}

///|
/// Fail the whole Job and retain a stable reason for the scheduler/audit log.
pub fn JobState::fail(self : JobState, reason : String) -> Unit raise JobError {
  if reason.is_empty() {
    raise JobError::EmptyReason
  }
  if self.phase is JobPhase::Succeeded || self.phase is JobPhase::Cancelled {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  self.failure_reason = Some(reason)
  self.phase = JobPhase::Failed
  for partition in self.partitions.values() {
    match partition.phase {
      JobPartitionPhase::Succeeded | JobPartitionPhase::Failed => ()
      _ => {
        partition.phase = JobPartitionPhase::Failed
        partition.failure_reason = Some(reason)
      }
    }
  }
}

///|
pub fn JobState::cancel(self : JobState) -> Unit raise JobError {
  if self.phase is JobPhase::Succeeded || self.phase is JobPhase::Failed {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  self.phase = JobPhase::Cancelled
  for partition in self.partitions.values() {
    match partition.phase {
      JobPartitionPhase::Succeeded | JobPartitionPhase::Failed => ()
      _ => {
        partition.phase = JobPartitionPhase::Failed
        partition.failure_reason = Some("cancelled")
      }
    }
  }
}
