///|
/// Begin a completion barrier for one partition. Sibling partitions may still
/// be running or awaiting an explicit failed-lease replacement.
pub fn JobState::drain_partition(
  self : JobState,
  partition : Int,
  worker_id : String,
  epoch : Int,
) -> Unit raise JobError {
  if self.phase != JobPhase::Running && self.phase != JobPhase::Draining {
    raise JobError::InvalidTransition(
      expected=JobPhase::Running,
      actual=self.phase,
    )
  }
  let current = self.checked_partition(partition)
  check_identity(current, worker_id, epoch)
  if current.phase != JobPartitionPhase::Running &&
    current.phase != JobPartitionPhase::Draining {
    raise JobError::PartitionNotRunning(partition)
  }
  if current.applied_offset != current.next_offset - 1L {
    raise JobError::NotCaughtUp(
      expected=current.next_offset - 1L,
      actual=current.applied_offset,
    )
  }
  current.phase = JobPartitionPhase::Draining
  if self.partitions
    .values()
    .all(value => {
      value.phase == JobPartitionPhase::Draining ||
      value.phase == JobPartitionPhase::Succeeded
    }) {
    self.phase = JobPhase::Draining
  }
}
