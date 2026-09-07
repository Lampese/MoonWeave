///|
test "multi-lease reassignment validates every replacement before mutation" {
  let state = JobState::new(
    @plan.JobSpec::new(
      "multi-reassign",
      "reachability",
      2,
      ["worker-a", "worker-b"],
      [],
    ),
  )
  ignore(state.assign(0, "worker-a", 0, 4L))
  ignore(state.assign(1, "worker-a", 2, 9L))
  state.start()
  state.fail_partition(0, "worker-a", 0, "disconnected")
  state.fail_partition(1, "worker-a", 2, "disconnected")
  let before = state.snapshot()
  let first = JobReassignment::new(0, "worker-b", 1, 4L)
  try
    state.reassign_many([first, JobReassignment::new(1, "worker-b", 2, 9L)])
  catch {
    JobError::EpochMismatch(expected=3, actual=2) => ()
    _ => fail("expected the second replacement to fail")
  } noraise {
    _ => fail("expected the second replacement to fail")
  }
  assert_eq(state.snapshot(), before)
  try state.reassign_many([first, first]) catch {
    JobError::AssignmentConflict(0) => ()
    _ => fail("expected duplicate partition to fail")
  } noraise {
    _ => fail("expected duplicate partition to fail")
  }
  assert_eq(state.snapshot(), before)
  let moved = state.reassign_many([
    first,
    JobReassignment::new(1, "worker-b", 3, 9L),
  ])
  assert_eq(moved.map(partition => partition.worker_id()), [
    "worker-b", "worker-b",
  ])
  assert_eq(moved.map(partition => partition.epoch()), [1, 3])
  assert_eq(moved.map(partition => partition.next_offset()), [4L, 9L])
  assert_eq(moved.map(partition => partition.phase()), [
    JobPartitionPhase::Running,
    JobPartitionPhase::Running,
  ])
}

///|
test "multi-lease reassignment preserves the Job drain phase" {
  let state = JobState::new(
    @plan.JobSpec::new(
      "draining-reassign",
      "reachability",
      2,
      ["worker-a", "worker-b"],
      [],
    ),
  )
  for partition in 0..<2 {
    ignore(state.assign(partition, "worker-a", 0, 0L))
  }
  state.start()
  state.drain()
  for partition in 0..<2 {
    state.fail_partition(partition, "worker-a", 0, "disconnected")
  }
  let moved = state.reassign_many([
    JobReassignment::new(0, "worker-b", 1, 0L),
    JobReassignment::new(1, "worker-b", 1, 0L),
  ])
  assert_eq(state.phase(), JobPhase::Draining)
  assert_true(
    moved.all(partition => partition.phase() == JobPartitionPhase::Draining),
  )
}
