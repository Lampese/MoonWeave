///|
fn job_spec() -> @plan.JobSpec raise @plan.PlanError {
  @plan.JobSpec::new(
    "job-lifecycle",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [],
  )
}

///|
test "job lifecycle reaches success only after every partition stops" {
  let state = JobState::new(job_spec())
  assert_eq(state.phase(), JobPhase::Created)
  try state.start() catch {
    JobError::NotFullyAssigned => ()
    _ => fail("expected incomplete assignment rejection")
  } noraise {
    _ => fail("expected incomplete assignment rejection")
  }
  let first = state.assign(0, "worker-a", 0, 0L)
  let second = state.assign(1, "worker-b", 0, 5L)
  assert_eq(first.phase(), JobPartitionPhase::Assigned)
  assert_eq(second.start_offset(), 5L)
  assert_eq(state.assign(0, "worker-a", 0, 0L), first)
  state.start()
  assert_eq(state.phase(), JobPhase::Running)
  assert_eq(state.partition(0).unwrap().phase(), JobPartitionPhase::Running)
  assert_eq(state.record_batch_sent(0, "worker-a", 0, 0L, 2L), 1L)
  assert_eq(state.record_batch_sent(1, "worker-b", 0, 5L, 1L), 5L)
  state.record_progress(0, "worker-a", 0, 1L, 1L)
  state.record_progress(1, "worker-b", 0, 5L, 5L)
  assert_eq(state.partition(0).unwrap().applied_offset(), 1L)
  state.drain()
  assert_eq(state.phase(), JobPhase::Draining)
  state.stop_partition(0, "worker-a", 0, 0)
  assert_eq(state.phase(), JobPhase::Draining)
  assert_eq(state.partition(0).unwrap().phase(), JobPartitionPhase::Succeeded)
  state.stop_partition(1, "worker-b", 0, 0)
  assert_eq(state.phase(), JobPhase::Succeeded)
  assert_eq(state.partition(1).unwrap().phase(), JobPartitionPhase::Succeeded)
}

///|
test "job lifecycle rejects stale identity, offset, and pending completion" {
  let state = JobState::new(job_spec())
  try state.assign(0, "worker-c", 0, 0L) catch {
    JobError::UnknownWorker(worker_id) => assert_eq(worker_id, "worker-c")
    _ => fail("expected unknown worker")
  } noraise {
    _ => fail("expected unknown worker")
  }
  ignore(state.assign(0, "worker-a", 1, 0L))
  ignore(state.assign(1, "worker-b", 1, 0L))
  state.start()
  try state.record_batch_sent(0, "worker-b", 1, 0L, 1L) catch {
    JobError::WorkerMismatch(expected~, actual~) => {
      assert_eq(expected, "worker-a")
      assert_eq(actual, "worker-b")
    }
    _ => fail("expected worker mismatch")
  } noraise {
    _ => fail("expected worker mismatch")
  }
  ignore(state.record_batch_sent(0, "worker-a", 1, 0L, 1L))
  try state.record_progress(0, "worker-a", 1, 2L, 2L) catch {
    JobError::ProgressAheadOfSent => ()
    _ => fail("expected progress bound")
  } noraise {
    _ => fail("expected progress bound")
  }
  state.record_progress(0, "worker-a", 1, 0L, 0L)
  try state.record_progress(0, "worker-a", 1, -1L, -1L) catch {
    JobError::OffsetRegression(expected_at_least~, actual~) => {
      assert_eq(expected_at_least, 0L)
      assert_eq(actual, -1L)
    }
    _ => fail("expected progress regression")
  } noraise {
    _ => fail("expected progress regression")
  }
  state.drain()
  try state.stop_partition(0, "worker-a", 1, 1) catch {
    JobError::PendingEvents(partition, count) => {
      assert_eq(partition, 0)
      assert_eq(count, 1)
    }
    _ => fail("expected pending completion rejection")
  } noraise {
    _ => fail("expected pending completion rejection")
  }
  state.fail("operator failed")
  assert_eq(state.phase(), JobPhase::Failed)
  assert_eq(state.failure_reason(), Some("operator failed"))
}

///|
test "job cancellation is terminal and cannot be overwritten" {
  let state = JobState::new(job_spec())
  state.cancel()
  assert_eq(state.phase(), JobPhase::Cancelled)
  try state.fail("late failure") catch {
    JobError::InvalidTransition(expected~, actual~) => {
      assert_eq(expected, JobPhase::Running)
      assert_eq(actual, JobPhase::Cancelled)
    }
    _ => fail("expected terminal cancellation")
  } noraise {
    _ => fail("expected terminal cancellation")
  }
}

///|
test "failed partitions can be fenced and reassigned at a higher epoch" {
  let state = JobState::new(job_spec())
  ignore(state.assign(0, "worker-a", 0, 0L))
  ignore(state.assign(1, "worker-b", 0, 0L))
  state.start()
  ignore(state.record_batch_sent(0, "worker-a", 0, 0L, 1L))
  state.record_progress(0, "worker-a", 0, 0L, 0L)
  state.fail_partition(0, "worker-a", 0, "worker disconnected")
  assert_eq(state.phase(), JobPhase::Running)
  assert_eq(state.partition(0).unwrap().phase(), JobPartitionPhase::Failed)
  assert_eq(
    state.partition(0).unwrap().failure_reason(),
    Some("worker disconnected"),
  )
  try state.drain() catch {
    JobError::PartitionFailed(partition) => assert_eq(partition, 0)
    _ => fail("expected failed partition to block drain")
  } noraise {
    _ => fail("expected failed partition to block drain")
  }
  let replacement = state.reassign(0, "worker-b", 1, 0L)
  assert_eq(replacement.epoch(), 1)
  assert_eq(replacement.phase(), JobPartitionPhase::Running)
  assert_eq(replacement.next_offset(), 0L)
  try state.record_progress(0, "worker-a", 0, 0L, 0L) catch {
    JobError::WorkerMismatch(expected~, actual~) => {
      assert_eq(expected, "worker-b")
      assert_eq(actual, "worker-a")
    }
    _ => fail("expected fenced worker identity")
  } noraise {
    _ => fail("expected fenced worker identity")
  }
  ignore(state.record_batch_sent(0, "worker-b", 1, 0L, 1L))
  state.record_progress(0, "worker-b", 1, 0L, 0L)
  state.drain()
  state.stop_partition(0, "worker-b", 1, 0)
  state.stop_partition(1, "worker-b", 0, 0)
  assert_eq(state.phase(), JobPhase::Succeeded)
}
