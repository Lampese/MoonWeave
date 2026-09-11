///|
test "coordinator rejects offset overflow without changing the assignment" {
  let state = CoordinatorState::new("cluster", "coordinator")
  let boot = @protocol.Incarnation::new("boot")
  ignore(state.register_worker("worker", boot, []))
  ignore(state.assign(0, "worker", start_offset=9223372036854775805L))
  let before = state.snapshot()
  try
    state.record_batch_sent(0, "worker", boot, 0, 9223372036854775805L, 3L)
  catch {
    CoordinatorError::InvalidEventCount(3L) => ()
    _ => fail("expected offset overflow rejection")
  } noraise {
    _ => fail("coordinator send cursor wrapped")
  }
  assert_eq(state.snapshot(), before)
  assert_eq(
    state.record_batch_sent(0, "worker", boot, 0, 9223372036854775805L, 2L),
    9223372036854775806L,
  )
  state.record_progress(
    0, "worker", boot, 0, 9223372036854775806L, 9223372036854775806L,
  )
  assert_eq(state.assignment(0).unwrap().next_offset(), 9223372036854775807L)
}

///|
test "job rejects offset overflow without changing partition progress" {
  let spec = @plan.JobSpec::new("job", "reachability", 1, ["worker"], [])
  let state = JobState::new(spec)
  ignore(state.assign(0, "worker", 0, 9223372036854775805L))
  state.start()
  let before = state.snapshot()
  try state.record_batch_sent(0, "worker", 0, 9223372036854775805L, 3L) catch {
    JobError::InvalidEventCount(3L) => ()
    _ => fail("expected offset overflow rejection")
  } noraise {
    _ => fail("job send cursor wrapped")
  }
  assert_eq(state.snapshot(), before)
  assert_eq(
    state.record_batch_sent(0, "worker", 0, 9223372036854775805L, 2L),
    9223372036854775806L,
  )
  state.record_progress(
    0, "worker", 0, 9223372036854775806L, 9223372036854775806L,
  )
  assert_eq(state.partition(0).unwrap().next_offset(), 9223372036854775807L)
}
