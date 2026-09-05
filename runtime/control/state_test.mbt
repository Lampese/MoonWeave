///|
fn control_state() -> CoordinatorState raise CoordinatorError {
  CoordinatorState::new("cluster-a", "coordinator")
}

///|
fn incarnation(
  value : String,
) -> @protocol.Incarnation raise @protocol.ProtocolError {
  @protocol.Incarnation::new(value)
}

///|
test "membership replacement requires explicit fencing" {
  let state = control_state()
  assert_eq(
    state.register_worker("worker-a", incarnation("boot-1"), []),
    RegisterStatus::Added,
  )
  let assigned = state.assign(0, "worker-a")
  assert_eq(assigned.epoch(), 0)
  assert_eq(
    state.register_worker("worker-a", incarnation("boot-1"), ["graph"]),
    RegisterStatus::Duplicate,
  )
  try state.register_worker("worker-a", incarnation("boot-2"), []) catch {
    CoordinatorError::IncarnationConflict => ()
    _ => fail("expected incarnation conflict")
  } noraise {
    _ => fail("expected incarnation conflict")
  }
  assert_eq(state.assignment(0), Some(assigned))
  state.fence_worker("worker-a", incarnation("boot-1"))
  try state.register_worker("worker-a", incarnation("boot-1"), []) catch {
    CoordinatorError::StaleIncarnation => ()
    _ => fail("expected fenced incarnation rejection")
  } noraise {
    _ => fail("expected fenced incarnation rejection")
  }
  assert_eq(
    state.register_worker("worker-a", incarnation("boot-2"), []),
    RegisterStatus::Refreshed,
  )
  let reassigned = state.assign(0, "worker-a")
  assert_eq(reassigned.epoch(), 1)
  try
    state.record_progress(0, "worker-a", incarnation("boot-1"), 1, -1L, -1L)
  catch {
    CoordinatorError::StaleIncarnation => ()
    _ => fail("expected stale incarnation")
  } noraise {
    _ => fail("expected stale incarnation")
  }
}

///|
test "send and progress cursors are monotone and bounded" {
  let state = control_state()
  ignore(state.register_worker("worker-a", incarnation("boot-1"), []))
  let assignment = state.assign(2, "worker-a", start_offset=10L)
  let end = state.record_batch_sent(
    2,
    "worker-a",
    incarnation("boot-1"),
    assignment.epoch(),
    10L,
    2L,
  )
  assert_eq(end, 11L)
  state.record_progress(
    2,
    "worker-a",
    incarnation("boot-1"),
    assignment.epoch(),
    11L,
    10L,
  )
  guard state.assignment(2) is Some(current) else { fail("assignment missing") }
  assert_eq(current.next_offset(), 12L)
  assert_eq(current.persisted_offset(), 11L)
  assert_eq(current.applied_offset(), 10L)
  try
    state.record_batch_sent(
      2,
      "worker-a",
      incarnation("boot-1"),
      assignment.epoch(),
      9L,
      1L,
    )
  catch {
    CoordinatorError::InvalidSendOffset(expected~, actual~) => {
      assert_eq(expected, 12L)
      assert_eq(actual, 9L)
    }
    _ => fail("expected invalid send offset")
  } noraise {
    _ => fail("expected invalid send offset")
  }
  try
    state.record_progress(
      2,
      "worker-a",
      incarnation("boot-1"),
      assignment.epoch(),
      12L,
      11L,
    )
  catch {
    CoordinatorError::ProgressAheadOfSent => ()
    _ => fail("expected progress bound")
  } noraise {
    _ => fail("expected progress bound")
  }
}

///|
test "fencing and reassignment advance the epoch" {
  let state = control_state()
  ignore(state.register_worker("worker-a", incarnation("boot-1"), []))
  ignore(state.register_worker("worker-b", incarnation("boot-1"), []))
  let first = state.assign(0, "worker-a")
  state.fence_worker("worker-a", incarnation("boot-1"))
  assert_eq(state.assignment(0), None)
  try state.assign(1, "worker-a") catch {
    CoordinatorError::WorkerNotActive(_) => ()
    _ => fail("expected fenced worker")
  } noraise {
    _ => fail("expected fenced worker")
  }
  let second = state.assign(0, "worker-b")
  assert_eq(second.epoch(), first.epoch() + 1)
  state.set_suspect("worker-b")
  try state.assign(2, "worker-b") catch {
    CoordinatorError::WorkerNotActive(_) => ()
    _ => fail("expected suspect worker rejection")
  } noraise {
    _ => fail("expected suspect worker rejection")
  }
}

///|
test "assignment replacement and resume bounds fail closed" {
  let state = control_state()
  ignore(state.register_worker("worker-a", incarnation("boot-1"), []))
  let assignment = state.assign(0, "worker-a")
  try state.assign(0, "worker-a", start_offset=1L) catch {
    CoordinatorError::AssignmentConflict(0) => ()
    _ => fail("expected assignment conflict")
  } noraise {
    _ => fail("expected assignment conflict")
  }
  state.record_resume(
    0,
    "worker-a",
    incarnation("boot-1"),
    assignment.epoch(),
    -1L,
  )
  guard state.assignment(0) is Some(current) else { fail("assignment missing") }
  assert_eq(current.next_offset(), 0L)
  assert_eq(current.persisted_offset(), -1L)
  try
    state.record_resume(
      0,
      "worker-a",
      incarnation("boot-1"),
      assignment.epoch(),
      3L,
    )
  catch {
    CoordinatorError::ProgressAheadOfSent => ()
    _ => fail("expected resume bound")
  } noraise {
    _ => fail("expected resume bound")
  }
  try
    state.record_progress(
      0,
      "worker-a",
      incarnation("boot-1"),
      assignment.epoch(),
      -2L,
      -2L,
    )
  catch {
    CoordinatorError::InvalidOffset(-2L) => ()
    _ => fail("expected invalid progress offset")
  } noraise {
    _ => fail("expected invalid progress offset")
  }
}

///|
test "control resume rewinds only the unconfirmed suffix" {
  let state = control_state()
  let boot = incarnation("boot-1")
  ignore(state.register_worker("worker-a", boot, []))
  let assignment = state.assign(0, "worker-a")
  ignore(
    state.record_batch_sent(0, "worker-a", boot, assignment.epoch(), 0L, 2L),
  )
  assert_eq(state.assignment(0).unwrap().next_offset(), 2L)
  state.record_resume(0, "worker-a", boot, assignment.epoch(), -1L)
  assert_eq(state.assignment(0).unwrap().next_offset(), 0L)
  ignore(
    state.record_batch_sent(0, "worker-a", boot, assignment.epoch(), 0L, 1L),
  )
  state.record_resume(0, "worker-a", boot, assignment.epoch(), 0L)
  assert_eq(state.assignment(0).unwrap().next_offset(), 1L)
}

///|
test "round robin planning is deterministic and idempotent" {
  let state = control_state()
  ignore(state.register_worker("worker-b", incarnation("boot-b"), []))
  ignore(state.register_worker("worker-a", incarnation("boot-a"), []))
  let assignments = state.assign_round_robin(5, [
    "worker-b", "worker-a", "worker-a",
  ])
  assert_eq(assignments.map(item => item.worker_id()), [
    "worker-a", "worker-b", "worker-a", "worker-b", "worker-a",
  ])
  assert_eq(assignments.map(item => item.epoch()), [0, 0, 0, 0, 0])
  assert_eq(state.assign_round_robin(5, ["worker-a", "worker-b"]).length(), 5)
}

///|
test "round robin planning rejects an empty worker pool" {
  let state = control_state()
  try state.assign_round_robin(1, []) catch {
    CoordinatorError::EmptyWorkerSet => ()
    _ => fail("expected empty worker pool")
  } noraise {
    _ => fail("expected empty worker pool")
  }
}

///|
test "heartbeat liveness is monotone and suspect is recoverable" {
  let state = control_state()
  let boot = incarnation("boot-1")
  ignore(state.register_worker("worker-a", boot, []))
  assert_eq(state.expire_workers(100L, 10L), [])
  state.record_heartbeat("worker-a", boot, 20L)
  assert_eq(state.expire_workers(29L, 10L), [])
  assert_eq(state.expire_workers(30L, 10L), ["worker-a"])
  guard state.worker("worker-a") is Some(suspect) else {
    fail("worker missing")
  }
  assert_eq(suspect.lifecycle(), WorkerLifecycle::Suspect)
  state.record_heartbeat("worker-a", boot, 31L)
  guard state.worker("worker-a") is Some(active) else { fail("worker missing") }
  assert_eq(active.lifecycle(), WorkerLifecycle::Active)
  assert_eq(active.last_heartbeat_tick(), Some(31L))
  try state.record_heartbeat("worker-a", boot, 30L) catch {
    CoordinatorError::HeartbeatRegression(expected_at_least~, actual~) => {
      assert_eq(expected_at_least, 31L)
      assert_eq(actual, 30L)
    }
    _ => fail("expected heartbeat regression")
  } noraise {
    _ => fail("expected heartbeat regression")
  }
}

///|
test "fenced workers cannot refresh liveness" {
  let state = control_state()
  let boot = incarnation("boot-1")
  ignore(state.register_worker("worker-a", boot, []))
  state.fence_worker("worker-a", boot)
  try state.record_heartbeat("worker-a", boot, 1L) catch {
    CoordinatorError::WorkerNotActive(_) => ()
    _ => fail("expected fenced heartbeat rejection")
  } noraise {
    _ => fail("expected fenced heartbeat rejection")
  }
}

///|
test "control snapshots preserve fencing epochs and progress" {
  let state = control_state()
  let boot_a = incarnation("boot-a")
  let boot_b = incarnation("boot-b")
  ignore(state.register_worker("worker-a", boot_a, []))
  ignore(state.register_worker("worker-b", boot_b, []))
  let first = state.assign(0, "worker-a")
  ignore(state.record_batch_sent(0, "worker-a", boot_a, first.epoch(), 0L, 1L))
  state.record_progress(0, "worker-a", boot_a, first.epoch(), 0L, 0L)
  state.fence_worker("worker-a", boot_a)
  let snapshot = state.snapshot()
  let restored = CoordinatorState::from_snapshot(snapshot)
  guard restored.worker("worker-a") is Some(fenced) else {
    fail("fenced worker missing after restore")
  }
  assert_eq(fenced.lifecycle(), WorkerLifecycle::Fenced)
  assert_eq(restored.assignment(0), None)
  let reassigned = restored.assign(0, "worker-b")
  assert_eq(reassigned.epoch(), first.epoch() + 1)
}

///|
test "reassign installs only after the old lease is revoked" {
  let state = control_state()
  let boot_a = incarnation("boot-a")
  let boot_b = incarnation("boot-b")
  ignore(state.register_worker("worker-a", boot_a, []))
  ignore(state.register_worker("worker-b", boot_b, []))
  let first = state.assign(0, "worker-a")
  try state.reassign(0, "worker-b") catch {
    CoordinatorError::AssignmentConflict(0) => ()
    _ => fail("expected active lease conflict")
  } noraise {
    _ => fail("expected active lease conflict")
  }
  state.fence_worker("worker-a", boot_a)
  let second = state.reassign(0, "worker-b")
  assert_eq(second.epoch(), first.epoch() + 1)
  assert_eq(second.worker_id(), "worker-b")
}

///|
test "fence and reassign moves every lease with one epoch step" {
  let state = control_state()
  let old_boot = incarnation("old")
  let replacement_boot = incarnation("replacement")
  ignore(state.register_worker("worker-a", old_boot, []))
  ignore(state.register_worker("worker-b", replacement_boot, []))
  let first = state.assign(0, "worker-a", start_offset=4L)
  let second = state.assign(2, "worker-a", start_offset=9L)
  let moved = state.fence_and_reassign("worker-a", old_boot, "worker-b")
  assert_eq(moved.map(item => item.partition()), [0, 2])
  assert_eq(moved.map(item => item.epoch()), [
    first.epoch() + 1,
    second.epoch() + 1,
  ])
  assert_eq(moved.map(item => item.start_offset()), [4L, 9L])
  guard state.worker("worker-a") is Some(fenced) else {
    fail("old worker missing")
  }
  assert_eq(fenced.lifecycle(), WorkerLifecycle::Fenced)
}

///|
test "liveness reports healthy suspect and fenced states" {
  let state = control_state()
  let boot = incarnation("boot-liveness")
  ignore(state.register_worker("worker-a", boot, []))
  assert_eq(state.liveness("worker-a", 0L, 10L), LivenessDecision::Healthy)
  state.record_heartbeat("worker-a", boot, 1L)
  assert_eq(state.liveness("worker-a", 10L, 9L), LivenessDecision::Suspect)
  state.fence_worker("worker-a", boot)
  assert_eq(state.liveness("worker-a", 100L, 1L), LivenessDecision::Fenced)
}
