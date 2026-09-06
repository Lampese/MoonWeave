///|
fn edge_event(
  origin : String,
  sequence : Int,
  source : String,
  target : String,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    [],
    "graph",
    Json::object({
      "source": Json::string(source),
      "target": Json::string(target),
    }),
  )
}

///|
fn security_event(
  origin : String,
  sequence : Int,
  parents : ArrayView[@causal.EventId],
  kind : String,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    parents,
    "user-1",
    Json::object({
      "user_id": Json::string("user-1"),
      "kind": Json::string(kind),
    }),
  )
}

///|
fn sum_event(
  origin : String,
  sequence : Int,
  key : String,
  value : Int64,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    [],
    key,
    Json::object({ "value": Json::string(value.to_string()) }),
  )
}

///|
test "task lifecycle and reachability outputs are deterministic" {
  let task = Task::new(
    "task-graph",
    "reachability",
    OperatorKind::Reachability,
    0,
    3,
  )
  assert_eq(task.phase(), TaskPhase::Created)
  task.start()
  let root = edge_event("graph", 0, "A", "B")
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("graph", 1),
    [root.id()],
    "graph",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  let outputs = task.process_batch(
    @protocol.EventBatch::new(0, 3, 0L, [root, child]),
  )
  assert_eq(outputs, [
    TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
    TaskOutput::Reachable(@reachability.Reachable::new("A", "C")),
    TaskOutput::Reachable(@reachability.Reachable::new("B", "C")),
  ])
  assert_eq(task.offset(), 1L)
  assert_eq(task.event_count(), 2)
  assert_eq(task.pending_count(), 0)
  assert_eq(task.outputs(), outputs)
  assert_eq(task.progress().emitted_outputs(), 3)
  task.drain()
  task.stop()
  assert_eq(task.phase(), TaskPhase::Stopped)
}

///|
test "task runs a keyed sum and snapshots its running totals" {
  let task = Task::new("task-sum", "keyed-sum", OperatorKind::KeyedSum, 0, 0)
  task.start()
  let outputs = task.process_batch(
    @protocol.EventBatch::new(0, 0, 0L, [
      sum_event("sales", 0, "north", 10L),
      sum_event("sales", 1, "north", 5L),
      sum_event("sales", 2, "south", 7L),
    ]),
  )
  assert_eq(outputs, [
    TaskOutput::KeyedSum(@aggregation.KeyedSum::new("north", 10L)),
    TaskOutput::KeyedSum(@aggregation.KeyedSum::new("north", 15L)),
    TaskOutput::KeyedSum(@aggregation.KeyedSum::new("south", 7L)),
  ])
  assert_eq(task.event_count(), 3)
  assert_eq(task.outputs(), outputs)
  let snapshot = task.snapshot()
  let restored = Task::new(
    "task-sum",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
  )
  restored.start()
  restored.restore(snapshot)
  assert_eq(restored.outputs(), outputs)
}

///|
test "task identity rejects an operator implementation mismatch" {
  try
    Task::new(
      "mismatched-operator",
      "security-chain",
      OperatorKind::Reachability,
      0,
      0,
    )
  catch {
    TaskError::OperatorMismatch(expected~, actual~) => {
      assert_eq(expected, "reachability")
      assert_eq(actual, "security-chain")
    }
    _ => fail("expected operator identity mismatch")
  } noraise {
    _ => fail("expected operator identity mismatch")
  }
}

///|
test "task routes security alerts through the same execution contract" {
  let task = Task::new(
    "task-security",
    "security-chain",
    OperatorKind::SecurityChain,
    2,
    1,
  )
  task.start()
  let login = security_event("security", 0, [], "login_success")
  let grant = security_event("security", 1, [login.id()], "grant_admin")
  let bulk_export_event = security_event(
    "security",
    2,
    [grant.id()],
    "bulk_export",
  )
  assert_eq(task.process(0L, TaskInput::new(login)), [])
  assert_eq(task.process(1L, TaskInput::new(grant)), [])
  assert_eq(task.process(2L, TaskInput::new(bulk_export_event)), [
    TaskOutput::SecurityAlert(@security.SecurityAlert::{ user_id: "user-1", }),
  ])
  assert_eq(task.progress().frontier_digest(), task.frontier().digest())
  assert_eq(task.outputs().length(), 1)
}

///|
test "task rejects bad lifecycle and preserves offset on operator failure" {
  let task = Task::new(
    "task-invalid",
    "security-chain",
    OperatorKind::SecurityChain,
    0,
    0,
  )
  try
    task.process(
      0L,
      TaskInput::new(security_event("invalid", 0, [], "grant_admin")),
    )
  catch {
    TaskError::WrongPhase(expected~, actual~) => {
      assert_eq(expected, TaskPhase::Running)
      assert_eq(actual, TaskPhase::Created)
    }
    _ => fail("expected lifecycle rejection")
  } noraise {
    _ => fail("expected lifecycle rejection")
  }
  task.start()
  try
    task.process(
      0L,
      TaskInput::new(security_event("invalid", 0, [], "grant_admin")),
    )
  catch {
    TaskError::Security(@security.SecurityError::InvalidTransition(_)) =>
      assert_eq(task.offset(), -1L)
    _ => fail("expected operator rejection")
  } noraise {
    _ => fail("expected operator rejection")
  }
}

///|
test "batch processing is atomic when a later event is rejected" {
  let task = Task::new(
    "task-atomic",
    "security-chain",
    OperatorKind::SecurityChain,
    0,
    0,
  )
  task.start()
  let login = security_event("atomic", 0, [], "login_success")
  let invalid = security_event("atomic", 1, [], "bulk_export")
  let batch = @protocol.EventBatch::new(0, 0, 0L, [login, invalid])
  try task.process_batch(batch) catch {
    TaskError::Security(@security.SecurityError::InvalidTransition(_)) => {
      assert_eq(task.offset(), -1L)
      assert_eq(task.frontier().length(), 0)
    }
    _ => fail("expected atomic batch rejection")
  } noraise {
    _ => fail("expected atomic batch rejection")
  }
}

///|
test "delta processing is atomic when a later event is rejected" {
  let task = Task::new(
    "task-delta-atomic",
    "security-chain",
    OperatorKind::SecurityChain,
    1,
    2,
  )
  task.start()
  let login = security_event("delta-atomic", 0, [], "login_success")
  let invalid = security_event("delta-atomic", 1, [], "bulk_export")
  let delta = @protocol.DeltaBatch::new("delta-atomic", 0, 1, 1, 2, [
    login, invalid,
  ])
  try task.process_delta(delta) catch {
    TaskError::Security(@security.SecurityError::InvalidTransition(_)) => {
      assert_eq(task.offset(), -1L)
      assert_eq(task.frontier().length(), 0)
    }
    _ => fail("expected atomic delta rejection")
  } noraise {
    _ => fail("expected atomic delta rejection")
  }
}

///|
test "exchange input updates the operator without consuming primary offset" {
  let task = Task::new(
    "task-delta",
    "reachability",
    OperatorKind::Reachability,
    1,
    2,
  )
  task.start()
  let event = edge_event("delta", 0, "A", "B")
  let delta = @protocol.DeltaBatch::new("delta-task", 0, 4, 1, 2, [event])
  assert_eq(task.process_delta(delta), [
    TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
  ])
  assert_eq(task.offset(), -1L)
  assert_eq(task.progress().emitted_outputs(), 1)
}

///|
test "task can be constructed from an assignment envelope" {
  let assignment = @protocol.AssignmentSpec::new(4, 9, "boot", start_offset=12L)
  let task = Task::from_assignment(
    "assigned-task",
    "reachability",
    OperatorKind::Reachability,
    assignment,
  )
  assert_eq(task.partition(), 4)
  assert_eq(task.epoch(), 9)
  assert_eq(task.offset(), 11L)
}

///|
test "task validates the live assignment before replay" {
  let spec = @protocol.AssignmentSpec::new(4, 9, "boot", start_offset=12L)
  let task = Task::from_assignment(
    "assigned-task",
    "reachability",
    OperatorKind::Reachability,
    spec,
  )
  let matching = @protocol.Assignment::new(
    4,
    9,
    @protocol.Incarnation::new("boot"),
    start_offset=12L,
  )
  task.validate_assignment(matching)
  let wrong_start = @protocol.Assignment::new(
    4,
    9,
    @protocol.Incarnation::new("boot"),
    start_offset=11L,
  )
  try task.validate_assignment(wrong_start) catch {
    TaskError::InvalidOffset(offset) => assert_eq(offset, 11L)
    _ => fail("expected assignment start offset mismatch")
  } noraise {
    _ => fail("expected assignment start offset mismatch")
  }
}

///|
test "task cannot stop with unresolved causal input" {
  let task = Task::new(
    "pending-task",
    "reachability",
    OperatorKind::Reachability,
    0,
    0,
  )
  task.start()
  let missing = @causal.EventId::new("pending", 0)
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("pending", 1),
    [missing],
    "graph",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  assert_eq(task.process(0L, TaskInput::new(child)), [])
  assert_eq(task.pending_count(), 1)
  task.drain()
  try task.stop() catch {
    TaskError::PendingEvents(count) => assert_eq(count, 1)
    _ => fail("expected unresolved causal input")
  } noraise {
    _ => fail("expected unresolved causal input")
  }
  assert_eq(task.phase(), TaskPhase::Draining)
}

///|
test "task snapshot preserves pending causal state and resumes deterministically" {
  let task = Task::new(
    "snapshot-task",
    "reachability",
    OperatorKind::Reachability,
    0,
    0,
  )
  task.start()
  let parent_id = @causal.EventId::new("snapshot", 0)
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("snapshot", 1),
    [parent_id],
    "graph",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  assert_eq(task.process(0L, TaskInput::new(child)), [])
  let snapshot = task.snapshot()
  assert_eq(snapshot.offset(), 0L)
  assert_eq(snapshot.events().length(), 1)
  assert_true(snapshot.operator_capability() is Some(_))

  let forged = TaskSnapshot::{
    ..snapshot,
    operator_capability: Some("forged-operator-capability"),
  }
  let forged_restore = Task::new(
    "snapshot-task",
    "reachability",
    OperatorKind::Reachability,
    0,
    0,
  )
  forged_restore.start()
  try forged_restore.restore(forged) catch {
    TaskError::OperatorContractMismatch => ()
    _ => fail("expected operator contract mismatch")
  } noraise {
    _ => fail("expected operator contract mismatch")
  }

  let restored = Task::new(
    "snapshot-task",
    "reachability",
    OperatorKind::Reachability,
    0,
    0,
  )
  restored.start()
  restored.restore(snapshot)
  assert_eq(restored.offset(), 0L)
  assert_eq(restored.pending_count(), 1)
  let parent = @protocol.EventRecord::new(
    parent_id,
    [],
    "graph",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  assert_eq(restored.process(1L, TaskInput::new(parent)), [
    TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
    TaskOutput::Reachable(@reachability.Reachable::new("A", "C")),
    TaskOutput::Reachable(@reachability.Reachable::new("B", "C")),
  ])
  assert_eq(restored.pending_count(), 0)
}
