///|
fn set_edge_event(
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
test "task set routes multiple partitions and exposes deterministic progress" {
  let job = @plan.JobSpec::new("multi-task", "reachability", 3, ["worker-a"], [])
  let set = TaskSet::from_job(job, [
    @protocol.AssignmentSpec::new(2, 4, "boot-a"),
    @protocol.AssignmentSpec::new(0, 4, "boot-a"),
  ])
  assert_eq(set.length(), 2)
  assert_eq(set.partitions(), [0, 2])
  assert_eq(set.phase(), TaskSetPhase::Created)
  set.start()
  assert_eq(set.phase(), TaskSetPhase::Running)

  let root_0 = set_edge_event("partition-0", 0, "A", "B")
  let root_2 = set_edge_event("partition-2", 0, "X", "Y")
  let first = set.process_batch(@protocol.EventBatch::new(2, 4, 0L, [root_2]))
  let second = set.process_batch(@protocol.EventBatch::new(0, 4, 0L, [root_0]))
  assert_eq(first.partition(), 2)
  assert_eq(second.partition(), 0)
  assert_eq(first.task_id(), "multi-task/p2/e4")
  assert_eq(second.outputs(), [
    TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
  ])
  assert_eq(set.progress()[0].partition(), 0)
  assert_eq(set.progress()[1].partition(), 2)
}

///|
test "task set rejects duplicate ownership and unknown partitions" {
  let job = @plan.JobSpec::new("duplicate", "reachability", 2, ["worker-a"], [])
  try
    TaskSet::from_job(job, [
      @protocol.AssignmentSpec::new(0, 1, "boot-a"),
      @protocol.AssignmentSpec::new(0, 2, "boot-a"),
    ])
  catch {
    TaskSetError::DuplicatePartition(partition) => assert_eq(partition, 0)
    _ => fail("expected duplicate partition")
  } noraise {
    _ => fail("expected duplicate partition")
  }
  let set = TaskSet::from_job(job, [
    @protocol.AssignmentSpec::new(0, 1, "boot-a"),
  ])
  set.start()
  let event = set_edge_event("unknown", 0, "A", "B")
  try set.process_batch(@protocol.EventBatch::new(1, 1, 0L, [event])) catch {
    TaskSetError::UnknownPartition(partition) => assert_eq(partition, 1)
    _ => fail("expected unknown partition")
  } noraise {
    _ => fail("expected unknown partition")
  }
}

///|
test "task set transitions atomically and fails a task on processing error" {
  let job = @plan.JobSpec::new(
    "set-lifecycle",
    "security-chain",
    2,
    ["worker-a"],
    [],
  )
  let set = TaskSet::from_job(job, [
    @protocol.AssignmentSpec::new(0, 0, "boot-a"),
    @protocol.AssignmentSpec::new(1, 0, "boot-a"),
  ])
  set.start()
  let bad = @protocol.EventRecord::new(
    @causal.EventId::new("bad", 0),
    [],
    "user-1",
    Json::object({
      "user_id": Json::string("user-1"),
      "kind": Json::string("bulk_export"),
    }),
  )
  try set.process_batch(@protocol.EventBatch::new(1, 0, 0L, [bad])) catch {
    TaskSetError::Task(partition~, error~) => {
      assert_eq(partition, 1)
      match error {
        TaskError::Security(@security.SecurityError::InvalidTransition(_)) => ()
        _ => fail("expected security transition error")
      }
    }
    _ => fail("expected task processing error")
  } noraise {
    _ => fail("expected task processing error")
  }
  assert_eq(set.task(1).unwrap().phase(), TaskPhase::Failed)
  assert_eq(set.task(0).unwrap().phase(), TaskPhase::Running)
  try set.drain() catch {
    TaskSetError::Task(partition~, error~) => {
      assert_eq(partition, 1)
      match error {
        TaskError::WrongPhase(expected~, actual~) => {
          assert_eq(expected, TaskPhase::Running)
          assert_eq(actual, TaskPhase::Failed)
        }
        _ => fail("expected failed task lifecycle error")
      }
    }
    _ => fail("expected failed task lifecycle error")
  } noraise {
    _ => fail("expected failed task lifecycle error")
  }
}
