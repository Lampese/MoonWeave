///|
test "job and assignment derive a deterministic task manifest" {
  let job = @plan.JobSpec::new(
    "job-graph",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [],
  )
  let assignment = @protocol.AssignmentSpec::new(
    1,
    4,
    "boot-b",
    start_offset=12L,
  )
  let spec = TaskSpec::from_job(job, assignment)
  assert_eq(spec.job_id(), "job-graph")
  assert_eq(spec.task_id(), "job-graph/p1/e4")
  assert_eq(spec.operator(), OperatorKind::Reachability)
  assert_eq(spec.start_offset(), 12L)
  let task = Task::from_spec(spec)
  assert_eq(task.task_id(), "job-graph/p1/e4")
  assert_eq(task.offset(), 11L)
}

///|
test "job task manifest rejects unknown operators and partitions" {
  let unknown = @plan.JobSpec::new(
    "job-unknown",
    "opaque-user-function",
    1,
    ["worker-a"],
    [],
  )
  let assignment = @protocol.AssignmentSpec::new(0, 0, "boot-a")
  try TaskSpec::from_job(unknown, assignment) catch {
    TaskError::UnknownOperator(operator_id) =>
      assert_eq(operator_id, "opaque-user-function")
    _ => fail("expected unknown operator rejection")
  } noraise {
    _ => fail("expected unknown operator rejection")
  }
  let job = @plan.JobSpec::new("job-graph", "reachability", 1, ["worker-a"], [])
  try
    TaskSpec::from_job(job, @protocol.AssignmentSpec::new(1, 0, "boot-a"))
  catch {
    TaskError::InvalidPartition(partition) => assert_eq(partition, 1)
    _ => fail("expected out-of-plan partition rejection")
  } noraise {
    _ => fail("expected out-of-plan partition rejection")
  }
}
