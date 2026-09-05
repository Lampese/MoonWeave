///|
async test "supervisor routes multiple sessions from one Worker by partition hint" {
  let directory = @fs.tmpdir(prefix="moonweave-multi-task-supervisor")
  let wal_0 = directory + "/partition-0.wal"
  let wal_1 = directory + "/partition-1.wal"
  defer {
    @fs.remove(wal_0) catch {
      _ => ()
    }
    @fs.remove(wal_0 + ".lock") catch {
      _ => ()
    }
    @fs.remove(wal_1) catch {
      _ => ()
    }
    @fs.remove(wal_1 + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let control = @control.CoordinatorState::new(
    "cluster-multi-task", "coordinator",
  )
  let boot = @protocol.Incarnation::new("boot-multi-task")
  ignore(control.register_worker("worker-a", boot, ["graph"]))
  let assignment_0 = control.assign(0, "worker-a")
  let assignment_1 = control.assign(1, "worker-a")
  let event_0 = @protocol.EventRecord::new(
    @causal.EventId::new("multi-task-0", 0),
    [],
    "partition-0",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let event_1 = @protocol.EventRecord::new(
    @causal.EventId::new("multi-task-1", 0),
    [],
    "partition-1",
    Json::object({ "source": Json::string("X"), "target": Json::string("Y") }),
  )
  let work_0 = SupervisorWork::new("worker-a", "multi-session-0", 0, [
    @protocol.EventBatch::new(0, assignment_0.epoch(), 0L, [event_0]),
  ])
  let work_1 = SupervisorWork::new("worker-a", "multi-session-1", 1, [
    @protocol.EventBatch::new(1, assignment_1.epoch(), 0L, [event_1]),
  ])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  @async_runtime.with_task_group <| group => {
    let supervisor_task = group.spawn() <| () => {
      run_supervisor(server, control, [work_0, work_1])
    }
    let task_0 = @task.Task::from_assignment(
      "multi-supervisor/p0/e0",
      "reachability",
      @task.OperatorKind::Reachability,
      @protocol.AssignmentSpec::new(0, assignment_0.epoch(), boot.value()),
    )
    let task_1 = @task.Task::from_assignment(
      "multi-supervisor/p1/e0",
      "reachability",
      @task.OperatorKind::Reachability,
      @protocol.AssignmentSpec::new(1, assignment_1.epoch(), boot.value()),
    )
    let work_task_0 = WorkerTaskWork::new(
      @socket.Tcp::connect(server.addr),
      "cluster-multi-task",
      "coordinator",
      "worker-a",
      boot.value(),
      ["graph"],
      wal_0,
      task_0,
      initial_credit_events=1L,
      initial_credit_bytes=2048L,
    )
    let work_task_1 = WorkerTaskWork::new(
      @socket.Tcp::connect(server.addr),
      "cluster-multi-task",
      "coordinator",
      "worker-a",
      boot.value(),
      ["graph"],
      wal_1,
      task_1,
      initial_credit_events=1L,
      initial_credit_bytes=2048L,
    )
    let observed : Array[(Int, @task.TaskOutput)] = []
    let output_handler : (String, Int, @task.TaskOutput) -> Unit raise = (
      _,
      partition,
      output,
    ) => observed.push((partition, output))
    let worker_summary = run_worker_task_set(
      [work_task_0, work_task_1],
      output_handler,
    )
    let supervisor_summary = supervisor_task.wait()
    assert_eq(worker_summary.completed_tasks(), 2)
    assert_eq(worker_summary.failed_tasks(), 0)
    assert_eq(worker_summary.live_batches(), 2)
    assert_eq(supervisor_summary.completed_workers(), 2)
    assert_eq(supervisor_summary.failed_workers(), 0)
    assert_eq(supervisor_summary.sent_batches(), 2)
    observed.sort_by((left, right) => left.0.compare(right.0))
    assert_eq(observed, [
      (0, @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B"))),
      (1, @task.TaskOutput::Reachable(@reachability.Reachable::new("X", "Y"))),
    ])
    assert_eq(task_0.phase(), @task.TaskPhase::Stopped)
    assert_eq(task_1.phase(), @task.TaskPhase::Stopped)
  }
}
