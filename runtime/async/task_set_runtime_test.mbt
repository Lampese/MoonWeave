///|
async test "one worker process runs multiple Task sessions concurrently" {
  let directory = @fs.tmpdir(prefix="moonweave-task-set-runtime")
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
  let event_0 = task_edge_event("set-runtime-0", 0, [], "A", "B")
  let event_1 = task_edge_event("set-runtime-1", 0, [], "X", "Y")
  let assignment_0 = @protocol.AssignmentSpec::new(0, 7, "boot-set")
  let assignment_1 = @protocol.AssignmentSpec::new(1, 7, "boot-set")
  let batch_0 = @protocol.EventBatch::new(0, 7, 0L, [event_0])
  let batch_1 = @protocol.EventBatch::new(1, 7, 0L, [event_1])
  let server_0 = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  let server_1 = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server_0.close()
  defer server_1.close()
  @async_runtime.with_task_group <| group => {
    let coordinator_0 = group.spawn() <| () => {
      let (connection, _) = server_0.accept()
      run_coordinator(
        connection,
        "cluster-task-set",
        "coordinator",
        "worker-set",
        "session-0",
        assignment_0,
        [batch_0],
      )
    }
    let coordinator_1 = group.spawn() <| () => {
      let (connection, _) = server_1.accept()
      run_coordinator(
        connection,
        "cluster-task-set",
        "coordinator",
        "worker-set",
        "session-1",
        assignment_1,
        [batch_1],
      )
    }
    let task_0 = @task.Task::from_assignment(
      "job-set/p0/e7",
      "reachability",
      @task.OperatorKind::Reachability,
      assignment_0,
    )
    let task_1 = @task.Task::from_assignment(
      "job-set/p1/e7",
      "reachability",
      @task.OperatorKind::Reachability,
      assignment_1,
    )
    let work_0 = WorkerTaskWork::new(
      @socket.Tcp::connect(server_0.addr),
      "cluster-task-set",
      "coordinator",
      "worker-set",
      "boot-set",
      [],
      wal_0,
      task_0,
      initial_credit_events=1L,
      initial_credit_bytes=1024L,
    )
    let work_1 = WorkerTaskWork::new(
      @socket.Tcp::connect(server_1.addr),
      "cluster-task-set",
      "coordinator",
      "worker-set",
      "boot-set",
      [],
      wal_1,
      task_1,
      initial_credit_events=1L,
      initial_credit_bytes=1024L,
    )
    let observed : Array[(Int, @task.TaskOutput)] = []
    let output_handler : (String, Int, @task.TaskOutput) -> Unit raise = (
      _,
      partition,
      output,
    ) => observed.push((partition, output))
    let summary = run_worker_task_set([work_0, work_1], output_handler)
    assert_eq(summary.completed_tasks(), 2)
    assert_eq(summary.failed_tasks(), 0)
    assert_eq(summary.live_batches(), 2)
    assert_eq(summary.replayed_batches(), 0)
    observed.sort_by((left, right) => left.0.compare(right.0))
    assert_eq(observed, [
      (0, @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B"))),
      (1, @task.TaskOutput::Reachable(@reachability.Reachable::new("X", "Y"))),
    ])
    assert_eq(task_0.phase(), @task.TaskPhase::Stopped)
    assert_eq(task_1.phase(), @task.TaskPhase::Stopped)
    ignore(coordinator_0.wait())
    ignore(coordinator_1.wait())
  }
}

///|
async test "worker task set validates duplicate task identities before networking" {
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let task_0 = @task.Task::new(
    "duplicate-task",
    "reachability",
    @task.OperatorKind::Reachability,
    0,
    0,
  )
  let task_1 = @task.Task::new(
    "duplicate-task",
    "reachability",
    @task.OperatorKind::Reachability,
    1,
    0,
  )
  let work_0 = WorkerTaskWork::new(
    @socket.Tcp::connect(server.addr),
    "cluster",
    "coordinator",
    "worker",
    "boot",
    [],
    "worker-0.wal",
    task_0,
  )
  let work_1 = WorkerTaskWork::new(
    @socket.Tcp::connect(server.addr),
    "cluster",
    "coordinator",
    "worker",
    "boot",
    [],
    "worker-1.wal",
    task_1,
  )
  let discard : (String, Int, @task.TaskOutput) -> Unit raise = (_, _, _) => ()
  try run_worker_task_set([work_0, work_1], discard) catch {
    FrameIoError::TaskSet(@task.TaskSetError::DuplicateTaskId(task_id)) =>
      assert_eq(task_id, "duplicate-task")
    _ => fail("expected duplicate task identity")
  } noraise {
    _ => fail("expected duplicate task identity")
  }
}
