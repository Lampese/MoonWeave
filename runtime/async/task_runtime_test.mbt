///|
fn task_edge_event(
  origin : String,
  sequence : Int,
  parents : ArrayView[@causal.EventId],
  source : String,
  target : String,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    parents,
    "graph",
    Json::object({
      "source": Json::string(source),
      "target": Json::string(target),
    }),
  )
}

///|
async test "worker task executes a built-in operator over real TCP" {
  let directory = @fs.tmpdir(prefix="moonweave-task-runtime")
  let wal_path = directory + "/worker.wal"
  defer {
    @fs.remove(wal_path) catch {
      _ => ()
    }
    @fs.remove(wal_path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let root = task_edge_event("task-tcp", 0, [], "A", "B")
  let child = task_edge_event("task-tcp", 1, [root.id()], "B", "C")
  let batch = @protocol.EventBatch::new(0, 4, 0L, [root, child])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = server.accept()
      defer connection.close()
      let assignment = @protocol.AssignmentSpec::new(0, 4, "boot-task")
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "cluster-task",
        "coordinator",
        "worker-task",
        "task-session",
        assignment~,
      )
      let resume_frame = read_frame(connection)
      coordinator.accept_frame(resume_frame)
      let progress_frame = read_frame(connection)
      coordinator.accept_frame(progress_frame)
      let credit_frame = read_frame(connection)
      coordinator.accept_frame(credit_frame)
      coordinator_send_batch(
        connection,
        coordinator,
        outbound,
        batch,
        @utf8.encode(batch.to_payload()).length().to_int64(),
      )
      let ack = read_frame(connection)
      coordinator.accept_frame(ack)
      let progress = read_frame(connection)
      coordinator.accept_frame(progress)
      let credit = read_frame(connection)
      coordinator.accept_frame(credit)
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
      write_frame(connection, drain)
    }
    let connection = @socket.Tcp::connect(addr)
    let task = @task.Task::new(
      "task-tcp",
      "reachability",
      @task.OperatorKind::Reachability,
      0,
      4,
    )
    let outputs : Array[@task.TaskOutput] = []
    let output_handler : (@task.TaskOutput) -> Unit raise = output => {
      outputs.push(output)
    }
    let summary = run_worker_task(
      connection,
      "cluster-task",
      "coordinator",
      "worker-task",
      "boot-task",
      [],
      wal_path,
      task,
      output_handler,
      initial_credit_events=2L,
      initial_credit_bytes=4096L,
    )
    assert_eq(summary.live_batches(), 1)
    assert_eq(outputs, [
      @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
      @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "C")),
      @task.TaskOutput::Reachable(@reachability.Reachable::new("B", "C")),
    ])
    assert_eq(task.phase(), @task.TaskPhase::Stopped)
    assert_eq(task.offset(), 1L)
    coordinator_task.wait()
  }
}

///|
test "failed task does not advance its primary offset" {
  let task = @task.Task::new(
    "task-failure",
    "security-chain",
    @task.OperatorKind::SecurityChain,
    0,
    0,
  )
  task.start()
  let invalid = @protocol.EventRecord::new(
    @causal.EventId::new("task-failure", 0),
    [],
    "user-1",
    Json::object({
      "user_id": Json::string("user-1"),
      "kind": Json::string("bulk_export"),
    }),
  )
  try task.process(0L, @task.TaskInput::new(invalid)) catch {
    @task.TaskError::Security(@security.SecurityError::InvalidTransition(_)) =>
      assert_eq(task.offset(), -1L)
    _ => fail("expected task operator failure")
  } noraise {
    _ => fail("expected task operator failure")
  }
  task.fail()
  assert_eq(task.phase(), @task.TaskPhase::Failed)
}

///|
async test "worker task rejects a mismatched assignment before replay" {
  let directory = @fs.tmpdir(prefix="moonweave-task-mismatch")
  let wal_path = directory + "/worker.wal"
  defer {
    @fs.remove(wal_path) catch {
      _ => ()
    }
    @fs.remove(wal_path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    group.spawn_bg(allow_failure=true) <| () => {
      let (connection, _) = server.accept()
      defer connection.close()
      ignore(
        coordinator_handshake(
          connection,
          "cluster-task",
          "coordinator",
          "worker-task",
          "mismatch-session",
          assignment=@protocol.AssignmentSpec::new(0, 1, "boot-task"),
        ),
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let task = @task.Task::new(
      "mismatched-task",
      "reachability",
      @task.OperatorKind::Reachability,
      1,
      1,
    )
    let discard : (@task.TaskOutput) -> Unit raise = _ => ()
    try
      run_worker_task(
        connection,
        "cluster-task",
        "coordinator",
        "worker-task",
        "boot-task",
        [],
        wal_path,
        task,
        discard,
      )
    catch {
      FrameIoError::Task(@task.TaskError::PartitionMismatch(expected~, actual~)) => {
        assert_eq(expected, 1)
        assert_eq(actual, 0)
      }
      _ => fail("expected Task assignment mismatch")
    } noraise {
      _ => fail("expected Task assignment mismatch")
    }
    assert_eq(task.phase(), @task.TaskPhase::Failed)
    assert_eq(task.offset(), -1L)
  }
}

///|
async test "worker task restores operator state and replays only the WAL suffix" {
  let directory = @fs.tmpdir(prefix="moonweave-task-restore")
  let wal_path = directory + "/worker.wal"
  let checkpoint_path = directory + "/worker.json"
  let output_log_path = directory + "/outputs.wal"
  defer {
    @fs.remove(wal_path) catch {
      _ => ()
    }
    @fs.remove(wal_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(checkpoint_path) catch {
      _ => ()
    }
    @fs.remove(checkpoint_path + ".tmp") catch {
      _ => ()
    }
    @fs.remove(output_log_path) catch {
      _ => ()
    }
    @fs.remove(output_log_path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let root = task_edge_event("restore-task", 0, [], "A", "B")
  let child = task_edge_event("restore-task", 1, [root.id()], "B", "C")
  let batch = @protocol.EventBatch::new(0, 2, 0L, [root, child])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = server.accept()
      defer connection.close()
      let assignment = @protocol.AssignmentSpec::new(0, 2, "boot-restore")
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "cluster-restore",
        "coordinator",
        "worker-restore",
        "restore-first",
        assignment~,
      )
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      coordinator_send_batch(
        connection,
        coordinator,
        outbound,
        batch,
        @utf8.encode(batch.to_payload()).length().to_int64(),
      )
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      write_frame(connection, outbound.build(@protocol.FrameKind::Drain, "{}"))
    }
    let connection = @socket.Tcp::connect(addr)
    let task = @task.Task::new(
      "restore-task",
      "reachability",
      @task.OperatorKind::Reachability,
      0,
      2,
    )
    let first_outputs : Array[@task.TaskOutput] = []
    let first_handler : (@task.TaskOutput) -> Unit raise = output => {
      first_outputs.push(output)
    }
    let first = run_worker_task(
      connection,
      "cluster-restore",
      "coordinator",
      "worker-restore",
      "boot-restore",
      [],
      wal_path,
      task,
      first_handler,
      checkpoint_path~,
      initial_credit_events=2L,
      initial_credit_bytes=4096L,
      output_log_path~,
    )
    assert_eq(first.live_batches(), 1)
    assert_eq(first.replayed_batches(), 0)
    assert_eq(first_outputs.length(), 3)
    let output_log = TaskOutputLog::open(output_log_path)
    assert_eq(output_log.records_for("restore-task").length(), 3)
    output_log.close()
    coordinator_task.wait()
  }

  let second_server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer second_server.close()
  let second_addr = second_server.addr
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = second_server.accept()
      defer connection.close()
      let assignment = @protocol.AssignmentSpec::new(0, 2, "boot-restore")
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "cluster-restore",
        "coordinator",
        "worker-restore",
        "restore-second",
        assignment~,
      )
      coordinator.restore_send_cursor(0, 2, 2L)
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      let duplicate = outbound.build(
        @protocol.FrameKind::EventBatch,
        batch.to_payload(),
      )
      write_frame(connection, duplicate)
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      write_frame(connection, outbound.build(@protocol.FrameKind::Drain, "{}"))
    }
    let connection = @socket.Tcp::connect(second_addr)
    let task = @task.Task::new(
      "restore-task",
      "reachability",
      @task.OperatorKind::Reachability,
      0,
      2,
    )
    let second_outputs : Array[@task.TaskOutput] = []
    let second_handler : (@task.TaskOutput) -> Unit raise = output => {
      second_outputs.push(output)
    }
    let second = run_worker_task(
      connection,
      "cluster-restore",
      "coordinator",
      "worker-restore",
      "boot-restore",
      [],
      wal_path,
      task,
      second_handler,
      checkpoint_path~,
      initial_credit_events=2L,
      initial_credit_bytes=4096L,
      output_log_path~,
    )
    assert_eq(second.replayed_batches(), 0)
    assert_eq(second.live_batches(), 0)
    assert_eq(second_outputs, [])
    assert_eq(task.offset(), 1L)
    assert_eq(task.outputs().length(), 3)
    coordinator_task.wait()
  }

  @fs.remove(checkpoint_path)
  let third_server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer third_server.close()
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = third_server.accept()
      defer connection.close()
      let assignment = @protocol.AssignmentSpec::new(0, 2, "boot-restore")
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "cluster-restore",
        "coordinator",
        "worker-restore",
        "restore-third",
        assignment~,
      )
      coordinator.restore_send_cursor(0, 2, 2L)
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      write_frame(
        connection,
        outbound.build(@protocol.FrameKind::EventBatch, batch.to_payload()),
      )
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      write_frame(connection, outbound.build(@protocol.FrameKind::Drain, "{}"))
    }
    let task = @task.Task::new(
      "restore-task",
      "reachability",
      @task.OperatorKind::Reachability,
      0,
      2,
    )
    let replay_outputs : Array[@task.TaskOutput] = []
    let replay_handler : (@task.TaskOutput) -> Unit raise = output => {
      replay_outputs.push(output)
    }
    let replayed = run_worker_task(
      @socket.Tcp::connect(third_server.addr),
      "cluster-restore",
      "coordinator",
      "worker-restore",
      "boot-restore",
      [],
      wal_path,
      task,
      replay_handler,
      initial_credit_events=2L,
      initial_credit_bytes=4096L,
      output_log_path~,
    )
    assert_eq(replayed.replayed_batches(), 1)
    assert_eq(replayed.live_batches(), 0)
    assert_eq(replay_outputs.length(), 3)
    assert_eq(task.outputs().length(), 3)
    coordinator_task.wait()
  }
}
