///|
async test "worker task merges primary and exchange inputs in one operator" {
  let directory = @fs.tmpdir(prefix="moonweave-task-exchange")
  let wal_path = directory + "/worker.wal"
  let delta_wal_path = directory + "/worker.delta.wal"
  defer {
    @fs.remove(wal_path) catch {
      _ => ()
    }
    @fs.remove(wal_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(delta_wal_path) catch {
      _ => ()
    }
    @fs.remove(delta_wal_path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let root = @protocol.EventRecord::new(
    @causal.EventId::new("exchange-task", 0),
    [],
    "graph",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("exchange-task", 1),
    [root.id()],
    "graph",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  let assignment = @protocol.AssignmentSpec::new(0, 5, "boot-exchange")
  let primary = @protocol.EventBatch::new(0, 5, 0L, [root])
  let delta = @protocol.DeltaBatch::new("task-exchange-delta", 1, 3, 0, 5, [
    child,
  ])
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
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "cluster-task-exchange",
        "coordinator",
        "worker-exchange",
        "task-exchange-session",
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
        primary,
        @utf8.encode(primary.to_payload()).length().to_int64(),
      )
      let ack = read_frame(connection)
      coordinator.accept_frame(ack)
      let progress = read_frame(connection)
      coordinator.accept_frame(progress)
      let credit = read_frame(connection)
      coordinator.accept_frame(credit)
      ignore(
        coordinator_send_delta(
          connection,
          coordinator,
          outbound,
          delta,
          @utf8.encode(delta.to_payload()).length().to_int64(),
        ),
      )
      let delta_ack = read_frame(connection)
      coordinator.accept_frame(delta_ack)
      let delta_credit = read_frame(connection)
      coordinator.accept_frame(delta_credit)
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
      write_frame(connection, drain)
    }
    let connection = @socket.Tcp::connect(addr)
    let task = @task.Task::from_assignment(
      "task-exchange",
      "reachability",
      @task.OperatorKind::Reachability,
      assignment,
    )
    let outputs : Array[@task.TaskOutput] = []
    let output_handler : (@task.TaskOutput) -> Unit raise = output => {
      outputs.push(output)
    }
    let summary = run_worker_task(
      connection,
      "cluster-task-exchange",
      "coordinator",
      "worker-exchange",
      "boot-exchange",
      [],
      wal_path,
      task,
      output_handler,
      initial_credit_events=2L,
      initial_credit_bytes=4096L,
      delta_wal_path~,
    )
    assert_eq(summary.live_batches(), 1)
    assert_eq(outputs, [
      @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
      @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "C")),
      @task.TaskOutput::Reachable(@reachability.Reachable::new("B", "C")),
    ])
    assert_eq(task.offset(), 0L)
    assert_eq(task.phase(), @task.TaskPhase::Stopped)
    coordinator_task.wait()
  }
}
