///|
async test "managed coordinator mirrors worker progress into control state" {
  let directory = @fs.tmpdir(prefix="moonweave-managed")
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
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let incarnation = @protocol.Incarnation::new("boot-1")
  ignore(control.register_worker("worker-a", incarnation, ["reachability"]))
  let assignment = control.assign(0, "worker-a")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("source", 0),
    [],
    "graph",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let second_event = @protocol.EventRecord::new(
    @causal.EventId::new("source", 1),
    [event.id()],
    "graph",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  let first_batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  let second_batch = @protocol.EventBatch::new(0, assignment.epoch(), 1L, [
    second_event,
  ])
  let batches = [first_batch, second_batch]
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    group.spawn_bg() <| () => {
      let (connection, _) = server.accept()
      ignore(
        run_managed_coordinator(
          connection, control, "worker-a", "session-managed", 0, batches,
        ),
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-1",
      ["reachability"],
      wal_path,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=1024L,
    )
    assert_eq(summary.live_batches(), 2)
  }
  @async_runtime.with_task_group <| group => {
    group.spawn_bg() <| () => {
      let (connection, _) = server.accept()
      ignore(
        run_managed_coordinator(
          connection, control, "worker-a", "session-managed-reconnect", 0, batches,
        ),
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-1",
      ["reachability"],
      wal_path,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=1024L,
    )
    assert_eq(summary.replayed_batches(), 2)
    assert_eq(summary.live_batches(), 0)
  }
  guard control.assignment(0) is Some(current) else {
    fail("managed assignment missing")
  }
  assert_eq(current.next_offset(), 2L)
  assert_eq(current.persisted_offset(), 1L)
  assert_eq(current.applied_offset(), 1L)
}

///|
async test "managed coordinator records heartbeat liveness" {
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let incarnation = @protocol.Incarnation::new("boot-heartbeat")
  ignore(control.register_worker("worker-a", incarnation, ["graph"]))
  let assignment = control.assign(0, "worker-a")
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("source-heartbeat", 0),
    [],
    "heartbeat",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = server.accept()
      run_managed_coordinator(
        connection,
        control,
        "worker-a",
        "session-heartbeat",
        0,
        [batch],
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let (worker, outbound) = worker_handshake(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-heartbeat",
      ["graph"],
    )
    let resume_message = @protocol.Resume::new(0, assignment.epoch(), -1L)
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Resume, resume_message.to_payload()),
    )
    let heartbeat = @protocol.Heartbeat::new(0, assignment.epoch(), 0, -1L)
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Heartbeat, heartbeat.to_payload()),
    )
    let credit = @protocol.Credit::new(0, assignment.epoch(), 1L, 1024L)
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Credit, credit.to_payload()),
    )
    let batch_frame = read_frame(connection)
    worker.accept_frame(batch_frame)
    worker.mark_persisted(0L)
    let ack = @protocol.AckPersisted::new(0, assignment.epoch(), 0L)
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::AckPersisted, ack.to_payload()),
    )
    worker.mark_applied(0L)
    let progress = @protocol.Progress::new(
      0,
      assignment.epoch(),
      0L,
      0L,
      "offset:0",
    )
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Progress, progress.to_payload()),
    )
    let drain = read_frame(connection)
    worker.accept_frame(drain)
    assert_eq(worker.phase(), @protocol.SessionPhase::Draining)
    let summary = coordinator_task.wait()
    assert_eq(summary.sent_batches(), 1)
    assert_eq(summary.persisted_offset(), 0L)
    assert_eq(summary.applied_offset(), 0L)
  }
  guard control.worker("worker-a") is Some(worker_info) else {
    fail("managed worker missing")
  }
  guard worker_info.last_heartbeat_tick() is Some(_) else {
    fail("managed heartbeat was not recorded")
  }
}

///|
async test "managed coordinator closes a targeted probe obligation" {
  let directory = @fs.tmpdir(prefix="moonweave-probe")
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
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let incarnation = @protocol.Incarnation::new("boot-probe")
  ignore(control.register_worker("worker-a", incarnation, ["graph"]))
  let assignment = control.assign(0, "worker-a")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("source-probe", 0),
    [],
    "probe",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  let probe = @protocol.Probe::new("query-1", 0, assignment.epoch(), 0L)
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = server.accept()
      run_managed_coordinator(
        connection,
        control,
        "worker-a",
        "session-probe",
        0,
        [batch],
        probe~,
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let worker_summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-probe",
      ["graph"],
      wal_path,
      handler,
    )
    assert_eq(worker_summary.live_batches(), 1)
    let coordinator_summary = coordinator_task.wait()
    assert_eq(coordinator_summary.sent_batches(), 1)
    guard coordinator_summary.probe_reply() is Some(reply) else {
      fail("coordinator should return a probe reply")
    }
    assert_eq(reply.query_id(), "query-1")
    assert_eq(reply.reached_offset(), 0L)
  }
  guard control.assignment(0) is Some(final_assignment) else {
    fail("probe assignment missing")
  }
  assert_eq(final_assignment.persisted_offset(), 0L)
  assert_eq(final_assignment.applied_offset(), 0L)
}

///|
async test "durable coordinator resends a batch after a send-window crash" {
  let directory = @fs.tmpdir(prefix="moonweave-durable-driver")
  let journal_path = directory + "/coordinator.journal"
  let checkpoint_path = directory + "/coordinator.json"
  let wal_path = directory + "/worker.wal"
  defer {
    @fs.remove(journal_path) catch {
      _ => ()
    }
    @fs.remove(journal_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(checkpoint_path) catch {
      _ => ()
    }
    @fs.remove(checkpoint_path + ".tmp") catch {
      _ => ()
    }
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
  let store = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  let boot = @protocol.Incarnation::new("boot-durable")
  ignore(store.register_worker("worker-a", boot, ["graph"]))
  let assignment = store.assign(0, "worker-a")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("source-durable", 0),
    [],
    "durable",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    group.spawn_bg(allow_failure=true) <| () => {
      let (connection, _) = server.accept()
      ignore(
        run_durable_managed_coordinator(
          connection,
          store,
          "worker-a",
          "session-crash",
          0,
          [batch],
        ),
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let (_, outbound) = worker_handshake(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-durable",
      ["graph"],
    )
    let resume_message = @protocol.Resume::new(0, assignment.epoch(), -1L)
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Resume, resume_message.to_payload()),
    )
    let progress = @protocol.Progress::new(
      0,
      assignment.epoch(),
      -1L,
      -1L,
      "offset:-1",
    )
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Progress, progress.to_payload()),
    )
    let credit = @protocol.Credit::new(0, assignment.epoch(), 1L, 1024L)
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Credit, credit.to_payload()),
    )
    ignore(read_frame(connection))
    connection.close()
  }
  guard store.snapshot().assignments() is [inflight] else {
    fail("durable assignment missing after crash window")
  }
  assert_eq(inflight.next_offset(), 1L)
  assert_eq(inflight.persisted_offset(), -1L)
  store.close()

  let restored_store = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  let second_server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer second_server.close()
  let second_addr = second_server.addr
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = second_server.accept()
      run_durable_managed_coordinator(
        connection,
        restored_store,
        "worker-a",
        "session-recovered",
        0,
        [batch],
      )
    }
    let connection = @socket.Tcp::connect(second_addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-durable",
      ["graph"],
      wal_path,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=1024L,
    )
    assert_eq(summary.live_batches(), 1)
    let coordinator_summary = coordinator_task.wait()
    assert_eq(coordinator_summary.sent_batches(), 1)
  }
  guard restored_store.snapshot().assignments() is [recovered] else {
    fail("recovered assignment missing")
  }
  assert_eq(recovered.next_offset(), 1L)
  assert_eq(recovered.persisted_offset(), 0L)
  assert_eq(recovered.applied_offset(), 0L)
  restored_store.close()
}
