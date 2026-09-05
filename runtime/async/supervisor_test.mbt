///|
async test "supervisor routes multiple workers by Hello identity" {
  let directory = @fs.tmpdir(prefix="moonweave-supervisor")
  let wal_a = directory + "/worker-a.wal"
  let wal_b = directory + "/worker-b.wal"
  defer {
    @fs.remove(wal_a) catch {
      _ => ()
    }
    @fs.remove(wal_a + ".lock") catch {
      _ => ()
    }
    @fs.remove(wal_b) catch {
      _ => ()
    }
    @fs.remove(wal_b + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let boot_a = @protocol.Incarnation::new("boot-a")
  let boot_b = @protocol.Incarnation::new("boot-b")
  ignore(control.register_worker("worker-a", boot_a, ["graph"]))
  ignore(control.register_worker("worker-b", boot_b, ["graph"]))
  let assignment_a = control.assign(0, "worker-a")
  let assignment_b = control.assign(1, "worker-b")
  let event_a = @protocol.EventRecord::new(
    @causal.EventId::new("source-a", 0),
    [],
    "a",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let event_b = @protocol.EventRecord::new(
    @causal.EventId::new("source-b", 0),
    [],
    "b",
    Json::object({ "source": Json::string("X"), "target": Json::string("Y") }),
  )
  let work_a = SupervisorWork::new("worker-a", "session-a", 0, [
    @protocol.EventBatch::new(0, assignment_a.epoch(), 0L, [event_a]),
  ])
  let work_b = SupervisorWork::new("worker-b", "session-b", 1, [
    @protocol.EventBatch::new(1, assignment_b.epoch(), 0L, [event_b]),
  ])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let supervisor_task = group.spawn() <| () => {
      run_supervisor(server, control, [work_a, work_b])
    }
    let worker_b_task = group.spawn() <| () => {
      let connection = @socket.Tcp::connect(addr)
      let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
      run_worker(
        connection,
        "cluster-a",
        "coordinator",
        "worker-b",
        "boot-b",
        ["graph"],
        wal_b,
        handler,
        initial_credit_events=1L,
        initial_credit_bytes=1024L,
      )
    }
    let worker_a_task = group.spawn() <| () => {
      let connection = @socket.Tcp::connect(addr)
      let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
      run_worker(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "boot-a",
        ["graph"],
        wal_a,
        handler,
        initial_credit_events=1L,
        initial_credit_bytes=1024L,
      )
    }
    let supervisor_summary = supervisor_task.wait()
    let worker_a_summary = worker_a_task.wait()
    let worker_b_summary = worker_b_task.wait()
    assert_eq(supervisor_summary.completed_workers(), 2)
    assert_eq(supervisor_summary.sent_batches(), 2)
    assert_eq(worker_a_summary.live_batches(), 1)
    assert_eq(worker_b_summary.live_batches(), 1)
  }
  guard control.assignment(0) is Some(final_a) else {
    fail("worker-a assignment missing")
  }
  guard control.assignment(1) is Some(final_b) else {
    fail("worker-b assignment missing")
  }
  assert_eq(final_a.worker_id(), "worker-a")
  assert_eq(final_a.applied_offset(), 0L)
  assert_eq(final_b.worker_id(), "worker-b")
  assert_eq(final_b.applied_offset(), 0L)
}

///|
async test "supervisor sends primary batches and exchange deltas on one link" {
  let directory = @fs.tmpdir(prefix="moonweave-supervisor-delta")
  let wal_path = directory + "/worker-b.wal"
  let delta_wal_path = directory + "/worker-b.delta.wal"
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
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let boot = @protocol.Incarnation::new("boot-b")
  ignore(control.register_worker("worker-b", boot, ["graph"]))
  let assignment = control.assign(1, "worker-b")
  let primary_event = @protocol.EventRecord::new(
    @causal.EventId::new("primary", 0),
    [],
    "primary",
    Json::object({ "source": Json::string("P"), "target": Json::string("Q") }),
  )
  let delta_event = @protocol.EventRecord::new(
    @causal.EventId::new("exchange", 0),
    [],
    "exchange",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let primary = @protocol.EventBatch::new(1, assignment.epoch(), 0L, [
    primary_event,
  ])
  let delta = @protocol.DeltaBatch::new(
    "supervisor-delta",
    0,
    0,
    1,
    assignment.epoch(),
    [delta_event],
  )
  let work = SupervisorWork::new("worker-b", "session-b", 1, [primary], deltas=[
    delta,
  ])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let supervisor_task = group.spawn() <| () => {
      run_supervisor(server, control, [work])
    }
    let connection = @socket.Tcp::connect(addr)
    let primary_handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let mut delta_count = 0
    let delta_handler : (@protocol.DeltaBatch) -> Unit raise = _ => {
      delta_count += 1
    }
    let worker_summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-b",
      "boot-b",
      ["graph"],
      wal_path,
      primary_handler,
      initial_credit_events=2L,
      initial_credit_bytes=4096L,
      delta_handler~,
      delta_wal_path~,
    )
    let supervisor_summary = supervisor_task.wait()
    assert_eq(worker_summary.live_batches(), 1)
    assert_eq(delta_count, 1)
    assert_eq(supervisor_summary.completed_workers(), 1)
    assert_eq(supervisor_summary.failed_workers(), 0)
    assert_eq(supervisor_summary.sent_batches(), 1)
    assert_eq(supervisor_summary.sent_deltas(), 1)
  }
}

///|
async test "supervisor retries a transient connection failure" {
  let directory = @fs.tmpdir(prefix="moonweave-supervisor-retry")
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
  let boot = @protocol.Incarnation::new("boot-retry")
  ignore(control.register_worker("worker-a", boot, ["graph"]))
  let assignment = control.assign(0, "worker-a")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("retry", 0),
    [],
    "retry",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let work = SupervisorWork::new("worker-a", "retry-session", 0, [
    @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event]),
  ])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  let policy = RetryPolicy::new(2, 1, 1)
  @async_runtime.with_task_group <| group => {
    let supervisor_task = group.spawn() <| () => {
      run_supervisor_with_retry(server, control, [work], policy)
    }
    let failed = @socket.Tcp::connect(addr)
    failed.close()
    @async_runtime.sleep(5)
    let connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let worker_summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      boot.value(),
      ["graph"],
      wal_path,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=2048L,
    )
    let supervisor_summary = supervisor_task.wait()
    assert_eq(worker_summary.live_batches(), 1)
    assert_eq(supervisor_summary.completed_workers(), 1)
    assert_eq(supervisor_summary.failed_workers(), 0)
    assert_eq(supervisor_summary.retried_connections(), 1)
    assert_eq(supervisor_summary.failures(), [])
  }
}

///|
async test "supervisor rejects ambiguous plans before accepting sockets" {
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let boot = @protocol.Incarnation::new("boot-plan")
  ignore(control.register_worker("worker-a", boot, []))
  let assignment = control.assign(0, "worker-a")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("plan", 0),
    [],
    "plan",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  let work = SupervisorWork::new("worker-a", "session-plan", 0, [batch])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  try run_supervisor(server, control, [work, work]) catch {
    FrameIoError::SupervisorPlan(
      SupervisorPlanError::DuplicateWorker(worker_id)
    ) => assert_eq(worker_id, "worker-a")
    _ => fail("expected duplicate worker plan rejection")
  } noraise {
    _ => fail("expected duplicate worker plan rejection")
  }
}
