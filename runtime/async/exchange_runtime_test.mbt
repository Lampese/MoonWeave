///|
async test "cross-worker exchange releases causal pending state" {
  let directory = @fs.tmpdir(prefix="moonweave-exchange")
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
  let source_incarnation = @protocol.Incarnation::new("boot-source")
  let target_incarnation = @protocol.Incarnation::new("boot-target")
  ignore(control.register_worker("worker-a", source_incarnation, ["graph"]))
  ignore(control.register_worker("worker-b", target_incarnation, ["graph"]))
  ignore(control.assign(0, "worker-a"))
  let target_assignment = control.assign(1, "worker-b")
  let root_id = @causal.EventId::new("source-a", 0)
  let root = @protocol.EventRecord::new(
    root_id,
    [],
    "graph",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("source-b", 0),
    [root_id],
    "graph",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  let child_delta = @protocol.DeltaBatch::new(
    "delta-child",
    0,
    0,
    1,
    target_assignment.epoch(),
    [child],
  )
  let root_delta = @protocol.DeltaBatch::new(
    "delta-root",
    0,
    0,
    1,
    target_assignment.epoch(),
    [root],
  )
  let engine = @reachability.Engine::new()
  let query = @reachability.Query::new("cross-worker-A-C", "A", "C", [1])
  engine.set_query(query)
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = server.accept()
      run_managed_exchange(
        connection,
        control,
        "worker-b",
        "session-exchange",
        1,
        [child_delta, root_delta],
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let primary_handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let delta_handler : (@protocol.DeltaBatch) -> Unit raise = delta => {
      for record in delta.events() {
        ignore(
          engine.apply_json_event(
            record.id(),
            record.parents(),
            record.key(),
            record.payload(),
          ),
        )
      }
    }
    let worker_summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-b",
      "boot-target",
      ["graph"],
      wal_path,
      primary_handler,
      initial_credit_events=2L,
      initial_credit_bytes=4096L,
      delta_handler~,
      delta_wal_path~,
    )
    assert_eq(worker_summary.live_batches(), 0)
    let coordinator_summary = coordinator_task.wait()
    assert_eq(coordinator_summary.sent_deltas(), 2)
    assert_eq(coordinator_summary.acked_deltas(), 2)
  }
  assert_eq(engine.event_count(), 2)
  assert_eq(engine.pending_count(), 0)
  assert_eq(engine.facts().length(), 3)
  guard engine.query() is Some(query_state) else {
    fail("cross-worker query missing")
  }
  assert_eq(query_state.status(), @reachability.QueryStatus::Tentative)
  assert_true(query_state.close_partition(1))
  assert_eq(query_state.status(), @reachability.QueryStatus::Stable)
  guard control.assignment(1) is Some(final_assignment) else {
    fail("target assignment missing")
  }
  assert_eq(final_assignment.next_offset(), 0L)
  assert_eq(final_assignment.applied_offset(), -1L)
}

///|
async test "worker replays Delta WAL without applying it twice" {
  let directory = @fs.tmpdir(prefix="moonweave-delta-recovery")
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
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let boot = @protocol.Incarnation::new("boot-delta-recovery")
  ignore(control.register_worker("worker-b", boot, ["graph"]))
  let assignment = control.assign(1, "worker-b")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("source-delta", 0),
    [],
    "delta",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let delta = @protocol.DeltaBatch::new(
    "delta-recovery",
    0,
    0,
    1,
    assignment.epoch(),
    [event],
  )
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  let mut applied = 0
  @async_runtime.with_task_group <| group => {
    group.spawn_bg() <| () => {
      let (connection, _) = server.accept()
      let (session, outbound) = coordinator_handshake(
        connection,
        "cluster-a",
        "coordinator",
        "worker-b",
        "delta-session-1",
        assignment=@protocol.AssignmentSpec::new(
          1,
          assignment.epoch(),
          boot.value(),
        ),
      )
      let resume_frame = read_frame(connection)
      session.accept_frame(resume_frame)
      let progress_frame = read_frame(connection)
      session.accept_frame(progress_frame)
      let credit_frame = read_frame(connection)
      session.accept_frame(credit_frame)
      ignore(
        coordinator_send_delta(
          connection,
          session,
          outbound,
          delta,
          @utf8.encode(delta.to_payload()).length().to_int64(),
        ),
      )
      ignore(read_frame(connection))
      ignore(read_frame(connection))
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
      write_frame(connection, drain)
    }
    let connection = @socket.Tcp::connect(addr)
    let primary_handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let delta_handler : (@protocol.DeltaBatch) -> Unit raise = _ => applied += 1
    ignore(
      run_worker(
        connection,
        "cluster-a",
        "coordinator",
        "worker-b",
        boot.value(),
        ["graph"],
        wal_path,
        primary_handler,
        delta_handler~,
        delta_wal_path~,
      ),
    )
  }
  assert_eq(applied, 1)
  let second_server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer second_server.close()
  let second_addr = second_server.addr
  @async_runtime.with_task_group <| group => {
    group.spawn_bg() <| () => {
      let (connection, _) = second_server.accept()
      let (session, outbound) = coordinator_handshake(
        connection,
        "cluster-a",
        "coordinator",
        "worker-b",
        "delta-session-2",
        assignment=@protocol.AssignmentSpec::new(
          1,
          assignment.epoch(),
          boot.value(),
        ),
      )
      let resume_frame = read_frame(connection)
      session.accept_frame(resume_frame)
      let progress_frame = read_frame(connection)
      session.accept_frame(progress_frame)
      let credit_frame = read_frame(connection)
      session.accept_frame(credit_frame)
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
      write_frame(connection, drain)
    }
    let connection = @socket.Tcp::connect(second_addr)
    let primary_handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let delta_handler : (@protocol.DeltaBatch) -> Unit raise = _ => applied += 1
    let summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-b",
      boot.value(),
      ["graph"],
      wal_path,
      primary_handler,
      delta_handler~,
      delta_wal_path~,
    )
    assert_eq(summary.replayed_deltas(), 1)
  }
  assert_eq(applied, 2)
}
