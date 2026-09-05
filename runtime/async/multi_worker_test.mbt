///|
async test "two managed workers process independent partitions concurrently" {
  let directory = @fs.tmpdir(prefix="moonweave-multi-worker")
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
  let incarnation_a = @protocol.Incarnation::new("boot-a")
  let incarnation_b = @protocol.Incarnation::new("boot-b")
  ignore(control.register_worker("worker-b", incarnation_b, ["graph"]))
  ignore(control.register_worker("worker-a", incarnation_a, ["graph"]))
  let assignments = control.assign_round_robin(2, ["worker-b", "worker-a"])
  let assignment_a = assignments[0]
  let assignment_b = assignments[1]
  assert_eq(assignment_a.worker_id(), "worker-a")
  assert_eq(assignment_b.worker_id(), "worker-b")

  let event_a = @protocol.EventRecord::new(
    @causal.EventId::new("source-a", 0),
    [],
    "partition-a",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let event_b = @protocol.EventRecord::new(
    @causal.EventId::new("source-b", 0),
    [],
    "partition-b",
    Json::object({ "source": Json::string("X"), "target": Json::string("Y") }),
  )
  let batch_a = @protocol.EventBatch::new(0, assignment_a.epoch(), 0L, [event_a])
  let batch_b = @protocol.EventBatch::new(1, assignment_b.epoch(), 0L, [event_b])
  let server_a = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  let server_b = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server_a.close()
  defer server_b.close()
  let addr_a = server_a.addr
  let addr_b = server_b.addr

  @async_runtime.with_task_group <| group => {
    let coordinator_a = group.spawn() <| () => {
      let (connection, _) = server_a.accept()
      run_managed_coordinator(connection, control, "worker-a", "session-a", 0, [
        batch_a,
      ])
    }
    let coordinator_b = group.spawn() <| () => {
      let (connection, _) = server_b.accept()
      run_managed_coordinator(connection, control, "worker-b", "session-b", 1, [
        batch_b,
      ])
    }
    let worker_a = group.spawn() <| () => {
      let connection = @socket.Tcp::connect(addr_a)
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
    let worker_b = group.spawn() <| () => {
      let connection = @socket.Tcp::connect(addr_b)
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
    let coordinator_summary_a = coordinator_a.wait()
    let coordinator_summary_b = coordinator_b.wait()
    let worker_summary_a = worker_a.wait()
    let worker_summary_b = worker_b.wait()
    assert_eq(coordinator_summary_a.sent_batches(), 1)
    assert_eq(coordinator_summary_b.sent_batches(), 1)
    assert_eq(worker_summary_a.live_batches(), 1)
    assert_eq(worker_summary_b.live_batches(), 1)
  }

  guard control.assignment(0) is Some(final_a) else {
    fail("partition 0 assignment missing")
  }
  guard control.assignment(1) is Some(final_b) else {
    fail("partition 1 assignment missing")
  }
  assert_eq(final_a.worker_id(), "worker-a")
  assert_eq(final_a.persisted_offset(), 0L)
  assert_eq(final_a.applied_offset(), 0L)
  assert_eq(final_b.worker_id(), "worker-b")
  assert_eq(final_b.persisted_offset(), 0L)
  assert_eq(final_b.applied_offset(), 0L)
}
