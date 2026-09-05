///|
async test "managed exchange consumes a closed outbox after acknowledgements" {
  let directory = @fs.tmpdir(prefix="moonweave-outbox-driver")
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
  let boot = @protocol.Incarnation::new("boot-outbox")
  ignore(control.register_worker("worker-b", boot, ["graph"]))
  let assignment = control.assign(1, "worker-b")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("source-outbox", 0),
    [],
    "outbox",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let delta = @protocol.DeltaBatch::new(
    "outbox-driver-delta",
    0,
    0,
    1,
    assignment.epoch(),
    [event],
  )
  let outbox = ExchangeOutbox::new(1)
  outbox.push(delta)
  outbox.close()
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn() <| () => {
      let (connection, _) = server.accept()
      run_managed_exchange_outbox(
        connection,
        control,
        "worker-b",
        "outbox-session",
        1,
        outbox,
        poll_interval_ms=1,
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let mut received = 0
    let delta_handler : (@protocol.DeltaBatch) -> Unit raise = _ => {
      received += 1
    }
    let summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-b",
      "boot-outbox",
      ["graph"],
      wal_path,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=2048L,
      delta_handler~,
      delta_wal_path~,
    )
    let coordinator_summary = coordinator_task.wait()
    assert_eq(summary.live_batches(), 0)
    assert_eq(received, 1)
    assert_eq(coordinator_summary.sent_deltas(), 1)
    assert_eq(coordinator_summary.acked_deltas(), 1)
  }
}
