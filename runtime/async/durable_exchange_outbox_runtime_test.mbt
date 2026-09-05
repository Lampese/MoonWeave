///|
async test "durable exchange driver sends and acknowledges recovered deltas" {
  let directory = @fs.tmpdir(prefix="moonweave-durable-exchange-driver")
  let outbox_path = directory + "/exchange.wal"
  let worker_wal = directory + "/worker.wal"
  let delta_wal = directory + "/worker.delta.wal"
  defer {
    @fs.remove(outbox_path) catch {
      _ => ()
    }
    @fs.remove(outbox_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(outbox_path + ".repair") catch {
      _ => ()
    }
    @fs.remove(worker_wal) catch {
      _ => ()
    }
    @fs.remove(worker_wal + ".lock") catch {
      _ => ()
    }
    @fs.remove(delta_wal) catch {
      _ => ()
    }
    @fs.remove(delta_wal + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let control = @control.CoordinatorState::new(
    "cluster-durable-exchange", "coordinator",
  )
  let boot = @protocol.Incarnation::new("boot-durable-exchange")
  ignore(control.register_worker("worker-b", boot, ["graph"]))
  let assignment = control.assign(1, "worker-b")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("durable-exchange", 0),
    [],
    "exchange",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let delta = @protocol.DeltaBatch::new(
    "durable-driver-delta",
    0,
    0,
    1,
    assignment.epoch(),
    [event],
  )
  let outbox = DurableExchangeOutbox::open(outbox_path, 4)
  outbox.push(delta)
  outbox.close()
  let recovered = DurableExchangeOutbox::open(outbox_path, 4)
  recovered.seal()
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  @async_runtime.with_task_group <| group => {
    let coordinator = group.spawn() <| () => {
      let (connection, _) = server.accept()
      run_managed_durable_exchange_outbox(
        connection,
        control,
        "worker-b",
        "durable-exchange-session",
        1,
        recovered,
        poll_interval_ms=1,
      )
    }
    let connection = @socket.Tcp::connect(server.addr)
    let primary_handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let mut received = 0
    let delta_handler : (@protocol.DeltaBatch) -> Unit raise = _ => {
      received += 1
    }
    let worker = run_worker(
      connection,
      "cluster-durable-exchange",
      "coordinator",
      "worker-b",
      boot.value(),
      ["graph"],
      worker_wal,
      primary_handler,
      initial_credit_events=1L,
      initial_credit_bytes=2048L,
      delta_handler~,
      delta_wal_path=delta_wal,
    )
    let summary = coordinator.wait()
    assert_eq(worker.live_batches(), 0)
    assert_eq(received, 1)
    assert_eq(summary.sent_deltas(), 1)
    assert_eq(summary.acked_deltas(), 1)
  }
  assert_eq(recovered.pending_count(), 0)
  assert_eq(recovered.acked_count(), 1)
  recovered.close()
}
