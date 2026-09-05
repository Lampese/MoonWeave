///|
async test "durable supervisor journals a managed worker session" {
  let directory = @fs.tmpdir(prefix="moonweave-durable-supervisor")
  let journal_path = directory + "/coordinator.wal"
  let checkpoint_path = directory + "/coordinator.json"
  let worker_wal = directory + "/worker.wal"
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
    @fs.remove(worker_wal) catch {
      _ => ()
    }
    @fs.remove(worker_wal + ".lock") catch {
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
  let boot = @protocol.Incarnation::new("boot-durable-supervisor")
  ignore(store.register_worker("worker-a", boot, ["graph"]))
  let assignment = store.assign(0, "worker-a")
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("durable-supervisor", 0),
    [],
    "durable-supervisor",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  let work = SupervisorWork::new("worker-a", "durable-supervisor-session", 0, [
    batch,
  ])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let supervisor_task = group.spawn() <| () => {
      run_durable_supervisor(server, store, [work])
    }
    let connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let worker_summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      boot.value(),
      ["graph"],
      worker_wal,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=2048L,
    )
    let supervisor_summary = supervisor_task.wait()
    assert_eq(worker_summary.live_batches(), 1)
    assert_eq(supervisor_summary.completed_workers(), 1)
    assert_eq(supervisor_summary.failed_workers(), 0)
  }
  assert_eq(store.snapshot().assignments().length(), 1)
  store.close()
  let restored = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  guard restored.snapshot().assignments() is [final_assignment] else {
    fail("durable supervisor checkpoint missing assignment")
  }
  assert_eq(final_assignment.applied_offset(), 0L)
  restored.close()
}
