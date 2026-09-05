///|
async fn with_coordinator_paths(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-coordinator")
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
  f(wal_path)
}

///|
fn coordinator_batches() -> Array[@protocol.EventBatch] raise @protocol.ProtocolError {
  let first_id = @causal.EventId::new("source", 0) catch {
    error => raise @protocol.ProtocolError::Causal(error)
  }
  let second_id = @causal.EventId::new("source", 1) catch {
    error => raise @protocol.ProtocolError::Causal(error)
  }
  let first_event = @protocol.EventRecord::new(
    first_id,
    [],
    "account-1",
    Json::object({ "edge": Json::string("a->b") }),
  )
  let second_event = @protocol.EventRecord::new(
    second_id,
    [first_id],
    "account-1",
    Json::object({ "edge": Json::string("b->c") }),
  )
  [
    @protocol.EventBatch::new(0, 3, 0L, [first_event]),
    @protocol.EventBatch::new(0, 3, 1L, [second_event]),
  ]
}

///|
async test "coordinator driver follows credit and drains after applied progress" {
  with_coordinator_paths <| wal_path => {
    let batches = coordinator_batches()
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let addr = server.addr
    let mut applied = 0
    let handler : (@protocol.EventBatch) -> Unit raise = batch => {
      applied += batch.events().length()
    }
    @async_runtime.with_task_group <| group => {
      group.spawn_bg() <| () => {
        let (connection, _) = server.accept()
        let assignment = @protocol.AssignmentSpec::new(0, 3, "boot-1")
        ignore(
          run_coordinator(
            connection, "cluster-a", "coordinator", "worker-a", "session-driver",
            assignment, batches,
          ),
        )
      }
      let connection = @socket.Tcp::connect(addr)
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
      assert_eq(summary.replayed_batches(), 0)
      assert_eq(summary.live_batches(), 2)
      assert_eq(summary.persisted_offset(), 1L)
      assert_eq(summary.applied_offset(), 1L)
    }
    assert_eq(applied, 2)
  }
}
