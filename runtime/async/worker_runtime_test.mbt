///|
fn recovery_batch() -> @protocol.EventBatch raise @protocol.ProtocolError {
  let id = @causal.EventId::new("source", 0) catch {
    error => raise @protocol.ProtocolError::Causal(error)
  }
  let event = @protocol.EventRecord::new(
    id,
    [],
    "account-1",
    Json::object({ "edge": Json::string("a->b") }),
  )
  @protocol.EventBatch::new(0, 1, 0L, [event])
}

///|
fn conflicting_recovery_batch() -> @protocol.EventBatch raise @protocol.ProtocolError {
  let id = @causal.EventId::new("source", 0) catch {
    error => raise @protocol.ProtocolError::Causal(error)
  }
  let event = @protocol.EventRecord::new(
    id,
    [],
    "account-1",
    Json::object({ "edge": Json::string("a->x") }),
  )
  @protocol.EventBatch::new(0, 1, 0L, [event])
}

///|
fn second_recovery_batch() -> @protocol.EventBatch raise @protocol.ProtocolError {
  let id = @causal.EventId::new("source", 1) catch {
    error => raise @protocol.ProtocolError::Causal(error)
  }
  let event = @protocol.EventRecord::new(
    id,
    [],
    "account-2",
    Json::object({ "edge": Json::string("b->c") }),
  )
  @protocol.EventBatch::new(0, 1, 1L, [event])
}

///|
fn conflicting_payload_batch() -> @protocol.EventBatch raise @protocol.ProtocolError {
  let first = recovery_batch()
  let original = first.events()[0]
  let event = @protocol.EventRecord::new(
    original.id(),
    original.parents(),
    original.key(),
    Json::object({ "edge": Json::string("different-payload") }),
  )
  @protocol.EventBatch::new(0, 1, 1L, [event])
}

///|
async fn with_worker_recovery_paths(f : async (String, String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-worker")
  let wal_path = directory + "/events.wal"
  let checkpoint_path = directory + "/worker.json"
  defer {
    @fs.remove(wal_path) catch {
      _ => ()
    }
    @fs.remove(wal_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(wal_path + ".repair") catch {
      _ => ()
    }
    @fs.remove(checkpoint_path) catch {
      _ => ()
    }
    @fs.remove(checkpoint_path + ".tmp") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  f(wal_path, checkpoint_path)
}

///|
async test "worker recovers WAL and suppresses a resent batch" {
  with_worker_recovery_paths <| ((wal_path, checkpoint_path) => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let addr = server.addr
    let mut handler_calls = 0
    let handler : (@protocol.EventBatch) -> Unit raise = batch => {
      ignore(batch)
      handler_calls += 1
    }
    let batch = recovery_batch()

    @async_runtime.with_task_group <| group => {
      group.spawn_bg() <| () => {
        let (connection, _) = server.accept()
        defer connection.close()
        let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-1")
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "session-first",
          assignment~,
        )
        let resume_frame = read_frame(connection)
        coordinator.accept_frame(resume_frame)
        let progress = read_frame(connection)
        coordinator.accept_frame(progress)
        let credit = read_frame(connection)
        coordinator.accept_frame(credit)
        coordinator_send_batch(
          connection,
          coordinator,
          outbound,
          batch,
          batch.to_payload().length().to_int64(),
        )
        let persisted = read_frame(connection)
        coordinator.accept_frame(persisted)
        let applied = read_frame(connection)
        coordinator.accept_frame(applied)
        let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
        write_frame(connection, drain)
      }
      let connection = @socket.Tcp::connect(addr)
      let first = run_worker(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "boot-1",
        ["reachability"],
        wal_path,
        handler,
        checkpoint_path~,
      )
      assert_eq(first.replayed_batches(), 0)
      assert_eq(first.live_batches(), 1)
      assert_eq(first.persisted_offset(), 0L)
      assert_eq(first.applied_offset(), 0L)
    }

    @async_runtime.with_task_group <| group => {
      group.spawn_bg() <| () => {
        let (connection, _) = server.accept()
        defer connection.close()
        let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-1")
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "session-second",
          assignment~,
        )
        coordinator.restore_send_cursor(0, 1, 1L)
        let resume_frame = read_frame(connection)
        coordinator.accept_frame(resume_frame)
        assert_eq(coordinator.last_persisted_offset(), 0L)
        let progress = read_frame(connection)
        coordinator.accept_frame(progress)
        assert_eq(coordinator.last_applied_offset(), 0L)
        let credit = read_frame(connection)
        coordinator.accept_frame(credit)
        let duplicate_frame = outbound.build(
          @protocol.FrameKind::EventBatch,
          batch.to_payload(),
        )
        write_frame(connection, duplicate_frame)
        let duplicate_ack = read_frame(connection)
        coordinator.accept_frame(duplicate_ack)
        let duplicate_progress = read_frame(connection)
        coordinator.accept_frame(duplicate_progress)
        assert_eq(coordinator.last_persisted_offset(), 0L)
        assert_eq(coordinator.last_applied_offset(), 0L)
        let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
        write_frame(connection, drain)
      }
      let connection = @socket.Tcp::connect(addr)
      let second = run_worker(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "boot-1",
        ["reachability"],
        wal_path,
        handler,
        checkpoint_path~,
      )
      assert_eq(second.replayed_batches(), 1)
      assert_eq(second.live_batches(), 0)
      assert_eq(second.persisted_offset(), 0L)
      assert_eq(second.applied_offset(), 0L)
    }
    assert_eq(handler_calls, 2)
    guard Checkpoint::load(checkpoint_path) is Some(checkpoint) else {
      fail("worker should leave a checkpoint")
    }
    assert_eq(checkpoint.partition(), 0)
    assert_eq(checkpoint.epoch(), 1)
    assert_eq(checkpoint.persisted_offset(), 0L)
    assert_eq(checkpoint.applied_offset(), 0L)

    let conflict_server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer conflict_server.close()
    let conflict_addr = conflict_server.addr
    @async_runtime.with_task_group <| group => {
      group.spawn_bg(allow_failure=true) <| () => {
        let (connection, _) = conflict_server.accept()
        defer connection.close()
        let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-1")
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "session-conflict",
          assignment~,
        )
        coordinator.restore_send_cursor(0, 1, 1L)
        let resume_frame = read_frame(connection)
        coordinator.accept_frame(resume_frame)
        let progress_frame = read_frame(connection)
        coordinator.accept_frame(progress_frame)
        let credit_frame = read_frame(connection)
        coordinator.accept_frame(credit_frame)
        let conflict = conflicting_recovery_batch()
        let conflict_frame = outbound.build(
          @protocol.FrameKind::EventBatch,
          conflict.to_payload(),
        )
        write_frame(connection, conflict_frame)
      }
      let connection = @socket.Tcp::connect(conflict_addr)
      let no_op : (@protocol.EventBatch) -> Unit raise = _ => ()
      try
        run_worker(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "boot-1",
          ["reachability"],
          wal_path,
          no_op,
          checkpoint_path~,
        )
      catch {
        FrameIoError::Handler(message) =>
          assert_eq(message, "resent batch conflicts with the recovered WAL")
        error => fail("unexpected error: \{@debug.to_string(error)}")
      } noraise {
        _ => fail("expected conflicting recovered batch")
      }
    }
  })
}

///|
async test "worker persists an empty recovery checkpoint at offset minus one" {
  with_worker_recovery_paths <| ((wal_path, checkpoint_path) => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let addr = server.addr
    @async_runtime.with_task_group <| group => {
      group.spawn_bg() <| () => {
        let (connection, _) = server.accept()
        let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-1")
        ignore(
          run_coordinator(
            connection,
            "cluster-a",
            "coordinator",
            "worker-a",
            "session-empty",
            assignment,
            [],
          ),
        )
      }
      let connection = @socket.Tcp::connect(addr)
      let no_op : (@protocol.EventBatch) -> Unit raise = _ => ()
      let summary = run_worker(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "boot-1",
        ["reachability"],
        wal_path,
        no_op,
        checkpoint_path~,
      )
      assert_eq(summary.replayed_batches(), 0)
      assert_eq(summary.live_batches(), 0)
      assert_eq(summary.persisted_offset(), -1L)
      assert_eq(summary.applied_offset(), -1L)
    }
    guard Checkpoint::load(checkpoint_path) is Some(checkpoint) else {
      fail("empty worker should leave a checkpoint")
    }
    assert_eq(checkpoint.wal_offset(), 0L)
    assert_eq(checkpoint.persisted_offset(), -1L)
    assert_eq(checkpoint.applied_offset(), -1L)
  })
}

///|
async test "worker accepts a WAL suffix after the last atomic checkpoint" {
  with_worker_recovery_paths <| ((wal_path, checkpoint_path) => {
    let first = recovery_batch()
    let second = second_recovery_batch()
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let addr = server.addr
    @async_runtime.with_task_group <| group => {
      let first_coordinator_task = group.spawn(allow_failure=true) <| () => {
        let (connection, _) = server.accept()
        defer connection.close()
        let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-1")
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "checkpoint-first",
          assignment~,
        )
        let resume_frame = read_frame(connection)
        assert_eq(
          @protocol.Resume::from_payload(resume_frame.payload()).persisted_offset(),
          -1L,
        )
        coordinator.accept_frame(resume_frame)
        let progress = read_frame(connection)
        coordinator.accept_frame(progress)
        let credit = read_frame(connection)
        coordinator.accept_frame(credit)
        coordinator_send_batch(
          connection,
          coordinator,
          outbound,
          first,
          first.to_payload().length().to_int64(),
        )
        let ack = read_frame(connection)
        coordinator.accept_frame(ack) catch {
          error => fail("checkpoint-first ack: \{@debug.to_string(error)}")
        }
        let applied = read_frame(connection)
        coordinator.accept_frame(applied) catch {
          error => fail("checkpoint-first applied: \{@debug.to_string(error)}")
        }
        let replenished = read_frame(connection)
        coordinator.accept_frame(replenished) catch {
          error =>
            fail("checkpoint-first replenished: \{@debug.to_string(error)}")
        }
        let drain_frame = outbound.build(@protocol.FrameKind::Drain, "{}")
        write_frame(connection, drain_frame)
      }
      let worker_task = group.spawn(allow_failure=true) <| () => {
        let connection = @socket.Tcp::connect(addr)
        let no_op : (@protocol.EventBatch) -> Unit raise = _ => ()
        run_worker(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "boot-1",
          [],
          wal_path,
          no_op,
          checkpoint_path~,
        )
      }
      first_coordinator_task.wait() catch {
        error => fail("first suffix coordinator: \{@debug.to_string(error)}")
      }
      let summary = worker_task.wait() catch {
        error => fail("first suffix worker: \{@debug.to_string(error)}")
      }
      assert_eq(summary.live_batches(), 1)
    }
    let wal = Wal::open(wal_path)
    ignore(wal.0.append(@utf8.encode(second.to_payload())))
    wal.0.close()
    assert_true(@fs.exists(checkpoint_path))
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let addr = server.addr
    @async_runtime.with_task_group <| group => {
      let coordinator_task = group.spawn(allow_failure=true) <| () => {
        let (connection, _) = server.accept()
        defer connection.close()
        let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-1")
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "checkpoint-second",
          assignment~,
        )
        coordinator.restore_send_cursor(0, 1, 2L)
        let resume_frame = read_frame(connection)
        coordinator.accept_frame(resume_frame)
        assert_eq(
          @protocol.Resume::from_payload(resume_frame.payload()).persisted_offset(),
          1L,
        )
        let progress = read_frame(connection)
        coordinator.accept_frame(progress)
        let credit = read_frame(connection)
        coordinator.accept_frame(credit)
        let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
        write_frame(connection, drain)
      }
      let worker_task = group.spawn(allow_failure=true) <| () => {
        let connection = @socket.Tcp::connect(addr)
        let no_op : (@protocol.EventBatch) -> Unit raise = _ => ()
        run_worker(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "boot-1",
          [],
          wal_path,
          no_op,
          checkpoint_path~,
        )
      }
      coordinator_task.wait() catch {
        error => fail("second suffix coordinator: \{@debug.to_string(error)}")
      }
      let summary = worker_task.wait() catch {
        error => fail("second suffix worker: \{@debug.to_string(error)}")
      }
      assert_eq(summary.replayed_batches(), 2)
      assert_eq(summary.live_batches(), 0)
      assert_eq(summary.applied_offset(), 1L)
    }
  })
}

///|
async test "worker rejects causal identity conflict before WAL append" {
  with_worker_recovery_paths <| ((wal_path, checkpoint_path) => {
    let first = recovery_batch()
    let conflict = conflicting_payload_batch()
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
        let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-1")
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "causal-conflict",
          assignment~,
        )
        let resume_frame = read_frame(connection)
        coordinator.accept_frame(resume_frame)
        let progress = read_frame(connection)
        coordinator.accept_frame(progress)
        let credit = read_frame(connection)
        coordinator.accept_frame(credit)
        coordinator_send_batch(
          connection,
          coordinator,
          outbound,
          first,
          first.to_payload().length().to_int64(),
        )
        ignore(read_frame(connection))
        ignore(read_frame(connection))
        ignore(read_frame(connection))
        coordinator_send_batch(
          connection,
          coordinator,
          outbound,
          conflict,
          conflict.to_payload().length().to_int64(),
        )
      }
      let connection = @socket.Tcp::connect(addr)
      let no_op : (@protocol.EventBatch) -> Unit raise = _ => ()
      try
        run_worker(
          connection,
          "cluster-a",
          "coordinator",
          "worker-a",
          "boot-1",
          [],
          wal_path,
          no_op,
          checkpoint_path~,
        )
      catch {
        FrameIoError::Handler(_) => ()
        _ => fail("expected causal identity conflict")
      } noraise {
        _ => fail("expected causal identity conflict")
      }
    }
    let (wal, replay) = Wal::open(wal_path)
    assert_eq(replay.records().length(), 1)
    wal.close()
  })
}

///|
async test "worker emits heartbeats while the connection is idle" {
  let directory = @fs.tmpdir(prefix="moonweave-heartbeat")
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
    group.spawn_bg() <| () => {
      let (connection, _) = server.accept()
      defer connection.close()
      let assignment = @protocol.AssignmentSpec::new(0, 2, "boot-1")
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "session-heartbeat",
        assignment~,
      )
      let resume_frame = read_frame(connection)
      coordinator.accept_frame(resume_frame)
      let progress_frame = read_frame(connection)
      coordinator.accept_frame(progress_frame)
      let credit_frame = read_frame(connection)
      coordinator.accept_frame(credit_frame)
      let heartbeat_frame = read_frame(connection)
      coordinator.accept_frame(heartbeat_frame)
      assert_eq(coordinator.last_persisted_offset(), -1L)
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
      write_frame(connection, drain)
    }
    let connection = @socket.Tcp::connect(addr)
    let no_op : (@protocol.EventBatch) -> Unit raise = _ => ()
    let summary = run_worker(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-1",
      ["heartbeat"],
      wal_path,
      no_op,
      heartbeat_interval_ms=5,
    )
    assert_eq(summary.live_batches(), 0)
    assert_eq(summary.applied_offset(), -1L)
  }
}
