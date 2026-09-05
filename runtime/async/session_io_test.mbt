///|
async test "real TCP runs Hello Welcome Progress and Drain sessions" {
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
      let assignment = @protocol.AssignmentSpec::new(0, 7, "boot-1")
      let (session, outbound) = coordinator_handshake(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "session-a",
        assignment~,
      )
      let progress_frame = read_frame(connection)
      session.accept_frame(progress_frame)
      assert_eq(session.last_persisted_offset(), -1L)
      assert_eq(session.last_applied_offset(), -1L)
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}")
      write_frame(connection, drain)
    }
    let connection = @socket.Tcp::connect(addr)
    defer connection.close()
    let (session, outbound) = worker_handshake(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-1",
      ["reachability"],
    )
    assert_eq(session.phase(), @protocol.SessionPhase::Running)
    let progress = @protocol.Progress::new(0, 7, -1L, -1L, "frontier--1")
    let progress_frame = outbound.build(
      @protocol.FrameKind::Progress,
      progress.to_payload(),
    )
    write_frame(connection, progress_frame)
    let drain = read_frame(connection)
    session.accept_frame(drain)
    assert_eq(session.phase(), @protocol.SessionPhase::Draining)
    assert_eq(session.next_sequence(), 2L)
  }
}

///|
async test "real TCP carries credit, event batch, persisted ack and progress" {
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
      let (coord_session, coord_outbound) = coordinator_handshake(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "session-data",
        assignment~,
      )
      let credit_frame = read_frame(connection)
      coord_session.accept_frame(credit_frame)
      assert_eq(coord_session.credit_events(), 2L)
      let root = @protocol.EventRecord::new(
        @causal.EventId::new("source", 0),
        [],
        "account-1",
        Json::object({ "edge": Json::string("a->b") }),
      )
      let child = @protocol.EventRecord::new(
        @causal.EventId::new("source", 1),
        [root.id()],
        "account-1",
        Json::object({ "edge": Json::string("b->c") }),
      )
      let batch = @protocol.EventBatch::new(0, 2, 0L, [root, child])
      coordinator_send_batch(
        connection, coord_session, coord_outbound, batch, 20L,
      )
      let ack_frame = read_frame(connection)
      coord_session.accept_frame(ack_frame)
      let progress_frame = read_frame(connection)
      coord_session.accept_frame(progress_frame)
      assert_eq(coord_session.last_persisted_offset(), 1L)
      assert_eq(coord_session.last_applied_offset(), 1L)
    }
    let connection = @socket.Tcp::connect(addr)
    defer connection.close()
    let (worker_session, worker_outbound) = worker_handshake(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-1",
      ["reachability"],
    )
    let credit = @protocol.Credit::new(0, 2, 2L, 20L)
    let credit_frame = worker_outbound.build(
      @protocol.FrameKind::Credit,
      credit.to_payload(),
    )
    write_frame(connection, credit_frame)
    let batch_frame = read_frame(connection)
    worker_session.accept_frame(batch_frame)
    assert_eq(worker_session.last_received_offset(), 1L)
    worker_session.mark_persisted(1L)
    let ack = @protocol.AckPersisted::new(0, 2, 1L)
    let ack_frame = worker_outbound.build(
      @protocol.FrameKind::AckPersisted,
      ack.to_payload(),
    )
    write_frame(connection, ack_frame)
    worker_session.mark_applied(1L)
    let progress = @protocol.Progress::new(0, 2, 1L, 1L, "frontier-1")
    let progress_frame = worker_outbound.build(
      @protocol.FrameKind::Progress,
      progress.to_payload(),
    )
    write_frame(connection, progress_frame)
  }
}

///|
async test "real TCP carries targeted probe, reply and seal" {
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
      let assignment = @protocol.AssignmentSpec::new(0, 5, "boot-1")
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "cluster-a",
        "coordinator",
        "worker-a",
        "session-probe",
        assignment~,
      )
      let initial = read_frame(connection)
      coordinator.accept_frame(initial)
      let probe = @protocol.Probe::new("query-1", 0, 5, -1L)
      coordinator_send_probe(connection, coordinator, outbound, probe)
      let reply_frame = read_frame(connection)
      coordinator.accept_frame(reply_frame)
      let reply = @protocol.ProbeReply::from_payload(reply_frame.payload())
      assert_eq(reply.query_id(), "query-1")
      assert_eq(reply.reached_offset(), -1L)
      let seal = @protocol.Seal::new(
        "query-1",
        0,
        5,
        reply.reached_offset(),
        reply.frontier_digest(),
      )
      coordinator_send_seal(connection, coordinator, outbound, seal)
    }
    let connection = @socket.Tcp::connect(addr)
    defer connection.close()
    let (worker, outbound) = worker_handshake(
      connection,
      "cluster-a",
      "coordinator",
      "worker-a",
      "boot-1",
      ["probe"],
    )
    let progress = @protocol.Progress::new(0, 5, -1L, -1L, "offset:-1")
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::Progress, progress.to_payload()),
    )
    let probe_frame = read_frame(connection)
    worker.accept_frame(probe_frame)
    let probe = @protocol.Probe::from_payload(probe_frame.payload())
    let reply = @protocol.ProbeReply::new(
      probe.query_id(),
      probe.partition(),
      probe.epoch(),
      -1L,
      "offset:-1",
    )
    write_frame(
      connection,
      outbound.build(@protocol.FrameKind::ProbeReply, reply.to_payload()),
    )
    let seal_frame = read_frame(connection)
    worker.accept_frame(seal_frame)
    let seal = @protocol.Seal::from_payload(seal_frame.payload())
    assert_eq(seal.query_id(), "query-1")
    assert_eq(seal.sealed_offset(), -1L)
  }
}
