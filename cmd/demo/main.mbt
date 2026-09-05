///|
async fn main {
  let directory = @fs.tmpdir(prefix="moonweave-demo")
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

  let root_id = @causal.EventId::new("auth", 0)
  let child_id = @causal.EventId::new("admin", 0)
  let root = @protocol.EventRecord::new(
    root_id,
    [],
    "user-42",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let child = @protocol.EventRecord::new(
    child_id,
    [root_id],
    "user-42",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  let control = @control.CoordinatorState::new("demo-cluster", "coordinator")
  let incarnation = @protocol.Incarnation::new("demo-1")
  ignore(control.register_worker("worker-1", incarnation, ["reachability"]))
  let assignment = control.assign(0, "worker-1")
  let engine = @reachability.Engine::new()
  let query = @reachability.Query::new("reach-A-C", "A", "C", [0])
  engine.set_query(query)
  let batches = [
    // Transport offsets remain contiguous, while event causality is released
    // only when the root arrives in the second batch.
    @protocol.EventBatch::new(0, assignment.epoch(), 0L, [child]),
    @protocol.EventBatch::new(0, assignment.epoch(), 1L, [root]),
  ]
  let probe = @protocol.Probe::new(
    "reach-A-C",
    assignment.partition(),
    assignment.epoch(),
    1L,
  )
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  println("moonweave demo listening on \{addr}")
  let mut probe_verified = false
  let mut probe_reply : @protocol.ProbeReply? = None
  @async.with_task_group <| group => {
    group.spawn_bg() <| () => {
      let (connection, _) = server.accept()
      let summary = @transport.run_managed_coordinator(
        connection,
        control,
        "worker-1",
        "demo-session",
        0,
        batches,
        probe~,
      )
      probe_verified = summary.probe_reply() is Some(_)
      probe_reply = summary.probe_reply()
      println(
        "coordinator sent=\{summary.sent_batches()} applied=\{summary.applied_offset()} probe_verified=\{probe_verified}",
      )
    }
    let connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = batch => {
      for record in batch.events() {
        let facts = engine.apply_json_event(
          record.id(),
          record.parents(),
          record.key(),
          record.payload(),
        )
        println(
          "worker admitted=\{engine.event_count()} pending=\{engine.pending_count()} new_facts=\{facts.length()}",
        )
      }
    }
    let summary = @transport.run_worker(
      connection,
      "demo-cluster",
      "coordinator",
      "worker-1",
      "demo-1",
      ["reachability", "causal-frontier"],
      wal_path,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=1024L,
    )
    println(
      "worker replayed=\{summary.replayed_batches()} live=\{summary.live_batches()} applied=\{summary.applied_offset()}",
    )
    guard engine.query() is Some(query_state) else {
      fail("demo query missing")
    }
    println("query before closure=\{@debug.to_string(query_state.status())}")
    guard control.assignment(0) is Some(control_assignment) else {
      fail("demo control assignment missing")
    }
    println(
      "control epoch=\{control_assignment.epoch()} applied=\{control_assignment.applied_offset()}",
    )
  }
  guard engine.query() is Some(query_state) else {
    fail("demo query missing after coordination")
  }
  if probe_verified {
    ignore(query_state.close_partition(0))
  }
  match probe_reply {
    Some(reply) => {
      let certificate = query_state.certificate(reply.frontier_digest(), [
        root_id, child_id,
      ])
      assert_true(certificate.verify(query_state, reply.frontier_digest()))
      println(
        "certificate status=\{@debug.to_string(certificate.status())} result=\{certificate.result_digest()} verified=true",
      )
    }
    None => println("certificate verified=false")
  }
  println(
    "query after seal=\{@debug.to_string(query_state.status())} facts=\{engine.facts().length()}",
  )
}
