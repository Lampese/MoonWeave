///|
fn live_edge_event(
  sequence : Int,
  parents : ArrayView[@causal.EventId],
  source : String,
  target : String,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new("live-source", sequence),
    parents,
    "graph",
    Json::object({
      "source": Json::string(source),
      "target": Json::string(target),
    }),
  )
}

///|
fn live_key_for_partition(plan : @plan.JobSpec, partition : Int) -> String {
  for index in 0..<10000 {
    let key = "live-key-" + index.to_string()
    if plan.partition_for_key(key) == partition {
      return key
    }
  }
  abort("unable to find a live source partition key")
}

///|
async fn with_live_supervisor_paths(
  f : async (String, String, String, String, String) -> Unit,
) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-live-supervisor")
  let coordinator_journal = directory + "/coordinator.wal"
  let coordinator_checkpoint = directory + "/coordinator.json"
  let job_journal = directory + "/job.wal"
  let job_checkpoint = directory + "/job.json"
  let source_path = directory + "/source.wal"
  defer @fs.rmdir(directory, recursive=true)
  f(
    coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, source_path,
  )
}

///|
async test "live supervisor streams durable source input before drain" {
  with_live_supervisor_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    source_path,
  ) => {
    let plan = @plan.JobSpec::new("live-job", "reachability", 1, ["worker-a"], [])
    let coordinator = CoordinatorStore::open(
      "live-cluster",
      "coordinator",
      coordinator_journal,
      checkpoint_path=coordinator_checkpoint,
    )
    defer coordinator.close()
    let job_store = JobStore::open(
      plan,
      job_journal,
      checkpoint_path=job_checkpoint,
    )
    defer job_store.close()
    let capability = @task.OperatorKind::Reachability.capability()
    let incarnation = @protocol.Incarnation::new("live-worker")
    ignore(coordinator.register_worker("worker-a", incarnation, [capability]))
    let assignment = coordinator.assign(0, "worker-a")
    ignore(
      job_store.assign(
        0,
        "worker-a",
        assignment.epoch(),
        assignment.start_offset(),
      ),
    )
    let live = SourceBackedJob::open(
      source_path,
      plan,
      coordinator,
      job_store,
      required_capability=capability,
    )
    defer live.close()
    live.refresh_work_plans()
    job_store.start()
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let root = live_edge_event(0, [], "A", "B")
    let child = live_edge_event(1, [root.id()], "B", "C")
    let outputs : Array[@task.TaskOutput] = []
    @async_runtime.with_timeout(10000, () => {
      @async_runtime.with_task_group <| group => {
        let supervisor = group.spawn(allow_failure=true) <| () => {
          Ok(run_live_source_supervisor(server, live, poll_interval_ms=1)) catch {
            error => Err(@debug.to_string(error))
          }
        }
        let worker = group.spawn(allow_failure=true) <| () => {
          let task = @task.Task::new(
            "live-job/p0/e0",
            "reachability",
            @task.OperatorKind::Reachability,
            0,
            0,
          )
          let handler : (@task.TaskOutput) -> Unit raise = output => {
            outputs.push(output)
          }
          Ok(
            run_worker_task(
              @socket.Tcp::connect(server.addr),
              "live-cluster",
              "coordinator",
              "worker-a",
              "live-worker",
              [capability],
              source_path + ".worker.wal",
              task,
              handler,
              checkpoint_path=source_path + ".worker.json",
              initial_credit_events=1L,
              initial_credit_bytes=4096L,
            ),
          ) catch {
            error => Err(@debug.to_string(error))
          }
        }
        @async_runtime.sleep(10)
        assert_eq(live.ingest(root), 0L)
        while outputs.length() < 1 {
          @async_runtime.sleep(1)
        }
        assert_eq(job_store.phase(), @control.JobPhase::Running)
        assert_eq(live.ingest(child), 1L)
        while outputs.length() < 3 {
          @async_runtime.sleep(1)
        }
        assert_eq(live.event_count(), 2)
        live.seal()
        try live.ingest(live_edge_event(2, [], "C", "D")) catch {
          FrameIoError::Handler(message) =>
            assert_eq(message, "live source is sealed")
          error => fail(@debug.to_string(error))
        } noraise {
          _ => fail("sealed source accepted input")
        }
        let supervisor_result = supervisor.wait()
        let worker_result = worker.wait()
        guard supervisor_result is Ok(summary) else {
          fail("live supervisor failed")
        }
        guard worker_result is Ok(worker_summary) else {
          fail("live Worker failed")
        }
        assert_eq(summary.completed_partitions(), 1)
        assert_eq(summary.failed_partitions(), 0)
        assert_eq(summary.sent_batches(), 2)
        assert_eq(worker_summary.live_batches(), 2)
      }
    })
    assert_eq(outputs, [
      @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
      @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "C")),
      @task.TaskOutput::Reachable(@reachability.Reachable::new("B", "C")),
    ])
    assert_eq(job_store.phase(), @control.JobPhase::Succeeded)
  })
}

///|
async test "live supervisor continuously routes source-backed exchange deltas" {
  with_live_supervisor_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    source_path,
  ) => {
    let plan = @plan.JobSpec::new(
      "live-exchange-job",
      "reachability",
      2,
      ["worker-a", "worker-b"],
      [@plan.ExchangeRoute::new(0, 1)],
    )
    let coordinator = CoordinatorStore::open(
      "live-exchange-cluster",
      "coordinator",
      coordinator_journal,
      checkpoint_path=coordinator_checkpoint,
    )
    defer coordinator.close()
    let job_store = JobStore::open(
      plan,
      job_journal,
      checkpoint_path=job_checkpoint,
    )
    defer job_store.close()
    let capability = @task.OperatorKind::Reachability.capability()
    let incarnation_a = @protocol.Incarnation::new("live-worker-a")
    let incarnation_b = @protocol.Incarnation::new("live-worker-b")
    ignore(coordinator.register_worker("worker-a", incarnation_a, [capability]))
    ignore(coordinator.register_worker("worker-b", incarnation_b, [capability]))
    let assignment_a = coordinator.assign(0, "worker-a")
    let assignment_b = coordinator.assign(1, "worker-b")
    ignore(
      job_store.assign(
        0,
        "worker-a",
        assignment_a.epoch(),
        assignment_a.start_offset(),
      ),
    )
    ignore(
      job_store.assign(
        1,
        "worker-b",
        assignment_b.epoch(),
        assignment_b.start_offset(),
      ),
    )
    let live = SourceBackedJob::open(
      source_path,
      plan,
      coordinator,
      job_store,
      required_capability=capability,
    )
    defer live.close()
    live.refresh_work_plans()
    job_store.start()
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let root = @protocol.EventRecord::new(
      @causal.EventId::new("live-exchange", 0),
      [],
      live_key_for_partition(plan, 0),
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    let child = @protocol.EventRecord::new(
      @causal.EventId::new("live-exchange", 1),
      [root.id()],
      live_key_for_partition(plan, 1),
      Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
    )
    let outputs : Array[@task.TaskOutput] = []
    @async_runtime.with_timeout(10000, () => {
      @async_runtime.with_task_group <| group => {
        let supervisor = group.spawn(allow_failure=true) <| () => {
          Ok(run_live_source_supervisor(server, live, poll_interval_ms=1)) catch {
            error => Err(@debug.to_string(error))
          }
        }
        let worker_a = group.spawn(allow_failure=true) <| () => {
          let task = @task.Task::new(
            "live-exchange-job/p0/e0",
            "reachability",
            @task.OperatorKind::Reachability,
            0,
            0,
          )
          Ok(
            run_worker_task(
              @socket.Tcp::connect(server.addr),
              "live-exchange-cluster",
              "coordinator",
              "worker-a",
              "live-worker-a",
              [capability],
              source_path + ".worker-a.wal",
              task,
              output => outputs.push(output),
              checkpoint_path=source_path + ".worker-a.json",
              delta_wal_path=source_path + ".worker-a.delta.wal",
              initial_credit_events=1L,
              initial_credit_bytes=4096L,
            ),
          ) catch {
            error => Err(@debug.to_string(error))
          }
        }
        let worker_b = group.spawn(allow_failure=true) <| () => {
          let task = @task.Task::new(
            "live-exchange-job/p1/e0",
            "reachability",
            @task.OperatorKind::Reachability,
            1,
            0,
          )
          Ok(
            run_worker_task(
              @socket.Tcp::connect(server.addr),
              "live-exchange-cluster",
              "coordinator",
              "worker-b",
              "live-worker-b",
              [capability],
              source_path + ".worker-b.wal",
              task,
              output => outputs.push(output),
              checkpoint_path=source_path + ".worker-b.json",
              delta_wal_path=source_path + ".worker-b.delta.wal",
              initial_credit_events=1L,
              initial_credit_bytes=4096L,
            ),
          ) catch {
            error => Err(@debug.to_string(error))
          }
        }
        @async_runtime.sleep(10)
        assert_eq(live.ingest(root), 0L)
        assert_eq(live.ingest(child), 0L)
        live.seal()
        let supervisor_result = supervisor.wait()
        let worker_a_result = worker_a.wait()
        let worker_b_result = worker_b.wait()
        guard supervisor_result is Ok(summary) else {
          fail("live exchange supervisor failed")
        }
        guard summary.completed_partitions() == 2 else {
          fail("live exchange failure: " + @debug.to_string(summary))
        }
        assert_eq(summary.failed_partitions(), 0)
        assert_eq(summary.sent_batches(), 2)
        assert_eq(summary.sent_deltas(), 1)
        assert_true(worker_a_result is Ok(_))
        assert_true(worker_b_result is Ok(_))
      }
    })
    outputs.sort()
    assert_true(
      outputs.contains(
        @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "C")),
      ),
    )
    assert_eq(job_store.phase(), @control.JobPhase::Succeeded)
  })
}
