///|
async fn bind_live_driver(
  directory : String,
  job_id : String,
) -> DriverCoordinator {
  let store = DriverStore::open(directory + "/driver.wal")
  DriverCoordinator::bind(
    CoordinatorNodeConfig::new(
      job_id,
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
      directory,
      [WorkerSlot::new("worker", "boot-worker")],
    ),
    DriverControlConfig::new(
      job_id,
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
    ),
    store,
  )
}

///|
async fn wait_live_checkpoint(
  path : String,
  offset : Int64,
) -> @runtime.Checkpoint {
  @async_runtime.with_timeout(5000, () => {
    for ;; {
      match @runtime.Checkpoint::load(path) {
        Some(checkpoint) if checkpoint.applied_offset() >= offset =>
          return checkpoint
        _ => @async_runtime.sleep(1)
      }
    }
  })
}

///|
async test "remote inputs compute before drain and retries do not add totals twice" {
  let root = @fs.tmpdir(prefix="moonweave-driver-live")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let job = JobDefinition::new("live-keyed-sum", OperatorRef::keyed_sum())
      .with_workers(["worker"])
      .with_sink(SinkSpec::durable("totals"))
      .compile()
    let coordinator = bind_live_driver(root, job.job_id())
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { coordinator.run() }
      let worker = group.spawn() <| () => {
        job.run_worker_node(
          WorkerNodeConfig::new_auto(
            job.job_id(),
            "coordinator",
            coordinator.data_endpoint(),
            "worker",
            "boot-worker",
            root + "/worker",
          ),
        )
      }
      let handle = job
        .submission_with_id("live-submit")
        .run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
      defer handle.close()
      let first = Event::keyed_sum(
        @causal.EventId::new("orders", 0),
        "store",
        12L,
      )
      let second = Event::keyed_sum(
        @causal.EventId::new("orders", 1),
        "store",
        8L,
      )
      assert_eq(handle.input([first]).accepted_events(), 1L)
      let checkpoint = wait_live_checkpoint(root + "/worker/worker-p0.json", 0L)
      assert_eq(checkpoint.state_schema(), "keyed-sum-state:v1")
      assert_eq(handle.refresh().phase(), JobRunPhase::Running)
      assert_eq(handle.input([first, second, second]).accepted_events(), 2L)
      let checkpoint = wait_live_checkpoint(root + "/worker/worker-p0.json", 1L)
      let snapshot : @task.TaskSnapshot = @json.from_json(
        checkpoint.operator_state().unwrap(),
      )
      assert_eq(snapshot.events().length(), 2)
      assert_eq(snapshot.emitted_outputs(), 2)
      assert_eq(handle.refresh().phase(), JobRunPhase::Running)
      assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
      let result = runner.wait()
      assert_eq(result.execution().unwrap().completed_partitions(), 1)
      assert_eq(worker.wait().live_batches(), 2)
      let outputs = @runtime.TaskOutputLog::open(root + "/worker/output-p0.wal")
      defer outputs.close()
      assert_eq(outputs.records_for(snapshot.task_id()).length(), 2)
    }
  })
}

///|
async test "remote JSONL sink reports retention proof before source compaction" {
  let root = @fs.tmpdir(prefix="moonweave-driver-live-jsonl")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let sink_path = root + "/outputs.jsonl"
    let job = JobDefinition::new("live-jsonl", OperatorRef::reachability())
      .with_workers(["worker"])
      .with_sink(SinkSpec::json_lines(sink_path))
      .compile()
    let coordinator = bind_live_driver(root, job.job_id())
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { coordinator.run() }
      let worker = group.spawn() <| () => {
        job.run_worker_node(
          WorkerNodeConfig::new_auto(
            job.job_id(),
            "coordinator",
            coordinator.data_endpoint(),
            "worker",
            "boot-worker",
            root + "/worker",
          ),
        )
      }
      let handle = job
        .submission_with_id("live-jsonl-submit")
        .run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
      defer handle.close()
      let event = Event::new(
        @causal.EventId::new("live-jsonl-event", 0),
        [],
        "account",
        Json::object({
          "source": Json::string("A"),
          "target": Json::string("B"),
        }),
      )
      assert_eq(handle.input([event]).accepted_events(), 1L)
      assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
      assert_true(@fs.read_file(sink_path).text().contains("A"))
      assert_true(@fs.read_file(sink_path).text().contains("B"))
      let source = @runtime.SourceLog::open(root + "/source.wal")
      assert_eq(source.event_count(), 0)
      assert_eq(source.next_offset(0), Some(1L))
      source.close()
      let output = @runtime.TaskOutputLog::open(root + "/worker/output-p0.wal")
      assert_eq(output.records().length(), 0)
      assert_eq(output.retained_from("live-jsonl/dynamic/worker"), 1L)
      output.close()
      assert_eq(runner.wait().status().phase(), JobRunPhase::Succeeded)
      assert_eq(worker.wait().live_batches(), 1)
    }
  })
}

///|
async test "Driver cancellation interrupts drain while no Worker has connected" {
  let root = @fs.tmpdir(prefix="moonweave-driver-cancel")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(5000, () => {
    let job = JobDefinition::new("cancel-live", OperatorRef::keyed_sum())
      .with_workers(["worker"])
      .compile()
    let submission = job.submission_with_id("cancel-submit")
    let coordinator = bind_live_driver(root, job.job_id())
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { coordinator.run() }
      let client = submission.run_remote(
        DriverConfig::new(
          job.job_id(),
          "coordinator",
          "driver",
          coordinator.control_endpoint(),
        ),
      )
      defer client.close()
      let other = submission.run_remote(
        DriverConfig::new(
          job.job_id(),
          "coordinator",
          "canceller",
          coordinator.control_endpoint(),
        ),
      )
      defer other.close()
      let draining = group.spawn() <| () => { client.drain() }
      while other.refresh().phase() != JobRunPhase::Draining {
        @async_runtime.sleep(1)
      }
      assert_eq(other.cancel().phase(), JobRunPhase::Cancelled)
      assert_eq(draining.wait().phase(), JobRunPhase::Cancelled)
      assert_eq(runner.wait().status().phase(), JobRunPhase::Cancelled)
    }
    let restored = DriverStore::open(root + "/driver.wal")
    defer restored.close()
    assert_eq(restored.phase(), JobRunPhase::Cancelled)
  })
}

///|
async test "Coordinator restart continues a running source prefix without replaying committed output" {
  let root = @fs.tmpdir(prefix="moonweave-driver-live-restart")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let job = JobDefinition::new("running-restart", OperatorRef::keyed_sum())
      .with_workers(["worker"])
      .with_sink(SinkSpec::durable("totals"))
      .compile()
    let submission = job.submission_with_id("restart-submit")
    let first = Event::keyed_sum(@causal.EventId::new("orders", 0), "store", 7L)
    for round in 0..<2 {
      let coordinator = bind_live_driver(root, job.job_id())
      @async_runtime.with_task_group <| group => {
        let runner = group.spawn(allow_failure=true) <| () => {
          Ok(coordinator.run()) catch {
            error => Err(@debug.to_string(error))
          }
        }
        let worker = group.spawn(allow_failure=true) <| () => {
          Ok(
            job.run_worker_node(
              WorkerNodeConfig::new_auto(
                job.job_id(),
                "coordinator",
                coordinator.data_endpoint(),
                "worker",
                "boot-worker",
                root + "/worker",
              ),
            ),
          ) catch {
            error => Err(@debug.to_string(error))
          }
        }
        let handle = submission.run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
        defer handle.close()
        assert_eq(handle.input([first]).accepted_events(), 1L)
        ignore(wait_live_checkpoint(root + "/worker/worker-p0.json", 0L))
        if round == 0 {
          coordinator.close()
          assert_true(runner.wait() is Err(_))
          assert_true(worker.wait() is Err(_))
        } else {
          let second = Event::keyed_sum(
            @causal.EventId::new("orders", 1),
            "store",
            3L,
          )
          assert_eq(handle.input([second]).accepted_events(), 2L)
          ignore(wait_live_checkpoint(root + "/worker/worker-p0.json", 1L))
          assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
          guard runner.wait() is Ok(summary) else {
            fail("restart did not finish")
          }
          assert_eq(summary.execution().unwrap().sent_batches(), 1)
          guard worker.wait() is Ok(summary) else {
            fail("Worker restart failed")
          }
          assert_eq(summary.replayed_batches(), 0)
          assert_eq(summary.live_batches(), 1)
        }
      }
    }
  })
}
