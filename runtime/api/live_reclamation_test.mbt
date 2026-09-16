///|
async test "Running job exceeds all retained input budgets through repeated checkpoints then restarts" {
  let root = @fs.tmpdir(prefix="moonweave-live-reclaim")
  defer @fs.rmdir(root, recursive=true)
  let sink_path = root + "/sink.jsonl"
  let job = JobDefinition::new("live-reclaim", OperatorRef::keyed_sum())
    .with_workers(["worker"])
    .with_state_limit(128)
    .with_sink(SinkSpec::json_lines(sink_path))
    .compile()
  @async_runtime.with_timeout(60000, () => {
    for attempt in 0..<2 {
      let store = DriverStore::open(
        root + "/driver.wal",
        maximum_events=32,
        maximum_bytes=65536L,
      )
      let coordinator = DriverCoordinator::bind(
        CoordinatorNodeConfig::new(
          job.job_id(),
          "coordinator",
          NodeEndpoint::new("127.0.0.1", 0),
          root,
          [WorkerSlot::new("worker", "boot-worker")],
        ).with_storage_budget(65536L),
        DriverControlConfig::new(
          job.job_id(),
          "coordinator",
          NodeEndpoint::new("127.0.0.1", 0),
        ),
        store,
      )
      @async_runtime.with_task_group <| group => {
        let runner = group.spawn() <| () => {
          coordinator.run(keep_terminal=true)
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
              ).with_storage_budget(65536L),
            ),
          ) catch {
            error => Err(@debug.to_string(error))
          }
        }
        let handle = job
          .submission_with_id("reclaim-submit")
          .run_remote(
            DriverConfig::new(
              job.job_id(),
              "coordinator",
              "driver",
              coordinator.control_endpoint(),
            ),
          )
        defer handle.close()
        let base = if attempt == 0 { 0 } else { 110 }
        let cycles = if attempt == 0 { 110 } else { 1 }
        if attempt > 0 {
          assert_eq(
            handle
            .input([
              Event::keyed_sum(@causal.EventId::new("input", 0L), "a", 1L),
            ])
            .accepted_events(),
            1760L,
          )
        }
        for cycle in base..<(base + cycles) {
          let events = Array::makei(16, index => {
            Event::keyed_sum(
              @causal.EventId::new("input", (cycle * 16 + index).to_int64()),
              "a",
              1L,
            )
          })
          assert_eq(
            handle.input(events).accepted_events(),
            ((cycle + 1) * 16).to_int64(),
          )
          @async_runtime.with_timeout(5000, () => {
            while store.inputs().length() >= 16 {
              if worker.try_wait() is Some(Err(error)) {
                fail("Worker stopped: " + error)
              }
              if store.phase() != JobRunPhase::Running {
                fail("job stopped: " + @debug.to_string(store.status()))
              }
              @async_runtime.sleep(5)
            }
          })
          assert_eq(handle.refresh().phase(), JobRunPhase::Running)
          assert_true(store.stats().physical_bytes() < 8192L)
          for file in ["source.wal", "job.wal", "coordinator.wal"] {
            assert_true(
              @fs.read_file(root + "/" + file).binary().length() < 65536,
            )
          }
        }
        if attempt > 0 {
          assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
          guard worker.wait() is Ok(_) else { fail("restarted Worker failed") }
        }
        coordinator.close()
        ignore(runner.wait())
        if attempt == 0 {
          ignore(worker.wait())
        }
      }
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
  let unique : Map[Int64, @task.TaskOutput] = Map([])
  for line in @fs.read_file(sink_path).text().split("\n") {
    if line.is_empty() {
      continue
    }
    let record : @runtime.TaskOutputRecord = @json.from_json(@json.parse(line))
    if unique.get(record.sequence()) is Some(previous) {
      assert_eq(previous, record.output())
    }
    unique[record.sequence()] = record.output()
    guard record.output() is @task.TaskOutput::KeyedSum(value) else {
      fail("expected sum")
    }
    assert_eq(value.total(), record.sequence() + 1L)
  }
  assert_eq(unique.length(), 1776)
}

///|
async test "global checkpoints bound sparse identities across two running partitions" {
  let root = @fs.tmpdir(prefix="moonweave-partition-reclaim")
  defer @fs.rmdir(root, recursive=true)
  let job = JobDefinition::new("partition-reclaim", OperatorRef::keyed_sum())
    .with_partition_count(2)
    .with_workers(["a", "b"])
    .with_state_limit(96)
    .with_sink(SinkSpec::json_lines(root + "/sink.jsonl"))
    .compile()
  let first = "a"
  let second = Array::makei(100, index => "key" + index.to_string()).filter(key => {
      job.partition_for_key(key) != job.partition_for_key(first)
    })[0]
  @async_runtime.with_timeout(30000, () => {
    let store = DriverStore::open(
      root + "/driver.wal",
      maximum_events=32,
      maximum_bytes=65536L,
    )
    let coordinator = DriverCoordinator::bind(
      CoordinatorNodeConfig::new(
        job.job_id(),
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        root,
        [WorkerSlot::new("a", "boot-a"), WorkerSlot::new("b", "boot-b")],
      ).with_storage_budget(65536L),
      DriverControlConfig::new(
        job.job_id(),
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
      store,
    )
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { coordinator.run() }
      let workers = ["a", "b"].map(id => {
        group.spawn(allow_failure=true, () => {
          Ok(
            job.run_worker_node(
              WorkerNodeConfig::new_auto(
                job.job_id(),
                "coordinator",
                coordinator.data_endpoint(),
                id,
                "boot-" + id,
                root + "/" + id,
              ).with_storage_budget(65536L),
            ),
          ) catch {
            error => Err(@debug.to_string(error))
          }
        })
      })
      let handle = job
        .submission_with_id("partition-submit")
        .run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
      defer handle.close()
      for cycle in 0..<40 {
        let events = Array::makei(16, index => {
          Event::keyed_sum(
            @causal.EventId::new("interleaved", (cycle * 16 + index).to_int64()),
            if index % 2 == 0 {
              first
            } else {
              second
            },
            1L,
          )
        })
        ignore(handle.input(events))
        @async_runtime.with_timeout(5000, () => {
          while store.inputs().length() >= 16 {
            for worker in workers {
              if worker.try_wait() is Some(Err(reason)) {
                fail(reason)
              }
            }
            if store.phase() != JobRunPhase::Running {
              fail(@debug.to_string(store.status()))
            }
            @async_runtime.sleep(5)
          }
        })
        assert_eq(handle.refresh().phase(), JobRunPhase::Running)
      }
      assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
      for worker in workers {
        guard worker.wait() is Ok(_) else { fail("partition Worker failed") }
      }
      ignore(runner.wait())
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
  let unique : Set[String] = Set([])
  let partitions : Set[Int] = Set([])
  for line in @fs.read_file(root + "/sink.jsonl").text().split("\n") {
    if line.is_empty() {
      continue
    }
    let record : @runtime.TaskOutputRecord = @json.from_json(@json.parse(line))
    unique.add(record.idempotency_key())
    partitions.add(record.partition())
    guard record.output() is @task.TaskOutput::KeyedSum(value) else {
      fail("expected sum")
    }
    assert_eq(value.total(), record.sequence() + 1L)
  }
  assert_eq(unique.length(), 640)
  assert_eq(partitions.length(), 2)
}
