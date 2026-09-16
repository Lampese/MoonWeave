///|
async fn wait_live_watermark(
  path : String,
  timestamp : Int64,
) -> @runtime.Checkpoint {
  @async_runtime.with_timeout(5000, () => {
    for ;; {
      match @runtime.Checkpoint::load(path) {
        Some(checkpoint) if checkpoint.watermark() >= timestamp =>
          return checkpoint
        _ => @async_runtime.sleep(1)
      }
    }
  })
}

///|
async test "remote windows emit bounded results while Running and retain late counts" {
  let root = @fs.tmpdir(prefix="moonweave-live-window")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let job = JobDefinition::new("live-window", OperatorRef::keyed_sum())
      .with_workers(["worker"])
      .with_event_time(@task.TaskEventTimeConfig::new(window_size_ms=10L))
      .with_sink(SinkSpec::durable("windows"))
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
        .submission_with_id("window-submit")
        .run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
      defer handle.close()
      let first = Event::keyed_sum(@causal.EventId::new("sales", 0L), "a", 3L).with_event_time(
        2L,
      )
      let second = Event::keyed_sum(@causal.EventId::new("sales", 1L), "a", 4L).with_event_time(
        7L,
      )
      let next = Event::keyed_sum(@causal.EventId::new("sales", 2L), "a", 8L).with_event_time(
        12L,
      )
      ignore(handle.input([first, second, next]))
      let saved = wait_live_watermark(root + "/worker/worker-p0.json", 12L)
      let snapshot : @task.TaskSnapshot = @json.from_json(
        saved.operator_state().unwrap(),
      )
      assert_eq(snapshot.emitted_outputs(), 1)
      assert_eq(handle.refresh().phase(), JobRunPhase::Running)
      let late = Event::keyed_sum(@causal.EventId::new("sales", 3L), "a", 99L).with_event_time(
        3L,
      )
      ignore(handle.input([first, late]))
      let saved = wait_live_checkpoint(root + "/worker/worker-p0.json", 3L)
      let snapshot : @task.TaskSnapshot = @json.from_json(
        saved.operator_state().unwrap(),
      )
      assert_eq(snapshot.late_events(), 1)
      assert_eq(snapshot.emitted_outputs(), 1)
      assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
      ignore(runner.wait())
      ignore(worker.wait())
      let log = @runtime.TaskOutputLog::open(root + "/worker/output-p0.wal")
      defer log.close()
      let records = log.records_for(snapshot.task_id())
      assert_eq(records.map(record => record.output()), [
        @task.TaskOutput::WindowedSum({
          key: "a",
          start_ms: 0L,
          end_ms: 10L,
          total: 7L,
        }),
        @task.TaskOutput::WindowedSum({
          key: "a",
          start_ms: 10L,
          end_ms: 20L,
          total: 8L,
        }),
      ])
    }
  })
}

///|
async test "remote interval join coordinates independent inputs and emits before drain" {
  let root = @fs.tmpdir(prefix="moonweave-live-join")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let job = JobDefinition::new("live-join", OperatorRef::keyed_sum())
      .with_workers(["worker"])
      .with_event_time(
        @task.TaskEventTimeConfig::new(join_before_ms=2L, join_after_ms=3L),
      )
      .with_sink(SinkSpec::durable("pairs"))
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
        .submission_with_id("join-submit")
        .run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
      defer handle.close()
      let left = Event::new(
        @causal.EventId::new("orders", 0L),
        [],
        "a",
        "order",
      ).with_event_time(10L, input_id="left")
      let right = Event::new(
        @causal.EventId::new("payments", 0L),
        [],
        "a",
        "payment",
      ).with_event_time(12L, input_id="right")
      ignore(handle.input([left]))
      let saved = wait_live_checkpoint(root + "/worker/worker-p0.json", 0L)
      assert_eq(saved.watermark(), -1L)
      ignore(handle.input([right]))
      let saved = wait_live_watermark(root + "/worker/worker-p0.json", 10L)
      let snapshot : @task.TaskSnapshot = @json.from_json(
        saved.operator_state().unwrap(),
      )
      assert_eq(snapshot.emitted_outputs(), 1)
      assert_eq(handle.refresh().phase(), JobRunPhase::Running)
      ignore(handle.input([left, right]))
      assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
      ignore(runner.wait())
      ignore(worker.wait())
      let log = @runtime.TaskOutputLog::open(root + "/worker/output-p0.wal")
      defer log.close()
      assert_eq(
        log.records_for(snapshot.task_id()).map(record => record.output()),
        [
          @task.TaskOutput::Custom(
            Json::object({ "left": "order", "right": "payment" }),
          ),
        ],
      )
    }
  })
}

///|
async test "two external file inputs advance independently and preserve join timestamps" {
  let root = @fs.tmpdir(prefix="moonweave-named-sources")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let left_path = root + "/orders.jsonl"
    let right_path = root + "/payments.jsonl"
    let left = Event::new(@causal.EventId::new("orders", 0L), [], "a", "order").with_event_time(
      10L,
    )
    let right = Event::new(
      @causal.EventId::new("payments", 0L),
      [],
      "a",
      "payment",
    ).with_event_time(12L)
    @fs.write_file(left_path, @json.to_json(left).stringify() + "\n")
    @fs.write_file(right_path, "")
    let job = JobDefinition::new("named-join", OperatorRef::keyed_sum())
      .with_workers(["worker"])
      .with_event_time(
        @task.TaskEventTimeConfig::new(join_before_ms=2L, join_after_ms=3L),
      )
      .with_source(
        SourceSpec::inputs([
          NamedSource::new("left", SourceSpec::json_lines(left_path)),
          NamedSource::new("right", SourceSpec::json_lines(right_path)),
        ]),
      )
      .with_sink(SinkSpec::durable("pairs"))
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
        .submission_with_id("named-submit")
        .run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
      defer handle.close()
      let saved = wait_live_checkpoint(root + "/worker/worker-p0.json", 0L)
      assert_eq(saved.watermark(), -1L)
      assert_true(handle.pause().is_paused())
      @fs.write_file(
        right_path,
        @json.to_json(right).stringify() + "\n",
        create_mode=@fs.CreateOrTruncate,
      )
      @async_runtime.sleep(30)
      assert_true(handle.refresh().is_paused())
      assert_false(handle.resume().is_paused())
      let saved = wait_live_watermark(root + "/worker/worker-p0.json", 10L)
      let snapshot : @task.TaskSnapshot = @json.from_json(
        saved.operator_state().unwrap(),
      )
      assert_eq(snapshot.emitted_outputs(), 1)
      assert_eq(handle.refresh().phase(), JobRunPhase::Running)
      assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
      ignore(runner.wait())
      ignore(worker.wait())
      let log = @runtime.TaskOutputLog::open(root + "/worker/output-p0.wal")
      defer log.close()
      assert_eq(
        log.records_for(snapshot.task_id()).map(record => record.output()),
        [
          @task.TaskOutput::Custom(
            Json::object({ "left": "order", "right": "payment" }),
          ),
        ],
      )
    }
    let left_source = JsonLinesSource::open(left_path)
    defer left_source.close()
    let right_source = JsonLinesSource::open(right_path)
    defer right_source.close()
    assert_eq(left_source.offset(), 1L)
    assert_eq(right_source.offset(), 1L)
  })
}

///|
test "local drain returns final windows once" {
  let run = JobDefinition::new("local-final-window", OperatorRef::keyed_sum())
    .with_event_time(@task.TaskEventTimeConfig::new(window_size_ms=10L))
    .run_local()
  assert_eq(
    run.submit([
      Event::keyed_sum(@causal.EventId::new("sales", 0L), "a", 2L).with_event_time(
        2L,
      ),
    ]),
    [],
  )
  assert_eq(run.drain(), [
    Output::WindowedSum({ key: "a", start_ms: 0L, end_ms: 10L, total: 2L, }),
  ])
  assert_eq(run.drain(), [])
}
