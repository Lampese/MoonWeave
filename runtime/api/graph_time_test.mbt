///|
async fn graph_window_outputs(path : String) -> Array[(String, Int64, Int64)] {
  if !@fs.exists(path) {
    return []
  }
  let result = []
  for line in @fs.read_file(path).text().split("\n") {
    if line.is_empty() {
      continue
    }
    let record : @runtime.TaskOutputRecord = @json.from_json(@json.parse(line))
    guard record.output() is @task.TaskOutput::WindowedSum(window) else {
      fail("expected window")
    }
    result.push((window.key(), window.start_ms(), window.total()))
  }
  result.sort()
  result
}

///|
async test "graph watermarks cross stage ACKs and recover active windows" {
  let root = @fs.tmpdir(prefix="moonweave-graph-time")
  defer @fs.rmdir(root, recursive=true)
  let output = root + "/out.jsonl"
  let workers = ["a", "b"]
  let map = JobDefinition::transform("map", [])
    .with_partition_count(2)
    .with_workers(workers)
  let window = JobDefinition::new("window", OperatorRef::keyed_sum())
    .with_partition_count(2)
    .with_workers(workers)
    .with_event_time(@task.TaskEventTimeConfig::new(window_size_ms=10L))
    .with_sink(SinkSpec::json_lines(output))
  let job = JobDefinition::dataflow(
    "timed-graph",
    GraphDefinition::new(
      [GraphStage::new("map", map), GraphStage::new("window", window)],
      [GraphEdge::new("map", "window")],
    ),
  ).compile()
  let event = (id : Int64, key : String, time : Int64, value : Int64) => {
    Event::keyed_sum(@causal.EventId::new("timed", id), key, value).with_event_time(
      time,
    )
  }
  let mut progress = "starting"
  @async_runtime.with_timeout(30000, () => {
    for attempt in 0..<2 {
      progress = "attempt \{attempt}: bind"
      let service = CoordinatorService::bind(
        CoordinatorNodeConfig::new(
          "graph-time",
          "coordinator",
          NodeEndpoint::new("127.0.0.1", 0),
          root + "/coordinator",
          [WorkerSlot::new("a", "boot-a"), WorkerSlot::new("b", "boot-b")],
        ),
        DriverControlConfig::new(
          "graph-time",
          "coordinator",
          NodeEndpoint::new("127.0.0.1", 0),
        ),
      )
      @async_runtime.with_task_group <| group => {
        let runner = group.spawn() <| () => { service.run() }
        defer service.close()
        let handle = job
          .submission_with_id("time-submit")
          .run_remote(
            DriverConfig::new(
              "graph-time",
              "coordinator",
              "driver",
              service.control_endpoint(),
            ),
          )
        defer handle.close()
        progress = "attempt \{attempt}: start workers"
        let tasks = workers.map(worker => {
          group.spawn(allow_failure=true) <| () => {
            Ok(
              job.run_graph_worker(
                WorkerNodeConfig::new_auto(
                  "graph-time",
                  "coordinator",
                  service.data_endpoint(),
                  worker,
                  "boot-" + worker,
                  root + "/" + worker,
                ),
                service.control_endpoint(),
              ),
            ) catch {
              error => Err(@debug.to_string(error))
            }
          }
        })
        if attempt == 0 {
          progress = "initial input"
          ignore(
            handle.input([
              event(0L, "x", 1L, 2L),
              event(1L, "x", 4L, 3L),
              event(2L, "y", 5L, 4L),
              event(3L, "x", 15L, 1L),
            ]),
          )
          progress = "initial window outputs"
          @async_runtime.with_timeout(8000, () => {
            while graph_window_outputs(output).length() < 2 {
              for task in tasks {
                if task.try_wait() is Some(Err(reason)) {
                  fail(reason)
                }
              }
              @async_runtime.sleep(5)
            }
          })
          assert_eq(graph_window_outputs(output), [("x", 0L, 5L), ("y", 0L, 4L)])
          assert_eq(handle.refresh().phase(), Running)
          progress = "stop initial coordinator"
          service.close()
          runner.wait()
          progress = "stop initial workers"
          // Managed Workers reconnect after a Coordinator outage. Stop these
          // attempts explicitly before reopening their durable state.
          for task in tasks {
            task.cancel()
          }
          for task in tasks {
            ignore(task.wait())
          }
        } else {
          progress = "recovered input"
          assert_eq(
            handle
            .input([
              event(0L, "x", 1L, 2L),
              event(4L, "x", 2L, 99L),
              event(5L, "y", 25L, 2L),
            ])
            .accepted_events(),
            6L,
          )
          progress = "drain recovered graph"
          let status = handle.drain()
          guard status.phase() == Succeeded else {
            fail(status.failure().unwrap_or("timed graph failed"))
          }
          progress = "wait for recovered workers"
          for task in tasks {
            match task.wait() {
              Ok(_) => ()
              Err(reason) => fail(reason)
            }
          }
          progress = "stop recovered coordinator"
          service.close()
          runner.wait()
        }
      }
    }
    assert_eq(graph_window_outputs(output), [
      ("x", 0L, 5L),
      ("x", 10L, 1L),
      ("y", 0L, 4L),
      ("y", 20L, 2L),
    ])
  }) catch {
    error => fail(progress + ": " + @debug.to_string(error))
  }
}
