///|
fn test_graph(root : String) -> CompiledJob raise ApiError {
  let workers = ["a", "b"]
  let map = JobDefinition::transform("map", [
      GraphTransform::Multiply("value", 2L),
      KeyBy("account"),
    ])
    .with_partition_count(2)
    .with_workers(workers)
  let branch = JobDefinition::transform("branch", [
      GraphTransform::KeepEqual("enabled", true),
    ])
    .with_partition_count(2)
    .with_workers(workers)
  let sum = JobDefinition::new("sum", OperatorRef::keyed_sum())
    .with_partition_count(2)
    .with_workers(workers)
    .with_sink(SinkSpec::json_lines(root + "/out.jsonl"))
  JobDefinition::dataflow(
    "graph",
    GraphDefinition::new(
      [
        GraphStage::new("map", map),
        GraphStage::new("left", branch),
        GraphStage::new("right", branch),
        GraphStage::new("sum", sum),
      ],
      [
        GraphEdge::new("map", "left"),
        GraphEdge::new("map", "right"),
        GraphEdge::new("left", "sum"),
        GraphEdge::new("right", "sum"),
      ],
    ),
  ).compile()
}

///|
test "graph compilation is canonical and rejects cycles" {
  let job = test_graph("/tmp/graph-unused")
  assert_eq(job.submission().compile().plan_digest(), job.plan_digest())
  let stage = GraphStage::new("self", JobDefinition::transform("map", []))
  let result = try? JobDefinition::dataflow(
    "cycle",
    GraphDefinition::new([stage], [GraphEdge::new("self", "self")]),
  ).compile()
  assert_true(result is Err(_))
}

///|
async test "distributed graph rekeys branches merges and preserves filtered causal parents" {
  let root = @fs.tmpdir(prefix="moonweave-graph")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(30000, () => {
    let job = test_graph(root)
    let service = CoordinatorService::bind(
      CoordinatorNodeConfig::new(
        "graph-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        root + "/coordinator",
        [WorkerSlot::new("a", "boot-a"), WorkerSlot::new("b", "boot-b")],
      ),
      DriverControlConfig::new(
        "graph-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
    )
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { service.run() }
      defer service.close()
      let handle = job
        .submission_with_id("graph-submit")
        .run_remote(
          DriverConfig::new(
            "graph-cluster",
            "coordinator",
            "graph-driver",
            service.control_endpoint(),
          ),
        )
      defer handle.close()
      let tasks = ["a", "b"].map(worker => {
        group.spawn() <| () => {
          Ok(
            job.run_graph_worker(
              WorkerNodeConfig::new_auto(
                "graph-cluster",
                "coordinator",
                service.data_endpoint(),
                worker,
                "boot-" + worker,
                root + "/" + worker,
              ),
              service.control_endpoint(),
            ),
          ) catch {
            error => {
              println("Worker " + worker + ": " + @debug.to_string(error))
              Err(@debug.to_string(error))
            }
          }
        }
      })
      let parent = Event::new(@causal.EventId::new("orders", 0L), [], "x", {
        "value": "100",
        "account": "a",
        "enabled": false,
      })
      let child = Event::new(
        @causal.EventId::new("orders", 1L),
        [parent.id()],
        "y",
        { "value": "3", "account": "b", "enabled": true },
      )
      let last = Event::new(
        @causal.EventId::new("orders", 2L),
        [child.id()],
        "z",
        { "value": "4", "account": "a", "enabled": true },
      )
      assert_eq(
        handle.input([child, last, parent, child]).accepted_events(),
        3L,
      )
      let status = handle.drain()
      guard status.phase() == Succeeded else {
        fail(status.failure().unwrap_or("graph drain failed"))
      }
      for task in tasks {
        match task.wait() {
          Ok(summary) => {
            assert_eq(summary.completed_tasks(), 4)
            assert_eq(summary.failed_tasks(), 0)
          }
          Err(reason) => fail(reason)
        }
      }
      let actual : Array[(String, Int64)] = []
      for line in @fs.read_file(root + "/out.jsonl").text().split("\n") {
        if line.is_empty() {
          continue
        }
        let record : @runtime.TaskOutputRecord = @json.from_json(
          @json.parse(line),
        )
        guard record.output() is @task.TaskOutput::KeyedSum(value) else {
          fail("unexpected graph output")
        }
        actual.push((value.key(), value.total()))
      }
      actual.sort()
      assert_eq(actual, [("a", 8L), ("a", 16L), ("b", 6L), ("b", 12L)])
      service.close()
      runner.wait()
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
}
