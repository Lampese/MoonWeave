///|
fn controlled_graph(root : String, limit : Int) -> CompiledJob raise ApiError {
  let source = JobDefinition::transform("source", [])
    .with_partition_count(4)
    .with_parallelism(2)
    .with_workers(["a", "b"])
    .with_state_limit(limit)
  let sum = JobDefinition::new("sum", OperatorRef::keyed_sum())
    .with_partition_count(4)
    .with_parallelism(2)
    .with_workers(["a", "b"])
    .with_state_limit(limit)
    .with_sink(SinkSpec::json_lines(root + "/out.jsonl"))
  JobDefinition::dataflow(
    "controlled-graph",
    GraphDefinition::new(
      [GraphStage::new("source", source), GraphStage::new("sum", sum)],
      [GraphEdge::new("source", "sum")],
    ),
  ).compile()
}

///|
async fn graph_output_count(path : String) -> Int {
  if !@fs.exists(path) {
    return 0
  }
  @fs.read_file(path)
  .text()
  .split("\n")
  .filter(line => !line.is_empty())
  .count()
}

///|
async test "graph barrier migrates every stage and applies a compatible update to live Workers" {
  let root = @fs.tmpdir(prefix="moonweave-graph-control")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(60000, () => {
    let job = controlled_graph(root, 128)
    let submission = job.submission_with_id("controlled-submit")
    let workers = ["a", "b", "c", "d"]
    let service = CoordinatorService::bind(
      CoordinatorNodeConfig::new(
        "c",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        root + "/coordinator",
        workers.map(id => WorkerSlot::new(id, "boot-" + id)),
      ),
      DriverControlConfig::new(
        "c",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
    )
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { service.run() }
      defer service.close()
      let client = DriverClient::connect(
        DriverConfig::new(
          "c",
          "coordinator",
          "driver",
          service.control_endpoint(),
        ),
      )
      defer client.close()
      assert_eq(client.submit(submission).parallelism(), 2)
      let tasks = workers.map(id => {
        group.spawn() <| () => {
          job.run_graph_worker(
            WorkerNodeConfig::new_auto(
              "c",
              "coordinator",
              service.data_endpoint(),
              id,
              "boot-" + id,
              root + "/" + id,
            ),
            service.control_endpoint(),
          )
        }
      })
      for round in 0..<3 {
        let events = Array::makei(8, index => {
          Event::new(
            @causal.EventId::new("input", (round * 8 + index).to_int64()),
            [],
            "key-" + index.to_string(),
            { "value": "1" },
          )
        })
        assert_eq(
          client.input(events).accepted_events(),
          ((round + 1) * 8).to_int64(),
        )
        while graph_output_count(root + "/out.jsonl") < (round + 1) * 8 {
          @async_runtime.sleep(10)
        }
        if round == 0 {
          let scaled = client.rescale(workers, parallelism=4)
          assert_eq(scaled.parallelism(), 4)
          assert_false(scaled.is_paused())
        } else if round == 1 {
          assert_true(client.pause().is_paused())
          let scaled = client.rescale(["a", "b"], parallelism=2)
          assert_true(scaled.is_paused())
          assert_eq(scaled.parallelism(), 2)
          let candidate = controlled_graph(root, 256).submission_with_id(
            "controlled-submit",
          )
          assert_eq(
            client.update(candidate).plan_digest(),
            candidate.job().plan_digest(),
          )
          let graph = service.graphs["controlled-submit"]
          for stage in graph.stages.values() {
            assert_eq(
              stage.store.compiled.unwrap().definition.max_state_events,
              Some(256),
            )
          }
          assert_false(client.resume().is_paused())
        }
      }
      assert_eq(client.drain().phase(), Succeeded)
      for task in tasks {
        assert_eq(task.wait().failed_tasks(), 0)
      }
      let outputs : Map[(String, Int64), @task.TaskOutput] = Map([])
      let totals : Map[String, Set[Int64]] = Map([])
      for line in @fs.read_file(root + "/out.jsonl").text().split("\n") {
        if line.is_empty() {
          continue
        }
        let record : @runtime.TaskOutputRecord = @json.from_json(
          @json.parse(line),
        )
        let identity = (record.task_id(), record.sequence())
        match outputs.get(identity) {
          Some(previous) => assert_eq(previous, record.output())
          None => {
            outputs[identity] = record.output()
            guard record.output() is @task.TaskOutput::KeyedSum(value) else {
              fail("unexpected output")
            }
            totals.get_or_init(value.key(), () => Set([])).add(value.total())
          }
        }
      }
      assert_eq(outputs.length(), 24)
      assert_eq(totals.length(), 8)
      for values in totals.values() {
        assert_eq(values.length(), 3)
        for total in [1L, 2L, 3L] {
          assert_true(values.contains(total))
        }
      }
      service.close()
      runner.wait()
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
}

///|
async test "graph update intent is recovered after one stage commits" {
  let root = @fs.tmpdir(prefix="moonweave-graph-update-recovery")
  defer @fs.rmdir(root, recursive=true)
  let config = CoordinatorNodeConfig::new(
    "c",
    "coordinator",
    NodeEndpoint::new("127.0.0.1", 0),
    root + "/coordinator",
    [WorkerSlot::new("a", "boot-a"), WorkerSlot::new("b", "boot-b")],
  )
  let control = DriverControlConfig::new(
    "c",
    "coordinator",
    NodeEndpoint::new("127.0.0.1", 0),
  )
  let service = CoordinatorService::bind(config, control)
  let original = controlled_graph(root, 128).submission_with_id("submit")
  let candidate = controlled_graph(root, 256).submission_with_id("submit")
  ignore(service.submitted_child(original))
  let graph = service.graphs["submit"]
  ignore(graph.apply_command(Pause))
  graph.parent.store.begin_graph_update(candidate)
  let next = candidate.job().compile()
  let (id, stage) = next.graph_stages[0]
  ignore(
    graph.stages[id].apply_command(
      Update(stage.submission_with_id(stage.job_id())),
    ),
  )
  graph.parent.store.compact()
  service.close()
  let restored = CoordinatorService::bind(config, control)
  defer restored.close()
  let graph = restored.graphs["submit"]
  assert_eq(graph.parent.store.graph_update, Some(candidate))
  graph.update(candidate)
  assert_eq(graph.parent.store.graph_update, None)
  assert_true(graph.parent.store.is_paused())
  for stage in graph.stages.values() {
    assert_eq(
      stage.store.compiled.unwrap().definition.max_state_events,
      Some(256),
    )
  }
  assert_eq(
    graph.parent.store.status().plan_digest(),
    candidate.job().plan_digest(),
  )
}
