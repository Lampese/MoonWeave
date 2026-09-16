///|
async test "graph static callbacks retain causal metadata and isolate multi-output identities" {
  let root = @fs.tmpdir(prefix="moonweave-graph-custom")
  defer @fs.rmdir(root, recursive=true)
  let implementation = @task.CustomOperator::new(
    "expand",
    1,
    "expand:v1",
    "stateless:v1",
    "json:v1",
    "json:v1",
    "moonweave-task:v1",
    "builtin:v1",
    true,
    false,
    _ => (),
    input => {
      let record = input.event()
      guard record.payload() is Object(fields) else {
        raise @task.TaskError::Custom("object required")
      }
      let expected : Int = @json.from_json(fields["parents"]) catch {
        _ => raise @task.TaskError::Custom("parents required")
      }
      if record.parents().length() != expected {
        raise @task.TaskError::Custom("lost callback parents")
      }
      if fields.get("expand") == Some(true) {
        [{ "key": "merged", "value": "2" }, { "key": "merged", "value": "3" }]
      } else {
        [{ "key": "merged", "value": fields["value"] }]
      }
    },
  )
  let registry = OperatorRegistry::builtins()
  registry.register_custom(implementation)
  let workers = ["a", "b"]
  let source = JobDefinition::transform("source", [])
    .with_workers(workers)
    .with_partition_count(2)
  let expand = JobDefinition::new("expand", OperatorRef::new("expand", 1))
    .with_workers(workers)
    .with_partition_count(2)
  let sum = JobDefinition::new("sum", OperatorRef::keyed_sum())
    .with_workers(workers)
    .with_partition_count(2)
    .with_sink(SinkSpec::json_lines(root + "/out.jsonl"))
  let job = JobDefinition::dataflow(
    "custom-graph",
    GraphDefinition::new(
      [
        GraphStage::new("source", source),
        GraphStage::new("expand", expand),
        GraphStage::new("sum", sum),
      ],
      [GraphEdge::new("source", "expand"), GraphEdge::new("expand", "sum")],
    ),
  ).compile(registry~)
  @async_runtime.with_timeout(30000, () => {
    let service = CoordinatorService::bind(
      CoordinatorNodeConfig::new(
        "custom-graph",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        root + "/coordinator",
        [WorkerSlot::new("a", "boot-a"), WorkerSlot::new("b", "boot-b")],
      ),
      DriverControlConfig::new(
        "custom-graph",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
      registry~,
    )
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { service.run() }
      defer service.close()
      let handle = job
        .submission_with_id("custom-submit")
        .run_remote(
          DriverConfig::new(
            "custom-graph",
            "coordinator",
            "driver",
            service.control_endpoint(),
          ),
          registry~,
        )
      defer handle.close()
      let tasks = workers.map(worker => {
        group.spawn() <| () => {
          job.run_graph_worker(
            WorkerNodeConfig::new_auto(
              "custom-graph",
              "coordinator",
              service.data_endpoint(),
              worker,
              "boot-" + worker,
              root + "/" + worker,
            ),
            service.control_endpoint(),
          )
        }
      })
      let parent = Event::new(@causal.EventId::new("orders", 0L), [], "x", {
        "expand": true,
        "parents": 0,
      })
      let collision = Event::new(
        @causal.EventId::new("item/0/orders", 0L),
        [],
        "x",
        { "value": "7", "parents": 0 },
      )
      let child = Event::new(
        @causal.EventId::new("orders", 1L),
        [parent.id()],
        "x",
        { "value": "1", "parents": 1 },
      )
      ignore(handle.input([child, collision, parent]))
      let status = handle.drain()
      guard status.phase() == Succeeded else {
        fail(status.failure().unwrap_or("custom graph failed"))
      }
      for task in tasks {
        assert_eq(task.wait().completed_tasks(), 3)
      }
      let values = []
      for line in @fs.read_file(root + "/out.jsonl").text().split("\n") {
        if line.is_empty() {
          continue
        }
        let record : @runtime.TaskOutputRecord = @json.from_json(
          @json.parse(line),
        )
        guard record.output() is @task.TaskOutput::KeyedSum(sum) else {
          fail("expected sum")
        }
        assert_eq(sum.key(), "merged")
        values.push(sum.total())
      }
      assert_eq(values, [7L, 9L, 12L, 13L])
      service.close()
      runner.wait()
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
}
