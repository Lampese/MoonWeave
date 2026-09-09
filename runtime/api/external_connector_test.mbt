///|
async test "local cluster consumes JSONL source and idempotent JSONL sink" {
  let directory = @fs.tmpdir(prefix="moonweave-external-connectors")
  let source_path = directory + "/input.jsonl"
  let sink_path = directory + "/output.jsonl"
  defer {
    for name in @fs.readdir(directory) {
      @fs.remove(directory + "/" + name) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let event = Event::new(
    @causal.EventId::new("external", 0),
    [],
    "external-key",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  @fs.write_file(
    source_path,
    @json.to_json(event).stringify() + "\n",
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  )
  let compiled = JobDefinition::new("external-job", OperatorRef::reachability())
    .with_source(SourceSpec::json_lines(source_path))
    .with_sink(SinkSpec::json_lines(sink_path))
    .compile()
  let summary = compiled.run_local_cluster(
    ClusterConfig::new("external-cluster", "coordinator", directory + "/state"),
    [],
  )
  assert_eq(summary.completed_partitions(), 1)
  assert_eq(summary.failed_partitions(), 0)
  assert_eq(summary.outputs().length(), 1)
  assert_true(@fs.exists(source_path + ".offset"))
  let sink_text = @fs.read_file(sink_path).text()
  assert_true(sink_text.contains("A"))
  assert_true(sink_text.contains("B"))
}

///|
test "API registry compiles and runs a custom JSON operator" {
  let custom = @task.CustomOperator::new(
    "api-custom",
    7,
    "custom:api:v7",
    "api-custom-state:v1",
    "json-event:v1",
    "json-output:v1",
    "moonweave-task-json-v1",
    "api-deps:v1",
    false,
    false,
    payload => {
      guard payload is Object(_) else {
        raise @task.TaskError::Custom("object payload required")
      }
    },
    input => [Json::object({ "echo": input.event().payload() })],
  )
  let registry = OperatorRegistry::new()
  registry.register_custom(custom)
  let compiled = JobDefinition::new(
    "api-custom-job",
    OperatorRef::new("api-custom", 7),
  ).compile(registry~)
  assert_false(compiled.operator_descriptor().deterministic())
  let run = compiled.run_local()
  assert_eq(
    run.submit([
      Event::new(
        @causal.EventId::new("api-custom", 0),
        [],
        "k",
        Json::object({ "value": Json::string("v") }),
      ),
    ]),
    [
      Output::Custom(
        Json::object({ "echo": Json::object({ "value": Json::string("v") }) }),
      ),
    ],
  )
  run.close()
}

///|
fn static_remote_registry() -> OperatorRegistry raise ApiError {
  let descriptor = OperatorDescriptor::custom(
    OperatorRef::new("remote-static", 1),
    "custom:remote-static:v1",
    "remote-static-state:v1",
    "json-event:v1",
    "json-output:v1",
    input => [Json::object({ "value": input.event().payload() })],
    _ => (),
    dependency_digest="remote-static-deps:v1",
  )
  let registry = OperatorRegistry::new()
  registry.register(descriptor)
  registry
}

///|
async test "static custom artifact runs through independent Driver and Worker registries" {
  let root = @fs.tmpdir(prefix="moonweave-static-artifact-e2e")
  let coordinator_dir = root + "/coordinator"
  let worker_dir = root + "/worker"
  @fs.mkdir(coordinator_dir, recursive=true)
  @fs.mkdir(worker_dir, recursive=true)
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(30000, () => {
    let submit_registry = static_remote_registry()
    let coordinator_registry = static_remote_registry()
    let worker_registry = static_remote_registry()
    let client_registry = static_remote_registry()
    let job = JobDefinition::new(
        "static-artifact-job",
        OperatorRef::new("remote-static", 1),
      )
      .with_workers(["worker-a"])
      .compile(registry=submit_registry)
    let store = DriverStore::open(
      coordinator_dir + "/driver.wal",
      registry=coordinator_registry,
    )
    let coordinator = DriverCoordinator::bind(
      CoordinatorNodeConfig::new(
        "static-artifact-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        coordinator_dir,
        [WorkerSlot::new("worker-a", "static-worker-1")],
      ),
      DriverControlConfig::new(
        "static-artifact-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
      store,
    )
    let data_endpoint = coordinator.data_endpoint()
    let control_endpoint = coordinator.control_endpoint()
    let worker_job = JobDefinition::new(
        "static-artifact-job",
        OperatorRef::new("remote-static", 1),
      )
      .with_workers(["worker-a"])
      .compile(registry=worker_registry)
    @async_runtime.with_task_group <| group => {
      let coordinator_task = group.spawn(allow_failure=true) <| () => {
        try coordinator.run() catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => Ok(value)
        }
      }
      let worker_task = group.spawn(allow_failure=true) <| () => {
        try
          worker_job.run_worker_node(
            WorkerNodeConfig::new_auto(
              "static-artifact-cluster", "coordinator", data_endpoint, "worker-a",
              "static-worker-1", worker_dir,
            ),
          )
        catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => Ok(value)
        }
      }
      let client = DriverClient::connect(
        DriverConfig::new(
          "static-artifact-cluster", "coordinator", "static-driver", control_endpoint,
        ),
        registry=client_registry,
      )
      let submission = job.submission_with_id("static-artifact-submission")
      assert_eq(client.submit(submission).phase(), JobRunPhase::Running)
      let event = Event::new(
        @causal.EventId::new("static-artifact-source", 0),
        [],
        "static-key",
        Json::object({ "value": Json::string("ok") }),
      )
      assert_eq(client.input([event]).accepted_events(), 1L)
      assert_eq(client.drain().phase(), JobRunPhase::Succeeded)
      client.close()
      let execution = coordinator_task.wait()
      let worker = worker_task.wait()
      guard execution is Ok(execution_summary) else {
        fail("static custom Coordinator execution failed")
      }
      guard worker is Ok(worker_summary) else {
        fail("static custom Worker execution failed")
      }
      assert_eq(execution_summary.status().phase(), JobRunPhase::Succeeded)
      assert_eq(worker_summary.live_batches(), 1)
      assert_eq(worker_summary.state_events(), 1)
    }
  })
}
