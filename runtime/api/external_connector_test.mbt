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
