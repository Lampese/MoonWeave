///|
test "Kafka records become stable causal events" {
  let parent = @causal.EventId::new("upstream", 4)
  let input = Event::new(
    @causal.EventId::new("producer", 99),
    [parent],
    "account-1",
    Json::object({ "value": Json::string("ok") }),
  )
  let record : @moonkafka.Record = {
    offset: 12L,
    timestamp: 0L,
    key: None,
    value: Some(@utf8.encode(@json.to_json(input).stringify())),
    headers: [],
  }
  let event = kafka_record_to_event("events", 3, record)
  assert_eq(event.id(), @causal.EventId::new("kafka/events/3", 12))
  assert_eq(event.parents(), [parent])
  assert_eq(event.key(), "account-1")
  assert_eq(event.payload(), Json::object({ "value": Json::string("ok") }))
}

///|
test "Kafka records require a complete MoonWeave Event value" {
  let record : @moonkafka.Record = {
    offset: 0L,
    timestamp: 0L,
    key: None,
    value: Some(@utf8.encode("{}")),
    headers: [],
  }
  try kafka_record_to_event("events", 0, record) catch {
    ApiError::Source(message) =>
      assert_true(message.contains("MoonWeave Event"))
    _ => fail("expected invalid Kafka event value")
  } noraise {
    _ => fail("expected invalid Kafka event value")
  }
}

///|
test "Kafka records reject invalid UTF-8 without lossy replacement" {
  let record : @moonkafka.Record = {
    offset: 0L,
    timestamp: 0L,
    key: None,
    value: Some([0xff]),
    headers: [],
  }
  try kafka_record_to_event("events", 0, record) catch {
    ApiError::Source(message) => assert_true(message.contains("UTF-8"))
    _ => fail("expected invalid UTF-8 rejection")
  } noraise {
    _ => fail("invalid UTF-8 was accepted")
  }
}

///|
test "Kafka offsets preserve Int64 precision and reject an overflowing next offset" {
  let input = Event::new(
    @causal.EventId::new("producer", 0),
    [],
    "account-1",
    Json::object({ "value": Json::string("ok") }),
  )
  for offset in [2147483648L, 9007199254740993L, 9223372036854775806L] {
    let record : @moonkafka.Record = {
      offset,
      timestamp: 0L,
      key: None,
      value: Some(@utf8.encode(@json.to_json(input).stringify())),
      headers: [],
    }
    let event = kafka_record_to_event("events", 0, record)
    assert_eq(event.id(), @causal.EventId::new("kafka/events/0", offset))
    let restored : Event = @json.from_json(
      @json.parse(@json.to_json(event).stringify()),
    )
    assert_eq(restored, event)
  }
  for offset in [-1L, 9223372036854775807L] {
    let record : @moonkafka.Record = {
      offset,
      timestamp: 0L,
      key: None,
      value: Some(@utf8.encode(@json.to_json(input).stringify())),
      headers: [],
    }
    try kafka_record_to_event("events", 0, record) catch {
      ApiError::Source(message) => assert_true(message.contains("offset"))
      _ => fail("expected invalid Kafka offset rejection")
    } noraise {
      _ => fail("invalid Kafka offset was accepted")
    }
  }
}

///|
test "Kafka runtime security must be supplied outside the JobDefinition" {
  let source = KafkaSourceSpec::new(["broker-a:9092"], "events", "workers", 0)
  let ssl_source = source.with_security_protocol(KafkaSecurityProtocol::Ssl)
  try KafkaRuntimeConfig::new().source_config(ssl_source) catch {
    ApiError::Cluster(message) => assert_true(message.contains("TLS"))
    _ => fail("expected missing Kafka source TLS options")
  } noraise {
    _ => fail("expected missing Kafka source TLS options")
  }
  let sasl_source = source.with_security_protocol(
    KafkaSecurityProtocol::SaslPlaintext,
  )
  try KafkaRuntimeConfig::new().source_config(sasl_source) catch {
    ApiError::Cluster(message) => assert_true(message.contains("SASL"))
    _ => fail("expected missing Kafka source SASL credentials")
  } noraise {
    _ => fail("expected missing Kafka source SASL credentials")
  }
  let credential_source = source.with_credential_ref("source-ref")
  try KafkaRuntimeConfig::new().source_config(credential_source) catch {
    ApiError::Cluster(message) => assert_true(message.contains("credential"))
    _ => fail("expected missing Kafka source credential reference")
  } noraise {
    _ => fail("expected missing Kafka source credential reference")
  }
  let sink = KafkaSinkSpec::new(["broker-a:9092"], "outputs")
  let ssl_sink = sink.with_security_protocol(KafkaSecurityProtocol::Ssl)
  try KafkaRuntimeConfig::new().sink_config(ssl_sink) catch {
    ApiError::Cluster(message) => assert_true(message.contains("TLS"))
    _ => fail("expected missing Kafka sink TLS options")
  } noraise {
    _ => fail("expected missing Kafka sink TLS options")
  }
  let credential_sink = sink.with_credential_ref("sink-ref")
  try KafkaRuntimeConfig::new().sink_config(credential_sink) catch {
    ApiError::Cluster(message) => assert_true(message.contains("credential"))
    _ => fail("expected missing Kafka sink credential reference")
  } noraise {
    _ => fail("expected missing Kafka sink credential reference")
  }
}

///|
test "Kafka runtime handles stay on process-local runner configs" {
  let runtime = KafkaRuntimeConfig::new()
  let endpoint = NodeEndpoint::new("127.0.0.1", 9090)
  let cluster = ClusterConfig::new("cluster", "coordinator", "/state").with_kafka_runtime(
    runtime,
  )
  let coordinator = CoordinatorNodeConfig::new(
    "cluster",
    "coordinator",
    NodeEndpoint::new("127.0.0.1", 0),
    "/state/coordinator",
    [WorkerSlot::new("worker", "incarnation")],
  ).with_kafka_runtime(runtime)
  let worker = WorkerNodeConfig::new_auto(
    "cluster", "coordinator", endpoint, "worker", "incarnation", "/state/worker",
  ).with_kafka_runtime(runtime)
  let worker_set = WorkerNodeSetConfig::new(
    "cluster", "coordinator", endpoint, "worker", "incarnation", "/state/worker",
  ).with_kafka_runtime(runtime)
  assert_true(cluster.kafka_runtime is Some(_))
  assert_true(coordinator.kafka_runtime is Some(_))
  assert_true(worker.kafka_runtime is Some(_))
  assert_true(worker_set.kafka_runtime is Some(_))
}

///|
test "live Driver paths accept Kafka connector specifications" {
  let source = KafkaSourceSpec::new(["broker-a:9092"], "events", "workers", 0)
  let source_job = JobDefinition::new(
      "live-kafka-source",
      OperatorRef::keyed_sum(),
    )
    .with_source(SourceSpec::kafka(source))
    .compile()
  assert_eq(source_job.definition.source is SourceSpec::Kafka(_), true)
  let sink = KafkaSinkSpec::new(["broker-a:9092"], "outputs")
  let sink_job = JobDefinition::new("live-kafka-sink", OperatorRef::keyed_sum())
    .with_sink(SinkSpec::kafka(sink))
    .compile()
  assert_eq(sink_job.definition.sink is SinkSpec::Kafka(_), true)
}

///|
async test "Kafka source execution rebuilds work from SourceLog without Driver input" {
  let root = @fs.tmpdir(prefix="moonweave-kafka-source-restart")
  defer @fs.rmdir(root, recursive=true)
  let listener = CoordinatorNode::bind(
    CoordinatorNodeConfig::new(
      "kafka-source-restart",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
      root,
      [WorkerSlot::new("worker", "worker-incarnation")],
    ),
  )
  defer listener.close()
  let job = JobDefinition::new("kafka-source-restart", OperatorRef::keyed_sum())
    .with_workers(["worker"])
    .with_source(
      SourceSpec::kafka(
        KafkaSourceSpec::new(
          ["broker-a:9092"],
          "events",
          "moonweave-workers",
          0,
        ),
      ),
    )
    .compile()
  let first = listener.open_source_backed_execution(job, [])
  first.source.ingest_all([
    Event::keyed_sum(
      @causal.EventId::new("kafka/events/0", 12),
      "account-1",
      7L,
    ).record,
  ])
  assert_eq(first.event_count(), 1)
  first.close()
  let restored = listener.open_source_backed_execution(job, [])
  defer restored.close()
  assert_eq(restored.event_count(), 1)
}
