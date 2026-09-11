///|
test "Kafka connector configurations validate broker identity" {
  let source = KafkaSourceConfig::new(
    ["broker-a:9092"],
    "events",
    "moonweave-workers",
    2,
  )
  assert_eq(source.topic(), "events")
  assert_eq(source.group_id(), "moonweave-workers")
  assert_eq(source.partition(), 2)
  assert_false(source.all_partitions())
  assert_eq(source.bootstrap_servers().to_owned(), ["broker-a:9092"])
  let bounded = source.with_max_poll_records(32)
  assert_eq(bounded.partition(), 2)
  try KafkaSourceConfig::new([], "events", "workers", 0) catch {
    ApiError::Source(message) => assert_true(message.contains("bootstrap"))
    _ => fail("expected missing Kafka bootstrap validation")
  } noraise {
    _ => fail("expected missing Kafka bootstrap validation")
  }
  try KafkaSourceConfig::new(["broker-a:9092"], "", "workers", 0) catch {
    ApiError::Source(message) => assert_true(message.contains("topic"))
    _ => fail("expected missing Kafka topic validation")
  } noraise {
    _ => fail("expected missing Kafka topic validation")
  }
}

///|
test "Kafka sink configuration rejects empty destinations" {
  let sink = KafkaSinkConfig::new(["broker-a:9092"], "outputs")
  assert_eq(sink.topic(), "outputs")
  assert_eq(sink.bootstrap_servers().to_owned(), ["broker-a:9092"])
  try KafkaSinkConfig::new([""], "outputs") catch {
    ApiError::Sink(message) => assert_true(message.contains("bootstrap"))
    _ => fail("expected empty Kafka sink bootstrap validation")
  } noraise {
    _ => fail("expected empty Kafka sink bootstrap validation")
  }
  try KafkaSinkConfig::new(["broker-a:9092"], "") catch {
    ApiError::Sink(message) => assert_true(message.contains("topic"))
    _ => fail("expected missing Kafka sink topic validation")
  } noraise {
    _ => fail("expected missing Kafka sink topic validation")
  }
}

///|
test "Kafka job specs round trip and keep runtime credentials out" {
  let source = KafkaSourceSpec::new(
      ["broker-a:9092", "broker-b:9092"],
      "events",
      "moonweave-workers",
      3,
    )
    .with_start_from(KafkaStartFrom::Latest)
    .with_security_protocol(KafkaSecurityProtocol::SaslSsl)
    .with_credential_ref("source-vault-ref")
    .with_max_poll_records(16)
  let sink = KafkaSinkSpec::new(["broker-a:9092"], "outputs")
    .with_security_protocol(KafkaSecurityProtocol::Ssl)
    .with_credential_ref("sink-vault-ref")
  let definition = JobDefinition::new("kafka-job", OperatorRef::keyed_sum())
    .with_source(SourceSpec::kafka(source))
    .with_sink(SinkSpec::kafka(sink))
  let encoded = @json.to_json(definition)
  let text = encoded.stringify()
  assert_true(text.contains("source-vault-ref"))
  assert_true(text.contains("sink-vault-ref"))
  assert_false(text.contains("runtime-only-secret"))
  let decoded : JobDefinition = @json.from_json(encoded)
  assert_eq(decoded, definition)
  let compiled = decoded.compile()
  assert_eq(compiled.recovery_capability(), RecoveryCapability::Replayable)
  assert_eq(compiled.delivery_guarantee(), DeliveryGuarantee::AtLeastOnce)
  let runtime = KafkaRuntimeConfig::new().with_source_security(
    "runtime-only-secret",
    None,
    None,
  )
  let _ = runtime
}

///|
test "Kafka job specs reject invalid fields during compile" {
  let source = KafkaSourceSpec::new(["broker-a:9092"], "events", "workers", 0)
  let invalid_bootstrap = KafkaSourceSpec::{
    ..source,
    bootstrap_servers: [""],
  }
  try
    JobDefinition::new("invalid-kafka-source", OperatorRef::keyed_sum())
    .with_source(SourceSpec::kafka(invalid_bootstrap))
    .compile()
  catch {
    ApiError::Source(message) => assert_true(message.contains("bootstrap"))
    _ => fail("expected invalid Kafka bootstrap rejection")
  } noraise {
    _ => fail("expected invalid Kafka bootstrap rejection")
  }
  let invalid_topic = KafkaSourceSpec::{ ..source, topic: "", }
  try
    JobDefinition::new("invalid-kafka-topic", OperatorRef::keyed_sum())
    .with_source(SourceSpec::kafka(invalid_topic))
    .compile()
  catch {
    ApiError::Source(message) => assert_true(message.contains("topic"))
    _ => fail("expected invalid Kafka topic rejection")
  } noraise {
    _ => fail("expected invalid Kafka topic rejection")
  }
  let invalid_group = KafkaSourceSpec::{ ..source, group_id: "", }
  try
    JobDefinition::new("invalid-kafka-group", OperatorRef::keyed_sum())
    .with_source(SourceSpec::kafka(invalid_group))
    .compile()
  catch {
    ApiError::Source(message) => assert_true(message.contains("group"))
    _ => fail("expected invalid Kafka group rejection")
  } noraise {
    _ => fail("expected invalid Kafka group rejection")
  }
  let invalid_partition = KafkaSourceSpec::{ ..source, partition: -2, }
  try
    JobDefinition::new("invalid-kafka-partition", OperatorRef::keyed_sum())
    .with_source(SourceSpec::kafka(invalid_partition))
    .compile()
  catch {
    ApiError::Source(message) => assert_true(message.contains("partition"))
    _ => fail("expected invalid Kafka partition rejection")
  } noraise {
    _ => fail("expected invalid Kafka partition rejection")
  }
  let invalid_max_poll = KafkaSourceSpec::{ ..source, max_poll_records: 0, }
  try
    JobDefinition::new("invalid-kafka-poll", OperatorRef::keyed_sum())
    .with_source(SourceSpec::kafka(invalid_max_poll))
    .compile()
  catch {
    ApiError::Source(message) => assert_true(message.contains("poll"))
    _ => fail("expected invalid Kafka poll limit rejection")
  } noraise {
    _ => fail("expected invalid Kafka poll limit rejection")
  }
}

///|
test "Kafka source can follow all topic partitions" {
  let source = KafkaSourceSpec::new_all(["broker-a:9092"], "events", "workers")
  assert_true(source.all_partitions())
  assert_eq(source.partition(), -1)
  let config = KafkaSourceConfig::new_all(
    ["broker-a:9092"],
    "events",
    "workers",
  )
  assert_true(config.all_partitions())
  assert_eq(config.partition(), -1)
  let compiled = JobDefinition::new("all-kafka", OperatorRef::keyed_sum())
    .with_source(SourceSpec::kafka(source))
    .compile()
  assert_true(compiled.definition.source is SourceSpec::Kafka(_))
}

///|
test "Kafka sink specs reject invalid fields during compile" {
  let sink = KafkaSinkSpec::new(["broker-a:9092"], "outputs")
  let invalid_bootstrap = KafkaSinkSpec::{ ..sink, bootstrap_servers: [""], }
  try
    JobDefinition::new("invalid-kafka-sink", OperatorRef::keyed_sum())
    .with_sink(SinkSpec::kafka(invalid_bootstrap))
    .compile()
  catch {
    ApiError::Sink(message) => assert_true(message.contains("bootstrap"))
    _ => fail("expected invalid Kafka sink bootstrap rejection")
  } noraise {
    _ => fail("expected invalid Kafka sink bootstrap rejection")
  }
  let invalid_topic = KafkaSinkSpec::{ ..sink, topic: "", }
  try
    JobDefinition::new("invalid-kafka-sink-topic", OperatorRef::keyed_sum())
    .with_sink(SinkSpec::kafka(invalid_topic))
    .compile()
  catch {
    ApiError::Sink(message) => assert_true(message.contains("topic"))
    _ => fail("expected invalid Kafka sink topic rejection")
  } noraise {
    _ => fail("expected invalid Kafka sink topic rejection")
  }
}
