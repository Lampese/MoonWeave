///|
async fn create_topic(
  group : @async.TaskGroup[Unit],
  brokers : String,
  topic : String,
) -> Unit {
  println("Connecting admin for " + topic)
  let admin = @moonkafka.Admin::connect_with_config(
    group~,
    @moonkafka.AdminConfig::new([brokers]),
  )
  defer admin.close()
  println("Creating " + topic)
  let result = admin.create_topics([
    {
      name: topic,
      num_partitions: 3,
      replication_factor: 1,
      assignments: [],
      configs: [],
    },
  ])
  assert_eq(result.length(), 1)
  assert_true(result[0].error_code == 0 || result[0].error_code == 36)
  println("Created " + topic)
}

///|
fn input_event(index : Int) -> @api.Event raise {
  @api.Event::keyed_sum(
    @causal.EventId::new("producer", index.to_int64()),
    "account",
    1L,
  )
}

///|
async fn check_source(
  group : @async.TaskGroup[Unit],
  brokers : String,
  produce : Bool,
) -> Unit {
  let topic = "source-batches"
  create_topic(group, brokers, topic)
  if produce {
    let producer = @moonkafka.Producer::connect_with_config(
      group~,
      @moonkafka.ProducerConfig::new([brokers], topic, linger_ms=100),
    )
    println("Producer connected")
    defer producer.close()
    let handles : Array[@moonkafka.SendHandle] = []
    for index in 0..<6 {
      handles.push(
        producer.send_handle(
          value=@utf8.encode(@json.to_json(input_event(index)).stringify()),
          partition=index % 3,
        ),
      )
    }
    for index, handle in handles {
      assert_eq(handle.await(), index.to_int64())
    }
    println("Produced six records across three partitions")
  }
  let config = @api.KafkaSourceConfig::new_all([brokers], topic, "source-group").with_max_poll_records(
    1,
  )
  let source = @api.KafkaSource::connect(group~, config)
  defer source.close()
  if produce {
    let seen : Array[@api.Event] = []
    while seen.length() < 6 {
      let batch = source.poll(max_wait_ms=100)
      for event in batch.events() {
        seen.push(event)
      }
      if !batch.events().is_empty() {
        source.commit_offsets(batch.next_offsets())
      }
    }
    for index in 0..<6 {
      let partition = index % 3
      let offset = (index / 3).to_int64()
      assert_true(
        seen.any(event => {
          event.id() ==
          @causal.EventId::new(
            "kafka/" + topic + "/" + partition.to_string(),
            offset,
          )
        }),
      )
    }
    println("PASS: multi-partition source, per-partition commit and recovery")
  } else {
    let positions = @async.with_timeout(10000, () => {
      for ;; {
        let batch = source.poll(max_wait_ms=100)
        if batch.next_offsets().length() == 3 {
          return batch.next_offsets()
        }
      }
    })
    assert_eq(positions.get(0), Some(2L))
    assert_eq(positions.get(1), Some(2L))
    assert_eq(positions.get(2), Some(2L))
    println("PASS: per-partition committed offsets survived broker restart")
  }
}

///|
async fn check_sink(group : @async.TaskGroup[Unit], brokers : String) -> Unit {
  let topic = "sink-records"
  create_topic(group, brokers, topic)
  let sink = @api.KafkaSink::connect(
    group~,
    @api.KafkaSinkConfig::new([brokers], topic),
  )
  defer sink.close()
  let record : @runtime.TaskOutputRecord = {
    version: 1,
    task_id: "integration-task",
    partition: 0,
    epoch: 3,
    sequence: 0L,
    output: @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
  }
  sink.write(record)
  let consumer = @moonkafka.Consumer::connect_with_config(
    group~,
    @moonkafka.ConsumerConfig::new(
      [brokers],
      topic,
      group_id=Some("sink-verifier"),
      enable_auto_commit=false,
      max_poll_records=1,
    ),
  )
  defer consumer.close()
  consumer.assign([(topic, 0)])
  let result = @async.with_timeout(10000, () => {
    for ;; {
      let records = consumer.poll(max_wait_ms=100)
      if !records.is_empty() {
        return records[0]
      }
    }
  })
  assert_eq(result.key_utf8(), Some(record.idempotency_key()))
  let decoded : @runtime.TaskOutputRecord = @json.from_json(
    @json.parse(result.value_utf8().unwrap()),
  )
  assert_eq(decoded.idempotency_key(), record.idempotency_key())
  assert_eq(decoded.output(), record.output())
  println("PASS: sink ACK, stable key and JSON payload")
}

///|
async fn main {
  let args = @env.args()
  guard args.length() == 4 else {
    fail("run using scripts/kafka_integration.mbtx")
  }
  @async.with_task_group <| group => {
    check_source(group, args[1], args[3] == "initial")
    if args[3] == "initial" {
      check_sink(group, args[1])
    }
  }
}
