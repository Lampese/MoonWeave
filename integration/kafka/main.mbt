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
      num_partitions: 1,
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
  @api.Event::keyed_sum(@causal.EventId::new("producer", index), "account", 1L)
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
          partition=0,
        ),
      )
    }
    for index, handle in handles {
      assert_eq(handle.await(), index.to_int64())
    }
    println("Produced one six-record batch")
  }
  let config = @api.KafkaSourceConfig::new([brokers], topic, "source-group", 0).with_max_poll_records(
    1,
  )
  let source = @api.KafkaSource::connect(group~, config)
  defer source.close()
  if produce {
    for index in 0..<6 {
      let batch = @async.with_timeout(10000, () => {
        for ;; {
          let batch = source.poll(max_wait_ms=100)
          if !batch.events().is_empty() {
            return batch
          }
        }
      })
      assert_eq(batch.events().length(), 1)
      assert_eq(
        batch.events()[0].id(),
        @causal.EventId::new("kafka/" + topic + "/0", index),
      )
      assert_eq(batch.next_offset(), index.to_int64() + 1L)
      let replay = source.poll(max_wait_ms=100)
      assert_eq(replay, batch)
      source.commit(batch.next_offset())
    }
    println("PASS: batched source, retry and committed offset")
  } else {
    assert_eq(source.position(), Some(6L))
    assert_true(source.poll(max_wait_ms=100).events().is_empty())
    println("PASS: committed offset survived broker restart")
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
