///|
async fn create_topic(
  group : @async.TaskGroup[Unit],
  brokers : String,
  topic : String,
) -> Unit {
  create_topic_partitions(group, brokers, topic, 3)
}

///|
async fn create_topic_partitions(
  group : @async.TaskGroup[Unit],
  brokers : String,
  topic : String,
  partitions : Int,
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
      num_partitions: partitions,
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
async fn produce_partitions(
  group : @async.TaskGroup[Unit],
  brokers : String,
  topic : String,
  partitions : Array[Int],
) -> Unit {
  let producer = @moonkafka.Producer::connect_with_config(
    group~,
    @moonkafka.ProducerConfig::new([brokers], topic, linger_ms=20),
  )
  defer producer.close()
  for partition in partitions {
    let offset = producer.send(
      value=@utf8.encode("rebalance-\{partition}"),
      partition~,
    )
    assert_eq(offset, 0L)
    println("Produced partition \{partition} offset \{offset}")
  }
}

///|
async fn wait_for_partition_count(
  group : @async.TaskGroup[Unit],
  brokers : String,
  topic : String,
  expected : Int,
) -> Unit {
  let admin = @moonkafka.Admin::connect_with_config(
    group~,
    @moonkafka.AdminConfig::new([brokers]),
  )
  defer admin.close()
  @async.with_timeout(30000, () => {
    for ;; {
      let descriptions = admin.describe_topic_partitions([topic])
      if descriptions.length() == 1 &&
        descriptions[0].partitions.length() >= expected {
        return
      }
      @async.sleep(100)
    }
  })
}

///|
fn same_assignment(
  left : Array[(String, Int)],
  right : Array[(String, Int)],
) -> Bool {
  if left.length() != right.length() {
    return false
  }
  for pair in left {
    if !right.contains(pair) {
      return false
    }
  }
  true
}

///|
fn assignment_text(pairs : Array[(String, Int)]) -> String {
  let mut out = ""
  for pair in pairs {
    if out.length() > 0 {
      out += ","
    }
    out += pair.0 + ":" + pair.1.to_string()
  }
  out
}

///|
async fn write_member_state(
  root : String,
  member : String,
  phase : String,
  consumer : @moonkafka.Consumer,
) -> Unit {
  let error = match consumer.membership_error() {
    Some(value) => value
    None => "<none>"
  }
  @fs.write_file(
    root + "/" + member + "." + phase,
    "member=" +
    consumer.member_identity() +
    "\nassignment_count=" +
    consumer.assignment().length().to_string() +
    "\nassignment=" +
    assignment_text(consumer.assignment()) +
    "\nerror=" +
    error +
    "\n",
  )
}

///|
async fn run_group_member(
  brokers : String,
  root : String,
  member : String,
) -> Unit {
  let topic = "rebalance-topic"
  let group_id = "rebalance-group"
  @async.with_task_group <| group => {
    let consumer = @moonkafka.Consumer::connect_with_config(
      group~,
      @moonkafka.ConsumerConfig::new(
        [brokers],
        topic,
        group_id=Some(group_id),
        enable_auto_commit=false,
        max_poll_records=1,
        heartbeat_interval_ms=500,
        session_timeout_ms=6000,
        max_poll_interval_ms=30000,
        group_protocol=@moonkafka.ClassicProtocol,
      ),
    )
    defer consumer.close()
    consumer.subscribe_classic([topic])
    let initial = @async.with_timeout(30000, () => {
      for ;; {
        let assignment = consumer.assignment()
        if !assignment.is_empty() {
          return assignment
        }
        @async.sleep(50)
      }
    })
    write_member_state(root, member, "initial", consumer)
    let mut wrote_rebalance = false
    let mut wrote_expanded = false
    for ;; {
      if @fs.exists(root + "/stop") {
        break
      }
      let records = consumer.poll_partitioned(max_wait_ms=100)
      if !records.is_empty() {
        let offsets : Map[Int, Int64] = Map([])
        for item in records {
          match consumer.position(item.partition()) {
            Some(offset) => offsets[item.partition()] = offset
            None => ()
          }
        }
        consumer.commit(offsets~)
      }
      if @fs.exists(root + "/check-rebalance") && !wrote_rebalance {
        let current = consumer.assignment()
        if !same_assignment(initial, current) || current.length() == 1 {
          write_member_state(root, member, "rebalanced", consumer)
          wrote_rebalance = true
        }
      }
      if @fs.exists(root + "/check-expand") && !wrote_expanded {
        let current = consumer.assignment()
        if current.length() >= 2 {
          write_member_state(root, member, "expanded", consumer)
          wrote_expanded = true
        }
      }
      @async.sleep(25)
    }
  }
}

///|
async fn check_group_prepare(
  group : @async.TaskGroup[Unit],
  brokers : String,
) -> Unit {
  let topic = "rebalance-topic"
  create_topic_partitions(group, brokers, topic, 2)
  produce_partitions(group, brokers, topic, [0, 1])
  println("PASS: created two-partition rebalance topic")
}

///|
async fn check_group_expand(
  group : @async.TaskGroup[Unit],
  brokers : String,
) -> Unit {
  let topic = "rebalance-topic"
  let admin = @moonkafka.Admin::connect_with_config(
    group~,
    @moonkafka.AdminConfig::new([brokers]),
  )
  defer admin.close()
  let result = admin.create_partitions([
    { name: topic, count: 4, assignments: None, },
  ])
  assert_eq(result.length(), 1)
  assert_eq(result[0].error_code, 0)
  wait_for_partition_count(group, brokers, topic, 4)
  produce_partitions(group, brokers, topic, [2, 3])
  println("PASS: expanded rebalance topic from two to four partitions")
}

///|
async fn check_group_commits(
  group : @async.TaskGroup[Unit],
  brokers : String,
) -> Unit {
  let consumer = @moonkafka.Consumer::connect_with_config(
    group~,
    @moonkafka.ConsumerConfig::new(
      [brokers],
      "rebalance-topic",
      group_id=Some("rebalance-group"),
      enable_auto_commit=false,
      group_protocol=@moonkafka.ClassicProtocol,
    ),
  )
  defer consumer.close()
  let committed = @async.with_timeout(30000, () => {
    for ;; {
      let values = consumer.committed(partitions=Some([0, 1, 2, 3]))
      if values.get(0) == Some(1L) &&
        values.get(1) == Some(1L) &&
        values.get(2) == Some(1L) &&
        values.get(3) == Some(1L) {
        return values
      }
      @async.sleep(100)
    }
  })
  assert_eq(committed.get(0), Some(1L))
  assert_eq(committed.get(1), Some(1L))
  assert_eq(committed.get(2), Some(1L))
  assert_eq(committed.get(3), Some(1L))
  println("committed partitions=0:1,1:1,2:1,3:1")
  println("PASS: each partition committed independently")
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
      // Kafka offsets are scoped to a partition, so each of the three
      // partitions receives offsets 0 and 1 rather than the global index.
      assert_eq(handle.await(), (index / 3).to_int64())
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
    @async.with_timeout(15000, () => {
      while seen.length() < 6 {
        let batch = source.poll(max_wait_ms=100) catch {
          error => {
            println("source poll error: \{@debug.to_string(error)}")
            raise error
          }
        }
        for event in batch.events() {
          seen.push(event)
        }
        if !batch.events().is_empty() {
          source.commit_offsets(batch.next_offsets()) catch {
            error => {
              println("source commit error: \{@debug.to_string(error)}")
              raise error
            }
          }
        }
      }
    }) catch {
      error => {
        println("source assignment=" + source.assignment().length().to_string())
        match source.membership_error() {
          Some(reason) => println("source membership error=" + reason)
          None => println("source membership error=<none>")
        }
        raise error
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
  // This check validates sink acknowledgement and payload fidelity. Keep the
  // topic single-partition so the verifier's explicit assignment is stable;
  // keyed multi-partition routing is covered by the producer tests above.
  create_topic_partitions(group, brokers, topic, 1)
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
  guard args.length() >= 4 else {
    fail("run using scripts/kafka_integration.mbtx")
  }
  @async.with_task_group <| group => {
    if args[3] == "initial" || args[3] == "restart" {
      check_source(group, args[1], args[3] == "initial")
    } else if args[3] == "prepare" {
      check_group_prepare(group, args[1])
    } else if args[3] == "expand" {
      check_group_expand(group, args[1])
    } else if args[3] == "verify" {
      check_group_commits(group, args[1])
    } else if args[3] == "member" {
      guard args.length() == 5 else { fail("member requires a name") }
      run_group_member(args[1], args[2], args[4])
    } else {
      fail("unknown Kafka integration mode")
    }
    if args[3] == "initial" {
      check_sink(group, args[1])
    }
  }
}
