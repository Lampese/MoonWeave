///|
async test "high-level cluster runner uses the durable TCP runtime" {
  let directory = @fs.tmpdir(prefix="moonweave-api-cluster")
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
  @async_runtime.with_timeout(30000, () => {
    let event = Event::new(
      @causal.EventId::new("cluster-api", 0),
      [],
      "cluster-key",
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    let compiled = JobDefinition::new(
        "cluster-api-job",
        OperatorRef::reachability(),
      )
      .with_sink(SinkSpec::durable("outputs"))
      .compile()
    let config = ClusterConfig::new("cluster-api", "coordinator", directory)
    let summary = compiled.run_local_cluster(config, [event]) catch {
      error => {
        println(@debug.to_string(error))
        fail(@debug.to_string(error))
      }
    }
    assert_eq(summary.completed_partitions(), 1)
    assert_eq(summary.failed_partitions(), 0)
    assert_eq(summary.sent_batches(), 1)
    assert_eq(summary.outputs().length(), 1)
    assert_true(@fs.exists(directory + "/manifest.wal"))
    assert_true(@fs.exists(directory + "/output-p0.wal"))
  })
}

///|
async test "high-level cluster runner routes an explicit exchange edge" {
  let directory = @fs.tmpdir(prefix="moonweave-api-cluster-exchange")
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
  @async_runtime.with_timeout(30000, () => {
    let route = @plan.ExchangeRoute::new(0, 1)
    let compiled = JobDefinition::new(
        "cluster-api-exchange",
        OperatorRef::reachability(),
      )
      .with_partition_count(2)
      .with_workers(["worker-a", "worker-b"])
      .with_routes([route])
      .compile()
    let key = if compiled.partition_for_key("exchange-a") == 0 {
      "exchange-a"
    } else {
      "exchange-b"
    }
    let event = Event::new(
      @causal.EventId::new("cluster-exchange", 0),
      [],
      key,
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    assert_eq(compiled.partition_for_key(key), 0)
    let config = ClusterConfig::new(
      "cluster-api-exchange", "coordinator", directory,
    )
    let summary = compiled.run_local_cluster(config, [event])
    assert_eq(summary.completed_partitions(), 2)
    assert_eq(summary.failed_partitions(), 0)
    assert_eq(summary.sent_batches(), 1)
    assert_true(summary.outputs().length() >= 2)
  })
}

///|
fn distinct_key(job : CompiledJob, first : String) -> String {
  let candidates = ["store-b", "store-c", "store-d", "store-e"]
  let mut selected = candidates[0]
  for candidate in candidates {
    if job.partition_for_key(first) != job.partition_for_key(candidate) {
      selected = candidate
      break
    }
  }
  selected
}

///|
async test "high-level cluster runner computes keyed totals on two workers" {
  let directory = @fs.tmpdir(prefix="moonweave-api-keyed-sum")
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
  @async_runtime.with_timeout(30000, () => {
    let compiled = JobDefinition::new("sales-totals", OperatorRef::keyed_sum())
      .with_partition_count(2)
      .with_workers(["worker-a", "worker-b"])
      .with_sink(SinkSpec::durable("sales-output"))
      .compile()
    let first_key = "store-a"
    let second_key = distinct_key(compiled, first_key)
    assert_true(
      compiled.partition_for_key(first_key) !=
      compiled.partition_for_key(second_key),
    )
    let events = [
      Event::new(
        @causal.EventId::new("sales", 0),
        [],
        first_key,
        Json::object({ "value": Json::string("120") }),
      ),
      Event::new(
        @causal.EventId::new("sales", 1),
        [],
        second_key,
        Json::object({ "value": Json::string("50") }),
      ),
      Event::new(
        @causal.EventId::new("sales", 2),
        [],
        first_key,
        Json::object({ "value": Json::string("80") }),
      ),
      Event::new(
        @causal.EventId::new("sales", 3),
        [],
        second_key,
        Json::object({ "value": Json::string("70") }),
      ),
    ]
    let config = ClusterConfig::new("sales-cluster", "coordinator", directory)
    let summary = compiled.run_local_cluster(config, events) catch {
      error => fail(@debug.to_string(error))
    }
    assert_eq(summary.completed_partitions(), 2)
    assert_eq(summary.failed_partitions(), 0)
    assert_eq(summary.sent_batches(), 2)
    assert_eq(summary.outputs(), [
      Output::KeyedSum(@aggregation.KeyedSum::new(first_key, 120L)),
      Output::KeyedSum(@aggregation.KeyedSum::new(first_key, 200L)),
      Output::KeyedSum(@aggregation.KeyedSum::new(second_key, 50L)),
      Output::KeyedSum(@aggregation.KeyedSum::new(second_key, 120L)),
    ])
  })
}
