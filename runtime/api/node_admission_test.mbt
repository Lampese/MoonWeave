///|
fn admission_capabilities(job : CompiledJob) -> Array[String] {
  [
    job.operator().id(),
    job.operator_descriptor().capability(),
    "causal-frontier",
    job.operator_descriptor().code_digest(),
  ]
}

///|
fn admission_hello(
  job : CompiledJob,
  partition : Int,
  token? : String,
  incarnation? : String = "boot-worker",
  worker? : String = "worker",
  capabilities? : Array[String],
) -> @protocol.Frame raise {
  let hello = @protocol.Hello::new(
    worker,
    incarnation,
    capabilities.unwrap_or(admission_capabilities(job)),
    requested_partition=partition,
    auth_token?=token,
  )
  @protocol.FrameBuilder::new(job.job_id(), worker, incarnation).build(
    @protocol.FrameKind::Hello,
    hello.to_payload(),
  )
}

///|
async fn assert_node_rejects_hello(
  endpoint : NodeEndpoint,
  frame : @protocol.Frame,
) -> Unit {
  @async_runtime.with_timeout(1000, () => {
    let connection = @socket.Tcp::connect_to_host(
      endpoint.host(),
      port=endpoint.port(),
    )
    defer connection.close()
    @runtime.write_frame(connection, frame)
    try @runtime.read_frame(connection) catch {
      @runtime.FrameIoError::Io(_) => ()
      error => fail("unexpected rejection: " + @debug.to_string(error))
    } noraise {
      _ => fail("unadmitted peer received a response")
    }
  })
}

///|
async test "Coordinator rejects invalid peers without consuming finite Worker slots" {
  let root = @fs.tmpdir(prefix="moonweave-node-admission")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let job = JobDefinition::new("node-admission", OperatorRef::keyed_sum())
      .with_partition_count(2)
      .with_workers(["worker"])
      .compile()
    let listener = CoordinatorNode::bind(
      CoordinatorNodeConfig::new(
        job.job_id(),
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        root + "/coordinator",
        [WorkerSlot::new("worker", "boot-worker")],
      ).with_auth_token("secret"),
    )
    let endpoint = listener.endpoint()
    @async_runtime.with_task_group <| group => {
      let coordinator = group.spawn() <| () => { listener.run(job, []) }
      let abandoned = @socket.Tcp::connect_to_host(
        endpoint.host(),
        port=endpoint.port(),
      )
      abandoned.close()
      let malformed = @protocol.FrameBuilder::new(
        job.job_id(),
        "worker",
        "boot-worker",
      ).build(@protocol.FrameKind::Hello, "{}")
      let wrong_kind = @protocol.FrameBuilder::new(
        job.job_id(),
        "worker",
        "boot-worker",
      ).build(
        @protocol.FrameKind::Credit,
        admission_hello(job, 0, token="secret").payload(),
      )
      for
        frame in [
          admission_hello(job, 0),
          admission_hello(job, 0, token="wrong"),
          admission_hello(job, 0, token="secret", incarnation="stale"),
          admission_hello(job, 0, token="secret", capabilities=[]),
          admission_hello(job, 0, token="secret", worker="unknown"),
          admission_hello(job, 9, token="secret"),
          malformed,
          wrong_kind,
        ] {
        assert_node_rejects_hello(endpoint, frame)
        assert_true(coordinator.try_wait() is None)
      }
      let worker = job.run_worker_node_set(
        WorkerNodeSetConfig::new(
          job.job_id(),
          "coordinator",
          endpoint,
          "worker",
          "boot-worker",
          root + "/worker",
        ).with_auth_token("secret"),
      )
      let summary = coordinator.wait()
      assert_eq(summary.failed_partitions(), 0)
      assert_eq(summary.completed_partitions(), 2)
      assert_eq(summary.failures(), [])
      assert_eq(worker.completed_tasks(), 2)
      assert_eq(worker.failed_tasks(), 0)
    }
    let saved = @runtime.JobStore::open(
      job.spec,
      root + "/coordinator/job.wal",
      checkpoint_path=root + "/coordinator/job.json",
    )
    defer saved.close()
    assert_eq(saved.phase(), @control.JobPhase::Succeeded)
  })
}

///|
async test "finite Coordinator entry rejects Kafka sources explicitly" {
  let root = @fs.tmpdir(prefix="moonweave-coordinator-kafka-entry")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let source = KafkaSourceSpec::new(["broker-a:9092"], "events", "workers", 0)
    let job = JobDefinition::new(
        "coordinator-kafka-entry",
        OperatorRef::keyed_sum(),
      )
      .with_workers(["worker"])
      .with_source(SourceSpec::kafka(source))
      .compile()
    let listener = CoordinatorNode::bind(
      CoordinatorNodeConfig::new(
        job.job_id(),
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        root,
        [WorkerSlot::new("worker", "boot-worker")],
      ),
    )
    defer listener.close()
    try listener.run(job, []) catch {
      ApiError::Source(message) => assert_true(message.contains("Kafka"))
      _ => fail("expected Kafka source entry-point rejection")
    } noraise {
      _ => fail("expected Kafka source entry-point rejection")
    }
  })
}

///|
async test "invalid and duplicate live peers leave running sessions and Job state intact" {
  let root = @fs.tmpdir(prefix="moonweave-live-admission")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(10000, () => {
    let job = JobDefinition::new("live-admission", OperatorRef::keyed_sum())
      .with_partition_count(2)
      .with_workers(["worker"])
      .compile()
    let coordinator = DriverCoordinator::bind(
      CoordinatorNodeConfig::new(
        job.job_id(),
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        root,
        [WorkerSlot::new("worker", "boot-worker")],
      ).with_auth_token("secret"),
      DriverControlConfig::new(
        job.job_id(),
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
      DriverStore::open(root + "/driver.wal"),
    )
    let inputs : Array[Event] = []
    for partition in 0..<2 {
      for index in 0..<1000 {
        let key = "key-" + index.to_string()
        if job.partition_for_key(key) == partition {
          inputs.push(
            Event::keyed_sum(
              @causal.EventId::new("input", partition.to_int64()),
              key,
              10L,
            ),
          )
          break
        }
      }
    }
    assert_eq(inputs.length(), 2)
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { coordinator.run() }
      let handle = job
        .submission_with_id("admission-submit")
        .run_remote(
          DriverConfig::new(
            job.job_id(),
            "coordinator",
            "driver",
            coordinator.control_endpoint(),
          ),
        )
      defer handle.close()
      ignore(handle.input(inputs))
      let first = group.spawn() <| () => {
        job.run_worker_node(
          WorkerNodeConfig::new_auto_for_partition(
            job.job_id(),
            "coordinator",
            coordinator.data_endpoint(),
            "worker",
            "boot-worker",
            root + "/worker",
            0,
          ).with_auth_token("secret"),
        )
      }
      ignore(wait_live_checkpoint(root + "/worker/worker-p0.json", 0L))
      for
        frame in [
          admission_hello(job, 0, token="wrong"),
          admission_hello(job, 1),
          admission_hello(job, 1, token="secret", incarnation="stale"),
          admission_hello(job, 1, token="secret", capabilities=[]),
          admission_hello(job, 0, token="secret"),
        ] {
        assert_node_rejects_hello(coordinator.data_endpoint(), frame)
        assert_eq(handle.refresh().phase(), JobRunPhase::Running)
        assert_true(first.try_wait() is None)
        assert_true(runner.try_wait() is None)
      }
      let second = group.spawn() <| () => {
        job.run_worker_node(
          WorkerNodeConfig::new_auto_for_partition(
            job.job_id(),
            "coordinator",
            coordinator.data_endpoint(),
            "worker",
            "boot-worker",
            root + "/worker",
            1,
          ).with_auth_token("secret"),
        )
      }
      ignore(wait_live_checkpoint(root + "/worker/worker-p1.json", 0L))
      assert_eq(handle.drain().phase(), JobRunPhase::Succeeded)
      assert_eq(first.wait().live_batches(), 1)
      assert_eq(second.wait().live_batches(), 1)
      let summary = runner.wait().execution().unwrap()
      assert_eq(summary.failed_partitions(), 0)
      assert_eq(summary.completed_partitions(), 2)
    }
  })
}
