///|
async test "coordinator assigns two remote worker nodes over TCP" {
  let coordinator_dir = @fs.tmpdir(prefix="moonweave-node-coordinator")
  let worker_a_dir = @fs.tmpdir(prefix="moonweave-node-worker-a")
  let worker_b_dir = @fs.tmpdir(prefix="moonweave-node-worker-b")
  defer {
    for name in @fs.readdir(coordinator_dir) {
      @fs.remove(coordinator_dir + "/" + name) catch {
        _ => ()
      }
    }
    @fs.rmdir(coordinator_dir) catch {
      _ => ()
    }
    for directory in [worker_a_dir, worker_b_dir] {
      for name in @fs.readdir(directory) {
        @fs.remove(directory + "/" + name) catch {
          _ => ()
        }
      }
      @fs.rmdir(directory) catch {
        _ => ()
      }
    }
  }
  @async_runtime.with_timeout(30000, () => {
    let job = JobDefinition::new("node-job", OperatorRef::reachability())
      .with_partition_count(2)
      .with_workers(["worker-a", "worker-b"])
      .compile()
    let coordinator_config = CoordinatorNodeConfig::new(
      "node-cluster",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
      coordinator_dir,
      [
        WorkerSlot::new("worker-a", "node-worker-a-1"),
        WorkerSlot::new("worker-b", "node-worker-b-1"),
      ],
    )
    let listener = CoordinatorNode::bind(coordinator_config)
    let endpoint = listener.endpoint()
    let first = Event::new(
      @causal.EventId::new("node-input", 0),
      [],
      "key-a",
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    let second = Event::new(
      @causal.EventId::new("node-input", 1),
      [],
      "key-b",
      Json::object({ "source": Json::string("X"), "target": Json::string("Y") }),
    )
    @async_runtime.with_task_group <| group => {
      let coordinator_task = group.spawn() <| () => {
        listener.run(job, [first, second])
      }
      let worker_a = group.spawn() <| () => {
        job.run_worker_node(
          WorkerNodeConfig::new(
            "node-cluster", "coordinator", endpoint, "worker-a", "node-worker-a-1",
            worker_a_dir, 0,
          ),
        )
      }
      let worker_b = group.spawn() <| () => {
        job.run_worker_node(
          WorkerNodeConfig::new(
            "node-cluster", "coordinator", endpoint, "worker-b", "node-worker-b-1",
            worker_b_dir, 1,
          ),
        )
      }
      let first_summary = worker_a.wait()
      let second_summary = worker_b.wait()
      let coordinator = coordinator_task.wait()
      assert_eq(first_summary.worker_id(), "worker-a")
      assert_eq(second_summary.worker_id(), "worker-b")
      assert_eq(first_summary.live_batches(), 1)
      assert_eq(second_summary.live_batches(), 1)
      assert_eq(first_summary.state_schema(), "reachability-state:v1")
      assert_eq(coordinator.completed_partitions(), 2)
      assert_eq(coordinator.failed_partitions(), 0)
      assert_eq(coordinator.sent_batches(), 2)
    }
  })
}

///|
test "node configuration validates remote endpoints" {
  try NodeEndpoint::new("", 7000) catch {
    ApiError::InvalidEndpoint(_) => ()
    _ => fail("expected empty endpoint rejection")
  } noraise {
    _ => fail("expected empty endpoint rejection")
  }
  try NodeEndpoint::new("127.0.0.1", 70000) catch {
    ApiError::InvalidPort(port) => assert_eq(port, 70000)
    _ => fail("expected invalid port rejection")
  } noraise {
    _ => fail("expected invalid port rejection")
  }
  try
    WorkerNodeConfig::new(
      "cluster",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
      "worker",
      "incarnation",
      "/tmp/worker",
      0,
    )
  catch {
    ApiError::InvalidEndpoint(_) => ()
    _ => fail("expected port zero connection rejection")
  } noraise {
    _ => fail("expected port zero connection rejection")
  }
  let automatic = WorkerNodeConfig::new_auto(
    "cluster",
    "coordinator",
    NodeEndpoint::new("127.0.0.1", 7000),
    "worker",
    "incarnation",
    "/tmp/worker",
  )
  assert_eq(automatic.partition(), -1)
  assert_eq(automatic.epoch(), 0)
}

///|
async test "wildcard Coordinator bind requires an advertised endpoint" {
  let base = CoordinatorNodeConfig::new(
    "cluster",
    "coordinator",
    NodeEndpoint::new("0.0.0.0", 0),
    "/tmp/moonweave-coordinator-node-test",
    [WorkerSlot::new("worker", "incarnation")],
  )
  try CoordinatorNode::bind(base) catch {
    ApiError::InvalidEndpoint(_) => ()
    _ => fail("expected wildcard bind to require advertised endpoint")
  } noraise {
    _ => fail("expected wildcard bind to require advertised endpoint")
  }
  let listener = CoordinatorNode::bind(
    base.with_advertised_endpoint(NodeEndpoint::new("127.0.0.1", 0)),
  )
  let endpoint = listener.endpoint()
  assert_eq(endpoint.host(), "127.0.0.1")
  assert_true(endpoint.port() > 0)
  listener.close()
}
