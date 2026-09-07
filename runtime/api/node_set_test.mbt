///|
async test "one Worker process runs every owned partition through independent TCP sessions" {
  let root = @fs.tmpdir(prefix="moonweave-node-set")
  let coordinator_dir = root + "/coordinator"
  let worker_dir = root + "/worker"
  @fs.mkdir(coordinator_dir, recursive=true)
  @fs.mkdir(worker_dir, recursive=true)
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(30000, () => {
    let job = JobDefinition::new("node-set-job", OperatorRef::reachability())
      .with_partition_count(2)
      .with_workers(["worker-a"])
      .compile()
    let first_key = "partition-0"
    let mut second_key = "partition-1"
    for index in 0..<1000 {
      if job.partition_for_key(first_key) == job.partition_for_key(second_key) {
        second_key = "partition-" + (index + 2).to_string()
      }
    }
    assert_true(
      job.partition_for_key(first_key) != job.partition_for_key(second_key),
    )
    let listener = CoordinatorNode::bind(
      CoordinatorNodeConfig::new(
        "node-set-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        coordinator_dir,
        [WorkerSlot::new("worker-a", "node-set-worker-1")],
      ),
    )
    let endpoint = listener.endpoint()
    let first = Event::new(
      @causal.EventId::new("node-set-source", 0),
      [],
      first_key,
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    let second = Event::new(
      @causal.EventId::new("node-set-source", 1),
      [],
      second_key,
      Json::object({ "source": Json::string("X"), "target": Json::string("Y") }),
    )
    @async_runtime.with_task_group <| group => {
      let coordinator = group.spawn(allow_failure=true) <| () => {
        try listener.run(job, [first, second]) catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => Ok(value)
        }
      }
      let worker = group.spawn(allow_failure=true) <| () => {
        try
          job.run_worker_node_set(
            WorkerNodeSetConfig::new(
              "node-set-cluster", "coordinator", endpoint, "worker-a", "node-set-worker-1",
              worker_dir,
            ),
          )
        catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => Ok(value)
        }
      }
      let coordinator_result = coordinator.wait()
      let worker_result = worker.wait()
      guard coordinator_result is Ok(summary) else {
        fail("Coordinator failed")
      }
      guard worker_result is Ok(worker_summary) else {
        fail("Worker TaskSet failed")
      }
      assert_eq(summary.completed_partitions(), 2)
      assert_eq(summary.failed_partitions(), 0)
      assert_eq(worker_summary.completed_tasks(), 2)
      assert_eq(worker_summary.failed_tasks(), 0)
      assert_eq(worker_summary.live_batches(), 2)
    }
  })
}
