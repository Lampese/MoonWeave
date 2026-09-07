///|
async test "Coordinator fences a disconnected Worker and recovers on a replacement lease" {
  let root = @fs.tmpdir(prefix="moonweave-node-recovery")
  let coordinator_dir = root + "/coordinator"
  let worker_dir = root + "/worker-b"
  @fs.mkdir(coordinator_dir, recursive=true)
  @fs.mkdir(worker_dir, recursive=true)
  defer (@fs.rmdir(root, recursive=true) catch { _ => () })
  @async_runtime.with_timeout(30000, () => {
    let job = JobDefinition::new("recovery-job", OperatorRef::reachability())
      .with_workers(["worker-a", "worker-b"])
      .compile()
    let listener = CoordinatorNode::bind(
      CoordinatorNodeConfig::new(
        "recovery-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        coordinator_dir,
        [
          WorkerSlot::new("worker-a", "recovery-a-1"),
          WorkerSlot::new("worker-b", "recovery-b-1"),
        ],
      ),
    )
    let endpoint = listener.endpoint()
    let event = Event::new(
      @causal.EventId::new("recovery-source", 0),
      [],
      "recovery-key",
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    @async_runtime.with_task_group <| group => {
      let coordinator = group.spawn(allow_failure=true) <| () => {
        try listener.run(job, [event]) catch {
          error => Err(@debug.to_string(error))
        } noraise {
          summary => Ok(summary)
        }
      }
      let failed_worker = group.spawn(allow_failure=true) <| () => {
        try {
          let connection = @socket.Tcp::connect_to_host(
            endpoint.host(),
            port=endpoint.port(),
          )
          let capabilities = [
            job.operator().id(),
            job.operator_descriptor().capability(),
            "causal-frontier",
            job.operator_descriptor().code_digest(),
          ]
          ignore(
            @runtime.worker_handshake(
              connection,
              "recovery-cluster",
              "coordinator",
              "worker-a",
              "recovery-a-1",
              capabilities,
              requested_partition=0,
            ),
          )
          connection.close()
        } catch {
          _ => ()
        }
      }
      failed_worker.wait()
      let replacement = group.spawn(allow_failure=true) <| () => {
        try
          Ok(
            job.run_worker_node(
              WorkerNodeConfig::new_auto_for_partition(
                "recovery-cluster", "coordinator", endpoint, "worker-b", "recovery-b-1",
                worker_dir, 0,
              ),
            ),
          )
        catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => value
        }
      }
      let result = coordinator.wait()
      let worker = replacement.wait()
      guard result is Ok(summary) else { fail("Coordinator recovery failed") }
      assert_eq(summary.completed_partitions(), 1)
      assert_eq(summary.failed_partitions(), 0)
      guard worker is Ok(worker_summary) else {
        fail("replacement Worker failed")
      }
      assert_eq(worker_summary.partition(), 0)
      assert_eq(worker_summary.live_batches(), 1)
      assert_eq(worker_summary.state_events(), 1)
    }
  })
}
