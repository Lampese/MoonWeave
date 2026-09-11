///|
async fn remove_driver_directory(directory : String) -> Unit {
  @fs.rmdir(directory, recursive=true)
}

///|
async test "DriverCoordinator executes a submitted Job with a waiting Worker" {
  let root = @fs.tmpdir(prefix="moonweave-driver-e2e")
  let coordinator_dir = root + "/coordinator"
  let worker_dir = root + "/worker"
  @fs.mkdir(coordinator_dir, recursive=true)
  @fs.mkdir(worker_dir, recursive=true)
  defer remove_driver_directory(root)
  @async_runtime.with_timeout(30000, () => {
    let job = JobDefinition::new("driver-e2e-job", OperatorRef::reachability())
      .with_workers(["worker-a"])
      .compile()
    let store = DriverStore::open(coordinator_dir + "/driver.wal")
    let coordinator = DriverCoordinator::bind(
      CoordinatorNodeConfig::new(
        "driver-e2e-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        coordinator_dir,
        [WorkerSlot::new("worker-a", "driver-worker-1")],
      ),
      DriverControlConfig::new(
        "driver-e2e-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
      store,
    )
    let data_endpoint = coordinator.data_endpoint()
    let control_endpoint = coordinator.control_endpoint()
    @async_runtime.with_task_group <| group => {
      let coordinator_task = group.spawn(allow_failure=true) <| () => {
        try coordinator.run() catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => Ok(value)
        }
      }
      let worker_task = group.spawn(allow_failure=true) <| () => {
        try
          job.run_worker_node(
            WorkerNodeConfig::new_auto(
              "driver-e2e-cluster", "coordinator", data_endpoint, "worker-a", "driver-worker-1",
              worker_dir,
            ),
          )
        catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => Ok(value)
        }
      }
      let client = DriverClient::connect(
        DriverConfig::new(
          "driver-e2e-cluster", "coordinator", "driver-e2e", control_endpoint,
        ),
      )
      let submission = job.submission_with_id("driver-e2e-submission")
      assert_eq(client.submit(submission).phase(), JobRunPhase::Running)
      let event = Event::new(
        @causal.EventId::new("driver-e2e-source", 0),
        [],
        "account",
        Json::object({
          "source": Json::string("A"),
          "target": Json::string("B"),
        }),
      )
      assert_eq(client.input([event]).accepted_events(), 1L)
      assert_eq(client.drain().phase(), JobRunPhase::Succeeded)
      client.close()
      let execution = coordinator_task.wait()
      let worker = worker_task.wait()
      guard execution is Ok(summary) else {
        fail("Coordinator execution failed")
      }
      guard worker is Ok(worker_summary) else {
        fail("Worker execution failed")
      }
      assert_eq(summary.status().phase(), JobRunPhase::Succeeded)
      guard summary.execution() is Some(execution_summary) else {
        fail("missing Coordinator execution summary")
      }
      assert_eq(execution_summary.completed_partitions(), 1)
      assert_eq(worker_summary.live_batches(), 1)
      assert_eq(worker_summary.state_events(), 1)
      assert_eq(worker_summary.pending_events(), 0)
      let restored_driver = DriverStore::open(coordinator_dir + "/driver.wal")
      assert_eq(restored_driver.phase(), JobRunPhase::Succeeded)
      assert_eq(restored_driver.accepted_events(), 1L)
      assert_eq(restored_driver.inputs(), [])
      restored_driver.close()
      let restored_source = @runtime.SourceLog::open(
        coordinator_dir + "/source.wal",
      )
      assert_eq(restored_source.event_count(), 0)
      assert_eq(restored_source.next_offset(0), Some(1L))
      restored_source.close()
    }
  })
}
