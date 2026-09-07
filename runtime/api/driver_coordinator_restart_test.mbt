///|
async test "DriverCoordinator resumes a persisted draining submission" {
  let root = @fs.tmpdir(prefix="moonweave-driver-restart")
  let coordinator_dir = root + "/coordinator"
  let worker_dir = root + "/worker"
  @fs.mkdir(coordinator_dir, recursive=true)
  @fs.mkdir(worker_dir, recursive=true)
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(30000, () => {
    let job = JobDefinition::new("restart-job", OperatorRef::reachability())
      .with_workers(["worker-a"])
      .compile()
    let store_path = coordinator_dir + "/driver.wal"
    let first_store = DriverStore::open(store_path)
    let submission = job.submission_with_id("restart-submission")
    let event = Event::new(
      @causal.EventId::new("restart-source", 0),
      [],
      "restart-key",
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    ignore(first_store.submit(submission))
    ignore(first_store.input([event]))
    assert_eq(first_store.drain().phase(), JobRunPhase::Draining)
    first_store.close()
    let restored_store = DriverStore::open(store_path)
    let coordinator = DriverCoordinator::bind(
      CoordinatorNodeConfig::new(
        "restart-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
        coordinator_dir,
        [WorkerSlot::new("worker-a", "restart-worker-1")],
      ),
      DriverControlConfig::new(
        "restart-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
      restored_store,
    )
    let data_endpoint = coordinator.data_endpoint()
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
              "restart-cluster", "coordinator", data_endpoint, "worker-a", "restart-worker-1",
              worker_dir,
            ),
          )
        catch {
          error => Err(@debug.to_string(error))
        } noraise {
          value => Ok(value)
        }
      }
      let result = coordinator_task.wait()
      let worker = worker_task.wait()
      guard result is Ok(summary) else { fail("restart execution failed") }
      guard worker is Ok(worker_summary) else { fail("restart Worker failed") }
      assert_eq(summary.status().phase(), JobRunPhase::Succeeded)
      assert_eq(worker_summary.live_batches(), 1)
    }
  })
}
