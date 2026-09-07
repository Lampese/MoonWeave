///|
async test "driver coordinator binds both endpoints before a submission" {
  let directory = @fs.tmpdir(prefix="moonweave-driver-coordinator")
  let store_path = directory + "/driver.wal"
  defer {
    @fs.remove(store_path) catch {
      _ => ()
    }
    @fs.remove(store_path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  @async_runtime.with_timeout(30000, () => {
    let store = DriverStore::open(store_path)
    let data_config = CoordinatorNodeConfig::new(
      "driver-coordinator-cluster",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
      directory + "/data",
      [WorkerSlot::new("worker-a", "boot-a")],
    )
    let control_config = DriverControlConfig::new(
      "driver-coordinator-cluster",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
    )
    let coordinator = DriverCoordinator::bind(
      data_config, control_config, store,
    )
    assert_true(coordinator.data_endpoint().port() > 0)
    assert_true(coordinator.control_endpoint().port() > 0)
    assert_true(
      coordinator.data_endpoint().port() !=
      coordinator.control_endpoint().port(),
    )
    coordinator.close()
  })
}
