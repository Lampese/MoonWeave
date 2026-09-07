///|
async test "remote job handle hides Driver transport details" {
  let directory = @fs.tmpdir(prefix="moonweave-remote-handle")
  let path = directory + "/driver.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  @async_runtime.with_timeout(30000, () => {
    let store = DriverStore::open(path)
    defer store.close()
    let listener = DriverControlListener::bind(
      DriverControlConfig::new(
        "remote-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
    )
    defer listener.close()
    let endpoint = listener.endpoint()
    @async_runtime.with_task_group <| group => {
      let server = group.spawn() <| () => {
        listener.run_once(command => store.handle(command))
      }
      let submission = JobDefinition::new(
          "remote-job",
          OperatorRef::reachability(),
        )
        .compile()
        .submission_with_id("remote-submission")
      let handle = submission.run_remote(
        DriverConfig::new(
          "remote-cluster", "coordinator", "driver-remote", endpoint,
        ),
      )
      assert_eq(handle.status().phase(), JobRunPhase::Running)
      let event = Event::new(
        @causal.EventId::new("remote-source", 0),
        [],
        "key",
        Json::object({
          "source": Json::string("A"),
          "target": Json::string("B"),
        }),
      )
      assert_eq(handle.input([event]).accepted_events(), 1L)
      assert_eq(handle.drain().phase(), JobRunPhase::Draining)
      assert_eq(handle.refresh().phase(), JobRunPhase::Draining)
      handle.close()
      server.wait()
    }
  })
}
