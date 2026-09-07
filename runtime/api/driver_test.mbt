///|
async test "driver client and control listener exchange durable commands over TCP" {
  let directory = @fs.tmpdir(prefix="moonweave-driver-control")
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
    defer store.close()
    let listener = DriverControlListener::bind(
      DriverControlConfig::new(
        "driver-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ),
    )
    let endpoint = listener.endpoint()
    defer listener.close()
    @async_runtime.with_task_group <| group => {
      let server = group.spawn() <| () => {
        listener.run_once(command => store.handle(command))
      }
      let client = DriverClient::connect(
        DriverConfig::new("driver-cluster", "coordinator", "driver-1", endpoint),
      )
      let submission = JobDefinition::new(
          "driver-control-job",
          OperatorRef::reachability(),
        )
        .compile()
        .submission_with_id("driver-control-submission")
      let submitted = client.submit(submission)
      assert_eq(submitted.phase(), JobRunPhase::Running)
      let event = Event::new(
        @causal.EventId::new("driver-control-source", 0),
        [],
        "account",
        Json::object({
          "source": Json::string("A"),
          "target": Json::string("B"),
        }),
      )
      assert_eq(client.input([event]).accepted_events(), 1L)
      assert_eq(client.status().accepted_events(), 1L)
      assert_eq(client.drain().phase(), JobRunPhase::Draining)
      client.close()
      server.wait()
    }
  })
}

///|
test "driver control configuration rejects unsafe endpoint and incarnation" {
  try
    DriverConfig::new(
      "cluster",
      "coordinator",
      "driver",
      NodeEndpoint::new("127.0.0.1", 0),
    )
  catch {
    ApiError::InvalidEndpoint(_) => ()
    _ => fail("expected port zero rejection")
  } noraise {
    _ => fail("expected port zero rejection")
  }
  let config = DriverConfig::new(
    "cluster",
    "coordinator",
    "driver",
    NodeEndpoint::new("127.0.0.1", 7000),
  )
  try config.with_incarnation("boot") catch {
    ApiError::Cluster(_) => ()
    _ => fail("expected invalid Driver incarnation")
  } noraise {
    _ => fail("expected invalid Driver incarnation")
  }
}
