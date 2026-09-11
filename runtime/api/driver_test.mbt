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
      ).with_auth_token("driver-secret"),
    )
    let endpoint = listener.endpoint()
    defer listener.close()
    @async_runtime.with_task_group <| group => {
      let server = group.spawn() <| () => {
        listener.run_once(command => store.handle(command))
      }
      let client = DriverClient::connect(
        DriverConfig::new("driver-cluster", "coordinator", "driver-1", endpoint).with_auth_token(
          "driver-secret",
        ),
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
async test "driver control rejects a missing authentication token" {
  let directory = @fs.tmpdir(prefix="moonweave-driver-auth")
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
        "driver-auth-cluster",
        "coordinator",
        NodeEndpoint::new("127.0.0.1", 0),
      ).with_auth_token("driver-secret"),
    )
    let endpoint = listener.endpoint()
    defer listener.close()
    @async_runtime.with_task_group <| group => {
      let server = group.spawn(allow_failure=true) <| () => {
        try listener.run_once(command => store.handle(command)) catch {
          error => Err(@debug.to_string(error))
        } noraise {
          _ => Ok(())
        }
      }
      let client = DriverClient::connect(
        DriverConfig::new(
          "driver-auth-cluster", "coordinator", "driver-1", endpoint,
        ),
      )
      let submission = JobDefinition::new(
          "driver-auth-job",
          OperatorRef::reachability(),
        )
        .compile()
        .submission_with_id("driver-auth-submission")
      try client.submit(submission) catch {
        ApiError::Cluster(_) => ()
        _ => fail("expected missing Driver token to close the session")
      } noraise {
        _ => fail("expected missing Driver token to close the session")
      }
      client.close()
      ignore(server.wait())
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

///|
test "driver authentication configuration rejects empty secrets" {
  let endpoint = NodeEndpoint::new("127.0.0.1", 7000)
  let driver = DriverConfig::new("cluster", "coordinator", "driver", endpoint)
  let control = DriverControlConfig::new("cluster", "coordinator", endpoint)
  try driver.with_auth_token("") catch {
    ApiError::Cluster(message) => assert_true(message.contains("token"))
    _ => fail("expected empty Driver token rejection")
  } noraise {
    _ => fail("expected empty Driver token rejection")
  }
  try control.with_auth_token("") catch {
    ApiError::Cluster(message) => assert_true(message.contains("token"))
    _ => fail("expected empty Driver token rejection")
  } noraise {
    _ => fail("expected empty Driver token rejection")
  }
  let configured_driver = driver.with_auth_token("shared-secret")
  let configured_control = control.with_auth_token("shared-secret")
  assert_eq(configured_driver.auth_token(), Some("shared-secret"))
  assert_eq(configured_control.auth_token(), Some("shared-secret"))
}
