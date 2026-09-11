///|
async fn with_driver_store(f : async (DriverStore, String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-driver-store")
  let path = directory + "/driver.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.remove(path + ".compact") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let store = DriverStore::open(path)
  defer store.close()
  f(store, path)
}

///|
async fn with_limited_driver_store(
  maximum_events : Int,
  f : async (DriverStore) -> Unit,
) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-driver-limit")
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
  let store = DriverStore::open(path, maximum_events~)
  defer store.close()
  f(store)
}

///|
fn driver_submission() -> JobSubmission raise ApiError {
  JobDefinition::new("driver-job", OperatorRef::reachability())
  .compile()
  .submission_with_id("driver-submission")
}

///|
fn static_driver_registry() -> OperatorRegistry raise ApiError {
  let custom = @task.CustomOperator::new(
    "driver-static-custom",
    1,
    "custom:driver-static:v1",
    "driver-static-state:v1",
    "json-event:v1",
    "json-output:v1",
    "moonweave-task-json-v1",
    "driver-static-deps:v1",
    true,
    false,
    _ => (),
    input => [Json::object({ "value": input.event().payload() })],
  ) catch {
    error => raise ApiError::InvalidArtifact(@debug.to_string(error))
  }
  let registry = OperatorRegistry::new()
  registry.register_custom(custom)
  registry
}

///|
fn static_driver_submission(
  registry : OperatorRegistry,
) -> JobSubmission raise ApiError {
  JobDefinition::new(
    "driver-static-custom-job",
    OperatorRef::new("driver-static-custom", 1),
  )
  .compile(registry~)
  .submission_with_id("driver-static-submission")
}

///|
fn driver_event(sequence : Int64) -> Event raise ApiError {
  Event::new(
    @causal.EventId::new("driver-source", sequence) catch {
      error => raise ApiError::InvalidEvent(@debug.to_string(error))
    },
    [],
    "account-1",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
}

///|
async test "driver input rejects growth beyond the configured event budget" {
  with_limited_driver_store(1, store => {
    ignore(store.submit(driver_submission()))
    ignore(store.input([driver_event(0L)]))
    try store.input([driver_event(1L)]) catch {
      ApiError::StateLimitExceeded(limit~, actual~) => {
        assert_eq(limit, 1)
        assert_eq(actual, 2)
      }
      _ => fail("expected Driver event budget rejection")
    } noraise {
      _ => fail("Driver accepted input beyond its event budget")
    }
    assert_eq(store.accepted_events(), 1L)
  })
}

///|
async test "driver store persists submission input lifecycle and idempotent retries" {
  with_driver_store <| ((store, path) => {
    let submission = driver_submission()
    assert_eq(store.submit(submission).phase(), JobRunPhase::Running)
    assert_eq(store.submit(submission).phase(), JobRunPhase::Running)
    let first = driver_event(0)
    let second = driver_event(1)
    let input = store.input([first])
    assert_eq(input.accepted_events(), 1L)
    assert_eq(store.input([second]).accepted_events(), 2L)
    assert_eq(store.input([first, second]).accepted_events(), 2L)
    assert_eq(store.inputs(), [first, second])
    let before_compact = store.stats().record_count()
    store.compact()
    assert_true(store.stats().record_count() < before_compact)
    assert_eq(store.drain().phase(), JobRunPhase::Draining)
    assert_eq(store.drain().phase(), JobRunPhase::Draining)
    store.close()

    let restored = DriverStore::open(path)
    defer restored.close()
    assert_eq(restored.status().phase(), JobRunPhase::Draining)
    assert_eq(restored.status().accepted_events(), 2L)
    assert_eq(restored.inputs(), [first, second])
  })
}

///|
async test "driver store rejects input for Kafka source jobs without journaling it" {
  with_driver_store <| ((store, _) => {
    let source = KafkaSourceSpec::new(
      ["broker-a:9092"],
      "events",
      "moonweave-workers",
      0,
    )
    let submission = JobDefinition::new(
        "driver-kafka-source",
        OperatorRef::reachability(),
      )
      .with_source(SourceSpec::kafka(source))
      .compile()
      .submission_with_id("driver-kafka-submission")
    ignore(store.submit(submission))
    let records_before = store.stats().record_count()
    try store.input([driver_event(0)]) catch {
      ApiError::InvalidDriverCommand(message) =>
        assert_true(message.contains("Kafka source"))
      _ => fail("expected Kafka source Driver input rejection")
    } noraise {
      _ => fail("Kafka source accepted Driver input")
    }
    assert_eq(store.accepted_events(), 0L)
    assert_eq(store.inputs(), [])
    assert_eq(store.stats().record_count(), records_before)
  })
}

///|
async test "driver store reopens static custom submissions with their registry" {
  let directory = @fs.tmpdir(prefix="moonweave-driver-static-custom")
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
  let registry = static_driver_registry()
  let submission = static_driver_submission(registry)
  let store = DriverStore::open(path, registry~)
  ignore(store.submit(submission))
  store.close()
  let restored = DriverStore::open(path, registry~)
  guard restored.submission() is Some(value) else {
    fail("custom submission was not restored")
  }
  assert_eq(value, submission)
  assert_eq(restored.status().phase(), JobRunPhase::Running)
  restored.close()
  try DriverStore::open(path) catch {
    ApiError::UnsupportedOperator(id) => assert_eq(id, "driver-static-custom")
    _ => fail("expected default registry to reject custom submission")
  } noraise {
    _ => fail("expected default registry to reject custom submission")
  }
}

///|
async test "driver store rejects conflict and preserves state across rejected commands" {
  with_driver_store <| ((store, _) => {
    let submission = driver_submission()
    ignore(store.submit(submission))
    let original = driver_event(0)
    ignore(store.input([original]))
    let conflicting = Event::new(
      @causal.EventId::new("driver-source", 0) catch {
        error => raise ApiError::InvalidEvent(@debug.to_string(error))
      },
      [],
      "account-1",
      Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
    )
    try store.input([conflicting]) catch {
      ApiError::RequestConflict(_) => ()
      _ => fail("expected input identity conflict")
    } noraise {
      _ => fail("expected input identity conflict")
    }
    assert_eq(store.status().accepted_events(), 1L)
    assert_eq(store.inputs(), [original])
    ignore(store.cancel())
    assert_eq(store.cancel().phase(), JobRunPhase::Cancelled)
  })
}

///|
async test "driver store handles wire commands without changing lifecycle on rejection" {
  with_driver_store <| ((store, _) => {
    let rejected = store.handle(JobCommand::input([]))
    assert_true(rejected is Err(_))
    assert_eq(store.phase(), JobRunPhase::Created)
    let submitted = store.handle(JobCommand::submit(driver_submission()))
    assert_true(submitted is Ok(_))
    let status = store.handle(JobCommand::status())
    guard status is Ok(value) else { fail("status should succeed") }
    assert_eq(value.phase(), JobRunPhase::Running)
  })
}
