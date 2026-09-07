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
fn driver_submission() -> JobSubmission raise ApiError {
  JobDefinition::new("driver-job", OperatorRef::reachability())
  .compile()
  .submission_with_id("driver-submission")
}

///|
fn driver_event(sequence : Int) -> Event raise ApiError {
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
