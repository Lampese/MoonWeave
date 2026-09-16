///|
async test "Driver pause and resume are durable" {
  let root = @fs.tmpdir(prefix="moonweave-f4-control")
  let path = root + "/driver.wal"
  defer @fs.rmdir(root, recursive=true)
  let store = DriverStore::open(path)
  let submission = JobDefinition::new("f4-control", OperatorRef::reachability())
    .compile()
    .submission_with_id("f4-submit")
  ignore(store.submit(submission))
  ignore(store.pause())
  assert_true(store.is_paused())
  try store.input([]) catch {
    ApiError::InvalidDriverCommand(_) => ()
    _ => fail("paused Driver accepted input")
  } noraise {
    _ => fail("paused Driver accepted input")
  }
  store.close()
  let restored = DriverStore::open(path)
  defer restored.close()
  assert_true(restored.is_paused())
  ignore(restored.resume())
  assert_false(restored.is_paused())
}

///|
async test "Driver update rejects topology and state-schema changes atomically" {
  let root = @fs.tmpdir(prefix="moonweave-f4-update-reject")
  let path = root + "/driver.wal"
  defer @fs.rmdir(root, recursive=true)
  let store = DriverStore::open(path)
  let original = JobDefinition::new(
      "f4-update-reject",
      OperatorRef::keyed_sum(),
    )
    .with_partition_count(2)
    .with_workers(["worker-a", "worker-b"])
    .compile()
    .submission_with_id("f4-update-reject-submit")
  ignore(store.submit(original))
  ignore(store.pause())
  let incompatible = JobDefinition::new(
      "f4-update-reject",
      OperatorRef::reachability(),
    )
    .with_partition_count(2)
    .with_workers(["worker-a", "worker-b"])
    .compile()
    .submission_with_id("f4-update-reject-submit")
  try store.update(incompatible) catch {
    ApiError::InvalidDriverCommand(reason) =>
      assert_true(reason.contains("topology") || reason.contains("schema"))
    _ => fail("expected incompatible update rejection")
  } noraise {
    _ => fail("expected incompatible update rejection")
  }
  assert_true(store.is_paused())
  assert_eq(store.status().plan_digest(), original.job().plan_digest())
  store.close()
}

///|
async test "paused compatible Driver update survives WAL restart" {
  let root = @fs.tmpdir(prefix="moonweave-f4-update-restart")
  let path = root + "/driver.wal"
  defer @fs.rmdir(root, recursive=true)
  let original = JobDefinition::new(
      "f4-update-restart",
      OperatorRef::keyed_sum(),
    )
    .with_state_limit(8)
    .compile()
    .submission_with_id("f4-update-restart-submit")
  let updated = JobDefinition::new(
      "f4-update-restart",
      OperatorRef::keyed_sum(),
    )
    .with_state_limit(16)
    .compile()
    .submission_with_id("f4-update-restart-submit")
  let store = DriverStore::open(path)
  ignore(store.submit(original))
  ignore(store.pause())
  ignore(store.update(updated))
  assert_true(store.is_paused())
  assert_eq(store.status().plan_digest(), updated.job().plan_digest())
  store.close()
  let restored = DriverStore::open(path)
  defer restored.close()
  assert_true(restored.is_paused())
  assert_eq(restored.status().plan_digest(), updated.job().plan_digest())
  assert_eq(restored.status().accepted_events(), 0L)
}
