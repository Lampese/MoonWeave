///|
async test "rescale intent survives compaction and preserves an existing pause" {
  let root = @fs.tmpdir(prefix="moonweave-rescale-intent")
  defer @fs.rmdir(root, recursive=true)
  let path = root + "/driver.wal"
  let store = DriverStore::open(path)
  let job = JobDefinition::new("rescale-intent", OperatorRef::keyed_sum())
    .with_partition_count(4)
    .with_parallelism(2)
    .compile()
    .submission_with_id("intent")
  assert_eq(store.submit(job).parallelism(), 2)
  ignore(store.pause())
  store.begin_rescale(["b", "a"], parallelism=4)
  store.compact()
  store.close()
  let restored = DriverStore::open(path)
  assert_eq(restored.rescale_workers, Some(["a", "b"]))
  assert_true(restored.is_paused())
  assert_eq(restored.status().parallelism(), 2)
  assert_eq(restored.rescale_parallelism, Some(4))
  restored.begin_rescale(["a", "b"], parallelism=4)
  try restored.begin_rescale(["a", "b"], parallelism=2) catch {
    ApiError::InvalidDriverCommand(_) => ()
    _ => fail("unexpected conflicting parallelism error")
  } noraise {
    _ => fail("accepted conflicting parallelism")
  }
  try restored.begin_rescale(["c"]) catch {
    ApiError::InvalidDriverCommand(_) => ()
    _ => fail("unexpected conflicting rescale error")
  } noraise {
    _ => fail("accepted conflicting rescale")
  }
  restored.complete_rescale()
  assert_true(restored.is_paused())
  restored.close()
  let complete = DriverStore::open(path)
  defer complete.close()
  assert_eq(complete.rescale_workers, None)
  assert_true(complete.is_paused())
  assert_eq(complete.status().parallelism(), 4)
}
