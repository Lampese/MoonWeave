///|
async test "Driver authority replaces unconfirmed inputs and preserves retirement and rescale" {
  let root = @fs.tmpdir(prefix="moonweave-driver-authority")
  defer @fs.rmdir(root, recursive=true)
  let submission = JobDefinition::new("authority", OperatorRef::keyed_sum())
    .with_partition_count(4)
    .with_parallelism(2)
    .compile()
    .submission_with_id("submit")
  let event = Event::new(@causal.EventId::new("in", 0L), [], "key", {
    "value": "3",
  })
  let leader = DriverStore::open(root + "/leader.wal")
  ignore(leader.submit(submission))
  ignore(leader.input([event]))
  leader.compact(retired=[@causal.EventRange::new("in", 0L, 0L)])
  leader.begin_rescale(["a", "b"], parallelism=4)
  let snapshot = leader.authority_snapshot()
  leader.close()
  let target = DriverStore::open(root + "/target.wal")
  ignore(target.submit(submission))
  ignore(
    target.input([
      Event::new(@causal.EventId::new("unconfirmed", 0L), [], "key", {
        "value": "100",
      }),
    ]),
  )
  target.replace_authority_snapshot(snapshot)
  assert_eq(target.inputs(), [])
  assert_eq(target.authority_snapshot(), snapshot)
  target.close()
  let restored = DriverStore::open(root + "/target.wal")
  defer restored.close()
  assert_eq(restored.authority_snapshot(), snapshot)
  restored.complete_rescale()
  assert_eq(restored.status().parallelism(), 4)
  assert_eq(restored.input([event]).accepted_events(), 1L)
  assert_eq(restored.inputs(), [])
}
