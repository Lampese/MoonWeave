///|
test "typed keyed sum facade runs a local job without wire types" {
  let job = KeyedSumJob::new("typed-local").with_partition_count(2)
  let plan = job.compile()
  let run = plan.run_local()
  let updates = run.submit([
    KeyedSumEvent::new(@causal.EventId::new("sales", 0), "north", 10L),
    KeyedSumEvent::new(@causal.EventId::new("sales", 1), "north", 5L),
    KeyedSumEvent::new(@causal.EventId::new("sales", 2), "south", 7L),
  ])
  assert_eq(updates, [
    KeyedSumUpdate::new("north", 10L),
    KeyedSumUpdate::new("north", 15L),
    KeyedSumUpdate::new("south", 7L),
  ])
  assert_eq(run.updates(), updates)
  assert_eq(run.totals(), [
    KeyedSumUpdate::new("north", 15L),
    KeyedSumUpdate::new("south", 7L),
  ])
  assert_true(run.plan_digest().has_prefix("plan-"))
  run.close()
}

///|
test "typed keyed sum facade preserves causal pending release" {
  let run = KeyedSumJob::new("typed-causal").run_local()
  let root = KeyedSumEvent::new(@causal.EventId::new("sales", 0), "north", 10L)
  let child = KeyedSumEvent::new_with_parents(
    @causal.EventId::new("sales", 1),
    [root.id()],
    "north",
    2L,
  )
  assert_eq(run.submit([child]), [])
  assert_eq(run.submit([root]), [
    KeyedSumUpdate::new("north", 10L),
    KeyedSumUpdate::new("north", 12L),
  ])
  assert_eq(run.totals(), [KeyedSumUpdate::new("north", 12L)])
  run.close()
}

///|
fn distinct_sum_key(job : KeyedSumPlan, first : String) -> String {
  let candidates = ["store-a", "store-b", "store-c", "store-d", "store-e"]
  let mut selected = candidates[0]
  for candidate in candidates {
    if job.partition_for_key(first) != job.partition_for_key(candidate) {
      selected = candidate
      break
    }
  }
  selected
}

///|
async test "typed keyed sum facade runs through the durable cluster path" {
  let directory = @fs.tmpdir(prefix="moonweave-api-typed-keyed-sum")
  defer {
    for name in @fs.readdir(directory) {
      @fs.remove(directory + "/" + name) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  @async_runtime.with_timeout(30000, () => {
    let plan = KeyedSumJob::new("typed-cluster")
      .with_partition_count(2)
      .with_workers(["worker-a", "worker-b"])
      .with_durable_sink("totals")
      .compile()
    let first_key = "north"
    let second_key = distinct_sum_key(plan, first_key)
    assert_true(
      plan.partition_for_key(first_key) != plan.partition_for_key(second_key),
    )
    let events = [
      KeyedSumEvent::new(@causal.EventId::new("sales", 0), first_key, 120L),
      KeyedSumEvent::new(@causal.EventId::new("sales", 1), first_key, 80L),
      KeyedSumEvent::new(@causal.EventId::new("sales", 2), second_key, 50L),
      KeyedSumEvent::new(@causal.EventId::new("sales", 3), second_key, 70L),
    ]
    let summary = plan.run_local_cluster(
      ClusterConfig::new("typed-cluster", "coordinator", directory),
      events,
    )
    assert_eq(summary.job_id(), "typed-cluster")
    assert_eq(summary.completed_partitions(), 2)
    assert_eq(summary.failed_partitions(), 0)
    assert_eq(summary.sent_batches(), 2)
    assert_eq(summary.totals(), [
      KeyedSumUpdate::new(first_key, 200L),
      KeyedSumUpdate::new(second_key, 120L),
    ])
    assert_eq(summary.failures(), [])
    assert_true(@fs.exists(directory + "/manifest.wal"))
  })
}
