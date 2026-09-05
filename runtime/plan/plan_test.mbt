///|
fn edge_event(origin : String, sequence : Int) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    [],
    "graph",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
}

///|
test "job plan routes keys and partitions deterministically" {
  let plan = JobSpec::new(
    "job-1",
    "reachability",
    3,
    ["worker-b", "worker-a"],
    [ExchangeRoute::new(0, 1), ExchangeRoute::new(1, 2)],
  )
  assert_eq(plan.workers(), ["worker-a", "worker-b"])
  assert_eq(plan.owner(0), "worker-a")
  assert_eq(plan.owner(1), "worker-b")
  assert_eq(plan.owner(2), "worker-a")
  assert_eq(
    plan.partition_for_key("user-42"),
    plan.partition_for_key("user-42"),
  )
  let target = plan.route_key("user-42")
  assert_eq(target.worker_id(), plan.owner(target.partition()))
  assert_eq(plan.exchange_targets(0).map(item => item.partition()), [1])
  assert_eq(plan.plan_partitions(start_offset=10L)[1].start_offset(), 10L)
}

///|
test "job plan creates stable routed delta identities" {
  let plan = JobSpec::new(
    "job-1",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [ExchangeRoute::new(0, 1)],
  )
  let event = edge_event("source", 0)
  let first = plan.delta(0, 1, 1, 3, [event])
  let second = plan.delta(0, 1, 1, 3, [event])
  assert_eq(first, second)
  assert_eq(first.delta_id(), second.delta_id())
  let routed = plan.route_batch(0, 1, 3, [event])
  assert_eq(routed.length(), 1)
  assert_eq(routed[0].target_partition(), 1)
  assert_eq(routed[0].delta_id(), first.delta_id())
  let other_job = JobSpec::new(
    "job-2",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [ExchangeRoute::new(0, 1)],
  )
  assert_true(
    other_job.delta(0, 1, 1, 3, [event]).delta_id() != first.delta_id(),
  )
  let changed = @protocol.EventRecord::new(
    event.id(),
    event.parents(),
    event.key(),
    Json::object({ "source": Json::string("A"), "target": Json::string("C") }),
  )
  assert_true(plan.delta(0, 1, 1, 3, [changed]).delta_id() != first.delta_id())
  try plan.delta(1, 0, 0, 0, [event]) catch {
    PlanError::MissingExchangeRoute(source~, target~) => {
      assert_eq(source, 1)
      assert_eq(target, 0)
    }
    _ => fail("expected undeclared exchange route")
  } noraise {
    _ => fail("expected undeclared exchange route")
  }
}
