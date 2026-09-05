///|
fn source_event(
  origin : String,
  sequence : Int,
  key : String,
  target : String,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    [],
    key,
    Json::object({ "source": Json::string("A"), "target": Json::string(target) }),
  )
}

///|
async test "source log recovers content and idempotent offsets" {
  let directory = @fs.tmpdir(prefix="moonweave-source-log")
  let path = directory + "/source.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.remove(path + ".repair") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let first = source_event("source", 0, "account", "B")
  let second = source_event("source", 1, "account", "C")
  let source = SourceLog::open(path)
  assert_eq(source.append(2, 10L, first), 10L)
  assert_eq(source.append(2, 10L, first), 10L)
  assert_eq(source.append(2, 10L, second), 11L)
  assert_eq(source.event_count(), 2)
  let batches = source.batches(2, 7, 10L, maximum_batch_events=2)
  assert_eq(batches.length(), 1)
  assert_eq(batches[0].first_offset(), 10L)
  assert_eq(batches[0].end_offset(), 11L)
  source.close()
  let restored = SourceLog::open(path)
  assert_eq(restored.event_count(), 2)
  assert_eq(restored.next_offset(2), Some(12L))
  assert_eq(restored.batches(2, 8, 10L)[0].epoch(), 8)
  let conflict = source_event("source", 0, "account", "X")
  try restored.append(2, 10L, conflict) catch {
    SourceLogError::IdentityConflict(id) => assert_eq(id, first.id())
    _ => fail("expected source identity conflict")
  } noraise {
    _ => fail("expected source identity conflict")
  }
  restored.close()
}

///|
async test "ingress rebuilds work under a reassigned epoch" {
  let directory = @fs.tmpdir(prefix="moonweave-ingress")
  let path = directory + "/source.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.remove(path + ".repair") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let plan = @plan.JobSpec::new(
    "job",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [],
  )
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let boot_a = @protocol.Incarnation::new("boot-a")
  let boot_b = @protocol.Incarnation::new("boot-b")
  ignore(control.register_worker("worker-a", boot_a, []))
  ignore(control.register_worker("worker-b", boot_b, []))
  for partition_plan in plan.plan_partitions() {
    ignore(
      control.assign(partition_plan.partition(), partition_plan.worker_id()),
    )
  }
  let event = source_event("ingress", 0, "account-42", "B")
  let target = plan.route_event(event)
  guard control.assignment(target.partition()) is Some(old_assignment) else {
    fail("planned assignment missing")
  }
  let router = IngressRouter::open(plan, control, path)
  let routed = router.route(event)
  assert_eq(routed.partition(), target.partition())
  assert_eq(routed.worker_id(), old_assignment.worker_id())
  let replacement = if old_assignment.worker_id() == "worker-a" {
    "worker-b"
  } else {
    "worker-a"
  }
  ignore(
    control.fence_and_reassign(
      old_assignment.worker_id(),
      old_assignment.incarnation(),
      replacement,
    ),
  )
  let work = router.supervisor_work(target.partition(), "recovered-session")
  assert_eq(work.worker_id(), replacement)
  guard work.batches() is [batch] else { fail("source batch missing") }
  assert_eq(batch.epoch(), old_assignment.epoch() + 1)
  assert_eq(batch.events(), [event])
  router.close()
}

///|
async test "ingress dispatch checks every exchange destination before enqueue" {
  let directory = @fs.tmpdir(prefix="moonweave-ingress-dispatch")
  let path = directory + "/source.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.remove(path + ".repair") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let plan = @plan.JobSpec::new(
    "job",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [@plan.ExchangeRoute::new(0, 1), @plan.ExchangeRoute::new(1, 0)],
  )
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let boot_a = @protocol.Incarnation::new("boot-a")
  let boot_b = @protocol.Incarnation::new("boot-b")
  ignore(control.register_worker("worker-a", boot_a, []))
  ignore(control.register_worker("worker-b", boot_b, []))
  for partition_plan in plan.plan_partitions() {
    ignore(
      control.assign(partition_plan.partition(), partition_plan.worker_id()),
    )
  }
  let router = IngressRouter::open(plan, control, path)
  let first_outbox = ExchangeOutbox::new(2)
  let event = source_event("dispatch", 0, "account-dispatch", "B")
  try router.dispatch_and_enqueue(event) catch {
    SourceLogError::MissingOutbox(partition) => assert_true(partition >= 0)
    _ => fail("expected missing exchange outbox")
  } noraise {
    _ => fail("expected missing exchange outbox")
  }
  router.close()
  let empty_source = SourceLog::open(path)
  assert_eq(empty_source.event_count(), 0)
  empty_source.close()
  let router = IngressRouter::open(plan, control, path)
  let target = plan.route_event(event)
  let exchange_target = plan.exchange_targets(target.partition())[0]
  router.register_outbox(exchange_target.partition(), first_outbox)
  let dispatch = router.dispatch_and_enqueue(event)
  assert_eq(dispatch.batch().events(), [event])
  assert_eq(dispatch.deltas().length(), 1)
  assert_eq(first_outbox.pop().delta_id(), dispatch.deltas()[0].delta_id())
  first_outbox.close()
  router.close()
}

///|
fn key_for_partition(plan : @plan.JobSpec, partition : Int) -> String {
  for index in 0..<10000 {
    let key = "partition-key-" + index.to_string()
    if plan.partition_for_key(key) == partition {
      return key
    }
  }
  abort("unable to find deterministic partition key")
}

///|
async test "ingress recovery rebuilds primary and inbound exchange work" {
  let directory = @fs.tmpdir(prefix="moonweave-ingress-recovery")
  let path = directory + "/source.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.remove(path + ".repair") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let plan = @plan.JobSpec::new(
    "job",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [@plan.ExchangeRoute::new(0, 1), @plan.ExchangeRoute::new(1, 0)],
  )
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let boot_a = @protocol.Incarnation::new("boot-a")
  let boot_b = @protocol.Incarnation::new("boot-b")
  ignore(control.register_worker("worker-a", boot_a, []))
  ignore(control.register_worker("worker-b", boot_b, []))
  for partition_plan in plan.plan_partitions() {
    ignore(
      control.assign(partition_plan.partition(), partition_plan.worker_id()),
    )
  }
  let event_0 = source_event("source-0", 0, key_for_partition(plan, 0), "B")
  let event_1 = source_event("source-1", 0, key_for_partition(plan, 1), "C")
  let router = IngressRouter::open(plan, control, path)
  assert_eq(router.route(event_0).partition(), 0)
  assert_eq(router.route(event_1).partition(), 1)
  let works = router.supervisor_works("recovery", maximum_batch_events=1)
  assert_eq(works.length(), 2)
  assert_eq(works[0].partition(), 0)
  assert_eq(works[0].session_id(), "recovery-p0")
  assert_eq(works[0].batches()[0].events(), [event_0])
  assert_eq(works[0].deltas().length(), 1)
  assert_eq(works[0].deltas()[0].events(), [event_1])
  assert_eq(works[0].deltas()[0].source_partition(), 1)
  assert_eq(works[1].partition(), 1)
  assert_eq(works[1].batches()[0].events(), [event_1])
  assert_eq(works[1].deltas().length(), 1)
  assert_eq(works[1].deltas()[0].events(), [event_0])
  let old_target_epoch = works[0].deltas()[0].target_epoch()
  let boot_c = @protocol.Incarnation::new("boot-c")
  ignore(control.register_worker("worker-c", boot_c, []))
  ignore(control.fence_and_reassign("worker-a", boot_a, "worker-c"))
  let recovered = router.supervisor_work(0, "after-fence")
  assert_eq(recovered.worker_id(), "worker-c")
  assert_eq(recovered.batches()[0].epoch(), old_target_epoch + 1)
  assert_eq(recovered.deltas()[0].target_epoch(), old_target_epoch + 1)
  router.close()
}

///|
async test "supervisor work rejects duplicate worker connections" {
  let directory = @fs.tmpdir(prefix="moonweave-ingress-duplicate-worker")
  let path = directory + "/source.wal"
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
  let plan = @plan.JobSpec::new("job", "operator", 2, ["worker-a"], [])
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  ignore(
    control.register_worker("worker-a", @protocol.Incarnation::new("boot-a"), []),
  )
  for partition_plan in plan.plan_partitions() {
    ignore(
      control.assign(partition_plan.partition(), partition_plan.worker_id()),
    )
  }
  let router = IngressRouter::open(plan, control, path)
  try router.supervisor_works("session") catch {
    SourceLogError::DuplicateWorkerConnection(worker_id) =>
      assert_eq(worker_id, "worker-a")
    _ => fail("expected duplicate Worker connection error")
  } noraise {
    _ => fail("expected duplicate Worker connection error")
  }
  router.close()
}
