///|
fn sim_id(
  origin : String,
  sequence : Int,
) -> @causal.EventId raise @causal.CausalError {
  @causal.EventId::new(origin, sequence)
}

///|
fn edge_event(
  origin : String,
  sequence : Int,
  parents : Array[@causal.EventId],
  source : String,
  target : String,
) -> @causal.Event[@reachability.Edge] raise {
  @causal.Event::new(
    sim_id(origin, sequence),
    parents,
    source,
    @reachability.Edge::new(source, target),
  )
}

///|
fn network_with_two_replicas() -> @sim.Network raise @sim.SimulationError {
  let network = @sim.Network::new()
  network.add_replica(@sim.Replica::new("worker-a"))
  network.add_replica(@sim.Replica::new("worker-b"))
  network
}

///|
test "reverse delivery waits for causal parents then catches up" {
  let network = network_with_two_replicas()
  let ab = edge_event("source", 0, [], "a", "b")
  let bc = edge_event("source", 1, [ab.id()], "b", "c")
  let cd = edge_event("source", 2, [bc.id()], "c", "d")
  network.enqueue("worker-a", "worker-b", cd)
  network.enqueue("worker-a", "worker-b", bc)
  network.enqueue("worker-a", "worker-b", ab)
  assert_eq(network.deliver(0).status(), @causal.SubmitStatus::Pending)
  assert_eq(network.deliver(0).status(), @causal.SubmitStatus::Pending)
  let catch_up = network.deliver(0)
  assert_eq(catch_up.admitted_ids(), [ab.id(), bc.id(), cd.id()])
  guard network.replica("worker-b") is Some(worker) else {
    fail("worker-b should exist")
  }
  assert_eq(worker.pending_count(), 0)
  assert_true(worker.is_reachable("a", "d"))
}

///|
test "duplicate delivery is harmless under at-least-once transport" {
  let network = network_with_two_replicas()
  let ab = edge_event("source", 0, [], "a", "b")
  network.enqueue("worker-a", "worker-b", ab)
  network.duplicate(0)
  let reports = network.deliver_all()
  assert_eq(reports.length(), 2)
  assert_eq(reports[0].status(), @causal.SubmitStatus::Admitted)
  assert_eq(reports[1].status(), @causal.SubmitStatus::Duplicate)
  guard network.replica("worker-b") is Some(worker) else {
    fail("worker-b should exist")
  }
  assert_eq(worker.event_count(), 1)
  assert_eq(worker.facts(), [@reachability.Reachable::new("a", "b")])
}

///|
test "dropped delivery can be retransmitted and converges" {
  let network = network_with_two_replicas()
  let ab = edge_event("source", 0, [], "a", "b")
  network.enqueue("worker-a", "worker-b", ab)
  ignore(network.drop(0))
  assert_eq(network.dropped_count(), 1)
  network.enqueue("worker-a", "worker-b", ab)
  ignore(network.deliver_all())
  guard network.replica("worker-b") is Some(worker) else {
    fail("worker-b should exist")
  }
  assert_true(worker.is_reachable("a", "b"))
}

///|
test "replicas converge under different concurrent event orders" {
  let left = @sim.Replica::new("left")
  let right = @sim.Replica::new("right")
  let ab = edge_event("a", 0, [], "a", "b")
  let bc = edge_event("b", 0, [], "b", "c")
  let cd = edge_event("c", 0, [], "c", "d")
  ignore(left.receive(ab))
  ignore(left.receive(bc))
  ignore(left.receive(cd))
  ignore(right.receive(cd))
  ignore(right.receive(ab))
  ignore(right.receive(bc))
  assert_eq(left.facts(), right.facts())
  assert_true(right.is_reachable("a", "d"))
}
