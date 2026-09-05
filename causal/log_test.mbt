///|
fn id(
  origin : String,
  sequence : Int,
) -> @causal.EventId raise @causal.CausalError {
  @causal.EventId::new(origin, sequence)
}

///|
fn event(
  origin : String,
  sequence : Int,
  parents : Array[@causal.EventId],
  payload : String,
) -> @causal.Event[String] raise @causal.CausalError {
  @causal.Event::new(id(origin, sequence), parents, "account-1", payload)
}

///|
test "causal log drains descendants in dependency order" {
  let log : @causal.CausalLog[String] = @causal.CausalLog::new()
  let root = event("auth", 0, [], "login")
  let grant = event("admin", 0, [root.id()], "grant")
  let export_event = event("storage", 0, [grant.id()], "export")
  let pending_export = log.submit(export_event)
  assert_eq(pending_export.status(), @causal.SubmitStatus::Pending)
  assert_eq(pending_export.missing(), [grant.id()])
  let pending_grant = log.submit(grant)
  assert_eq(pending_grant.status(), @causal.SubmitStatus::Pending)
  assert_eq(log.pending_count(), 2)
  let admitted = log.submit(root)
  assert_eq(admitted.status(), @causal.SubmitStatus::Admitted)
  assert_eq(admitted.admitted().map(item => item.id()), [
    root.id(),
    grant.id(),
    export_event.id(),
  ])
  assert_eq(log.admitted_count(), 3)
  assert_eq(log.pending_count(), 0)
  assert_eq(log.frontier().ids(), [export_event.id()])
}

///|
test "equal retransmission is idempotent" {
  let log : @causal.CausalLog[String] = @causal.CausalLog::new()
  let root = event("auth", 0, [], "login")
  assert_eq(log.submit(root).status(), @causal.SubmitStatus::Admitted)
  assert_eq(log.submit(root).status(), @causal.SubmitStatus::Duplicate)
  assert_eq(log.admitted_count(), 1)
}

///|
test "parent order is canonical and does not create a false conflict" {
  let log : @causal.CausalLog[String] = @causal.CausalLog::new()
  let left = event("left", 0, [], "left")
  let right = event("right", 0, [], "right")
  ignore(log.submit(left))
  ignore(log.submit(right))
  let first = event("join", 0, [left.id(), right.id()], "joined")
  let reordered = event("join", 0, [right.id(), left.id()], "joined")
  assert_eq(first, reordered)
  assert_eq(log.submit(first).status(), @causal.SubmitStatus::Admitted)
  assert_eq(log.submit(reordered).status(), @causal.SubmitStatus::Duplicate)
}

///|
test "same identity with different content is rejected" {
  let log : @causal.CausalLog[String] = @causal.CausalLog::new()
  let original = event("auth", 0, [], "login")
  let conflict = event("auth", 0, [], "logout")
  ignore(log.submit(original))
  try log.submit(conflict) catch {
    @causal.CausalError::IdentityConflict(conflict_id) =>
      assert_eq(conflict_id, conflict.id())
    _ => fail("expected identity conflict")
  } noraise {
    _ => fail("expected identity conflict")
  }
}

///|
test "frontier advances under causal order rather than set inclusion" {
  let log : @causal.CausalLog[String] = @causal.CausalLog::new()
  let left = event("left", 0, [], "left")
  let right = event("right", 0, [], "right")
  ignore(log.submit(left))
  let first = log.frontier()
  ignore(log.submit(right))
  let concurrent = log.frontier()
  let joined = event("join", 0, [left.id(), right.id()], "joined")
  ignore(log.submit(joined))
  let final_frontier = log.frontier()
  assert_true(log.frontier_covers(concurrent, first))
  assert_true(log.frontier_covers(final_frontier, concurrent))
  assert_eq(final_frontier.ids(), [joined.id()])
  assert_eq(final_frontier.digest(), final_frontier.digest())
  assert_true(final_frontier.digest() != concurrent.digest())
}

///|
test "pending events are bounded" {
  let log : @causal.CausalLog[String] = @causal.CausalLog::new(
    maximum_pending=1,
  )
  let missing_a = id("missing", 0)
  let missing_b = id("missing", 1)
  ignore(log.submit(event("child", 0, [missing_a], "a")))
  try log.submit(event("child", 1, [missing_b], "b")) catch {
    @causal.CausalError::PendingLimitExceeded(limit~) => assert_eq(limit, 1)
    _ => fail("expected pending limit error")
  } noraise {
    _ => fail("expected pending limit error")
  }
}
