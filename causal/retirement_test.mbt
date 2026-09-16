///|
test "checkpointed causal membership stays bounded without forgetting duplicate or parent identity" {
  let log : @causal.CausalLog[Int] = @causal.CausalLog::new()
  for cycle in 0..<200 {
    let ids = []
    for item in 0..<50 {
      let sequence = (cycle * 50 + item).to_int64()
      let id = @causal.EventId::new("source", sequence)
      ids.push(id)
      assert_eq(
        log.submit(@causal.Event::new(id, [], "key", 1)).status(),
        @causal.SubmitStatus::Admitted,
      )
    }
    let before = log.frontier()
    log.compact(
      @causal.CausalRetirement::from_ids("checkpoint-" + cycle.to_string(), ids),
    )
    assert_eq(log.state_count(), 1)
    assert_eq(log.admitted_count(), 0)
    assert_true(log.frontier_covers(log.frontier(), before))
  }
  assert_eq(log.retired_ranges(), [@causal.EventRange::new("source", 0L, 9999L)])
  let old = @causal.EventId::new("source", 0L)
  assert_eq(
    log.submit(@causal.Event::new(old, [], "key", 1)).status(),
    @causal.SubmitStatus::Duplicate,
  )
  let child = @causal.Event::new(
    @causal.EventId::new("next", 0L),
    [old],
    "key",
    2,
  )
  assert_eq(log.submit(child).status(), @causal.SubmitStatus::Admitted)
  assert_eq(log.pending_count(), 0)
}

///|
test "retirement intervals preserve holes and nonzero starting offsets" {
  let log : @causal.CausalLog[Int] = @causal.CausalLog::new()
  let one = @causal.EventId::new("source", 1L)
  let three = @causal.EventId::new("source", 3L)
  ignore(log.submit(@causal.Event::new(one, [], "key", 1)))
  ignore(log.submit(@causal.Event::new(three, [], "key", 1)))
  log.compact(@causal.CausalRetirement::from_ids("sparse", [three, one, one]))
  assert_eq(log.retired_ranges(), [
    @causal.EventRange::new("source", 1L, 1L),
    @causal.EventRange::new("source", 3L, 3L),
  ])
  assert_true(!log.contains(@causal.EventId::new("source", 0L)))
  let missing = @causal.EventId::new("source", 2L)
  assert_eq(
    log.submit(@causal.Event::new(missing, [], "key", 2)).status(),
    @causal.SubmitStatus::Admitted,
  )
  log.compact(@causal.CausalRetirement::from_ids("fill-hole", [missing]))
  assert_eq(log.retired_ranges(), [@causal.EventRange::new("source", 1L, 3L)])
}

///|
test "a checkpoint cannot retire pending events or invent their missing parents" {
  let log : @causal.CausalLog[Int] = @causal.CausalLog::new()
  let parent = @causal.EventId::new("parent", 0L)
  let child = @causal.EventId::new("child", 0L)
  ignore(log.submit(@causal.Event::new(child, [parent], "key", 2)))
  let before = log.frontier()
  for ids in [[parent], [child], [parent, child]] {
    try log.compact(@causal.CausalRetirement::from_ids("invalid", ids)) catch {
      @causal.CausalError::InvalidRetirement(_) => ()
      _ => fail("unexpected retirement error")
    } noraise {
      _ => fail("pending history was lost")
    }
    assert_eq(log.frontier(), before)
    assert_eq(log.pending_count(), 1)
    assert_eq(log.retired_ranges().length(), 0)
  }
  assert_eq(
    log.submit(@causal.Event::new(parent, [], "key", 1)).admitted().length(),
    2,
  )
}

///|
test "retired Int64 maximum remains exact across a copied log and serialized certificate" {
  let id = @causal.EventId::new("maximum", 9223372036854775807L)
  let certificate = @causal.CausalRetirement::from_ids("max-checkpoint", [id])
  let restored : @causal.CausalRetirement = @json.from_json(
    @json.to_json(certificate),
  )
  let log : @causal.CausalLog[Int] = @causal.CausalLog::new()
  ignore(log.submit(@causal.Event::new(id, [], "key", 1)))
  log.compact(restored)
  let copy = log.copy()
  assert_true(copy.contains(id))
  assert_true(
    !copy.contains(@causal.EventId::new("maximum", 9223372036854775806L)),
  )
  assert_eq(copy.frontier(), log.frontier())
}

///|
test "snapshot restore handles reverse identity order without exceeding a small live pending budget" {
  let log : @causal.CausalLog[Int] = @causal.CausalLog::new(maximum_pending=1)
  let root = @causal.EventId::new("z", 0L)
  ignore(log.submit(@causal.Event::new(root, [], "key", 0)))
  for origin in ["a", "b", "c"] {
    ignore(
      log.submit(
        @causal.Event::new(@causal.EventId::new(origin, 0L), [root], "key", 1),
      ),
    )
  }
  let restored = @causal.CausalLog::restore(log.snapshot())
  assert_eq(restored.events(), log.events())
  assert_eq(restored.pending_count(), 0)
  assert_eq(restored.frontier(), log.frontier())
}
