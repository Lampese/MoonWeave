///|
fn outbox_delta() -> @protocol.DeltaBatch raise {
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("outbox", 0),
    [],
    "outbox",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  @protocol.DeltaBatch::new("outbox-delta", 0, 0, 1, 0, [event])
}

///|
test "exchange outbox enforces a bounded non-dropping queue" {
  let outbox = ExchangeOutbox::new(1)
  let delta = outbox_delta()
  assert_true(outbox.try_push(delta))
  assert_false(outbox.try_push(delta))
  assert_eq(outbox.capacity(), 1)
  outbox.close()
  try outbox.try_push(delta) catch {
    OutboxError::Closed(_) => ()
    _ => fail("expected closed outbox")
  } noraise {
    _ => fail("expected closed outbox")
  }
}

///|
async test "exchange outbox push unblocks after pop" {
  let outbox = ExchangeOutbox::new(1)
  let delta = outbox_delta()
  outbox.push(delta)
  @async_runtime.with_task_group <| group => {
    let pushed = group.spawn() <| () => { outbox.push(delta) }
    let popped = outbox.pop()
    assert_eq(popped.delta_id(), "outbox-delta")
    ignore(pushed.wait())
    assert_eq(outbox.pop().delta_id(), "outbox-delta")
  }
  outbox.close()
}

///|
async test "exchange outbox preserves routed delta order" {
  let outbox = ExchangeOutbox::new(2)
  let first = outbox_delta()
  let second = @protocol.DeltaBatch::new(
    "outbox-delta-2",
    0,
    0,
    1,
    0,
    first.events().to_owned(),
  )
  outbox.push_all([first, second])
  assert_eq(outbox.pop().delta_id(), "outbox-delta")
  assert_eq(outbox.pop().delta_id(), "outbox-delta-2")
  outbox.close()
}
