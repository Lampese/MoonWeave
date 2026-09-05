///|
async test "outbox compacts freshly acknowledged deltas without reviving them" {
  with_durable_outbox_path <| path => {
    let first = durable_test_delta("compact-acked", "A", "B")
    let second = durable_test_delta("compact-pending", "B", "C")
    let outbox = DurableExchangeOutbox::open(path, 2)
    outbox.push(first)
    outbox.push(second)
    assert_eq(outbox.try_pop(), Some(first))
    outbox.ack(first.delta_id())
    assert_eq(outbox.try_pop(), Some(second))
    let original_bytes = @fs.read_file(path).binary().length()
    outbox.compact()
    assert_true(@fs.read_file(path).binary().length() < original_bytes)
    assert_eq(outbox.pending(), [second])
    assert_eq(outbox.inflight_count(), 1)
    assert_eq(outbox.try_pop(), None)
    outbox.compact()
    outbox.close()

    let restored = DurableExchangeOutbox::open(path, 1)
    let saved_bytes = @fs.read_file(path).binary()
    assert_true(restored.try_push(first))
    restored.push(first)
    restored.ack(first.delta_id())
    assert_eq(@fs.read_file(path).binary(), saved_bytes)
    assert_eq(restored.pending_count(), 1)
    assert_eq(restored.acked_count(), 1)
    assert_eq(restored.try_pop(), Some(second))
    let conflict = durable_test_delta(first.delta_id(), "A", "different")
    try restored.try_push(conflict) catch {
      error => assert_true(error is DurableOutboxError::IdentityConflict(_))
    } noraise {
      _ => fail("expected conflicting retransmission")
    }
    restored.ack(second.delta_id())
    restored.seal()
    restored.compact()
    restored.close()

    let completed = DurableExchangeOutbox::open(path, 1)
    defer completed.close()
    assert_eq(completed.pending_count(), 0)
    assert_eq(completed.acked_count(), 2)
    completed.ack(first.delta_id())
    completed.ack(second.delta_id())
    assert_eq(completed.try_pop(), None)
  }
}

///|
async test "outbox tombstones use semantic JSON identity" {
  with_durable_outbox_path <| path => {
    let event_id = @causal.EventId::new("canonical", 0)
    let first_payload = @json.parse(
      "{\"source\":\"A\",\"target\":\"B\",\"meta\":[{\"n\":1,\"z\":0}]}",
    )
    let reordered_payload = @json.parse(
      "{\"meta\":[{\"z\":-0.0,\"n\":1e0}],\"target\":\"B\",\"source\":\"A\"}",
    )
    let first = @protocol.DeltaBatch::new("canonical-delta", 0, 0, 1, 0, [
      @protocol.EventRecord::new(event_id, [], "key", first_payload),
    ])
    let reordered = @protocol.DeltaBatch::new("canonical-delta", 0, 0, 1, 0, [
      @protocol.EventRecord::new(event_id, [], "key", reordered_payload),
    ])
    assert_eq(first, reordered)
    let outbox = DurableExchangeOutbox::open(path, 1)
    outbox.push(first)
    outbox.ack(first.delta_id())
    outbox.compact()
    outbox.close()
    let restored = DurableExchangeOutbox::open(path, 1)
    defer restored.close()
    restored.push(reordered)
    assert_eq(restored.pending_count(), 0)
  }
}

///|
async test "outbox push errors release the lock and preserve queued data" {
  with_durable_outbox_path <| path => {
    let first = durable_test_delta("mutex-first", "A", "B")
    let second = durable_test_delta("mutex-second", "B", "C")
    let outbox = DurableExchangeOutbox::open(path, 1)
    defer outbox.close()
    outbox.push(first)
    let conflict = durable_test_delta(first.delta_id(), "X", "Y")
    try outbox.push(conflict) catch {
      error => assert_true(error is DurableOutboxError::IdentityConflict(_))
    } noraise {
      _ => fail("expected conflicting retransmission")
    }
    @async_runtime.with_timeout(5000, () => {
      outbox.ack(first.delta_id())
      outbox.push(second)
      outbox.compact()
    })
    assert_eq(outbox.pending(), [second])
  }
}

///|
async test "outbox owns JSON payloads across enqueue and inspection" {
  with_durable_outbox_path <| path => {
    let delta = durable_test_delta("copy-delta", "A", "B")
    let original = delta.to_payload()
    let outbox = DurableExchangeOutbox::open(path, 1)
    outbox.push(delta)
    guard delta.events()[0].payload() is Object(fields) else {
      fail("expected object")
    }
    fields["target"] = Json::string("changed-input")
    let inspected = outbox.pending()[0]
    guard inspected.events()[0].payload() is Object(copy) else {
      fail("expected object")
    }
    copy["target"] = Json::string("changed-view")
    assert_eq(outbox.try_pop().unwrap().to_payload(), original)
    outbox.compact()
    outbox.close()
    let restored = DurableExchangeOutbox::open(path, 1)
    defer restored.close()
    assert_eq(restored.try_pop().unwrap().to_payload(), original)
  }
}

///|
async test "closed outbox cannot lease or acknowledge more data" {
  with_durable_outbox_path <| path => {
    let outbox = DurableExchangeOutbox::open(path, 1)
    let delta = durable_test_delta("closed-delta", "A", "B")
    outbox.push(delta)
    outbox.close()
    assert_eq(outbox.try_pop(), None)
    try outbox.pop() catch {
      error => assert_eq(error, DurableOutboxError::Closed)
    } noraise {
      _ => fail("expected closed outbox")
    }
    try outbox.ack(delta.delta_id()) catch {
      error => assert_eq(error, DurableOutboxError::Closed)
    } noraise {
      _ => fail("expected closed outbox")
    }
    try outbox.compact() catch {
      error => assert_eq(error, DurableOutboxError::Closed)
    } noraise {
      _ => fail("expected closed outbox")
    }
    let restored = DurableExchangeOutbox::open(path, 1)
    defer restored.close()
    assert_eq(restored.pending(), [delta])
  }
}
