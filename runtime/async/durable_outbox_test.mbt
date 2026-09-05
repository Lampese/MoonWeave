///|
fn durable_test_delta(
  delta_id : String,
  source : String,
  target : String,
) -> @protocol.DeltaBatch raise {
  let event = @protocol.EventRecord::new(
    @causal.EventId::new(delta_id, 0),
    [],
    delta_id,
    Json::object({
      "source": Json::string(source),
      "target": Json::string(target),
    }),
  )
  @protocol.DeltaBatch::new(delta_id, 0, 0, 1, 0, [event])
}

///|
async fn with_durable_outbox_path(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-durable-outbox")
  let path = directory + "/exchange.wal"
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
    @fs.remove(path + ".compact") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  f(path)
}

///|
async test "durable exchange outbox recovers unacked deltas and deduplicates puts" {
  with_durable_outbox_path <| path => {
    let first = durable_test_delta("durable-1", "A", "B")
    let second = durable_test_delta("durable-2", "B", "C")
    let outbox = DurableExchangeOutbox::open(path, 2)
    assert_true(outbox.try_push(first))
    assert_true(outbox.try_push(first))
    assert_true(outbox.try_push(second))
    assert_false(outbox.try_push(durable_test_delta("durable-3", "C", "D")))
    let popped = outbox.try_pop().unwrap()
    assert_eq(popped.delta_id(), "durable-1")
    outbox.ack("durable-1")
    assert_eq(outbox.pending_count(), 1)
    outbox.close()

    let restored = DurableExchangeOutbox::open(path, 2)
    assert_eq(restored.pending_count(), 1)
    assert_eq(restored.acked_count(), 1)
    assert_eq(restored.try_pop().unwrap().delta_id(), "durable-2")
    try restored.try_push(durable_test_delta("durable-2", "X", "Y")) catch {
      DurableOutboxError::IdentityConflict(_) => ()
      _ => fail("expected durable delta identity conflict")
    } noraise {
      _ => fail("expected durable delta identity conflict")
    }
    restored.requeue_all()
    restored.compact()
    restored.close()

    let compacted = DurableExchangeOutbox::open(path, 2)
    assert_eq(compacted.pending_count(), 1)
    assert_eq(compacted.acked_count(), 1)
    assert_true(compacted.try_push(first))
    assert_eq(compacted.pending_count(), 1)
    compacted.close()
  }
}

///|
async test "durable exchange outbox applies async backpressure without blocking" {
  with_durable_outbox_path <| path => {
    let outbox = DurableExchangeOutbox::open(path, 1)
    let first = durable_test_delta("backpressure-1", "A", "B")
    let second = durable_test_delta("backpressure-2", "B", "C")
    outbox.push(first)
    @async_runtime.with_task_group <| group => {
      let producer = group.spawn() <| () => { outbox.push(second) }
      let first_popped = outbox.pop()
      assert_eq(first_popped.delta_id(), "backpressure-1")
      outbox.ack(first_popped.delta_id())
      ignore(producer.wait())
      let second_popped = outbox.pop()
      assert_eq(second_popped.delta_id(), "backpressure-2")
      outbox.ack(second_popped.delta_id())
    }
    outbox.close()
  }
}
