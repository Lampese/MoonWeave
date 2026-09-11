///|
test "wire frames preserve large event identities dependencies and offsets" {
  let parent = @protocol.EventRecord::new(
    @causal.EventId::new("kafka/events/3", 9007199254740992L),
    [],
    "key",
    Json::null(),
  )
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("kafka/events/3", 9007199254740993L),
    [parent.id()],
    "key",
    Json::null(),
  )
  let batch = @protocol.EventBatch::new(3, 2, 9007199254740992L, [parent, child])
  let frame = @protocol.Frame::new(
    @protocol.protocol_version(),
    "cluster",
    "session",
    "message",
    "coordinator",
    "boot",
    9007199254740993L,
    2,
    @protocol.FrameKind::EventBatch,
    batch.to_payload(),
  )
  let decoded = @protocol.Frame::decode_payload(frame.encode()[4:].to_owned())
  assert_eq(decoded, frame)
  let restored = @protocol.EventBatch::from_payload(decoded.payload())
  assert_eq(restored, batch)
  assert_eq(restored.end_offset(), 9007199254740993L)
  assert_eq(restored.events()[1].to_event().parents(), [parent.id()])
  let delta = @protocol.DeltaBatch::new("delta", 3, 2, 4, 2, [parent, child])
  assert_eq(@protocol.DeltaBatch::from_payload(delta.to_payload()), delta)
}

///|
test "event batches reject an unrepresentable next offset" {
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("source", 9223372036854775807L),
    [],
    "key",
    Json::null(),
  )
  let last = @protocol.EventBatch::new(0, 0, 9223372036854775806L, [event])
  assert_eq(last.end_offset(), 9223372036854775806L)
  for
    batch in [
      @protocol.EventBatch::{ ..last, first_offset: 9223372036854775807L, },
      @protocol.EventBatch::{ ..last, events: [event, event], },
    ] {
    try
      @protocol.EventBatch::new(
        batch.partition(),
        batch.epoch(),
        batch.first_offset(),
        batch.events().to_owned(),
      )
    catch {
      @protocol.ProtocolError::InvalidBatchOffset => ()
      _ => fail("expected offset overflow rejection")
    } noraise {
      _ => fail("batch accepted an overflowing next offset")
    }
    try @protocol.EventBatch::from_payload(batch.to_payload()) catch {
      @protocol.MessageError::Protocol(
        @protocol.ProtocolError::InvalidBatchOffset
      ) => ()
      _ => fail("expected wire offset overflow rejection")
    } noraise {
      _ => fail("wire batch accepted an overflowing next offset")
    }
  }
}

///|
test "worker send cursor overflow does not consume credits" {
  let boot = @protocol.Incarnation::new("boot")
  let state = @protocol.WorkerState::new("worker", boot)
  state.assign(
    @protocol.Assignment::new(0, 0, boot, start_offset=9223372036854775806L),
  )
  state.grant_credit(0, 3L, 30L)
  state.reserve_batch(0, 1L, 10L)
  let current = state.assignment().unwrap()
  assert_eq(current.next_offset(), 9223372036854775807L)
  try state.reserve_batch(0, 1L, 10L) catch {
    @protocol.ProtocolError::InvalidOffset(offset) =>
      assert_eq(offset, 9223372036854775807L)
    _ => fail("expected offset overflow rejection")
  } noraise {
    _ => fail("worker send cursor wrapped")
  }
  assert_eq(current.next_offset(), 9223372036854775807L)
  assert_eq(current.credit_events(), 2L)
  assert_eq(current.credit_bytes(), 20L)
  state.ack_persisted(0, 9223372036854775806L)
  assert_eq(current.acked_offset(), 9223372036854775806L)
}
