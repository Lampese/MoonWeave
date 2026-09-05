///|
fn message_id(
  origin : String,
  sequence : Int,
) -> @causal.EventId raise @causal.CausalError {
  @causal.EventId::new(origin, sequence)
}

///|
test "hello and welcome payloads round trip" {
  let hello = @protocol.Hello::new(
    "worker-a",
    "boot-1",
    ["reachability", "json-events"],
    last_session_id="session-old",
  )
  let hello_decoded = @protocol.Hello::from_payload(hello.to_payload())
  assert_eq(hello_decoded, hello)
  let assignment = @protocol.AssignmentSpec::new(
    2,
    9,
    "boot-1",
    start_offset=12L,
  )
  let welcome = @protocol.Welcome::new("session-new", assignment~)
  let welcome_decoded = @protocol.Welcome::from_payload(welcome.to_payload())
  assert_eq(welcome_decoded, welcome)
}

///|
test "event batch carries causal event records" {
  let root = @protocol.EventRecord::new(
    message_id("source", 0),
    [],
    "account-1",
    Json::object({ "edge": Json::string("a->b") }),
  )
  let child = @protocol.EventRecord::new(
    message_id("source", 1),
    [root.id()],
    "account-1",
    Json::object({ "edge": Json::string("b->c") }),
  )
  let batch = @protocol.EventBatch::new(3, 4, 20L, [root, child])
  let decoded = @protocol.EventBatch::from_payload(batch.to_payload())
  assert_eq(decoded, batch)
  let event = decoded.events()[1].to_event()
  assert_eq(event.parents(), [root.id()])
}

///|
test "delta batch and acknowledgement payloads round trip" {
  let event = @protocol.EventRecord::new(
    message_id("source", 0),
    [],
    "exchange",
    Json::object({ "edge": Json::string("A->B") }),
  )
  let delta = @protocol.DeltaBatch::new("delta-1", 0, 2, 1, 4, [event])
  assert_eq(@protocol.DeltaBatch::from_payload(delta.to_payload()), delta)
  let ack = @protocol.DeltaAck::new("delta-1", 1, 4)
  assert_eq(@protocol.DeltaAck::from_payload(ack.to_payload()), ack)
}

///|
test "credit, ack, heartbeat and progress payloads round trip" {
  let credit = @protocol.Credit::new(1, 2, 50L, 4096L)
  assert_eq(@protocol.Credit::from_payload(credit.to_payload()), credit)
  let ack = @protocol.AckPersisted::new(1, 2, 20L)
  assert_eq(@protocol.AckPersisted::from_payload(ack.to_payload()), ack)
  let heartbeat = @protocol.Heartbeat::new(1, 2, 4, 20L)
  assert_eq(
    @protocol.Heartbeat::from_payload(heartbeat.to_payload()),
    heartbeat,
  )
  let progress = @protocol.Progress::new(1, 2, 20L, 19L, "frontier-20")
  assert_eq(@protocol.Progress::from_payload(progress.to_payload()), progress)
}

///|
test "typed payload decoder rejects malformed content" {
  try @protocol.EventBatch::from_payload("not-json") catch {
    @protocol.MessageError::Malformed => ()
    @protocol.MessageError::WrongKind(..) => fail("unexpected kind error")
    @protocol.MessageError::Protocol(_) => fail("unexpected protocol error")
  } noraise {
    _ => fail("expected malformed payload")
  }
}

///|
test "constructors reject invalid flow-control values" {
  try @protocol.Credit::new(0, 0, 0L, 0L) catch {
    @protocol.ProtocolError::InvalidCredit => ()
    _ => fail("expected invalid credit")
  } noraise {
    _ => fail("expected invalid credit")
  }
  try @protocol.EventBatch::new(0, 0, 0L, []) catch {
    @protocol.ProtocolError::EmptyBatch => ()
    _ => fail("expected empty batch")
  } noraise {
    _ => fail("expected empty batch")
  }
}

///|
test "probe, reply and seal payloads round trip" {
  let probe = @protocol.Probe::new("query-1", 2, 7, 19L)
  assert_eq(@protocol.Probe::from_payload(probe.to_payload()), probe)
  let reply = @protocol.ProbeReply::new("query-1", 2, 7, 18L, "frontier-18")
  assert_eq(@protocol.ProbeReply::from_payload(reply.to_payload()), reply)
  let seal = @protocol.Seal::new("query-1", 2, 7, 18L, "frontier-18")
  assert_eq(@protocol.Seal::from_payload(seal.to_payload()), seal)
}

///|
test "probe messages reject invalid query and digest fields" {
  try @protocol.Probe::new("", 0, 0, -1L) catch {
    @protocol.ProtocolError::InvalidQueryId => ()
    _ => fail("expected invalid query id")
  } noraise {
    _ => fail("expected invalid query id")
  }
  try @protocol.ProbeReply::new("query-1", 0, 0, 0L, "") catch {
    @protocol.ProtocolError::InvalidDigest => ()
    _ => fail("expected invalid digest")
  } noraise {
    _ => fail("expected invalid digest")
  }
}

///|
test "progress rejects applied offset ahead of persisted offset" {
  try @protocol.Progress::new(0, 0, 3L, 4L, "frontier") catch {
    @protocol.ProtocolError::InvalidFrameField => ()
    _ => fail("expected invalid progress ordering")
  } noraise {
    _ => fail("expected invalid progress ordering")
  }
}
