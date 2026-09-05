///|
fn frame() -> @protocol.Frame raise @protocol.ProtocolError {
  @protocol.Frame::new(
    1,
    "cluster-a",
    "session-a",
    "message-1",
    "worker-a",
    "boot-1",
    4L,
    7,
    @protocol.FrameKind::Heartbeat,
    "{\"queue_depth\":2}",
  )
}

///|
test "frame round trip preserves metadata and payload" {
  let original = frame()
  let encoded = original.encode()
  assert_true(encoded.length() > original.payload().length())
  let body_length = (encoded[0].to_int() << 24) +
    (encoded[1].to_int() << 16) +
    (encoded[2].to_int() << 8) +
    encoded[3].to_int()
  assert_eq(body_length, encoded.length() - 4)
  let decoded = Frame::decode_payload(encoded[4:].to_owned())
  assert_eq(decoded, original)
}

///|
test "frame rejects oversized payloads before encoding" {
  let original = frame()
  try original.encode(maximum_frame_bytes=4) catch {
    @protocol.ProtocolError::FrameTooLarge(actual~, limit~) => {
      assert_true(actual > limit)
      assert_eq(limit, 4)
    }
    _ => fail("expected frame size error")
  } noraise {
    _ => fail("expected frame size error")
  }
}

///|
test "frame rejects malformed JSON payload" {
  try @protocol.Frame::decode_payload(b"not-json") catch {
    @protocol.ProtocolError::MalformedFrame => ()
    _ => fail("expected malformed frame")
  } noraise {
    _ => fail("expected malformed frame")
  }
}

///|
test "frame rejects unsupported protocol version" {
  let json = @json.to_json(frame())
  guard json is Json::Object(fields) else { fail("expected frame object") }
  fields["version"] = Json::number(2.0)
  let body = @utf8.encode(Json::object(fields).stringify())
  try @protocol.Frame::decode_payload(body) catch {
    @protocol.ProtocolError::InvalidProtocolVersion(version) =>
      assert_eq(version, 2)
    _ => fail("expected unsupported protocol version")
  } noraise {
    _ => fail("expected unsupported protocol version")
  }
}
