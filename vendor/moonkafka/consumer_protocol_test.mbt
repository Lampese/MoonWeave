// ConsumerProtocol wire tests use hand-written bytes from Kafka's classic
// schema. Round-tripping our own encoder is insufficient because both sides
// could share the same width mistake.

///|
test "subscription uses int32 array and userData lengths" {
  let expected : Bytes = b"\x00\x00\x00\x00\x00\x01\x00\x0esource-batches\x00\x00\x00\x00"
  let encoded = @moonkafka.encode_consumer_protocol_subscription([
    "source-batches",
  ])
  assert_eq(encoded, expected)
  let decoded = @moonkafka.decode_consumer_protocol_subscription(expected)
  assert_eq(decoded, ["source-batches"])
}

///|
test "assignment uses int32 array and partition lengths" {
  let expected : Bytes = b"\x00\x00\x00\x00\x00\x01\x00\x06events\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00"
  let encoded = @moonkafka.encode_consumer_protocol_assignment([
    ("events", [0, 2]),
  ])
  assert_eq(encoded, expected)
  let decoded = @moonkafka.decode_consumer_protocol_assignment(expected)
  assert_eq(decoded, [("events", [0, 2])])
}

///|
test "subscription accepts nullable userData length" {
  let metadata : Bytes = b"\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff"
  assert_eq(@moonkafka.decode_consumer_protocol_subscription(metadata), [])
}
