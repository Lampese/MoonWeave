// The embedded consumer-group protocol payloads (Phase 4 classic path):
// ConsumerProtocolSubscription rides JoinGroup metadata and
// ConsumerProtocolAssignment rides SyncGroup assignments. The version and
// string lengths are fixed-width INT16; array counts and bytes lengths are
// INT32, matching Kafka's classic ConsumerProtocol schemas.

///|
pub const CONSUMER_PROTOCOL_SUBSCRIPTION_VERSION : Int = 0

///|
pub const CONSUMER_PROTOCOL_ASSIGNMENT_VERSION : Int = 0

///|
/// Serialize a ConsumerProtocolSubscription: the subscribed topic names
/// (userData stays empty).
pub fn encode_consumer_protocol_subscription(topics : Array[String]) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i16(CONSUMER_PROTOCOL_SUBSCRIPTION_VERSION)
  body.write_i32(topics.length())
  for topic in topics {
    let bytes = @utf8.encode(topic)
    body.write_i16(bytes.length())
    body.write_bytes(bytes)
  }
  body.write_i32(0) // userData: empty
  body.to_bytes()
}

///|
/// Parse a ConsumerProtocolSubscription: the subscribed topic names.
pub fn decode_consumer_protocol_subscription(
  data : Bytes,
) -> Array[String] raise {
  let d = @buf.Decoder::new(data)
  let version = d.read_i16()
  guard version == CONSUMER_PROTOCOL_SUBSCRIPTION_VERSION else {
    raise ProtocolError::ProtocolError(
      "unsupported ConsumerProtocolSubscription version \{version}",
    )
  }
  let n = d.read_i32()
  if n < 0 {
    raise ProtocolError::ProtocolError(
      "negative ConsumerProtocolSubscription topic count",
    )
  }
  let topics : Array[String] = []
  for _ in 0..<n {
    let len = d.read_i16()
    topics.push(@utf8.decode_lossy(d.read_bytes(len)[:]))
  }
  let user_data_len = d.read_i32()
  if user_data_len < -1 {
    raise ProtocolError::ProtocolError(
      "invalid ConsumerProtocolSubscription userData length",
    )
  }
  if user_data_len > 0 {
    ignore(d.read_bytes(user_data_len))
  }
  topics
}

///|
/// Serialize a ConsumerProtocolAssignment: topic partitions per member.
pub fn encode_consumer_protocol_assignment(
  assignment : Array[(String, Array[Int])],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i16(CONSUMER_PROTOCOL_ASSIGNMENT_VERSION)
  body.write_i32(assignment.length())
  for entry in assignment {
    let (topic, partitions) = entry
    let bytes = @utf8.encode(topic)
    body.write_i16(bytes.length())
    body.write_bytes(bytes)
    body.write_i32(partitions.length())
    for partition in partitions {
      body.write_i32(partition)
    }
  }
  body.write_i32(0) // userData: empty
  body.to_bytes()
}

///|
/// Parse a ConsumerProtocolAssignment: topic partitions per member.
pub fn decode_consumer_protocol_assignment(
  data : Bytes,
) -> Array[(String, Array[Int])] raise {
  let d = @buf.Decoder::new(data)
  let version = d.read_i16()
  guard version == CONSUMER_PROTOCOL_ASSIGNMENT_VERSION else {
    raise ProtocolError::ProtocolError(
      "unsupported ConsumerProtocolAssignment version \{version}",
    )
  }
  let n = d.read_i32()
  if n < 0 {
    raise ProtocolError::ProtocolError(
      "negative ConsumerProtocolAssignment topic count",
    )
  }
  let out : Array[(String, Array[Int])] = []
  for _ in 0..<n {
    let len = d.read_i16()
    let topic = @utf8.decode_lossy(d.read_bytes(len)[:])
    let parts = d.read_i32()
    if parts < 0 {
      raise ProtocolError::ProtocolError(
        "negative ConsumerProtocolAssignment partition count",
      )
    }
    let partitions : Array[Int] = []
    for _ in 0..<parts {
      partitions.push(d.read_i32())
    }
    out.push((topic, partitions))
  }
  let user_data_len = d.read_i32()
  if user_data_len < -1 {
    raise ProtocolError::ProtocolError(
      "invalid ConsumerProtocolAssignment userData length",
    )
  }
  if user_data_len > 0 {
    ignore(d.read_bytes(user_data_len))
  }
  out
}
