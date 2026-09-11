///|
test "event identities retain all Int64 bits in JSON comparison and hashing" {
  let sequences : Array[Int64] = [
    0L, 2147483647L, 2147483648L, 9007199254740991L, 9007199254740992L, 9007199254740993L,
    9223372036854775806L, 9223372036854775807L,
  ]
  let unique : Set[@causal.EventId] = Set([])
  let ordered = []
  for sequence in sequences {
    let id = @causal.EventId::new("kafka/orders/7", sequence)
    let encoded = @json.to_json(id)
    assert_eq(
      encoded,
      Json::object({
        "origin": Json::string("kafka/orders/7"),
        "sequence": Json::string(sequence.to_string()),
      }),
    )
    let decoded : @causal.EventId = @json.from_json(
      @json.parse(encoded.stringify()),
    )
    assert_eq(decoded, id)
    assert_eq(decoded.sequence(), sequence)
    assert_true(unique.add_and_check(decoded))
    assert_true(unique.contains(id))
    ordered.push(id)
  }
  ordered.sort()
  assert_eq(ordered.map(id => id.sequence()), sequences)
}

///|
test "event identities reject negative sequences and lossy JSON representations" {
  for sequence in [-1L, -9223372036854775808L] {
    try @causal.EventId::new("source", sequence) catch {
      @causal.CausalError::NegativeSequence(actual) =>
        assert_eq(actual, sequence)
      _ => fail("expected negative sequence rejection")
    } noraise {
      _ => fail("negative sequence was accepted")
    }
  }
  for
    sequence in [
      Json::number(9007199254740992.0),
      Json::string("9223372036854775808"),
      Json::string("1.5"),
    ] {
    try {
      let _ : @causal.EventId = @json.from_json(
        Json::object({ "origin": Json::string("source"), "sequence": sequence }),
      )
    } catch {
      @json.JsonDecodeError(_) => ()
    } noraise {
      _ => fail("invalid Int64 sequence encoding was accepted")
    }
  }
}

///|
test "large adjacent identities preserve causal dependencies and duplicate suppression" {
  let parent_id = @causal.EventId::new("kafka/orders/0", 9007199254740992L)
  let child_id = @causal.EventId::new("kafka/orders/0", 9007199254740993L)
  let parent = @causal.Event::new(parent_id, [], "account", "parent")
  let child = @causal.Event::new(child_id, [parent_id], "account", "child")
  let log = @causal.CausalLog::new()
  assert_eq(log.submit(child).status(), @causal.SubmitStatus::Pending)
  assert_eq(log.submit(parent).admitted().map(event => event.id()), [
    parent_id, child_id,
  ])
  assert_eq(log.submit(child).status(), @causal.SubmitStatus::Duplicate)
  assert_eq(log.admitted_count(), 2)
  assert_eq(log.pending_count(), 0)
  assert_eq(log.frontier().ids(), [child_id])
}
