///|
struct OutboxReplayFixture {
  version : Int
  kind : String
  delta_id : String
  delta : @protocol.DeltaBatch?
  delta_digest : String?
} derive(ToJson)

///|
async test "outbox rejects tombstones without a usable digest" {
  let invalid_digests : Array[String?] = [None, Some(""), Some("delta-xyz")]
  for digest in invalid_digests {
    with_durable_outbox_path <| path => {
      let (wal, _) = Wal::open(path)
      let fixture = OutboxReplayFixture::{
        version: 1,
        kind: "tombstone",
        delta_id: "bad-tombstone",
        delta: None,
        delta_digest: digest,
      }
      ignore(wal.append(@utf8.encode(@json.to_json(fixture).stringify())))
      wal.close()
      try DurableExchangeOutbox::open(path, 1) catch {
        error => assert_eq(error, DurableOutboxError::Malformed)
      } noraise {
        value => {
          value.close()
          fail("expected invalid tombstone")
        }
      }
    }
  }
}

///|
async test "outbox replay rejects payload conflicts after a compacted tombstone" {
  with_durable_outbox_path <| path => {
    let delta = durable_test_delta("replayed-conflict", "A", "B")
    let outbox = DurableExchangeOutbox::open(path, 1)
    outbox.push(delta)
    outbox.ack(delta.delta_id())
    outbox.compact()
    outbox.close()
    let conflicting = durable_test_delta(delta.delta_id(), "changed", "B")
    let (wal, _) = Wal::open(path)
    let fixture = OutboxReplayFixture::{
      version: 1,
      kind: "put",
      delta_id: delta.delta_id(),
      delta: Some(conflicting),
      delta_digest: None,
    }
    ignore(wal.append(@utf8.encode(@json.to_json(fixture).stringify())))
    wal.close()
    try DurableExchangeOutbox::open(path, 1) catch {
      DurableOutboxError::IdentityConflict(_) => ()
      error => fail("unexpected recovery error: " + @debug.to_string(error))
    } noraise {
      value => {
        value.close()
        fail("expected conflicting WAL record")
      }
    }
  }
}

///|
async test "outbox revalidates Delta envelopes before persistence" {
  with_durable_outbox_path <| path => {
    let valid = durable_test_delta("validated-delta", "A", "B")
    let invalid : @protocol.DeltaBatch = { ..valid, target_epoch: -1, }
    let outbox = DurableExchangeOutbox::open(path, 1)
    defer outbox.close()
    try outbox.push(invalid) catch {
      error => assert_eq(error, DurableOutboxError::Malformed)
    } noraise {
      _ => fail("expected invalid Delta")
    }
    assert_eq(@fs.read_file(path).binary().length(), 0)
    outbox.push(valid)
    assert_eq(outbox.pending_count(), 1)
  }
}
