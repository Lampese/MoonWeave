///|
/// One append-only outbox command. A `put` records the Delta payload; an `ack`
/// records that the target Worker durably admitted it.
struct DurableOutboxEntry {
  version : Int
  kind : String
  delta_id : String
  delta : @protocol.DeltaBatch?
  delta_digest : String?
} derive(Debug, Eq, ToJson, FromJson)

///|
fn durable_outbox_key(delta_id : String) -> String {
  delta_id.length().to_string() + ":" + delta_id
}

///|
fn outbox_copy_json(value : Json, normalize_numbers? : Bool = false) -> Json {
  match value {
    Object(fields) => {
      let keys = fields.keys().to_array()
      keys.sort()
      let canonical : Map[String, Json] = Map([])
      for key in keys {
        canonical[key] = outbox_copy_json(fields[key], normalize_numbers~)
      }
      Json::object(canonical)
    }
    Array(items) =>
      Json::array(items.map(item => outbox_copy_json(item, normalize_numbers~)))
    Number(number, ..) if normalize_numbers =>
      Json::number(if number == 0.0 { 0.0 } else { number })
    _ => value
  }
}

///|
/// A local consistency fingerprint, not an authentication hash. Canonicalize
/// object key order and number spelling before hashing semantic JSON values.
fn durable_delta_digest(delta : @protocol.DeltaBatch) -> String {
  let mut hash : UInt64 = 14695981039346656037UL
  for
    byte in @utf8.encode(
      outbox_copy_json(@json.to_json(delta), normalize_numbers=true).stringify(),
    ) {
    hash = (hash ^ byte.to_uint64()) * 1099511628211UL
  }
  "delta-" + hash.to_string(radix=16)
}

///|
fn validate_durable_delta(
  delta : @protocol.DeltaBatch,
) -> @protocol.DeltaBatch raise DurableOutboxError {
  @protocol.DeltaBatch::from_payload(delta.to_payload()) catch {
    _ => raise DurableOutboxError::Malformed
  }
}

///|
fn copy_durable_delta(delta : @protocol.DeltaBatch) -> @protocol.DeltaBatch {
  {
    delta_id: delta.delta_id(),
    source_partition: delta.source_partition(),
    source_epoch: delta.source_epoch(),
    target_partition: delta.target_partition(),
    target_epoch: delta.target_epoch(),
    events: delta
    .events()
    .map(event => @protocol.EventRecord::{
      id: event.id(),
      parents: event.parents().to_owned(),
      key: event.key(),
      payload: outbox_copy_json(event.payload()),
    }),
  }
}

///|
fn validate_durable_digest(digest : String) -> Unit raise DurableOutboxError {
  if !digest.has_prefix("delta-") ||
    digest.length() <= 6 ||
    digest.length() > 22 {
    raise DurableOutboxError::Malformed
  }
  for character in digest[6:] {
    if !(character is ('0'..='9' | 'a'..='f')) {
      raise DurableOutboxError::Malformed
    }
  }
}

///|
fn durable_put_entry(delta : @protocol.DeltaBatch) -> DurableOutboxEntry {
  {
    version: 1,
    kind: "put",
    delta_id: delta.delta_id(),
    delta: Some(delta),
    delta_digest: Some(durable_delta_digest(delta)),
  }
}

///|
fn durable_ack_entry(delta_id : String) -> DurableOutboxEntry {
  { version: 1, kind: "ack", delta_id, delta: None, delta_digest: None, }
}

///|
fn durable_tombstone_entry(
  delta_id : String,
  delta_digest : String,
) -> DurableOutboxEntry {
  {
    version: 1,
    kind: "tombstone",
    delta_id,
    delta: None,
    delta_digest: Some(delta_digest),
  }
}

///|
fn apply_durable_outbox_entry(
  pending : Array[@protocol.DeltaBatch],
  known : Map[String, @protocol.DeltaBatch],
  known_digests : Map[String, String],
  acked : Set[String],
  entry : DurableOutboxEntry,
) -> Unit raise DurableOutboxError {
  if entry.version != 1 {
    raise DurableOutboxError::UnsupportedVersion(entry.version)
  }
  if entry.delta_id.is_empty() {
    raise DurableOutboxError::Malformed
  }
  let key = durable_outbox_key(entry.delta_id)
  match entry.kind {
    "put" => {
      let delta = match entry.delta {
        Some(value) => validate_durable_delta(value)
        None => raise DurableOutboxError::Malformed
      }
      if delta.delta_id() != entry.delta_id {
        raise DurableOutboxError::IdentityConflict(key)
      }
      let digest = durable_delta_digest(delta)
      match entry.delta_digest {
        Some(expected) if expected != digest =>
          raise DurableOutboxError::IdentityConflict(key)
        _ => ()
      }
      match known_digests.get(entry.delta_id) {
        Some(previous) if previous != digest =>
          raise DurableOutboxError::IdentityConflict(key)
        _ => ()
      }
      match known.get(entry.delta_id) {
        Some(previous) =>
          if previous != delta {
            raise DurableOutboxError::IdentityConflict(key)
          }
        None => {
          known_digests[entry.delta_id] = digest
          if !acked.contains(entry.delta_id) {
            known[entry.delta_id] = delta
            pending.push(delta)
          }
        }
      }
    }
    "ack" => {
      if entry.delta is Some(_) || entry.delta_digest is Some(_) {
        raise DurableOutboxError::Malformed
      }
      guard known_digests.contains(entry.delta_id) else {
        raise DurableOutboxError::UnknownDelta(entry.delta_id)
      }
      acked.add(entry.delta_id)
      known.remove(entry.delta_id)
      let mut index = 0
      while index < pending.length() {
        if pending[index].delta_id() == entry.delta_id {
          ignore(pending.remove(index))
          break
        }
        index += 1
      }
    }
    "tombstone" => {
      guard entry.delta is None && entry.delta_digest is Some(digest) else {
        raise DurableOutboxError::Malformed
      }
      validate_durable_digest(digest)
      match known_digests.get(entry.delta_id) {
        Some(previous) if previous != digest =>
          raise DurableOutboxError::IdentityConflict(key)
        _ => ()
      }
      known_digests[entry.delta_id] = digest
      acked.add(entry.delta_id)
      known.remove(entry.delta_id)
      let mut index = 0
      while index < pending.length() {
        if pending[index].delta_id() == entry.delta_id {
          ignore(pending.remove(index))
          break
        }
        index += 1
      }
    }
    _ => raise DurableOutboxError::Malformed
  }
}
