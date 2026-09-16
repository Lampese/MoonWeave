///|
/// Progress of one logical input. Idle inputs stop holding back the minimum;
/// resuming an idle input never moves the published frontier backwards.
pub(all) struct InputWatermark {
  input_id : String
  source_id : String?
  maximum_timestamp : Int64
  last_activity : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct SourceWatermarkSnapshot {
  inputs : Array[InputWatermark]
  watermark : Int64
  idle_timeout_ms : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub struct SourceWatermarks {
  inputs : Array[InputWatermark]
  mut watermark : Int64
  idle_timeout_ms : Int64
}

///|
/// Construct a minimum frontier for the declared inputs. Zero disables idle
/// detection; a positive timeout is measured using the supplied clock.
pub fn SourceWatermarks::new(
  input_ids : ArrayView[String],
  now : Int64,
  idle_timeout_ms? : Int64 = 0L,
) -> SourceWatermarks raise FrameIoError {
  if input_ids.is_empty() || now < 0L || idle_timeout_ms < 0L {
    raise FrameIoError::Handler("invalid source watermark configuration")
  }
  let ids = input_ids.to_owned()
  ids.sort()
  let seen : Set[String] = Set([])
  let inputs : Array[InputWatermark] = []
  for input_id in ids {
    if input_id.is_empty() || !seen.add_and_check(input_id) {
      raise FrameIoError::Handler("empty or duplicate source input")
    }
    inputs.push({
      input_id,
      source_id: None,
      maximum_timestamp: -1L,
      last_activity: now,
    })
  }
  { inputs, watermark: -1L, idle_timeout_ms, }
}

///|
pub fn SourceWatermarks::snapshot(
  self : SourceWatermarks,
) -> SourceWatermarkSnapshot {
  {
    inputs: self.inputs.copy(),
    watermark: self.watermark,
    idle_timeout_ms: self.idle_timeout_ms,
  }
}

///|
fn SourceWatermarks::copy(self : SourceWatermarks) -> SourceWatermarks {
  {
    inputs: self.inputs.copy(),
    watermark: self.watermark,
    idle_timeout_ms: self.idle_timeout_ms,
  }
}

///|
pub fn SourceWatermarks::restore(
  snapshot : SourceWatermarkSnapshot,
) -> SourceWatermarks raise FrameIoError {
  let logical_ids : Set[String] = Set([])
  for input in snapshot.inputs {
    logical_ids.add(input.input_id)
  }
  let result = SourceWatermarks::new(
    logical_ids.to_array(),
    0L,
    idle_timeout_ms=snapshot.idle_timeout_ms,
  )
  if snapshot.watermark < -1L || snapshot.watermark == 9223372036854775807L {
    raise FrameIoError::Handler("invalid source watermark snapshot")
  }
  let identities : Set[(String, String?)] = Set([])
  let placeholders : Set[String] = Set([])
  let physical_inputs : Set[String] = Set([])
  for input in snapshot.inputs {
    if !identities.add_and_check((input.input_id, input.source_id)) ||
      input.source_id == Some("") ||
      input.maximum_timestamp < -1L ||
      input.maximum_timestamp == 9223372036854775807L ||
      input.last_activity < 0L {
      raise FrameIoError::Handler("invalid source input snapshot")
    }
    match input.source_id {
      None => placeholders.add(input.input_id)
      Some(_) => physical_inputs.add(input.input_id)
    }
  }
  for input_id in placeholders {
    if physical_inputs.contains(input_id) {
      raise FrameIoError::Handler("mixed logical and physical watermark input")
    }
  }
  result.inputs.clear()
  result.inputs.push_iter(snapshot.inputs.iter())
  result.watermark = snapshot.watermark
  result
}

///|
pub fn SourceWatermarks::observe(
  self : SourceWatermarks,
  input_id : String,
  timestamp : Int64,
  now : Int64,
  source_id? : String,
) -> Unit raise FrameIoError {
  if timestamp < 0L || timestamp == 9223372036854775807L || now < 0L {
    raise FrameIoError::Handler("invalid source event timestamp")
  }
  if !self.inputs.any(input => {
      input.input_id == input_id &&
      (input.source_id is None || input.source_id == source_id)
    }) {
    raise FrameIoError::Handler("unknown source input: " + input_id)
  }
  for index in 0..<self.inputs.length() {
    let input = self.inputs[index]
    // One physical stream may multiplex both sides of a join. Its frontier
    // covers both logical inputs, including when a partition emits only one
    // side. Independently consumed sources use distinct physical identities.
    if (input.source_id is None && input.input_id == input_id) ||
      (source_id is Some(_) && input.source_id == source_id) {
      self.inputs[index] = {
        input_id: input.input_id,
        source_id: input.source_id,
        maximum_timestamp: if timestamp > input.maximum_timestamp {
          timestamp
        } else {
          input.maximum_timestamp
        },
        last_activity: now,
      }
    }
  }
}

///|
/// Coordinate all assigned Kafka partitions separately. An assigned partition
/// with no timestamp holds the frontier just like an unseen logical input.
pub fn SourceWatermarks::assign_partitions(
  self : SourceWatermarks,
  input_id : String,
  source_ids : ArrayView[String],
  now : Int64,
) -> Unit raise FrameIoError {
  let previous = self.inputs.filter(input => input.input_id == input_id)
  if now < 0L || previous.is_empty() {
    raise FrameIoError::Handler("unknown watermark input")
  }
  let ids = source_ids.to_owned()
  ids.sort()
  let seen : Set[String] = Set([])
  let next : Array[InputWatermark] = []
  for id in ids {
    if id.is_empty() || !seen.add_and_check(id) {
      raise FrameIoError::Handler("invalid source partitions")
    }
    let existing = previous
      .iter()
      .find_first(input => input.source_id == Some(id))
    next.push(
      existing.unwrap_or({
        input_id,
        source_id: Some(id),
        maximum_timestamp: -1L,
        last_activity: now,
      }),
    )
  }
  if next.is_empty() {
    next.push(
      previous
      .iter()
      .find_first(input => input.source_id is None)
      .unwrap_or({
        input_id,
        source_id: None,
        maximum_timestamp: -1L,
        last_activity: now,
      }),
    )
  }
  let retained = self.inputs.filter(input => input.input_id != input_id)
  retained.push_iter(next.iter())
  retained.sort_by((a, b) => {
    (a.input_id, a.source_id).compare((b.input_id, b.source_id))
  })
  self.inputs.clear()
  self.inputs.push_iter(retained.iter())
}

///|
fn SourceWatermarks::input_ids(self : SourceWatermarks) -> Array[String] {
  let ids : Set[String] = Set([])
  for input in self.inputs {
    ids.add(input.input_id)
  }
  let sorted = ids.to_array()
  sorted.sort()
  sorted
}

///|
pub fn SourceWatermarks::advance(
  self : SourceWatermarks,
  now : Int64,
) -> Int64 raise FrameIoError {
  if now < 0L {
    raise FrameIoError::Handler("invalid watermark clock")
  }
  let mut minimum : Int64? = None
  for input in self.inputs {
    let idle = self.idle_timeout_ms > 0L &&
      now >= input.last_activity &&
      now - input.last_activity >= self.idle_timeout_ms
    if idle {
      continue
    }
    if input.maximum_timestamp < 0L {
      return self.watermark
    }
    minimum = Some(
      match minimum {
        Some(previous) if previous < input.maximum_timestamp => previous
        _ => input.maximum_timestamp
      },
    )
  }
  match minimum {
    Some(value) if value > self.watermark => self.watermark = value
    _ => ()
  }
  self.watermark
}

///|
struct SourceWatermarkStore {
  wal : Wal
  mut progress : SourceWatermarks
}

///|
async fn SourceWatermarkStore::open(
  path : String,
  input_ids : ArrayView[String],
  idle_timeout_ms : Int64,
  maximum_bytes : Int64?,
) -> SourceWatermarkStore raise FrameIoError {
  let (wal, replay) = Wal::open(path, maximum_bytes?) catch {
    error => raise FrameIoError::Wal(error)
  }
  errdefer wal.close()
  let now = @async.now()
  let mut progress = SourceWatermarks::new(input_ids, now, idle_timeout_ms~)
  for bytes in replay.records() {
    let snapshot : SourceWatermarkSnapshot = @json.from_json(
      @json.parse(@utf8.decode(bytes)),
    ) catch {
      _ => raise FrameIoError::Handler("malformed source watermark WAL")
    }
    let restored = SourceWatermarks::restore(snapshot)
    if restored.input_ids() != progress.input_ids() ||
      restored.idle_timeout_ms != idle_timeout_ms ||
      restored.watermark < progress.watermark {
      raise FrameIoError::Handler(
        "source watermark configuration or history mismatch",
      )
    }
    progress = restored
  }
  // Clock epochs need not survive a machine restart. Re-enter all inputs as
  // active while preserving the durable, monotone event-time frontier.
  for index in 0..<progress.inputs.length() {
    progress.inputs[index] = { ..progress.inputs[index], last_activity: now, }
  }
  { wal, progress, }
}

///|
async fn SourceWatermarkStore::save(
  self : SourceWatermarkStore,
  candidate : SourceWatermarks,
) -> Unit raise FrameIoError {
  self.wal.rewrite([
    @utf8.encode(@json.to_json(candidate.snapshot()).stringify()),
  ]) catch {
    error => raise FrameIoError::Wal(error)
  }
  self.progress = candidate
}
