///|
fn join_expiry(timestamp : Int64, interval : Int64) -> Int64 {
  if timestamp > 9223372036854775807L - interval {
    9223372036854775807L
  } else {
    timestamp + interval
  }
}

///|
/// Validate the buffers that cannot be reconstructed by replaying only the
/// event set: watermark delivery order determines lateness and emitted pairs.
fn Task::validate_event_time_state(self : Task) -> Unit raise TaskError {
  let config = match self.event_time_config {
    None => {
      if !self.window_buckets.is_empty() ||
        !self.join_left.is_empty() ||
        !self.join_right.is_empty() ||
        !self.join_emitted.is_empty() {
        raise TaskError::InvalidTransition
      }
      return
    }
    Some(config) => config
  }
  if self.watermark_requested != 9223372036854775807L &&
    self.watermark_requested >= 0L {
    let maximum = if self.watermark_requested <= config.allowed_lateness_ms() {
      0L
    } else {
      self.watermark_requested - config.allowed_lateness_ms()
    }
    if self.watermark > maximum {
      raise TaskError::InvalidTransition
    }
  }
  if config.window_size_ms() > 0L {
    if !self.join_left.is_empty() ||
      !self.join_right.is_empty() ||
      !self.join_emitted.is_empty() {
      raise TaskError::InvalidTransition
    }
    let buckets : Set[(String, Int64)] = Set([])
    for bucket in self.window_buckets {
      if bucket.key.is_empty() ||
        bucket.start_ms < 0L ||
        bucket.start_ms % config.window_size_ms() != 0L ||
        bucket.start_ms > 9223372036854775807L - config.window_size_ms() ||
        bucket.start_ms + config.window_size_ms() <= self.watermark ||
        !buckets.add_and_check((bucket.key, bucket.start_ms)) {
        raise TaskError::InvalidTransition
      }
    }
    return
  }
  if !self.window_buckets.is_empty() {
    raise TaskError::InvalidTransition
  }
  let seen : Set[@causal.EventId] = Set([])
  for
    (side, records, interval) in [
      ("left", self.join_left, config.join_after_ms()),
      ("right", self.join_right, config.join_before_ms()),
    ] {
    for record in records {
      guard record.event_time() is Some(timestamp) else {
        raise TaskError::InvalidTransition
      }
      if record.input_id() != side ||
        !seen.add_and_check(record.id()) ||
        self.watermark == 9223372036854775807L ||
        join_expiry(timestamp, interval) < self.watermark ||
        !self.event_time_log.unwrap().contains(record.id()) {
        raise TaskError::InvalidTransition
      }
      if self.event_time_log.unwrap().event(record.id()) is Some(event) &&
        event != task_event_causal(record) {
        raise TaskError::InvalidTransition
      }
    }
  }
  let expected : Set[(@causal.EventId, @causal.EventId)] = Set([])
  if self.watermark >= 0L {
    for left in self.join_left {
      for right in self.join_right {
        let lt = left.event_time().unwrap()
        let rt = right.event_time().unwrap()
        if left.key() == right.key() &&
          (
            (rt >= lt && rt - lt <= config.join_after_ms()) ||
            (lt > rt && lt - rt <= config.join_before_ms())
          ) {
          expected.add((left.id(), right.id()))
        }
      }
    }
  }
  if expected != self.join_emitted {
    raise TaskError::InvalidTransition
  }
}
