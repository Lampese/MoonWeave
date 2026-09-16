///|
/// File and Kafka inputs use the same ingress lock and durable source plan.
/// Stop acknowledges only after every in-flight append/offset commit ends.
async fn SourceBackedExecution::pump_json_lines_source(
  self : SourceBackedExecution,
  source : JsonLinesSource,
  input_id : String?,
) -> Unit raise ApiError {
  defer {
    self.active_source_pumps -= 1
  }
  while !self.kafka_stop_requested && self.phase() == JobRunPhase::Running {
    let (batch, next_offset) = source.poll_with_offset(maximum_events=128)
    if self.kafka_stop_requested || self.phase() != JobRunPhase::Running {
      break
    }
    let events = batch.map(event => {
      match input_id {
        None => event
        Some(input_id) => event.bind_input(input_id)
      }
    })
    for event in events {
      self.job.validate_event(event)
    }
    if !events.is_empty() {
      self.source.ingest_all(events.map(event => event.record)) catch {
        error => raise live_execution_error("persist file source batch", error)
      }
    }
    if self.confirm_authority is Some(confirm) {
      confirm()
    }
    source.commit(next_offset)
    if events.is_empty() {
      @async_runtime.sleep(10) catch {
        error => raise source_error("poll file", error)
      }
    }
  }
}
