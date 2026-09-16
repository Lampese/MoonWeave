///|
/// A frontier becomes available to the next stage only when every shard has
/// durably applied it and delivered all outputs it caused.
pub fn SourceBackedJob::confirmed_watermark(self : SourceBackedJob) -> Int64 {
  let mut minimum = 9223372036854775807L
  for partition in 0..<self.plan.partition_count() {
    let value = self.confirmed_watermarks.get(partition).unwrap_or(-1L)
    if value < minimum {
      minimum = value
    }
  }
  minimum
}

///|
pub async fn SourceBackedJob::advance_graph_watermark(
  self : SourceBackedJob,
  edge : String,
  watermark : Int64,
) -> Unit raise FrameIoError {
  if watermark < 0L || watermark == 9223372036854775807L {
    return
  }
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  guard self.external_watermark_store is Some(store) else { return }
  let before = store.progress.inputs
    .iter()
    .find_first(input => input.input_id == edge)
  if before is Some(input) && input.maximum_timestamp >= watermark {
    return
  }
  let candidate = store.progress.copy()
  candidate.observe(edge, watermark, @async.now())
  ignore(candidate.advance(@async.now()))
  store.save(candidate)
}
