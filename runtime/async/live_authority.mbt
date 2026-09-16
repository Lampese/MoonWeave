///|
/// Logical Coordinator authority for one execution. Worker checkpoints stay
/// with their Workers; these records preserve the plans needed to resume them.
pub(all) struct LiveJobAuthoritySnapshot {
  version : Int
  coordinator : @control.CoordinatorSnapshot
  job : JobStoreAuthoritySnapshot
  source : SourceLogSnapshot
  watermark : SourceWatermarkSnapshot?
  graph_watermark : SourceWatermarkSnapshot?
  confirmed_watermarks : Array[(Int, Int64)]
  acknowledged_cuts : Array[(Int, Array[Int64])]
  retirement : @causal.CausalRetirement?
  retirement_acks : Array[(Int, String)]
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Require quorum publication before delivering a source cut to any Worker.
pub fn SourceBackedJob::set_authority_barrier(
  self : SourceBackedJob,
  confirm : async () -> Unit raise FrameIoError,
) -> Unit {
  self.authority_barrier = Some(confirm)
}

///|
/// Capture ingress and control state under the same lock order as recovery.
pub async fn SourceBackedJob::authority_snapshot(
  self : SourceBackedJob,
) -> LiveJobAuthoritySnapshot raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.job_store.write_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.job_store.write_lock.release()
  self.coordinator.write_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.coordinator.write_lock.release()
  let confirmed_watermarks = self.confirmed_watermarks.to_array()
  confirmed_watermarks.sort_by((a, b) => a.0.compare(b.0))
  let acknowledged_cuts = self.acknowledged_cuts
    .to_array()
    .map(entry => (entry.0, entry.1.copy()))
  acknowledged_cuts.sort_by((a, b) => a.0.compare(b.0))
  let retirement_acks = self.retirement_acks.to_array()
  retirement_acks.sort_by((a, b) => a.0.compare(b.0))
  {
    version: 1,
    coordinator: self.coordinator.snapshot(),
    job: self.job_store.authority_snapshot(),
    source: self.source.snapshot(),
    watermark: self.watermark_store.map(store => store.progress.snapshot()),
    graph_watermark: self.external_watermark_store.map(store => {
      store.progress.snapshot()
    }),
    confirmed_watermarks,
    acknowledged_cuts,
    retirement: self.retirement,
    retirement_acks,
  }
}

///|
async fn restore_authority_watermark(
  store : SourceWatermarkStore?,
  snapshot : SourceWatermarkSnapshot?,
) -> Unit raise FrameIoError {
  match (store, snapshot) {
    (None, None) => ()
    (Some(store), Some(snapshot)) => {
      let candidate = SourceWatermarks::restore(snapshot)
      if candidate.input_ids() != store.progress.input_ids() ||
        candidate.idle_timeout_ms != store.progress.idle_timeout_ms {
        raise FrameIoError::Handler("authority watermark topology mismatch")
      }
      let now = @async.now()
      for index in 0..<candidate.inputs.length() {
        candidate.inputs[index] = {
          ..candidate.inputs[index],
          last_activity: now,
        }
      }
      store.save(candidate)
    }
    _ =>
      raise FrameIoError::Handler("authority watermark configuration mismatch")
  }
}

///|
/// Used only while opening an execution, before its supervisor is started.
async fn SourceBackedJob::restore_source_authority(
  self : SourceBackedJob,
  snapshot : LiveJobAuthoritySnapshot,
) -> Unit raise FrameIoError {
  if snapshot.version != 1 ||
    snapshot.job.snapshot.job_id() != self.plan.job_id() ||
    snapshot.source.entries.any(entry => {
      entry.partition >= self.plan.partition_count()
    }) ||
    snapshot.source.offsets.any(entry => {
      entry.partition >= self.plan.partition_count()
    }) {
    raise FrameIoError::Handler("invalid live authority topology")
  }
  let count = self.plan.partition_count()
  let seen : Set[Int] = Set([])
  for (partition, timestamp) in snapshot.confirmed_watermarks {
    if partition < 0 ||
      partition >= count ||
      timestamp < -1L ||
      !seen.add_and_check(partition) {
      raise FrameIoError::Handler("invalid confirmed authority watermark")
    }
  }
  seen.clear()
  for (partition, cuts) in snapshot.acknowledged_cuts {
    if partition < 0 ||
      partition >= count ||
      !seen.add_and_check(partition) ||
      cuts.length() != count ||
      cuts.any(cut => cut < 0L) {
      raise FrameIoError::Handler("invalid authority checkpoint cut")
    }
  }
  let retirement = snapshot.retirement.map(value => {
    @causal.CausalRetirement::new(value.checkpoint_id(), value.ranges()) catch {
      _ => raise FrameIoError::Handler("invalid authority retirement")
    }
  })
  seen.clear()
  for (partition, id) in snapshot.retirement_acks {
    if partition < 0 ||
      partition >= count ||
      !seen.add_and_check(partition) ||
      (retirement is Some(value) && value.checkpoint_id() != id) {
      raise FrameIoError::Handler("invalid authority retirement ACK")
    }
  }
  self.source.replace_snapshot(snapshot.source) catch {
    error => raise FrameIoError::SourceLog(error)
  }
  restore_authority_watermark(self.watermark_store, snapshot.watermark)
  restore_authority_watermark(
    self.external_watermark_store,
    snapshot.graph_watermark,
  )
  for (partition, timestamp) in snapshot.confirmed_watermarks {
    self.confirmed_watermarks[partition] = timestamp
  }
  // Session ACKs are re-established after reconnect. Retaining their payload
  // in authority makes an interrupted retirement inspectable, but they cannot
  // authorize another deletion until current sessions confirm the same cut.
  self.retirement = retirement
}
