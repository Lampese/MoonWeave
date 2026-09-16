///|
struct LiveWorkCut {
  work : SupervisorWork
  watermark : Int64
  retained_from : Int64
  source_offsets : Array[Int64]
  generation : Int64
}

///|
/// Bound retained ingress independently from a slow sink. Capacity returns
/// after the coordinated checkpoint releases its acknowledged source prefix.
async fn SourceBackedJob::acquire_ingress(
  self : SourceBackedJob,
  events : ArrayView[@protocol.EventRecord],
) -> Unit raise FrameIoError {
  for ;; {
    if self.lifecycle.phase() == LiveJobPhase::Paused {
      @async.sleep(5) catch {
        error => raise FrameIoError::Io(@debug.to_string(error))
      }
      continue
    }
    self.ingress_gate.acquire() catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
    if !self.supervising ||
      self.source.event_count() < 16 ||
      events.all(event => {
        self.source.locations.contains(event.id()) ||
        self.source.retired.any(range => range.contains(event.id()))
      }) {
      return
    }
    self.ingress_gate.release()
    self.check_open()
    if self.lifecycle.phase() != LiveJobPhase::Open {
      if self.lifecycle.phase() == LiveJobPhase::Paused {
        continue
      }
      raise FrameIoError::Handler("live source is sealed")
    }
    @async.sleep(5) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
  }
}

///|
fn SourceBackedJob::source_offsets(self : SourceBackedJob) -> Array[Int64] {
  Array::makei(self.plan.partition_count(), partition => {
    self.source.next_offset(partition).unwrap_or(0L)
  })
}

///|
fn SourceBackedJob::input_floor(
  self : SourceBackedJob,
  partition : Int,
  start : Int64,
) -> Int64 {
  self.source.retention
  .get(partition)
  .map(marker => marker.retain_from)
  .unwrap_or(start)
}

///|
pub fn SourceBackedJob::retired_inputs(
  self : SourceBackedJob,
) -> Array[@causal.EventRange] {
  self.source.retired.copy()
}

///|
/// Finish deletions covered by the atomic Job checkpoint. Replaying this
/// after any of the individual source WAL rewrites is safe and idempotent.
async fn SourceBackedJob::finish_source_reclamation(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  for partition in 0..<self.plan.partition_count() {
    match self.job_store.work_plans.get(partition) {
      Some(plan) =>
        match plan.retained_from {
          Some(cut) if cut >
            self.source.retention
            .get(partition)
            .map(marker => marker.retain_from)
            .unwrap_or(0L) => {
            let barrier = self.source.retention_barrier(partition, cut) catch {
              error => raise FrameIoError::SourceLog(error)
            }
            self.source.compact(barrier) catch {
              error => raise FrameIoError::SourceLog(error)
            }
          }
          _ => ()
        }
      None => ()
    }
  }
}

///|
/// Ingress pauses while sessions finish their current durable work cut. The
/// separate gate leaves work reads and ACK processing free to make progress.
async fn SourceBackedJob::reclaim_running(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  if self.source.event_count() < 16 ||
    self.lifecycle.phase() != LiveJobPhase::Open {
    return
  }
  self.ingress_gate.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_gate.release()
  let cuts = self.source_offsets()
  while !Array::makei(self.plan.partition_count(), partition => partition).all(partition => {
          self.acknowledged_cuts.get(partition) == Some(cuts)
        }) {
    if self.closed || self.lifecycle.phase() != LiveJobPhase::Open {
      return
    }
    @async.sleep(5) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
  }
  let ranges = self.source.retired.copy()
  for location in self.source.locations.values() {
    let id = location.event.id()
    ranges.push(
      @causal.EventRange::new(id.origin(), id.sequence(), id.sequence()) catch {
        _ => raise FrameIoError::Handler("invalid source identity")
      },
    )
  }
  let certificate = @causal.CausalRetirement::new(
    self.plan.job_id() +
    "-checkpoint-" +
    self.job_store.journal_generation().to_string(),
    ranges,
  ) catch {
    _ => raise FrameIoError::Handler("invalid source retirement")
  }
  self.retirement = Some(certificate)
  self.retirement_acks.clear()
  // A drain may race an already-sent request. Keep its identity available
  // until the session has consumed the corresponding acknowledgement.
  defer (if self.lifecycle.phase() == LiveJobPhase::Open ||
    self.retirement_acks.length() == self.plan.partition_count() {
    self.retirement = None
  })
  while self.retirement_acks.length() < self.plan.partition_count() {
    if self.closed || self.lifecycle.phase() != LiveJobPhase::Open {
      return
    }
    @async.sleep(5) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
  }
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  if self.closed || self.lifecycle.phase() != LiveJobPhase::Open {
    return
  }
  self.coordinator.checkpoint_and_compact() catch {
    error => raise FrameIoError::CoordinatorStore(error)
  }
  self.job_store.retire_live_work(cuts) catch {
    error => raise FrameIoError::JobStore(error)
  }
  self.finish_source_reclamation()
  @crash_boundary.hit("coordinator.source.reclaimed", self.plan.job_id()) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
}
