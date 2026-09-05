///|
/// Policy for turning a stale Worker into a fenced/reassigned lease.
pub struct LivenessPolicy {
  heartbeat_timeout_ms : Int64
  scan_interval_ms : Int
  replacement_workers : Array[String]
}

///|
pub fn LivenessPolicy::new(
  heartbeat_timeout_ms : Int64,
  scan_interval_ms : Int,
  replacement_workers : ArrayView[String],
) -> LivenessPolicy raise FrameIoError {
  if heartbeat_timeout_ms <= 0L || scan_interval_ms <= 0 {
    raise FrameIoError::Handler("invalid liveness policy")
  }
  {
    heartbeat_timeout_ms,
    scan_interval_ms,
    replacement_workers: replacement_workers.to_owned(),
  }
}

///|
pub fn LivenessPolicy::heartbeat_timeout_ms(self : LivenessPolicy) -> Int64 {
  self.heartbeat_timeout_ms
}

///|
pub fn LivenessPolicy::scan_interval_ms(self : LivenessPolicy) -> Int {
  self.scan_interval_ms
}

///|
pub fn LivenessPolicy::replacement_workers(
  self : LivenessPolicy,
) -> ArrayView[String] {
  self.replacement_workers[:]
}

///|
/// One deterministic liveness scan. It never fences without an explicit
/// replacement choice; callers can persist each decision through Store.
pub async fn run_liveness_scan(
  control : @control.CoordinatorState,
  store : CoordinatorStore?,
  now_tick : Int64,
  policy : LivenessPolicy,
) -> Array[String] raise FrameIoError {
  let stale = control
    .workers()
    .filter_map(worker => {
      match worker.last_heartbeat_tick() {
        Some(last) if worker.lifecycle() != @control.WorkerLifecycle::Fenced &&
          now_tick >= last &&
          now_tick - last >= policy.heartbeat_timeout_ms() =>
          Some(worker.worker_id())
        _ => None
      }
    })
  for worker_id in stale {
    guard control.worker(worker_id) is Some(worker) else { continue }
    if worker.lifecycle() is @control.WorkerLifecycle::Active {
      match store {
        Some(durable) =>
          durable.mark_suspect(worker_id) catch {
            error => raise FrameIoError::CoordinatorStore(error)
          }
        None =>
          control.set_suspect(worker_id) catch {
            error => raise FrameIoError::Coordinator(error)
          }
      }
    }
  }
  let replacements = policy.replacement_workers()
  let actions = []
  for worker_id in stale {
    guard control.worker(worker_id) is Some(worker) else { continue }
    let mut replacement : String? = None
    for worker_id_2 in replacements {
      if replacement is None &&
        worker_id_2 != worker_id &&
        control.worker(worker_id_2) is Some(candidate) &&
        candidate.lifecycle() is @control.WorkerLifecycle::Active {
        replacement = Some(worker_id_2)
      }
    }
    match replacement {
      None => ()
      Some(replacement_id) => {
        match store {
          Some(durable) =>
            ignore(
              durable.fence_and_reassign(
                worker_id,
                worker.incarnation(),
                replacement_id,
              ) catch {
                error => raise FrameIoError::CoordinatorStore(error)
              },
            )
          None =>
            ignore(
              control.fence_and_reassign(
                worker_id,
                worker.incarnation(),
                replacement_id,
              ) catch {
                error => raise FrameIoError::Coordinator(error)
              },
            )
        }
        actions.push(worker_id)
      }
    }
  }
  actions
}

///|
/// Durable entry point which keeps the mutable Coordinator state encapsulated
/// behind its journaled store commands.
pub async fn run_durable_liveness_scan(
  store : CoordinatorStore,
  now_tick : Int64,
  policy : LivenessPolicy,
) -> Array[String] raise FrameIoError {
  run_liveness_scan(store.state(), Some(store), now_tick, policy)
}

///|
/// Run watchdog scans until the caller cancels the async task.
pub async fn run_liveness_watchdog(
  control : @control.CoordinatorState,
  store : CoordinatorStore?,
  policy : LivenessPolicy,
) -> Unit raise FrameIoError {
  for ;; {
    @async.sleep(policy.scan_interval_ms()) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
    ignore(run_liveness_scan(control, store, @async.now(), policy))
  }
}
