///|
async test "liveness scan fences and reassigns stale worker" {
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let old = @protocol.Incarnation::new("old")
  let replacement = @protocol.Incarnation::new("replacement")
  ignore(control.register_worker("worker-a", old, []))
  ignore(control.register_worker("worker-b", replacement, []))
  let first = control.assign(0, "worker-a", start_offset=3L)
  control.record_heartbeat("worker-a", old, 5L)
  let policy = LivenessPolicy::new(10L, 1, ["worker-b"])
  assert_eq(run_liveness_scan(control, None, 14L, policy), [])
  assert_eq(run_liveness_scan(control, None, 15L, policy), ["worker-a"])
  guard control.assignment(0) is Some(moved) else {
    fail("assignment should be reallocated")
  }
  assert_eq(moved.worker_id(), "worker-b")
  assert_eq(moved.epoch(), first.epoch() + 1)
  assert_eq(moved.start_offset(), 3L)
}

///|
async test "suspect worker is reconsidered when a replacement appears" {
  let control = @control.CoordinatorState::new("cluster-a", "coordinator")
  let old = @protocol.Incarnation::new("old-delayed")
  ignore(control.register_worker("worker-a", old, []))
  let first = control.assign(0, "worker-a")
  control.record_heartbeat("worker-a", old, 1L)
  let waiting = LivenessPolicy::new(5L, 1, [])
  assert_eq(run_liveness_scan(control, None, 6L, waiting), [])
  guard control.worker("worker-a") is Some(suspect) else {
    fail("suspect worker missing")
  }
  assert_eq(suspect.lifecycle(), @control.WorkerLifecycle::Suspect)
  let replacement = @protocol.Incarnation::new("replacement-delayed")
  ignore(control.register_worker("worker-b", replacement, []))
  let recover = LivenessPolicy::new(5L, 1, ["worker-b"])
  assert_eq(run_liveness_scan(control, None, 7L, recover), ["worker-a"])
  guard control.assignment(0) is Some(moved) else {
    fail("delayed replacement assignment missing")
  }
  assert_eq(moved.worker_id(), "worker-b")
  assert_eq(moved.epoch(), first.epoch() + 1)
}

///|
async test "durable suspect and delayed reassignment survive restart" {
  let directory = @fs.tmpdir(prefix="moonweave-liveness-store")
  let journal_path = directory + "/coordinator.journal"
  let checkpoint_path = directory + "/coordinator.json"
  defer {
    @fs.remove(journal_path) catch {
      _ => ()
    }
    @fs.remove(journal_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(journal_path + ".repair") catch {
      _ => ()
    }
    @fs.remove(checkpoint_path) catch {
      _ => ()
    }
    @fs.remove(checkpoint_path + ".tmp") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let store = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  let old = @protocol.Incarnation::new("old-durable")
  ignore(store.register_worker("worker-a", old, []))
  ignore(store.assign(0, "worker-a", start_offset=4L))
  store.record_heartbeat("worker-a", old, 1L)
  let waiting = LivenessPolicy::new(5L, 1, [])
  assert_eq(run_durable_liveness_scan(store, 6L, waiting), [])
  store.close()
  let restored = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  guard restored.snapshot().workers() is [suspect] else {
    fail("restored suspect missing")
  }
  assert_eq(suspect.lifecycle(), @control.WorkerLifecycle::Suspect)
  let replacement = @protocol.Incarnation::new("replacement-durable")
  ignore(restored.register_worker("worker-b", replacement, []))
  let recover = LivenessPolicy::new(5L, 1, ["worker-b"])
  assert_eq(run_durable_liveness_scan(restored, 7L, recover), ["worker-a"])
  restored.checkpoint()
  restored.close()
  let final_store = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  guard final_store.snapshot().assignments() is [moved] else {
    fail("restored reassignment missing")
  }
  assert_eq(moved.worker_id(), "worker-b")
  assert_eq(moved.start_offset(), 4L)
  final_store.close()
}
