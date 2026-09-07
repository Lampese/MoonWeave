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

///|
async test "durable liveness recovery moves Job and Coordinator leases together" {
  let directory = @fs.tmpdir(prefix="moonweave-liveness-job")
  defer @fs.rmdir(directory, recursive=true)
  let coordinator = CoordinatorStore::open(
    "liveness-job-cluster",
    "coordinator",
    directory + "/coordinator.wal",
  )
  defer coordinator.close()
  let spec = @plan.JobSpec::new(
    "liveness-job",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [],
  )
  let job = JobStore::open(spec, directory + "/job.wal")
  defer job.close()
  let old = @protocol.Incarnation::new("old")
  let replacement = @protocol.Incarnation::new("replacement")
  ignore(coordinator.register_worker("worker-a", old, []))
  ignore(coordinator.register_worker("worker-b", replacement, []))
  for partition in 0..<2 {
    let assignment = coordinator.assign(partition, "worker-a")
    ignore(job.assign(partition, "worker-a", assignment.epoch(), 0L))
  }
  job.start()
  coordinator.record_heartbeat("worker-a", old, 5L)
  let policy = LivenessPolicy::new(10L, 1, ["worker-b"])
  assert_eq(
    run_durable_liveness_scan(coordinator, 15L, policy, job_store=job),
    ["worker-a"],
  )
  assert_eq(
    coordinator.snapshot().assignments().map(value => value.worker_id()),
    ["worker-b", "worker-b"],
  )
  assert_eq(job.snapshot().partitions().map(value => value.worker_id()), [
    "worker-b", "worker-b",
  ])
  assert_eq(job.snapshot().partitions().map(value => value.epoch()), [1, 1])
  assert_eq(job.phase(), @control.JobPhase::Running)
  let replayed = reconcile_job_coordinator(coordinator, job)
  assert_eq(replayed.recovered_transactions(), 0)
  assert_eq(replayed.repaired_transactions(), 0)
}
