///|
fn multi_recovery_spec() -> @plan.JobSpec raise {
  @plan.JobSpec::new(
    "multi-recovery",
    "reachability",
    3,
    ["worker-a", "worker-b"],
    [],
  )
}

///|
async fn open_multi_recovery_stores(
  directory : String,
) -> (CoordinatorStore, JobStore) {
  let coordinator = CoordinatorStore::open(
    "multi-cluster",
    "coordinator",
    directory + "/coordinator.wal",
  )
  errdefer coordinator.close()
  let job = JobStore::open(multi_recovery_spec(), directory + "/job.wal")
  (coordinator, job)
}

///|
async fn initialize_multi_recovery(
  coordinator : CoordinatorStore,
  job : JobStore,
) -> Unit {
  for worker in ["worker-a", "worker-b"] {
    ignore(
      coordinator.register_worker(
        worker,
        @protocol.Incarnation::new(worker + "-boot"),
        ["reachability"],
      ),
    )
  }
  for partition in 0..<3 {
    let worker = if partition == 1 { "worker-b" } else { "worker-a" }
    let start_offset = partition.to_int64() * 4L
    let lease = coordinator.assign(partition, worker, start_offset~)
    ignore(job.assign(partition, worker, lease.epoch(), start_offset))
  }
  job.start()
  for partition in [0, 2] {
    job.fail_partition(partition, "worker-a", 0, "connection lost")
  }
}

///|
fn assert_multi_recovery(
  coordinator : CoordinatorStore,
  job : JobStore,
) -> Unit raise {
  assert_eq(
    coordinator.snapshot().assignments().map(lease => lease.worker_id()),
    ["worker-b", "worker-b", "worker-b"],
  )
  assert_eq(coordinator.snapshot().assignments().map(lease => lease.epoch()), [
    1, 0, 1,
  ])
  assert_eq(job.snapshot().partitions().map(partition => partition.epoch()), [
    1, 0, 1,
  ])
  assert_eq(
    job.snapshot().partitions().map(partition => partition.worker_id()),
    ["worker-b", "worker-b", "worker-b"],
  )
  assert_eq(
    job.snapshot().partitions().map(partition => partition.next_offset()),
    [0L, 4L, 8L],
  )
  assert_true(
    job
    .snapshot()
    .partitions()
    .all(partition => partition.phase() == @control.JobPartitionPhase::Running),
  )
  guard coordinator.state().worker("worker-a") is Some(worker) else {
    fail("missing fenced Worker")
  }
  assert_eq(worker.lifecycle(), @control.WorkerLifecycle::Fenced)
  try
    coordinator
    .state()
    .record_progress(
      0,
      "worker-a",
      @protocol.Incarnation::new("worker-a-boot"),
      0,
      -1L,
      -1L,
    )
  catch {
    @control.CoordinatorError::StaleEpoch(expected=1, actual=0) => ()
    _ => fail("expected stale epoch")
  } noraise {
    _ => fail("expected stale epoch")
  }
}

///|
async test "multi-lease recovery replays every crash boundary without another epoch step" {
  for boundary in 0..<3 {
    let root = @fs.tmpdir(prefix="moonweave-multi-recovery")
    defer (@fs.rmdir(root, recursive=true) catch { _ => () })
    let (coordinator, job) = open_multi_recovery_stores(root)
    initialize_multi_recovery(coordinator, job)
    let plan = build_worker_recovery_plan(
      coordinator,
      job,
      "worker-a",
      @protocol.Incarnation::new("worker-a-boot"),
      "worker-b",
    )
    let log = ReconciliationLog::open(
      job.journal_path() + ".coordinator-reconcile",
    )
    log.prepare("crash-boundary", plan)
    if boundary >= 1 {
      ignore(
        coordinator.fence_and_reassign(
          "worker-a",
          @protocol.Incarnation::new("worker-a-boot"),
          "worker-b",
        ),
      )
    }
    if boundary >= 2 {
      let before = job.journal_offset()
      ignore(job.reassign_many(worker_recovery_reassignments(plan)))
      assert_true(job.journal_offset() > before)
    }
    log.compact()
    log.close()
    job.close()
    coordinator.close()
    let (restored_coordinator, restored_job) = open_multi_recovery_stores(root)
    defer restored_coordinator.close()
    defer restored_job.close()
    let summary = reconcile_job_coordinator(restored_coordinator, restored_job)
    assert_eq(summary.recovered_transactions(), 1)
    assert_eq(summary.repaired_partitions(), 2)
    assert_multi_recovery(restored_coordinator, restored_job)
    let second = reconcile_job_coordinator(restored_coordinator, restored_job)
    assert_eq(second.recovered_transactions(), 0)
    assert_multi_recovery(restored_coordinator, restored_job)
  }
}

///|
async test "multi-lease recovery rejects a still-running sibling before either WAL changes" {
  let root = @fs.tmpdir(prefix="moonweave-multi-recovery-reject")
  defer (@fs.rmdir(root, recursive=true) catch { _ => () })
  let (coordinator, job) = open_multi_recovery_stores(root)
  defer coordinator.close()
  defer job.close()
  initialize_multi_recovery(coordinator, job)
  // Leave one sibling at a different epoch to simulate a conflicting scheduler.
  ignore(job.reassign(2, "worker-a", 1, 8L))
  let before_job = job.snapshot()
  let before_coordinator = coordinator.snapshot()
  let job_offset = job.journal_offset()
  let coordinator_offset = coordinator.journal_offset()
  try
    recover_worker_leases(
      coordinator,
      job,
      "worker-a",
      @protocol.Incarnation::new("worker-a-boot"),
      "worker-b",
    )
  catch {
    ReconciliationError::AssignmentMismatch(2) => ()
    error =>
      fail(
        "expected a conflicting running partition: " + @debug.to_string(error),
      )
  } noraise {
    _ => fail("expected a conflicting running partition")
  }
  assert_eq(job.snapshot(), before_job)
  assert_eq(coordinator.snapshot(), before_coordinator)
  assert_eq(job.journal_offset(), job_offset)
  assert_eq(coordinator.journal_offset(), coordinator_offset)
}

///|
async test "multi-lease recovery API resumes its prepared intent and rejects stale replacements" {
  let root = @fs.tmpdir(prefix="moonweave-multi-recovery-retry")
  defer (@fs.rmdir(root, recursive=true) catch { _ => () })
  let (coordinator, job) = open_multi_recovery_stores(root)
  defer coordinator.close()
  defer job.close()
  initialize_multi_recovery(coordinator, job)
  let boot = @protocol.Incarnation::new("worker-a-boot")
  let plan = build_worker_recovery_plan(
    coordinator, job, "worker-a", boot, "worker-b",
  )
  let log = ReconciliationLog::open(
    job.journal_path() + ".coordinator-reconcile",
  )
  log.prepare("retry-api", plan)
  log.close()
  ignore(coordinator.fence_and_reassign("worker-a", boot, "worker-b"))
  let moved = recover_worker_leases(
    coordinator, job, "worker-a", boot, "worker-b",
  )
  assert_eq(moved.map(lease => lease.partition()), [0, 2])
  assert_multi_recovery(coordinator, job)
}
