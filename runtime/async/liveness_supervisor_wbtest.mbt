///|
async test "liveness recovery leaves both stores unchanged when supervisor claim is busy" {
  let directory = @fs.tmpdir(prefix="moonweave-liveness-busy")
  let coordinator = CoordinatorStore::open(
    "liveness-busy-cluster",
    "coordinator",
    directory + "/coordinator.wal",
  )
  let spec = @plan.JobSpec::new(
    "liveness-busy-job",
    "reachability",
    1,
    ["worker-a", "worker-b"],
    [],
  )
  let job = JobStore::open(spec, directory + "/job.wal")
  defer {
    coordinator.close()
    job.close()
    @fs.rmdir(directory, recursive=true) catch {
      _ => ()
    }
  }
  let old = @protocol.Incarnation::new("busy-old")
  let replacement = @protocol.Incarnation::new("busy-replacement")
  ignore(coordinator.register_worker("worker-a", old, []))
  ignore(coordinator.register_worker("worker-b", replacement, []))
  let assignment = coordinator.assign(0, "worker-a")
  ignore(job.assign(0, "worker-a", assignment.epoch(), 0L))
  job.start()
  coordinator.record_heartbeat("worker-a", old, 1L)
  let before_coordinator = coordinator.snapshot()
  let before_job = job.snapshot()
  let coordinator_offset = coordinator.journal_offset()
  let job_offset = job.journal_offset()
  assert_true(job.supervisor_lock.try_acquire())
  try
    run_durable_liveness_scan(
      coordinator,
      11L,
      LivenessPolicy::new(10L, 1, ["worker-b"]),
      job_store=job,
    )
  catch {
    FrameIoError::Reconciliation(
      ReconciliationError::JobStore(JobStoreError::AlreadyRunning)
    ) => ()
    error =>
      fail("expected active supervisor rejection: " + @debug.to_string(error))
  } noraise {
    _ => fail("active supervisor was accepted")
  }
  job.supervisor_lock.release()
  assert_eq(coordinator.snapshot(), before_coordinator)
  assert_eq(job.snapshot(), before_job)
  assert_eq(coordinator.journal_offset(), coordinator_offset)
  assert_eq(job.journal_offset(), job_offset)
}

///|
async test "liveness recovery leaves both stores unchanged on replacement capability mismatch" {
  let directory = @fs.tmpdir(prefix="moonweave-liveness-capability")
  let coordinator = CoordinatorStore::open(
    "liveness-capability-cluster",
    "coordinator",
    directory + "/coordinator.wal",
  )
  let spec = @plan.JobSpec::new(
    "liveness-capability-job",
    "reachability",
    1,
    ["worker-a", "worker-b"],
    [],
  )
  let job = JobStore::open(spec, directory + "/job.wal")
  defer {
    coordinator.close()
    job.close()
    @fs.rmdir(directory, recursive=true) catch {
      _ => ()
    }
  }
  let old = @protocol.Incarnation::new("capability-old")
  let replacement = @protocol.Incarnation::new("capability-replacement")
  let capability = @task.OperatorKind::Reachability.capability()
  ignore(coordinator.register_worker("worker-a", old, [capability]))
  ignore(coordinator.register_worker("worker-b", replacement, []))
  let assignment = coordinator.assign(0, "worker-a")
  ignore(job.assign(0, "worker-a", assignment.epoch(), 0L))
  job.bind_work(
    SupervisorWork::new(
      "worker-a",
      "capability-bound-session",
      0,
      [],
      required_capability=capability,
    ),
  )
  job.start()
  coordinator.record_heartbeat("worker-a", old, 1L)
  let before_coordinator = coordinator.snapshot()
  let before_job = job.snapshot()
  let coordinator_offset = coordinator.journal_offset()
  let job_offset = job.journal_offset()
  try
    run_durable_liveness_scan(
      coordinator,
      11L,
      LivenessPolicy::new(10L, 1, ["worker-b"]),
      job_store=job,
    )
  catch {
    FrameIoError::Reconciliation(ReconciliationError::IdentityMismatch) => ()
    error => fail("expected capability mismatch: " + @debug.to_string(error))
  } noraise {
    _ => fail("capability mismatch was accepted")
  }
  assert_eq(coordinator.snapshot(), before_coordinator)
  assert_eq(job.snapshot(), before_job)
  assert_eq(coordinator.journal_offset(), coordinator_offset)
  assert_eq(job.journal_offset(), job_offset)
}
