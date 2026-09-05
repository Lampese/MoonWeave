///|
async fn with_reconciliation_paths(
  f : async (String, String, String, String, String) -> Unit,
) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-reconciliation")
  let coordinator_journal = directory + "/coordinator.wal"
  let coordinator_checkpoint = directory + "/coordinator.json"
  let job_journal = directory + "/job.wal"
  let job_checkpoint = directory + "/job.json"
  let reconcile = directory + "/reconcile.wal"
  defer {
    for
      path in [
        coordinator_journal,
        coordinator_journal + ".lock",
        coordinator_journal + ".repair",
        coordinator_checkpoint,
        coordinator_checkpoint + ".tmp",
        job_journal,
        job_journal + ".lock",
        job_journal + ".repair",
        job_checkpoint,
        job_checkpoint + ".tmp",
        reconcile,
        reconcile + ".lock",
        reconcile + ".repair",
      ] {
      @fs.remove(path) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  @async.with_timeout(10000, () => {
    f(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, reconcile,
    )
  })
}

///|
fn reconciliation_spec(job_id : String) -> @plan.JobSpec raise {
  @plan.JobSpec::new(job_id, "reachability", 1, ["worker"], [])
}

///|
fn reconciliation_batch(
  job_id : String,
  epoch : Int,
) -> @protocol.EventBatch raise {
  let event = @protocol.EventRecord::new(
    @causal.EventId::new(job_id, 0),
    [],
    "key",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  @protocol.EventBatch::new(0, epoch, 0L, [event])
}

///|
async fn open_reconciliation_stores(
  coordinator_journal : String,
  coordinator_checkpoint : String,
  job_journal : String,
  job_checkpoint : String,
  job_id : String,
) -> (
  CoordinatorStore,
  JobStore,
  @protocol.Incarnation,
  @control.PartitionAssignment,
) {
  let coordinator = CoordinatorStore::open(
    "reconciliation-cluster",
    "coordinator",
    coordinator_journal,
    checkpoint_path=coordinator_checkpoint,
  )
  let job = JobStore::open(
    reconciliation_spec(job_id),
    job_journal,
    checkpoint_path=job_checkpoint,
  )
  let incarnation = @protocol.Incarnation::new("boot-reconciliation")
  ignore(coordinator.register_worker("worker", incarnation, ["reachability"]))
  let assignment = coordinator.assign(0, "worker")
  ignore(job.assign(0, "worker", assignment.epoch(), assignment.start_offset()))
  (coordinator, job, incarnation, assignment)
}

///|
async test "reconciliation advances the lagging Coordinator after a Job-side send intent" {
  with_reconciliation_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    reconcile,
  ) => {
    let (coordinator, job, _, assignment) = open_reconciliation_stores(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, "job-reconcile-job-ahead",
    )
    job.start()
    let batch = reconciliation_batch(
      "job-reconcile-job-ahead",
      assignment.epoch(),
    )
    let work = SupervisorWork::new("worker", "reconcile-session", 0, [batch])
    job.bind_work(work)
    ignore(job.record_batch_sent(0, "worker", assignment.epoch(), 0L, 1L))
    let summary = reconcile_job_coordinator(
      coordinator,
      job,
      reconcile_path=reconcile,
    )
    assert_eq(summary.repaired_transactions(), 1)
    assert_eq(summary.recovered_transactions(), 0)
    assert_eq(summary.repaired_partitions(), 1)
    assert_eq(coordinator.snapshot().assignments()[0].next_offset(), 1L)
    assert_eq(job.snapshot().partitions()[0].next_offset(), 1L)
    job.close()
    coordinator.close()
  })
}

///|
async test "reconciliation advances the lagging Job after a Coordinator-side send intent" {
  with_reconciliation_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    reconcile,
  ) => {
    let (coordinator, job, incarnation, assignment) = open_reconciliation_stores(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, "job-reconcile-coordinator-ahead",
    )
    job.start()
    let batch = reconciliation_batch(
      "job-reconcile-coordinator-ahead",
      assignment.epoch(),
    )
    job.bind_work(
      SupervisorWork::new("worker", "reconcile-session", 0, [batch]),
    )
    ignore(
      coordinator.record_batch_sent(
        0,
        "worker",
        incarnation,
        assignment.epoch(),
        0L,
        1L,
      ),
    )
    let summary = reconcile_job_coordinator(
      coordinator,
      job,
      reconcile_path=reconcile,
    )
    assert_eq(summary.repaired_transactions(), 1)
    assert_eq(job.snapshot().partitions()[0].next_offset(), 1L)
    assert_eq(coordinator.snapshot().assignments()[0].next_offset(), 1L)
    job.close()
    coordinator.close()
  })
}

///|
async test "reconciliation repairs progress in either durable store without regressing offsets" {
  with_reconciliation_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    reconcile,
  ) => {
    let (coordinator, job, incarnation, assignment) = open_reconciliation_stores(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, "job-reconcile-progress",
    )
    job.start()
    let batch = reconciliation_batch(
      "job-reconcile-progress",
      assignment.epoch(),
    )
    job.bind_work(
      SupervisorWork::new("worker", "reconcile-session", 0, [batch]),
    )
    ignore(job.record_batch_sent(0, "worker", assignment.epoch(), 0L, 1L))
    ignore(
      coordinator.record_batch_sent(
        0,
        "worker",
        incarnation,
        assignment.epoch(),
        0L,
        1L,
      ),
    )
    ignore(job.record_progress(0, "worker", assignment.epoch(), 0L, 0L))
    let first = reconcile_job_coordinator(
      coordinator,
      job,
      reconcile_path=reconcile,
    )
    assert_eq(first.repaired_transactions(), 1)
    assert_eq(coordinator.snapshot().assignments()[0].applied_offset(), 0L)

    let second = reconcile_job_coordinator(
      coordinator,
      job,
      reconcile_path=reconcile,
    )
    assert_eq(second.repaired_transactions(), 0)
    assert_eq(second.recovered_transactions(), 0)
    assert_eq(second.repaired_partitions(), 0)
    assert_eq(job.snapshot().partitions()[0].applied_offset(), 0L)
    job.close()
    coordinator.close()
  })
}

///|
async test "a prepared reconciliation transaction is replayed after a simulated crash between store writes" {
  with_reconciliation_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    reconcile,
  ) => {
    let (coordinator, job, _, assignment) = open_reconciliation_stores(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, "job-reconcile-crash",
    )
    job.start()
    let batch = reconciliation_batch("job-reconcile-crash", assignment.epoch())
    job.bind_work(
      SupervisorWork::new("worker", "reconcile-session", 0, [batch]),
    )
    let target = ReconciliationPartition::{
      partition: 0,
      worker_id: "worker",
      epoch: assignment.epoch(),
      start_offset: 0L,
      next_offset: 1L,
      persisted_offset: -1L,
      applied_offset: -1L,
    }
    let transaction = ReconciliationPlan::{
      job_id: "job-reconcile-crash",
      partitions: [target],
    }
    let log = ReconciliationLog::open(reconcile)
    log.prepare("crash-before-commit", transaction)
    // Simulate a process stop after the Job journal write but before the
    // Coordinator journal write. The sidecar remains at `prepare`.
    ignore(
      job.reconcile_record_batch_unlocked(
        0,
        "worker",
        assignment.epoch(),
        0L,
        1L,
      ),
    )
    log.close()
    let summary = reconcile_job_coordinator(
      coordinator,
      job,
      reconcile_path=reconcile,
    )
    assert_eq(summary.repaired_transactions(), 0)
    assert_eq(summary.recovered_transactions(), 1)
    assert_eq(summary.repaired_partitions(), 1)
    assert_eq(job.snapshot().partitions()[0].next_offset(), 1L)
    assert_eq(coordinator.snapshot().assignments()[0].next_offset(), 1L)
    job.close()
    coordinator.close()
  })
}

///|
async test "reconciliation fails closed on epoch conflicts and missing frozen plans" {
  with_reconciliation_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    reconcile,
  ) => {
    let (coordinator, job, _, assignment) = open_reconciliation_stores(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, "job-reconcile-conflict",
    )
    job.start()
    ignore(job.record_batch_sent(0, "worker", assignment.epoch(), 0L, 1L))
    try
      reconcile_job_coordinator(coordinator, job, reconcile_path=reconcile)
    catch {
      error => assert_eq(error, ReconciliationError::MissingPlan(0))
    } noraise {
      _ => fail("reconciliation repaired a cursor without a frozen plan")
    }
    ignore(coordinator.revoke(0))
    ignore(coordinator.reassign(0, "worker", start_offset=0L))
    try
      reconcile_job_coordinator(coordinator, job, reconcile_path=reconcile)
    catch {
      error =>
        assert_true(
          error is ReconciliationError::AssignmentMismatch(0) ||
          error is ReconciliationError::StaleIntent(_),
        )
    } noraise {
      _ => fail("reconciliation accepted an epoch conflict")
    }
    job.close()
    coordinator.close()
  })
}

///|
async test "durable Job supervisor reconciles before and after a terminal run" {
  with_reconciliation_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    reconcile,
  ) => {
    let (coordinator, job, _, assignment) = open_reconciliation_stores(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, "job-reconcile-supervisor",
    )
    job.start()
    job.drain_partition(0, "worker", assignment.epoch())
    job.stop_partition(0, "worker", assignment.epoch(), 0)
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let summary = run_durable_job_supervisor(
      server,
      coordinator,
      job,
      [SupervisorWork::new("worker", "terminal-session", 0, [])],
      reconcile_path=reconcile,
    )
    assert_eq(summary.completed_workers(), 0)
    assert_eq(summary.failed_workers(), 0)
    assert_eq(job.phase(), @control.JobPhase::Succeeded)
    assert_eq(coordinator.snapshot().assignments()[0].next_offset(), 0L)
    job.close()
    coordinator.close()
    let restored_coordinator = CoordinatorStore::open(
      "reconciliation-cluster",
      "coordinator",
      coordinator_journal,
      checkpoint_path=coordinator_checkpoint,
    )
    let restored_job = JobStore::open(
      reconciliation_spec("job-reconcile-supervisor"),
      job_journal,
      checkpoint_path=job_checkpoint,
    )
    assert_eq(restored_coordinator.snapshot().assignments().length(), 1)
    assert_eq(restored_job.phase(), @control.JobPhase::Succeeded)
    restored_job.close()
    restored_coordinator.close()
  })
}

///|
async test "durable Job supervisor keeps Coordinator and Job journals aligned during TCP execution" {
  with_reconciliation_paths <| ((
    coordinator_journal,
    coordinator_checkpoint,
    job_journal,
    job_checkpoint,
    reconcile,
  ) => {
    let (coordinator, job, incarnation, assignment) = open_reconciliation_stores(
      coordinator_journal, coordinator_checkpoint, job_journal, job_checkpoint, "job-reconcile-live-supervisor",
    )
    let worker_wal = reconcile + ".worker"
    defer {
      @fs.remove(worker_wal) catch {
        _ => ()
      }
      @fs.remove(worker_wal + ".lock") catch {
        _ => ()
      }
    }
    let batch = reconciliation_batch(
      "job-reconcile-live-supervisor",
      assignment.epoch(),
    )
    let work = SupervisorWork::new("worker", "live-reconcile-session", 0, [
      batch,
    ])
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    @async.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_durable_job_supervisor(
          server,
          coordinator,
          job,
          [work],
          reconcile_path=reconcile,
        )
      }
      let worker = run_worker(
        @socket.Tcp::connect(server.addr),
        "reconciliation-cluster",
        "coordinator",
        "worker",
        incarnation.value(),
        ["reachability"],
        worker_wal,
        _ => (),
        initial_credit_events=1L,
        initial_credit_bytes=4096L,
      )
      let summary = supervisor.wait()
      assert_eq(worker.live_batches(), 1)
      assert_eq(summary.failed_workers(), 0)
      assert_eq(summary.completed_workers(), 1)
    }
    assert_eq(job.phase(), @control.JobPhase::Succeeded)
    assert_eq(coordinator.snapshot().assignments()[0].applied_offset(), 0L)
    assert_eq(job.snapshot().partitions()[0].applied_offset(), 0L)
    assert_true(@fs.exists(coordinator_checkpoint))
    assert_true(@fs.exists(job_checkpoint))
    job.close()
    coordinator.close()
    let restored_coordinator = CoordinatorStore::open(
      "reconciliation-cluster",
      "coordinator",
      coordinator_journal,
      checkpoint_path=coordinator_checkpoint,
    )
    let restored_job = JobStore::open(
      reconciliation_spec("job-reconcile-live-supervisor"),
      job_journal,
      checkpoint_path=job_checkpoint,
    )
    assert_eq(
      restored_coordinator.snapshot().assignments()[0].applied_offset(),
      0L,
    )
    assert_eq(restored_job.snapshot().partitions()[0].applied_offset(), 0L)
    restored_job.close()
    restored_coordinator.close()
  })
}
