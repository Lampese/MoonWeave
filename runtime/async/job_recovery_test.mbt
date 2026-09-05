///|
async fn with_job_recovery_paths(
  f : async (String, String, String) -> Unit,
) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-job-recovery")
  let journal = directory + "/job.wal"
  let checkpoint = directory + "/job.json"
  let worker_wal = directory + "/worker.wal"
  defer {
    for
      path in [
        journal,
        checkpoint,
        worker_wal,
        journal + ".lock",
        worker_wal + ".lock",
        checkpoint + ".tmp",
      ] {
      @fs.remove(path) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  @async_runtime.with_timeout(10000, () => f(journal, checkpoint, worker_wal))
}

///|
fn recovered_job_control(partitions : Int) -> @control.CoordinatorState raise {
  let control = @control.CoordinatorState::new(
    "recovery-cluster", "coordinator",
  )
  ignore(
    control.register_worker("worker", @protocol.Incarnation::new("boot"), []),
  )
  for partition in 0..<partitions {
    ignore(control.assign(partition, "worker"))
  }
  control
}

///|
async fn recovered_job_worker(
  addr : @socket.Addr,
  partition : Int,
  wal_path : String,
) -> WorkerRunSummary {
  let task = @task.Task::new(
    "recovered/p" + partition.to_string(),
    "reachability",
    @task.OperatorKind::Reachability,
    partition,
    0,
  )
  let discard : (@task.TaskOutput) -> Unit raise = _ => ()
  run_worker_task(
    @socket.Tcp::connect(addr),
    "recovery-cluster",
    "coordinator",
    "worker",
    "boot",
    [],
    wal_path,
    task,
    discard,
    initial_credit_events=2L,
    initial_credit_bytes=4096L,
  )
}

///|
async test "running job resumes a persisted prefix without reserving its suffix twice" {
  with_job_recovery_paths <| ((journal, checkpoint, worker_wal) => {
    let spec = @plan.JobSpec::new(
      "recover-running",
      "reachability",
      1,
      ["worker"],
      [],
    )
    let first = @protocol.EventBatch::new(0, 0, 0L, [
      task_edge_event("recover", 0, [], "A", "B"),
    ])
    let second = @protocol.EventBatch::new(0, 0, 1L, [
      task_edge_event("recover", 1, [], "B", "C"),
    ])
    let store = JobStore::open(spec, journal, checkpoint_path=checkpoint)
    ignore(store.assign(0, "worker", 0, 0L))
    store.start()
    ignore(store.record_batch_sent(0, "worker", 0, 0L, 1L))
    ignore(store.record_batch_sent(0, "worker", 0, 1L, 1L))
    store.checkpoint()
    store.close()
    let (wal, _) = Wal::open(worker_wal)
    ignore(wal.append(@utf8.encode(first.to_payload())))
    wal.close()
    let restored = JobStore::open(spec, journal, checkpoint_path=checkpoint)
    let control = recovered_job_control(1)
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let work = SupervisorWork::new("worker", "resumed-session", 0, [
      first, second,
    ])
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_job_supervisor(server, control, restored, [work])
      }
      let summary = recovered_job_worker(server.addr, 0, worker_wal)
      let outcome = supervisor.wait()
      assert_eq(outcome.failed_workers(), 0)
      assert_eq(outcome.sent_batches(), 1)
      assert_eq(summary.replayed_batches(), 1)
      assert_eq(summary.live_batches(), 1)
      assert_eq(restored.snapshot().partitions()[0].next_offset(), 2L)
      assert_eq(restored.phase(), @control.JobPhase::Succeeded)
    }
    restored.close()
    let completed = JobStore::open(spec, journal, checkpoint_path=checkpoint)
    defer completed.close()
    assert_eq(
      run_job_supervisor(server, control, completed, [work]).completed_workers(),
      0,
    )
    let changed = @protocol.EventBatch::new(0, 0, 1L, [
      task_edge_event("recover", 1, [], "B", "changed"),
    ])
    let before = completed.journal_offset()
    try
      run_job_supervisor(server, control, completed, [
        SupervisorWork::new("worker", "different-session", 0, [first, changed]),
      ])
    catch {
      error =>
        assert_eq(error, FrameIoError::JobStore(JobStoreError::PlanMismatch(0)))
    } noraise {
      _ => fail("changed recovery input accepted")
    }
    assert_eq(completed.journal_offset(), before)
  })
}

///|
async test "draining job resumes only unfinished partitions" {
  with_job_recovery_paths <| ((journal, checkpoint, worker_wal) => {
    let spec = @plan.JobSpec::new(
      "recover-draining",
      "reachability",
      2,
      ["worker"],
      [],
    )
    let batch = @protocol.EventBatch::new(1, 0, 0L, [
      task_edge_event("draining", 0, [], "A", "B"),
    ])
    let store = JobStore::open(spec, journal, checkpoint_path=checkpoint)
    ignore(store.assign(0, "worker", 0, 0L))
    ignore(store.assign(1, "worker", 0, 0L))
    store.start()
    ignore(store.record_batch_sent(1, "worker", 0, 0L, 1L))
    store.record_progress(1, "worker", 0, 0L, 0L)
    store.drain()
    store.stop_partition(0, "worker", 0, 0)
    store.checkpoint()
    store.close()
    let (wal, _) = Wal::open(worker_wal)
    ignore(wal.append(@utf8.encode(batch.to_payload())))
    wal.close()
    let restored = JobStore::open(spec, journal, checkpoint_path=checkpoint)
    defer restored.close()
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let works = [
      SupervisorWork::new("worker", "finished", 0, []),
      SupervisorWork::new("worker", "unfinished", 1, [batch]),
    ]
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_job_supervisor(server, recovered_job_control(2), restored, works)
      }
      let worker = recovered_job_worker(server.addr, 1, worker_wal)
      let outcome = supervisor.wait()
      assert_eq(outcome.completed_workers(), 1)
      assert_eq(outcome.failed_workers(), 0)
      assert_eq(outcome.sent_batches(), 0)
      assert_eq(worker.replayed_batches(), 1)
      assert_eq(restored.phase(), @control.JobPhase::Succeeded)
    }
  })
}

///|
async test "job with unresolved causal input never records success" {
  with_job_recovery_paths <| ((journal, checkpoint, worker_wal) => {
    let spec = @plan.JobSpec::new(
      "pending-job",
      "reachability",
      1,
      ["worker"],
      [],
    )
    let store = JobStore::open(spec, journal, checkpoint_path=checkpoint)
    defer store.close()
    ignore(store.assign(0, "worker", 0, 0L))
    let child = task_edge_event(
      "pending-job",
      1,
      [@causal.EventId::new("pending-job", 0)],
      "B",
      "C",
    )
    let work = SupervisorWork::new("worker", "pending-session", 0, [
      @protocol.EventBatch::new(0, 0, 0L, [child]),
    ])
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_job_supervisor(server, recovered_job_control(1), store, [work])
      }
      try recovered_job_worker(server.addr, 0, worker_wal) catch {
        error => assert_true(error is FrameIoError::PendingEvents(1))
      } noraise {
        _ => fail("pending task completed")
      }
      let outcome = supervisor.wait()
      assert_eq(outcome.completed_workers(), 0)
      assert_eq(outcome.failed_workers(), 1)
      assert_eq(
        store.snapshot().partitions()[0].phase(),
        @control.JobPartitionPhase::Failed,
      )
      assert_true(store.phase() != @control.JobPhase::Succeeded)
    }
  })
}

///|
async test "job completion requires an acknowledgement not a closed connection" {
  for failure in ["disconnect", "close-only", "timeout"] {
    with_job_recovery_paths <| ((journal, checkpoint, _) => {
      let spec = @plan.JobSpec::new(
        "missing-ack",
        "reachability",
        1,
        ["worker"],
        [],
      )
      let store = JobStore::open(spec, journal, checkpoint_path=checkpoint)
      defer store.close()
      ignore(store.assign(0, "worker", 0, 0L))
      let server = @socket.TcpServer(
        @socket.Addr::parse("127.0.0.1:0"),
        reuse_addr=true,
      )
      defer server.close()
      @async_runtime.with_task_group <| group => {
        let supervisor = group.spawn() <| () => {
          run_job_supervisor(
            server,
            recovered_job_control(1),
            store,
            [SupervisorWork::new("worker", "missing-ack-session", 0, [])],
            drain_timeout_ms=30,
          )
        }
        let connection = @socket.Tcp::connect(server.addr)
        defer connection.close()
        let (_, outbound) = worker_handshake(
          connection,
          "recovery-cluster",
          "coordinator",
          "worker",
          "boot",
          [],
        )
        write_frame(
          connection,
          outbound.build(
            @protocol.FrameKind::Resume,
            @protocol.Resume::new(0, 0, -1L).to_payload(),
          ),
        )
        write_frame(
          connection,
          outbound.build(
            @protocol.FrameKind::Progress,
            @protocol.Progress::new(0, 0, -1L, -1L, "empty").to_payload(),
          ),
        )
        write_frame(
          connection,
          outbound.build(
            @protocol.FrameKind::Credit,
            @protocol.Credit::new(0, 0, 1L, 1024L).to_payload(),
          ),
        )
        let request = read_frame(connection)
        assert_eq(request.kind(), @protocol.FrameKind::DrainRequest)
        if failure == "disconnect" {
          connection.close()
        }
        if failure == "close-only" {
          write_frame(
            connection,
            outbound.build(@protocol.FrameKind::Drain, "{}"),
          )
        }
        let summary = supervisor.wait()
        assert_eq(summary.failed_workers(), 1)
        assert_eq(summary.completed_workers(), 0)
        assert_true(store.phase() != @control.JobPhase::Succeeded)
        assert_eq(
          store.snapshot().partitions()[0].phase(),
          @control.JobPartitionPhase::Failed,
        )
      }
    })
  }
}

///|
async test "job execution freezes input and refuses simultaneous supervisors" {
  with_job_recovery_paths <| ((journal, checkpoint, worker_wal) => {
    let spec = @plan.JobSpec::new(
      "exclusive-job",
      "reachability",
      1,
      ["worker"],
      [],
    )
    let store = JobStore::open(spec, journal, checkpoint_path=checkpoint)
    defer store.close()
    ignore(store.assign(0, "worker", 0, 0L))
    let batch = @protocol.EventBatch::new(0, 0, 0L, [
      task_edge_event("frozen", 0, [], "A", "B"),
    ])
    let expected_payload = batch.to_payload()
    let works = [SupervisorWork::new("worker", "frozen-session", 0, [batch])]
    let control = recovered_job_control(1)
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_job_supervisor(server, control, store, works)
      }
      @async_runtime.pause()
      try run_job_supervisor(server, control, store, works) catch {
        error =>
          assert_eq(
            error,
            FrameIoError::JobStore(JobStoreError::AlreadyRunning),
          )
      } noraise {
        _ => fail("concurrent job supervisor accepted")
      }
      while store.phase() == @control.JobPhase::Created {
        @async_runtime.sleep(1)
      }
      guard batch.events()[0].payload() is Object(fields) else {
        fail("expected event object")
      }
      fields["target"] = Json::string("mutated-after-binding")
      ignore(recovered_job_worker(server.addr, 0, worker_wal))
      assert_eq(supervisor.wait().failed_workers(), 0)
    }
    let (wal, replay) = Wal::open(worker_wal)
    defer wal.close()
    assert_eq(@utf8.decode(replay.records()[0]), expected_payload)
  })
}
