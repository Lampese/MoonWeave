///|
async test "job supervisor commits durable success after all worker sessions finish" {
  let directory = @fs.tmpdir(prefix="moonweave-job-supervisor")
  let job_journal = directory + "/job.journal"
  let job_checkpoint = directory + "/job.json"
  let worker_wal = directory + "/worker.wal"
  defer {
    @fs.remove(job_journal) catch {
      _ => ()
    }
    @fs.remove(job_journal + ".lock") catch {
      _ => ()
    }
    @fs.remove(job_journal + ".repair") catch {
      _ => ()
    }
    @fs.remove(job_checkpoint) catch {
      _ => ()
    }
    @fs.remove(job_checkpoint + ".tmp") catch {
      _ => ()
    }
    @fs.remove(worker_wal) catch {
      _ => ()
    }
    @fs.remove(worker_wal + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let spec = @plan.JobSpec::new(
    "supervised-job",
    "reachability",
    1,
    ["worker-a"],
    [],
  )
  let control = @control.CoordinatorState::new("cluster-job", "coordinator")
  let boot = @protocol.Incarnation::new("boot-job")
  ignore(control.register_worker("worker-a", boot, ["graph"]))
  let assignment = control.assign(0, "worker-a")
  let job_store = JobStore::open(
    spec,
    job_journal,
    checkpoint_path=job_checkpoint,
  )
  ignore(
    job_store.assign(
      0,
      "worker-a",
      assignment.epoch(),
      assignment.start_offset(),
    ),
  )
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("supervised-job", 0),
    [],
    "job-key",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  let work = SupervisorWork::new("worker-a", "job-session", 0, [batch])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    let supervisor_task = group.spawn() <| () => {
      run_job_supervisor(server, control, job_store, [work])
    }
    let worker_connection = @socket.Tcp::connect(addr)
    let handler : (@protocol.EventBatch) -> Unit raise = _ => ()
    let worker_summary = run_worker(
      worker_connection,
      "cluster-job",
      "coordinator",
      "worker-a",
      "boot-job",
      ["graph"],
      worker_wal,
      handler,
      initial_credit_events=1L,
      initial_credit_bytes=2048L,
    )
    let summary = supervisor_task.wait()
    assert_eq(worker_summary.live_batches(), 1)
    assert_eq(summary.completed_workers(), 1)
    assert_eq(summary.failed_workers(), 0)
    assert_eq(job_store.phase(), @control.JobPhase::Succeeded)
    assert_eq(
      job_store.snapshot().partitions()[0].phase(),
      @control.JobPartitionPhase::Succeeded,
    )
  }
  job_store.close()
  let restored = JobStore::open(
    spec,
    job_journal,
    checkpoint_path=job_checkpoint,
  )
  assert_eq(restored.phase(), @control.JobPhase::Succeeded)
  restored.close()
}

///|
async test "job supervisor isolates a failed partition from sibling sessions" {
  let directory = @fs.tmpdir(prefix="moonweave-job-supervisor-failure")
  let job_journal = directory + "/job.journal"
  let job_checkpoint = directory + "/job.json"
  let worker_wal = directory + "/worker-1.wal"
  defer {
    @fs.remove(job_journal) catch {
      _ => ()
    }
    @fs.remove(job_journal + ".lock") catch {
      _ => ()
    }
    @fs.remove(job_checkpoint) catch {
      _ => ()
    }
    @fs.remove(job_checkpoint + ".tmp") catch {
      _ => ()
    }
    @fs.remove(worker_wal) catch {
      _ => ()
    }
    @fs.remove(worker_wal + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let spec = @plan.JobSpec::new(
    "supervised-failure-job",
    "reachability",
    2,
    ["worker-a"],
    [],
  )
  let control = @control.CoordinatorState::new(
    "cluster-job-failure", "coordinator",
  )
  let boot = @protocol.Incarnation::new("boot-job-failure")
  ignore(control.register_worker("worker-a", boot, ["graph"]))
  let assignment_0 = control.assign(0, "worker-a")
  let assignment_1 = control.assign(1, "worker-a")
  let job_store = JobStore::open(
    spec,
    job_journal,
    checkpoint_path=job_checkpoint,
  )
  ignore(job_store.assign(0, "worker-a", assignment_0.epoch(), 0L))
  ignore(job_store.assign(1, "worker-a", assignment_1.epoch(), 0L))
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("failure-partition-1", 0),
    [],
    "partition-1",
    Json::object({ "source": Json::string("X"), "target": Json::string("Y") }),
  )
  let work_0 = SupervisorWork::new("worker-a", "failure-session-0", 0, [])
  let work_1 = SupervisorWork::new("worker-a", "failure-session-1", 1, [
    @protocol.EventBatch::new(1, assignment_1.epoch(), 0L, [event]),
  ])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  @async_runtime.with_task_group <| group => {
    let supervisor_task = group.spawn() <| () => {
      run_job_supervisor(server, control, job_store, [work_0, work_1])
    }
    let failed_connection = @socket.Tcp::connect(server.addr)
    let failed_outbound = @protocol.FrameBuilder::new(
      "cluster-job-failure",
      "worker-a",
      boot.value(),
    )
    let failed_hello = @protocol.Hello::new(
      "worker-a",
      boot.value(),
      ["graph"],
      requested_partition=0,
    )
    let failed_frame = failed_outbound.build(
      @protocol.FrameKind::Hello,
      failed_hello.to_payload(),
    )
    write_frame(failed_connection, failed_frame)
    failed_connection.close()
    let worker = group.spawn() <| () => {
      let task = @task.Task::from_assignment(
        "failure-job/p1/e0",
        "reachability",
        @task.OperatorKind::Reachability,
        @protocol.AssignmentSpec::new(1, assignment_1.epoch(), boot.value()),
      )
      let discard : (@task.TaskOutput) -> Unit raise = _ => ()
      run_worker_task(
        @socket.Tcp::connect(server.addr),
        "cluster-job-failure",
        "coordinator",
        "worker-a",
        boot.value(),
        ["graph"],
        worker_wal,
        task,
        discard,
        initial_credit_events=1L,
        initial_credit_bytes=2048L,
      )
    }
    let summary = supervisor_task.wait()
    ignore(worker.wait())
    assert_eq(summary.completed_workers(), 1)
    assert_eq(summary.failed_workers(), 1)
    assert_eq(summary.failures()[0].partition(), Some(0))
    assert_eq(job_store.phase(), @control.JobPhase::Running)
    assert_eq(
      job_store.snapshot().partitions()[0].phase(),
      @control.JobPartitionPhase::Failed,
    )
    assert_eq(
      job_store.snapshot().partitions()[1].phase(),
      @control.JobPartitionPhase::Running,
    )
  }
  job_store.close()
}
