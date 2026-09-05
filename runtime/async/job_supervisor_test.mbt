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
