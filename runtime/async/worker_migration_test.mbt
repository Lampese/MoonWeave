///|
fn migration_event(
  origin : String,
  sequence : Int64,
  source : String,
  target : String,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    [],
    "migration-key",
    Json::object({
      "source": Json::string(source),
      "target": Json::string(target),
    }),
  )
}

///|
async test "Worker migration captures and atomically publishes a rebound generation" {
  let source_dir = @fs.tmpdir(prefix="moonweave-migration-source")
  let target_dir = @fs.tmpdir(prefix="moonweave-migration-target")
  defer {
    @fs.rmdir(source_dir, recursive=true) catch {
      _ => ()
    }
    @fs.rmdir(target_dir, recursive=true) catch {
      _ => ()
    }
  }
  let source_wal = source_dir + "/worker.wal"
  let source_checkpoint = source_dir + "/worker.json"
  let source_output = source_dir + "/output.wal"
  let target_wal = target_dir + "/worker.wal"
  let target_checkpoint = target_dir + "/worker.json"
  let target_output = target_dir + "/output.wal"
  let task_id = "migration-job/p0"
  let task = @task.Task::new(
    task_id,
    "reachability",
    @task.OperatorKind::Reachability,
    0,
    0,
  )
  task.start()
  let event = migration_event("migration-input", 0, "A", "B")
  let batch = @protocol.EventBatch::new(0, 0, 0L, [event])
  let outputs = task.process_batch(batch)
  let snapshot = task.snapshot()
  let (source_wal_handle, _) = Wal::open(source_wal)
  ignore(source_wal_handle.append(@utf8.encode(batch.to_payload())))
  let source_wal_offset = source_wal_handle.write_offset()
  source_wal_handle.close()
  let output_log = TaskOutputLog::open(source_output)
  output_log.append(task_id, 0, 0, 0L, outputs[:])
  output_log.close()
  let checkpoint = Checkpoint::new(
    0,
    0,
    source_wal_offset,
    0L,
    0L,
    snapshot.frontier_digest(),
    state_schema=snapshot.state_schema(),
    operator_state=@json.to_json(snapshot),
  )
  checkpoint.save(source_checkpoint)
  let identity = WorkerMigrationIdentity::new(
    "migration-cluster", "migration-coordinator", "migration-job", 0, 0L, "worker-a",
    "boot-a", 0, "worker-b", "boot-b", 1,
  )
  let source_paths = WorkerStoragePaths::new(
    source_wal,
    source_checkpoint,
    None,
    Some(source_output),
  )
  let bundle = WorkerMigration::capture(identity, source_paths)
  assert_eq(bundle.task_id(), task_id)
  assert_eq(bundle.persisted_offset(), 0L)
  assert_eq(WorkerMigration::from_bytes(bundle.to_bytes()), bundle)
  let bundle_path = source_dir + "/transfer.json"
  bundle.save(bundle_path)
  let loaded = WorkerMigration::load(bundle_path)
  assert_eq(loaded, bundle)
  let transfer_server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer transfer_server.close()
  @async_runtime.with_task_group <| group => {
    let sender = group.spawn(allow_failure=true) <| () => {
      let (connection, _) = transfer_server.accept()
      defer connection.close()
      bundle.send(connection)
    }
    let wire_loaded = WorkerMigration::receive(
      @socket.Tcp::connect(transfer_server.addr),
    )
    assert_eq(wire_loaded, bundle)
    sender.wait()
  }
  let target_paths = WorkerStoragePaths::new(
    target_wal,
    target_checkpoint,
    None,
    Some(target_output),
  )
  loaded.install(identity, target_paths)
  let staged = target_paths.resolve()
  // Simulate a crash after generation staging but before pointer publication.
  @fs.remove(target_wal + ".migration")
  @fs.remove(staged.checkpoint_path)
  loaded.install(identity, target_paths)
  loaded.install(identity, target_paths)
  let active = target_paths.resolve()
  let target_checkpoint_value = Checkpoint::load(active.checkpoint_path)
  guard target_checkpoint_value is Some(target_checkpoint_value) else {
    fail("installed checkpoint is missing")
  }
  assert_eq(target_checkpoint_value.epoch(), 1)
  assert_eq(target_checkpoint_value.persisted_offset(), 0L)
  let (target_wal_handle, target_replay) = Wal::open(active.wal_path)
  assert_eq(target_replay.records().length(), 1)
  let target_batch : @protocol.EventBatch = @json.from_json(
    @json.parse(@utf8.decode(target_replay.records()[0])),
  )
  assert_eq(target_batch.epoch(), 1)
  let target_output_log = TaskOutputLog::open(active.output_log_path.unwrap())
  let target_records = target_output_log.records_for(task_id)
  assert_eq(target_records.length(), 1)
  assert_eq(target_records[0].sequence(), 0L)
  assert_eq(target_records[0].epoch(), 1)
  assert_eq(
    target_records[0].idempotency_key(),
    task_id.length().to_string() + ":" + task_id + ":0",
  )
  target_output_log.close()
  target_wal_handle.close()
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  @async_runtime.with_task_group <| group => {
    let coordinator_task = group.spawn(allow_failure=true) <| () => {
      let (connection, _) = server.accept()
      defer connection.close()
      let assignment = @protocol.AssignmentSpec::new(0, 1, "boot-b")
      let (coordinator, outbound) = coordinator_handshake(
        connection,
        "migration-cluster",
        "migration-coordinator",
        "worker-b",
        "migration-session",
        assignment~,
      )
      coordinator.restore_send_cursor(0, 1, 1L)
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      coordinator.accept_frame(read_frame(connection))
      write_frame(connection, outbound.build(@protocol.FrameKind::Drain, "{}"))
    }
    let restored_task = @task.Task::new(
      task_id,
      "reachability",
      @task.OperatorKind::Reachability,
      0,
      0,
    )
    let restored_outputs : Array[@task.TaskOutput] = []
    let summary = run_worker_task(
      @socket.Tcp::connect(server.addr),
      "migration-cluster",
      "migration-coordinator",
      "worker-b",
      "boot-b",
      [],
      target_wal,
      restored_task,
      output => restored_outputs.push(output),
      checkpoint_path=target_checkpoint,
      output_log_path=target_output,
      dynamic_assignment=true,
    ) catch {
      error => fail("migrated Worker failed: " + @debug.to_string(error))
    }
    assert_eq(summary.replayed_batches(), 0)
    assert_eq(summary.live_batches(), 0)
    assert_eq(restored_task.epoch(), 1)
    assert_eq(restored_task.offset(), 0L)
    assert_eq(restored_task.outputs().length(), 1)
    assert_eq(restored_outputs, [])
    coordinator_task.wait()
  }
  let (advanced_wal, _) = Wal::open(active.wal_path)
  let suffix = @protocol.EventBatch::new(0, 1, 1L, [
    migration_event("migration-input-2", 1, "A", "B"),
  ])
  ignore(advanced_wal.append(@utf8.encode(suffix.to_payload())))
  advanced_wal.close()
  // A retry after the replacement has appended data must preserve that suffix.
  loaded.install(identity, target_paths)
  let (verified_wal, verified_replay) = Wal::open(active.wal_path)
  assert_eq(verified_replay.records().length(), 2)
  verified_wal.close()
  let coordinator = CoordinatorStore::open(
    "migration-cluster",
    "migration-coordinator",
    target_dir + "/coordinator.wal",
  )
  let spec = @plan.JobSpec::new(
    "migration-job",
    "reachability",
    1,
    ["worker-a", "worker-b"],
    [],
  )
  let job = JobStore::open(spec, target_dir + "/job.wal")
  for worker in ["worker-a", "worker-b"] {
    let suffix = if worker == "worker-a" { "a" } else { "b" }
    ignore(
      coordinator.register_worker(
        worker,
        @protocol.Incarnation::new("boot-" + suffix),
        ["reachability"],
      ),
    )
  }
  let lease = coordinator.assign(0, "worker-a")
  ignore(job.assign(0, "worker-a", lease.epoch(), lease.start_offset()))
  job.start()
  ignore(
    recover_worker_leases(
      coordinator,
      job,
      "worker-a",
      @protocol.Incarnation::new("boot-a"),
      "worker-b",
    ),
  )
  commit_worker_migration(coordinator, job, loaded)
  let lease_after = coordinator.snapshot().assignments()[0]
  assert_eq(lease_after.epoch(), 1)
  assert_eq(lease_after.next_offset(), 1L)
  assert_eq(lease_after.persisted_offset(), 0L)
  let job_after = job.snapshot().partitions()[0]
  assert_eq(job_after.next_offset(), 1L)
  assert_eq(job_after.applied_offset(), 0L)
  commit_worker_migration(coordinator, job, loaded)
  job.close()
  coordinator.close()
  let foreign = WorkerMigrationIdentity::new(
    "migration-cluster", "migration-coordinator", "migration-job", 0, 0L, "worker-a",
    "boot-a", 0, "worker-c", "boot-c", 1,
  )
  loaded.install(foreign, target_paths) catch {
    WorkerMigrationError::InvalidIdentity => ()
    _ => fail("foreign migration target was accepted")
  }
}
