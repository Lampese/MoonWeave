///|
async fn with_job_output_paths(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-job-output")
  defer {
    for name in @fs.readdir(directory) {
      @fs.remove(directory + "/" + name) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  @async_runtime.with_timeout(10000, () => f(directory))
}

///|
fn manifest_output(target : String) -> @task.TaskOutput {
  @task.TaskOutput::Reachable(@reachability.Reachable::new("A", target))
}

///|
fn manifest_spec() -> @plan.JobSpec raise {
  @plan.JobSpec::new("manifest-job", "reachability", 1, ["worker"], [])
}

///|
async test "job output manifest persists partition digests and reopens deterministically" {
  with_job_output_paths <| directory => {
    let output_path = directory + "/task.wal"
    let manifest_path = directory + "/job-output.wal"
    let log = TaskOutputLog::open(output_path)
    log.append("manifest-job/p0/e0", 0, 0, 0L, [
      manifest_output("B"),
      manifest_output("C"),
    ])
    let manifest = JobOutputManifest::open(
      manifest_path, "manifest-job", "reachability", 1,
    )
    manifest.record_partition(
      0, "manifest-job/p0/e0", "worker", 0, log, "frontier-0", "state-0", true,
    )
    assert_true(manifest.is_complete())
    assert_false(manifest.is_fully_consumed())
    let partition = manifest.partition(0).unwrap()
    assert_eq(partition.output_count(), 2L)
    assert_eq(partition.output_log_path(), output_path)
    assert_true(partition.output_digest().has_prefix("manifest-"))
    let frontier_digest = manifest.frontier_digest()
    let output_digest = manifest.output_digest()
    let final_digest = manifest.final_digest()
    let snapshot = manifest.snapshot()
    assert_eq(snapshot.final_digest(), final_digest)
    manifest.close()
    log.close()

    let reopened_log = TaskOutputLog::open(output_path)
    defer reopened_log.close()
    let reopened = JobOutputManifest::open(
      manifest_path, "manifest-job", "reachability", 1,
    )
    defer reopened.close()
    assert_eq(reopened.frontier_digest(), frontier_digest)
    assert_eq(reopened.output_digest(), output_digest)
    assert_eq(reopened.final_digest(), final_digest)
    assert_eq(reopened.snapshot(), snapshot)
  }
}

///|
async test "job output manifest binds consumers and a final certificate" {
  with_job_output_paths <| directory => {
    let log = TaskOutputLog::open(directory + "/task.wal")
    defer log.close()
    log.append("manifest-job/p0/e0", 0, 0, 0L, [manifest_output("B")])
    let manifest = JobOutputManifest::open(
      directory + "/job-output.wal",
      "manifest-job",
      "reachability",
      1,
    )
    defer manifest.close()
    manifest.record_partition(
      0, "manifest-job/p0/e0", "worker", 0, log, "frontier-0", "state-0", true,
    )
    let consumer = TaskOutputConsumer::open(
      log,
      directory + "/consumer.wal",
      "sink",
      "manifest-job/p0/e0",
      0,
      0,
    )
    defer consumer.close()
    manifest.sync_consumer(consumer)
    assert_false(manifest.is_fully_consumed())
    consumer.ack(consumer.read()[0])
    manifest.sync_consumer(consumer)
    assert_true(manifest.is_fully_consumed())
    let certificate = JobOutputCertificate::new(
      "query-1",
      "manifest-job",
      "reachability",
      "Stable",
      "match:A->B",
      manifest.frontier_digest(),
      manifest.output_digest(),
      manifest.task_state_digest(),
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    manifest.publish_certificate(certificate)
    assert_true(manifest.certificate() is Some(_))
    assert_true(manifest.final_digest() != manifest.output_digest())
    let expected_final_digest = manifest.final_digest()
    manifest.close()
    let reopened = JobOutputManifest::open(
      directory + "/job-output.wal",
      "manifest-job",
      "reachability",
      1,
    )
    defer reopened.close()
    assert_true(reopened.is_fully_consumed())
    assert_eq(reopened.certificate(), Some(certificate))
    assert_eq(reopened.final_digest(), expected_final_digest)
  }
}

///|
async test "job output manifest can bind a terminal Job and rejects premature completion" {
  with_job_output_paths <| directory => {
    let spec = manifest_spec()
    let job = JobStore::open(spec, directory + "/job.wal")
    defer job.close()
    ignore(job.assign(0, "worker", 0, 0L))
    job.start()
    let log = TaskOutputLog::open(directory + "/task.wal")
    defer log.close()
    log.append("manifest-job/p0/e0", 0, 0, 0L, [manifest_output("B")])
    let manifest = JobOutputManifest::open_for_job(
      directory + "/job-output.wal",
      job,
    )
    defer manifest.close()
    try
      manifest.record_job_partition(
        job, 0, "manifest-job/p0/e0", log, "frontier-0", "state-0", true,
      )
    catch {
      error => assert_eq(error, JobOutputManifestError::NotComplete)
    } noraise {
      _ => fail("running Job published a complete output partition")
    }
    job.drain_partition(0, "worker", 0)
    job.stop_partition(0, "worker", 0, 0)
    manifest.record_job_partition(
      job, 0, "manifest-job/p0/e0", log, "frontier-0", "state-0", true,
    )
    assert_true(manifest.is_complete())
  }
}

///|
async test "job output manifest fails closed on identity, cursor, and certificate conflicts" {
  with_job_output_paths <| directory => {
    let log = TaskOutputLog::open(directory + "/task.wal")
    defer log.close()
    log.append("manifest-job/p0/e0", 0, 0, 0L, [manifest_output("B")])
    let manifest = JobOutputManifest::open(
      directory + "/job-output.wal",
      "manifest-job",
      "reachability",
      1,
    )
    defer manifest.close()
    manifest.record_partition(
      0, "manifest-job/p0/e0", "worker", 0, log, "frontier-0", "state-0", false,
    )
    try
      manifest.record_partition(
        0, "manifest-job/p0/e1", "worker", 1, log, "frontier-1", "state-1", false,
      )
    catch {
      error => assert_true(error is JobOutputManifestError::IdentityConflict(_))
    } noraise {
      _ => fail("partition identity was replaced")
    }
    let consumer = TaskOutputConsumer::open(
      log,
      directory + "/consumer.wal",
      "sink",
      "manifest-job/p0/e0",
      0,
      0,
    )
    defer consumer.close()
    manifest.sync_consumer(consumer)
    let bad_certificate = JobOutputCertificate::new(
      "query-1",
      "manifest-job",
      "reachability",
      "Stable",
      "match:A->B",
      "wrong-frontier",
      "wrong-output",
      "wrong-state",
      Json::null(),
    )
    try manifest.publish_certificate(bad_certificate) catch {
      error => assert_eq(error, JobOutputManifestError::NotComplete)
    } noraise {
      _ => fail("incomplete manifest published a certificate")
    }
  }
}

///|
async test "job output manifest rejects malformed configuration and repairs a torn WAL tail" {
  with_job_output_paths <| directory => {
    try
      JobOutputManifest::open(directory + "/bad.wal", "", "reachability", 1)
    catch {
      error => assert_eq(error, JobOutputManifestError::EmptyJobId)
    } noraise {
      _ => fail("empty Job ID accepted")
    }
    try JobOutputManifest::open(directory + "/bad.wal", "job", "", 1) catch {
      error => assert_eq(error, JobOutputManifestError::EmptyOperatorId)
    } noraise {
      _ => fail("empty operator ID accepted")
    }
    try
      JobOutputManifest::open(directory + "/bad.wal", "job", "reachability", 0)
    catch {
      error =>
        assert_eq(error, JobOutputManifestError::InvalidPartitionCount(0))
    } noraise {
      _ => fail("empty partition set accepted")
    }
    let path = directory + "/job-output.wal"
    let manifest = JobOutputManifest::open(path, "job", "reachability", 1)
    manifest.close()
    let bytes = @fs.read_file(path).binary()
    @fs.write_file(
      path,
      bytes[:bytes.length() - 1].to_owned(),
      create_mode=@fs.CreateOrTruncate,
      sync=@fs.Full,
    )
    let restored = JobOutputManifest::open(path, "job", "reachability", 1)
    defer restored.close()
    assert_eq(restored.partitions(), [])
    assert_eq(restored.consumers(), [])
    assert_eq(restored.certificate(), None)
  }
}

///|
async test "empty successful partitions are already fully consumed" {
  with_job_output_paths <| directory => {
    let manifest = JobOutputManifest::open(
      directory + "/job-output.wal",
      "empty-job",
      "reachability",
      1,
    )
    defer manifest.close()
    let log = TaskOutputLog::open(directory + "/empty-task.wal")
    defer log.close()
    manifest.record_partition(
      0, "empty-job/p0/e0", "worker", 0, log, "frontier-empty", "state-empty", true,
    )
    assert_true(manifest.is_complete())
    assert_true(manifest.is_fully_consumed())
  }
}

///|
async test "durable Job supervisor automatically records Worker output summaries" {
  with_job_output_paths <| directory => {
    let coordinator_journal = directory + "/coordinator.wal"
    let coordinator_checkpoint = directory + "/coordinator.json"
    let job_journal = directory + "/job.wal"
    let job_checkpoint = directory + "/job.json"
    let reconcile = directory + "/reconcile.wal"
    let worker_wal = directory + "/worker.wal"
    let output_wal = directory + "/worker-output.wal"
    let output_manifest_path = directory + "/job-output.wal"
    let coordinator = CoordinatorStore::open(
      "manifest-cluster",
      "coordinator",
      coordinator_journal,
      checkpoint_path=coordinator_checkpoint,
    )
    let job = JobStore::open(
      @plan.JobSpec::new("manifest-job", "reachability", 1, ["worker"], []),
      job_journal,
      checkpoint_path=job_checkpoint,
    )
    let incarnation = @protocol.Incarnation::new("manifest-worker")
    ignore(coordinator.register_worker("worker", incarnation, ["reachability"]))
    let assignment = coordinator.assign(0, "worker")
    ignore(job.assign(0, "worker", assignment.epoch(), 0L))
    let manifest = JobOutputManifest::open_for_job(output_manifest_path, job)
    let event = @protocol.EventRecord::new(
      @causal.EventId::new("manifest-input", 0),
      [],
      "manifest-key",
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_durable_job_supervisor(
          server,
          coordinator,
          job,
          [SupervisorWork::new("worker", "manifest-session", 0, [batch])],
          reconcile_path=reconcile,
          output_manifest=manifest,
        )
      }
      let task = @task.Task::new(
        "manifest-job/p0/e0",
        "reachability",
        @task.OperatorKind::Reachability,
        0,
        assignment.epoch(),
      )
      let discard : (@task.TaskOutput) -> Unit raise = _ => ()
      let worker_summary = run_worker_task(
        @socket.Tcp::connect(server.addr),
        "manifest-cluster",
        "coordinator",
        "worker",
        incarnation.value(),
        ["reachability"],
        worker_wal,
        task,
        discard,
        output_log_path=output_wal,
        initial_credit_events=1L,
        initial_credit_bytes=4096L,
      )
      let supervisor_summary = supervisor.wait()
      assert_eq(worker_summary.live_batches(), 1)
      assert_eq(supervisor_summary.failed_workers(), 0)
      assert_eq(supervisor_summary.completed_workers(), 1)
    }
    assert_true(manifest.is_complete())
    guard manifest.partition(0) is Some(summary) else {
      fail("automatic output summary missing")
    }
    assert_eq(summary.task_id(), "manifest-job/p0/e0")
    assert_eq(summary.worker_id(), "worker")
    assert_eq(summary.output_count(), 1L)
    assert_true(summary.output_digest().has_prefix("manifest-"))
    assert_true(summary.output_log_path().has_prefix("worker://worker/"))
    manifest.close()
    job.close()
    coordinator.close()
  }
}

///|
async test "verified certificate replays Task snapshot and rejects forged output proofs" {
  with_job_output_paths <| directory => {
    let task_id = "proof-job/p0/e0"
    let log = TaskOutputLog::open(directory + "/task.wal")
    defer log.close()
    let task = @task.Task::new(
      task_id,
      "reachability",
      @task.OperatorKind::Reachability,
      0,
      0,
    )
    task.start()
    let event = @protocol.EventRecord::new(
      @causal.EventId::new("proof-input", 0),
      [],
      "proof-key",
      Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
    )
    let outputs = task.process_batch(
      @protocol.EventBatch::new(0, 0, 0L, [event]),
    )
    let snapshot = task.snapshot()
    log.append(task_id, 0, 0, 0L, outputs[:])
    let manifest = JobOutputManifest::open(
      directory + "/job-output.wal",
      "proof-job",
      "reachability",
      1,
    )
    defer manifest.close()
    manifest.record_partition(
      0,
      task_id,
      "worker",
      0,
      log,
      snapshot.frontier_digest(),
      task_snapshot_digest(snapshot),
      true,
      applied_offset=0L,
    )
    let proof = JobOutputTaskProof::new(
      0,
      task_id,
      snapshot,
      log.records_for(task_id)[:],
    )
    let certificate = manifest.publish_verified_certificate(
      "proof-query",
      JobOutputQuery::Reachable("A", "B"),
      [proof],
    )
    assert_eq(certificate.status(), "Stable")
    assert_eq(certificate.result_digest(), "match:A->B")
    assert_eq(certificate.job_id(), "proof-job")
    assert_eq(certificate.output_digest(), manifest.output_digest())
    assert_eq(certificate.task_state_digest(), manifest.task_state_digest())
    assert_true(manifest.verify_certificate(certificate))
    let forged = JobOutputTaskProof::{ ..proof, outputs: [], }
    try
      manifest.publish_verified_certificate(
        "forged-query",
        JobOutputQuery::Reachable("A", "B"),
        [forged],
      )
    catch {
      error => assert_eq(error, JobOutputManifestError::InvalidProof(0))
    } noraise {
      _ => fail("forged output proof accepted")
    }
  }
}

///|
async test "verified certificate accepts an empty successful Task at offset minus one" {
  with_job_output_paths <| directory => {
    let task_id = "empty-proof/p0/e0"
    let task = @task.Task::new(
      task_id,
      "reachability",
      @task.OperatorKind::Reachability,
      0,
      0,
    )
    task.start()
    let snapshot = task.snapshot()
    let log = TaskOutputLog::open(directory + "/empty-task.wal")
    defer log.close()
    let manifest = JobOutputManifest::open(
      directory + "/empty-job-output.wal",
      "empty-proof",
      "reachability",
      1,
    )
    defer manifest.close()
    manifest.record_partition(
      0,
      task_id,
      "worker",
      0,
      log,
      snapshot.frontier_digest(),
      task_snapshot_digest(snapshot),
      true,
      applied_offset=-1L,
    )
    let proof = JobOutputTaskProof::new(0, task_id, snapshot, [])
    let certificate = manifest.publish_verified_certificate(
      "empty-query",
      JobOutputQuery::Reachable("A", "B"),
      [proof],
    )
    assert_eq(certificate.status(), "ResolvedNoMatch")
    assert_eq(certificate.result_digest(), "no-match:A->B")
    assert_true(manifest.verify_certificate(certificate))
  }
}
