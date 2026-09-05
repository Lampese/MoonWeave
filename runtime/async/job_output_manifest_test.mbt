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
      0, "manifest-job/p0/e0", "worker", 0, log, "frontier-0", true,
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
      0, "manifest-job/p0/e0", "worker", 0, log, "frontier-0", true,
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
      "reachability",
      "Stable",
      "match:A->B",
      manifest.frontier_digest(),
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
        job, 0, "manifest-job/p0/e0", log, "frontier-0", true,
      )
    catch {
      error => assert_eq(error, JobOutputManifestError::NotComplete)
    } noraise {
      _ => fail("running Job published a complete output partition")
    }
    job.drain_partition(0, "worker", 0)
    job.stop_partition(0, "worker", 0, 0)
    manifest.record_job_partition(
      job, 0, "manifest-job/p0/e0", log, "frontier-0", true,
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
      0, "manifest-job/p0/e0", "worker", 0, log, "frontier-0", false,
    )
    try
      manifest.record_partition(
        0, "manifest-job/p0/e1", "worker", 1, log, "frontier-1", false,
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
      "reachability",
      "Stable",
      "match:A->B",
      "wrong-frontier",
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
      0, "empty-job/p0/e0", "worker", 0, log, "frontier-empty", true,
    )
    assert_true(manifest.is_complete())
    assert_true(manifest.is_fully_consumed())
  }
}
