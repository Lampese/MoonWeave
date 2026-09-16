///|
async test "source authority preserves compacted offsets identities and rejects gaps atomically" {
  let root = @fs.tmpdir(prefix="moonweave-source-authority")
  defer @fs.rmdir(root, recursive=true)
  let first = source_event("authority", 0L, "k", "B")
  let second = source_event("authority", 1L, "k", "C")
  let source = SourceLog::open(root + "/leader.wal")
  ignore(source.append(0, 10L, first))
  ignore(source.append(0, 10L, second))
  source.compact(source.retention_barrier(0, 11L))
  let snapshot = source.snapshot()
  source.close()
  let target = SourceLog::open(root + "/follower.wal")
  target.replace_snapshot(snapshot)
  assert_eq(target.snapshot(), snapshot)
  try
    target.replace_snapshot({
      ..snapshot,
      offsets: [{ partition: 0, next_offset: 13L, }],
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("accepted a source offset gap")
  }
  assert_eq(target.snapshot(), snapshot)
  target.close()
  let restored = SourceLog::open(root + "/follower.wal")
  defer restored.close()
  assert_eq(restored.snapshot(), snapshot)
  ignore(restored.append(0, 11L, first))
  assert_eq(restored.event_count(), 1)
  assert_eq(restored.next_offset(0), Some(12L))
  assert_eq(restored.append(0, 11L, second), 11L)
  restored.compact(restored.retention_barrier(0, 12L))
  let empty = restored.snapshot()
  restored.replace_snapshot(empty)
  assert_eq(restored.next_offset(0), Some(12L))
}

///|
async test "job authority restores delivery plans into independent storage" {
  let root = @fs.tmpdir(prefix="moonweave-job-authority")
  defer @fs.rmdir(root, recursive=true)
  let spec = @plan.JobSpec::new(
    "authority-job",
    "reachability",
    1,
    ["worker"],
    [],
  )
  let coordinator = CoordinatorStore::open(
    "cluster",
    "coordinator",
    root + "/control.wal",
  )
  defer coordinator.close()
  ignore(
    coordinator.register_worker("worker", @protocol.Incarnation::new("boot"), [
      "reachability",
    ]),
  )
  let assignment = coordinator.assign(0, "worker")
  let job = JobStore::open(spec, root + "/job.wal")
  ignore(job.assign(0, "worker", assignment.epoch(), 0L))
  job.start()
  let source = SourceBackedJob::open(
    root + "/source.wal",
    spec,
    coordinator,
    job,
  )
  ignore(source.ingest(source_event("source-authority", 0L, "k", "B")))
  ignore(job.record_batch_sent(0, "worker", assignment.epoch(), 0L, 1L))
  let snapshot = job.authority_snapshot()
  assert_eq(snapshot.work_plans.length(), 1)
  source.close()
  job.close()
  let target = JobStore::open(spec, root + "/other.wal")
  target.replace_authority_snapshot(snapshot)
  assert_eq(target.authority_snapshot(), snapshot)
  try target.replace_authority_snapshot({ ..snapshot, work_plans: [], }) catch {
    JobStoreError::PlanMismatch(0) => ()
    _ => fail("unexpected authority plan error")
  } noraise {
    _ => fail("accepted cursor without delivery plan")
  }
  assert_eq(target.authority_snapshot(), snapshot)
  target.close()
  let restored = JobStore::open(spec, root + "/other.wal")
  defer restored.close()
  assert_eq(restored.authority_snapshot(), snapshot)
}

///|
async test "manifest authority carries ACK retention and certificate without local output files" {
  let root = @fs.tmpdir(prefix="moonweave-manifest-authority")
  defer @fs.rmdir(root, recursive=true)
  let leader = JobOutputManifest::open(
    root + "/leader.wal",
    "job",
    "reachability",
    1,
  )
  leader.record_partition_summary(
    0,
    "task",
    "worker",
    0,
    2L,
    "outputs",
    "frontier",
    "state",
    true,
    consumer_id="sink",
    acked_sequence=1L,
    retained_from=2L,
  )
  let certificate = JobOutputCertificate::new(
    "q",
    "job",
    "reachability",
    "Stable",
    "result",
    leader.frontier_digest(),
    leader.output_digest(),
    leader.task_state_digest(),
    Json::null(),
  )
  leader.publish_certificate(certificate)
  let snapshot = leader.snapshot()
  leader.close()
  let target = JobOutputManifest::open(
    root + "/target.wal",
    "job",
    "reachability",
    1,
  )
  target.replace_snapshot(snapshot)
  assert_true(target.is_retention_safe())
  assert_true(target.verify_certificate(certificate))
  try target.replace_snapshot({ ..snapshot, consumers: [], }) catch {
    JobOutputManifestError::RetentionNotSafe(0) => ()
    _ => fail("wrong missing retention ACK rejection")
  } noraise {
    _ => fail("accepted retention without ACK")
  }
  assert_eq(target.snapshot(), snapshot)
  target.close()
  let restored = JobOutputManifest::open(
    root + "/target.wal",
    "job",
    "reachability",
    1,
  )
  defer restored.close()
  assert_eq(restored.snapshot(), snapshot)
}
