///|
async test "Worker rejects a foreign migration before opening any local state" {
  let root = @fs.tmpdir(prefix="moonweave-migration-routing")
  defer @fs.rmdir(root, recursive=true)
  let job = JobDefinition::new("job", OperatorRef::keyed_sum()).compile()
  let config = WorkerNodeConfig::new_auto(
    "cluster",
    "coordinator",
    NodeEndpoint::new("127.0.0.1", 1),
    "source",
    "boot-source",
    root + "/never-opened",
  )
  let intent : NodeMigrationIntent = {
    version: 1,
    phase: "prepared",
    transaction_id: "move",
    cluster_id: "cluster",
    coordinator_id: "coordinator",
    job_id: "other-job",
    partition: 0,
    source_worker_id: "source",
    source_incarnation: "boot-source",
    source_epoch: 0,
    start_offset: 0L,
    target_worker_id: "target",
    target_incarnation: "boot-target",
    target_epoch: 1,
    source_storage_dir: "/untrusted/source",
    target_storage_dir: "/untrusted/target",
    bundle_path: "/untrusted/bundle",
    online: Some(true),
  }
  let report = job.perform_worker_migration(config, { intent, bundle: None, })
  assert_true(report.result is Err(_))
  assert_false(@fs.exists(config.storage_dir))
  let report = job.perform_worker_migration(config, {
    intent: { ..intent, job_id: "job", source_incarnation: "stale-boot", },
    bundle: None,
  })
  assert_true(report.result is Err(_))
  assert_false(@fs.exists(config.storage_dir))
}
