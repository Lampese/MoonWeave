///|
async fn capture_online_migration(
  job : CompiledJob,
  config : CoordinatorNodeConfig,
  intent : NodeMigrationIntent,
) -> Unit raise ApiError {
  if config.migration_rpc is Some(rpc) {
    guard rpc(intent, None) is Some(bundle) else {
      raise ApiError::Cluster("Worker capture returned no migration bundle")
    }
    job.validate_migration_state(bundle)
    bundle.save(intent.bundle_path) catch {
      error => raise driver_error("save remote Worker migration", error)
    }
    return
  }
  let source = WorkerNodeConfig::new(
    config.cluster_id,
    config.coordinator_id,
    NodeEndpoint::new("127.0.0.1", 1),
    intent.source_worker_id,
    intent.source_incarnation,
    intent.source_storage_dir,
    intent.partition,
  ).with_assignment(intent.source_epoch, intent.start_offset)
  for attempt in 0..<1000 {
    try
      job.capture_worker_migration(
        source,
        intent.target_worker_id,
        intent.target_incarnation,
        intent.target_epoch,
        intent.bundle_path,
      )
    catch {
      ApiError::Cluster(reason) if reason.contains("Busy(") && attempt < 999 =>
        @async_runtime.sleep(10) catch {
          error => raise driver_error("wait for Worker quiescence", error)
        }
      error => raise error
    } noraise {
      _ => return
    }
  }
}

///|
async fn resume_online_migration(
  coordinator : @runtime.CoordinatorStore,
  job_store : @runtime.JobStore,
  job : CompiledJob,
  config : CoordinatorNodeConfig,
  assignments : Map[Int, @control.PartitionAssignment],
  log : NodeMigrationLog,
  intent : NodeMigrationIntent,
  confirm_authority? : async () -> Unit raise ApiError,
) -> Unit raise ApiError {
  guard node_migration_slot(config, intent.source_worker_id) is Some(source) &&
    node_migration_slot(config, intent.target_worker_id) is Some(target) &&
    (
      config.migration_rpc is Some(_) ||
      (
        source.storage_dir() == Some(intent.source_storage_dir) &&
        target.storage_dir() == Some(intent.target_storage_dir)
      )
    ) &&
    source.incarnation() == intent.source_incarnation &&
    target.incarnation() == intent.target_incarnation else {
    raise ApiError::Cluster("online migration storage differs from intent")
  }
  let exists = @fs.exists(intent.bundle_path) catch {
    error => raise driver_error("find migration bundle", error)
  }
  if !exists {
    capture_online_migration(job, config, intent)
  }
  @crash_boundary.hit("migration.captured", intent.bundle_path) catch {
    error => raise driver_error("migration capture boundary", error)
  }
  let bundle = @runtime.WorkerMigration::load(intent.bundle_path) catch {
    error => raise driver_error("load online migration", error)
  }
  let identity = bundle.identity()
  if identity.job_id() != intent.job_id ||
    identity.cluster_id() != intent.cluster_id ||
    identity.coordinator_id() != intent.coordinator_id ||
    identity.partition() != intent.partition ||
    identity.source_worker_id() != intent.source_worker_id ||
    identity.source_incarnation() != intent.source_incarnation ||
    identity.source_epoch() != intent.source_epoch ||
    identity.start_offset() != intent.start_offset ||
    identity.target_worker_id() != intent.target_worker_id ||
    identity.target_epoch() != intent.target_epoch ||
    identity.target_incarnation() != intent.target_incarnation {
    raise ApiError::Cluster("online migration bundle differs from intent")
  }
  guard assignments.get(intent.partition) is Some(current) else {
    raise ApiError::InvalidWorkerAssignment("online migration has no lease")
  }
  if confirm_authority is Some(confirm) {
    confirm()
  }
  if current.worker_id() == intent.source_worker_id &&
    current.epoch() == intent.source_epoch {
    let moved = coordinator.move_partition(
      intent.partition,
      intent.source_worker_id,
      @protocol.Incarnation::new(intent.source_incarnation),
      intent.target_worker_id,
    ) catch {
      error => raise driver_error("move online Coordinator lease", error)
    }
    assignments[intent.partition] = moved
  } else if current.worker_id() != intent.target_worker_id ||
    current.epoch() != intent.target_epoch {
    raise ApiError::Cluster("online migration lease differs from intent")
  }
  let saved = job_store.snapshot().partitions()[intent.partition]
  if saved.worker_id() == intent.source_worker_id &&
    saved.epoch() == intent.source_epoch {
    ignore(
      job_store.move_partition(
        intent.partition,
        intent.source_worker_id,
        intent.source_epoch,
        intent.target_worker_id,
      ) catch {
        error => raise driver_error("move online Job lease", error)
      },
    )
  } else if saved.worker_id() != intent.target_worker_id ||
    saved.epoch() != intent.target_epoch {
    raise ApiError::Cluster("online migration Job lease differs from intent")
  }
  @crash_boundary.hit("migration.moved", intent.transaction_id) catch {
    error => raise driver_error("migration lease boundary", error)
  }
  let target = WorkerNodeConfig::new(
    config.cluster_id,
    config.coordinator_id,
    NodeEndpoint::new("127.0.0.1", 1),
    intent.target_worker_id,
    intent.target_incarnation,
    intent.target_storage_dir,
    intent.partition,
  ).with_assignment(intent.target_epoch, intent.start_offset)
  match config.migration_rpc {
    Some(rpc) => ignore(rpc(intent, Some(bundle)))
    None => job.install_worker_migration(target, intent.bundle_path)
  }
  @crash_boundary.hit("migration.installed", intent.transaction_id) catch {
    error => raise driver_error("migration install boundary", error)
  }
  @runtime.commit_worker_migration(
    coordinator,
    job_store,
    bundle,
    reconcile_path=config.storage_dir +
      "/rescale-cursors-p" +
      intent.partition.to_string() +
      ".wal",
  ) catch {
    error => raise driver_error("commit online migration", error)
  }
  @crash_boundary.hit("migration.committed", intent.transaction_id) catch {
    error => raise driver_error("migration commit boundary", error)
  }
  log.complete(intent.transaction_id)
}
