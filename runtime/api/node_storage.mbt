///|
fn worker_node_storage_paths(
  storage_dir : String,
  partition : Int,
  sink : SinkSpec,
) -> @runtime.WorkerStoragePaths raise ApiError {
  let suffix = partition.to_string()
  let output = match sink {
    SinkSpec::Discard => None
    SinkSpec::Durable(_) | SinkSpec::JsonLines(_) =>
      Some(storage_dir + "/output-p" + suffix + ".wal")
  }
  @runtime.WorkerStoragePaths::new(
    storage_dir + "/worker-p" + suffix + ".wal",
    storage_dir + "/worker-p" + suffix + ".json",
    Some(storage_dir + "/delta-p" + suffix + ".wal"),
    output,
  ) catch {
    error =>
      raise ApiError::Cluster(
        "build Worker storage paths: " + @debug.to_string(error),
      )
  }
}

///|
fn node_reconciliation_path(config : CoordinatorNodeConfig) -> String {
  config.storage_dir + "/node-reconcile.wal"
}

///|
async fn restore_node_assignments(
  coordinator : @runtime.CoordinatorStore,
  job_store : @runtime.JobStore,
  job : CompiledJob,
  config : CoordinatorNodeConfig,
) -> Map[Int, @control.PartitionAssignment] raise ApiError {
  // Finish lease transactions before installing state or comparing stores.
  // Ordinary cursor reconciliation runs after node migration and plan binding.
  ignore(
    @runtime.reconcile_job_coordinator(
      coordinator,
      job_store,
      reconcile_path=node_reconciliation_path(config),
      replay_only=true,
    ),
  ) catch {
    error =>
      raise ApiError::Cluster(
        "reconcile restored node: " + @debug.to_string(error),
      )
  }
  let assignments : Map[Int, @control.PartitionAssignment] = Map([])
  for item in coordinator.snapshot().assignments() {
    let assignment = @control.PartitionAssignment::from_snapshot(item) catch {
      error =>
        raise ApiError::Cluster(
          "decode restored Worker assignment: " + @debug.to_string(error),
        )
    }
    assignments[assignment.partition()] = assignment
  }
  resume_pending_node_migrations(
    coordinator, job_store, job, config, assignments,
  )
  assignments
}
