///|
struct MigrationAuthoritySnapshot {
  intent : NodeMigrationIntent
  bundle : @runtime.WorkerMigration?
} derive(Debug, Eq, ToJson, FromJson)

///|
async fn SourceBackedExecution::migration_authority(
  self : SourceBackedExecution,
) -> Array[MigrationAuthoritySnapshot] raise ApiError {
  let log = match self.active_migrations {
    Some(log) => log
    None =>
      NodeMigrationLog::open(
        self.listener.config.storage_dir + "/node-migrations.wal",
        maximum_bytes=self.listener.config.storage_budget_bytes,
      )
  }
  defer (if self.active_migrations is None { log.wal.close() })
  let intents = log.pending.values().to_array()
  intents.sort_by((a, b) => a.transaction_id.compare(b.transaction_id))
  let result : Array[MigrationAuthoritySnapshot] = []
  for intent in intents {
    let exists = @fs.exists(intent.bundle_path) catch {
      error => raise driver_error("find authority migration", error)
    }
    let bundle = if exists {
      Some(
        @runtime.WorkerMigration::load(intent.bundle_path) catch {
          error => raise driver_error("capture authority migration", error)
        },
      )
    } else {
      None
    }
    result.push({ intent, bundle, })
  }
  result
}

///|
async fn restore_migration_authority(
  config : CoordinatorNodeConfig,
  snapshots : Array[MigrationAuthoritySnapshot],
) -> Unit raise ApiError {
  let log = NodeMigrationLog::open(
    config.storage_dir + "/node-migrations.wal",
    maximum_bytes=config.storage_budget_bytes,
  )
  defer log.wal.close()
  let entries : Array[Bytes] = []
  let seen : Set[String] = Set([])
  for snapshot in snapshots {
    validate_node_migration_intent(snapshot.intent)
    if !seen.add_and_check(snapshot.intent.transaction_id) ||
      snapshot.intent.cluster_id != config.cluster_id ||
      snapshot.intent.coordinator_id != config.coordinator_id {
      raise ApiError::Cluster("invalid migration authority")
    }
    // Bundles belong to the receiving Coordinator's own directory. The old
    // path is metadata only and is never opened during authority installation.
    let intent = {
      ..snapshot.intent,
      bundle_path: config.storage_dir +
      "/authority-migration-p" +
      snapshot.intent.partition.to_string() +
      "-e" +
      snapshot.intent.target_epoch.to_string() +
      ".json",
    }
    if snapshot.bundle is Some(bundle) {
      bundle.save(intent.bundle_path) catch {
        error => raise driver_error("restore authority migration bundle", error)
      }
    }
    entries.push(
      @utf8.encode(
        @json.to_json(NodeMigrationIntentEntry::{
          version: 1,
          kind: "phase",
          transaction_id: intent.transaction_id,
          intent: Some(intent),
        }).stringify(),
      ),
    )
  }
  log.wal.rewrite(entries) catch {
    error => raise driver_error("restore migration authority", error)
  }
}
