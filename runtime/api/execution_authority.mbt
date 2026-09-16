///|
struct ExecutionAuthoritySnapshot {
  live : @runtime.LiveJobAuthoritySnapshot
  manifest : @runtime.JobOutputManifestSnapshot?
  migrations : Array[MigrationAuthoritySnapshot]
} derive(Debug, Eq, ToJson, FromJson)

///|
async fn SourceBackedExecution::authority_snapshot(
  self : SourceBackedExecution,
) -> ExecutionAuthoritySnapshot raise ApiError {
  let live = self.source.authority_snapshot() catch {
    error => raise driver_error("capture execution authority", error)
  }
  let manifest = match self.output_manifest {
    Some(manifest) =>
      Some(
        manifest.authority_snapshot() catch {
          error => raise driver_error("capture output authority", error)
        },
      )
    None => None
  }
  let migrations = self.migration_authority()
  { live, manifest, migrations, }
}

///|
async fn DriverCoordinator::stop_for_authority(
  self : DriverCoordinator,
) -> Unit raise ApiError {
  if self.live_task is Some(task) {
    task.cancel()
    ignore(task.wait() catch { _ => Err("authority replacement") })
    self.live_task = None
  }
  if self.live_execution is Some(execution) {
    execution.close()
    self.live_execution = None
  }
}
