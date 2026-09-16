///|
let migration_result_capability = "moonweave:migration-result:v1"

///|
// Transfer envelopes are bounded independently from ordinary data frames.
let migration_frame_bytes : Int = 64 * 1024 * 1024

///|
struct WorkerMigrationRequest {
  intent : NodeMigrationIntent
  bundle : @runtime.WorkerMigration?
} derive(Eq, ToJson, FromJson)

///|
struct WorkerMigrationReport {
  transaction_id : String
  installing : Bool
  result : Result[@runtime.WorkerMigration?, String]
} derive(ToJson, FromJson)

///|
struct WorkerMigrationExchange {
  request : WorkerMigrationRequest
  mut result : Result[@runtime.WorkerMigration?, String]?
}

///|
/// The durable migration log owns recovery. This mailbox only joins one
/// authenticated Worker poll to the current attempt; no filesystem location
/// supplied by the Coordinator is opened on a Worker.
async fn CoordinatorService::request_worker_migration(
  self : CoordinatorService,
  intent : NodeMigrationIntent,
  bundle : @runtime.WorkerMigration?,
) -> @runtime.WorkerMigration? raise ApiError {
  let worker = if bundle is Some(_) {
    intent.target_worker_id
  } else {
    intent.source_worker_id
  }
  let key = (intent.job_id, worker)
  let exchange = WorkerMigrationExchange::{
    request: { intent, bundle, },
    result: None,
  }
  if self.migrations.contains(key) {
    raise ApiError::InvalidDriverCommand("Worker migration is already pending")
  }
  self.migrations[key] = exchange
  defer self.migrations.remove(key)
  @async_runtime.with_timeout(10000, () => {
    for ;; {
      if self.closed {
        raise ApiError::DriverClosed
      }
      if exchange.result is Some(result) {
        return match result {
          Ok(bundle) => bundle
          Err(reason) => raise ApiError::Cluster("Worker migration: " + reason)
        }
      }
      @async_runtime.sleep(10)
    }
  }) catch {
    error => {
      if @async_runtime.is_being_cancelled() {
        raise driver_error("cancel Worker migration", error)
      }
      match error {
        ApiError::Cluster(reason) => raise ApiError::Cluster(reason)
        _ => raise ApiError::WorkerMigrationUnavailable(worker)
      }
    }
  }
}

///|
async fn CoordinatorService::receive_worker_migration_report(
  self : CoordinatorService,
  connection : @socket.Tcp,
  first : @protocol.Frame,
  hello : @protocol.Hello,
) -> Unit {
  let frame = @runtime.read_frame(
    connection,
    maximum_frame_bytes=migration_frame_bytes,
  )
  if frame.kind() != @protocol.FrameKind::WorkerMigrationResult ||
    frame.cluster_id() != first.cluster_id() ||
    frame.sender_id() != hello.worker_id() ||
    frame.worker_incarnation() != hello.incarnation() ||
    frame.session_id() != first.message_id() {
    raise ApiError::InvalidWorkerAssignment("invalid migration report")
  }
  let report : WorkerMigrationReport = @json.from_json(
    @json.parse(frame.payload()),
  )
  if self.migrations.get((hello.job_id().unwrap(), hello.worker_id()))
    is Some(exchange) {
    let intent = exchange.request.intent
    let expected_boot = if report.installing {
      intent.target_incarnation
    } else {
      intent.source_incarnation
    }
    if report.transaction_id == intent.transaction_id &&
      report.installing == (exchange.request.bundle is Some(_)) &&
      hello.incarnation() == expected_boot {
      exchange.result = Some(report.result)
    }
  }
  // A reply may be retried after its previous acknowledgement was lost, or
  // after failover discarded an ephemeral mailbox. The durable intent decides
  // whether the operation must be requested again.
}

///|
async fn CompiledJob::perform_worker_migration(
  self : CompiledJob,
  config : WorkerNodeConfig,
  request : WorkerMigrationRequest,
) -> WorkerMigrationReport {
  let intent = request.intent
  let installing = request.bundle is Some(_)
  let result = try {
    validate_node_migration_intent(intent)
    if intent.job_id != self.job_id() ||
      intent.cluster_id != config.cluster_id ||
      intent.coordinator_id != config.coordinator_id ||
      intent.partition >= self.partition_count() ||
      (if installing {
        intent.target_worker_id != config.worker_id ||
        intent.target_incarnation != config.incarnation
      } else {
        intent.source_worker_id != config.worker_id ||
        intent.source_incarnation != config.incarnation
      }) {
      raise ApiError::InvalidWorkerAssignment("foreign migration request")
    }
    let local = {
      ..config,
      partition: intent.partition,
      epoch: if installing {
        intent.target_epoch
      } else {
        intent.source_epoch
      },
      start_offset: intent.start_offset,
      assignment_known: true,
    }
    let path = config.storage_dir +
      "/transfer-p" +
      intent.partition.to_string() +
      ".json"
    match request.bundle {
      None => {
        self.capture_worker_migration(
          local,
          intent.target_worker_id,
          intent.target_incarnation,
          intent.target_epoch,
          path,
        )
        let bundle = @runtime.WorkerMigration::load(path)
        @fs.remove(path)
        Ok(Some(bundle))
      }
      Some(bundle) => {
        let identity = bundle.identity()
        if identity.source_worker_id() != intent.source_worker_id ||
          identity.source_incarnation() != intent.source_incarnation ||
          identity.source_epoch() != intent.source_epoch ||
          identity.target_epoch() != intent.target_epoch ||
          identity.start_offset() != intent.start_offset {
          raise ApiError::InvalidWorkerAssignment(
            "migration bundle differs from request",
          )
        }
        bundle.save(path)
        self.install_worker_migration(local, path)
        @fs.remove(path)
        Ok(None)
      }
    }
  } catch {
    error => Err(@debug.to_string(error))
  }
  { transaction_id: intent.transaction_id, installing, result, }
}
