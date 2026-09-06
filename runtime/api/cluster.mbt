///|
fn cluster_error_at(stage : String, error : Error) -> ApiError {
  ApiError::Cluster(stage + ": " + @debug.to_string(error))
}

///|
fn cluster_event_groups(
  job : CompiledJob,
  events : ArrayView[Event],
) -> Map[Int, Array[Event]] {
  let grouped : Map[Int, Array[Event]] = Map([])
  for event in events {
    let partition = job.spec.route_event(event.record).partition()
    match grouped.get(partition) {
      Some(current) => current.push(event)
      None => grouped[partition] = [event]
    }
  }
  grouped
}

///|
/// Run a finite Job through real local Coordinator/Worker TCP sessions. This
/// is the first high-level ClusterRunner: it owns durable stores and Worker
/// sessions but deliberately accepts only registered built-in operators.
pub async fn CompiledJob::run_local_cluster(
  self : CompiledJob,
  config : ClusterConfig,
  events : ArrayView[Event],
) -> ClusterRunSummary raise ApiError {
  for event in events {
    self.descriptor.validate_event(event)
  }
  let storage_exists = @fs.exists(config.storage_dir) catch {
    error => raise cluster_error_at("check storage", error)
  }
  if !storage_exists {
    @fs.mkdir(config.storage_dir, recursive=true) catch {
      error => raise cluster_error_at("mkdir", error)
    }
  }
  let coordinator_journal = config.storage_dir + "/coordinator.wal"
  let coordinator_checkpoint = config.storage_dir + "/coordinator.json"
  let job_journal = config.storage_dir + "/job.wal"
  let job_checkpoint = config.storage_dir + "/job.json"
  let coordinator = @runtime.CoordinatorStore::open(
    config.cluster_id,
    config.coordinator_id,
    coordinator_journal,
    checkpoint_path=coordinator_checkpoint,
  ) catch {
    error => raise cluster_error_at("open coordinator", error)
  }
  defer coordinator.close()
  let job_store = @runtime.JobStore::open(
    self.spec,
    job_journal,
    checkpoint_path=job_checkpoint,
  ) catch {
    error => raise cluster_error_at("open job", error)
  }
  defer job_store.close()
  let descriptor = self.descriptor
  let capabilities = [
    self.definition.operator.id(),
    descriptor.capability(),
    "causal-frontier",
    descriptor.code_digest(),
  ]
  let incarnations : Map[String, @protocol.Incarnation] = Map([])
  for worker_id in self.spec.workers() {
    let incarnation = @protocol.Incarnation::new(
      "api-" + self.definition.job_id + "-" + worker_id,
    ) catch {
      error => raise cluster_error_at("register worker", error)
    }
    ignore(
      coordinator.register_worker(worker_id, incarnation, capabilities) catch {
        error => raise cluster_error_at("register worker", error)
      },
    )
    incarnations[worker_id] = incarnation
  }
  let assignments : Map[Int, @control.PartitionAssignment] = Map([])
  for partition in 0..<self.spec.partition_count() {
    let worker_id = self.spec.owner(partition) catch {
      error => raise cluster_error_at("owner", error)
    }
    let assignment = coordinator.assign(partition, worker_id) catch {
      error => raise cluster_error_at("assign", error)
    }
    ignore(
      job_store.assign(
        partition,
        worker_id,
        assignment.epoch(),
        assignment.start_offset(),
      ) catch {
        error => raise cluster_error_at("job assign", error)
      },
    )
    assignments[partition] = assignment
  }
  let grouped = cluster_event_groups(self, events)
  let deltas_by_partition : Map[Int, Array[@protocol.DeltaBatch]] = Map([])
  for source_partition in 0..<self.spec.partition_count() {
    guard grouped.get(source_partition) is Some(source_events) else { continue }
    guard assignments.get(source_partition) is Some(source_assignment) else {
      raise ApiError::Cluster("missing source assignment")
    }
    let records = source_events.map(event => event.record)
    let deltas = self.spec.route_batch(
      source_partition,
      source_assignment.epoch(),
      0,
      records,
    ) catch {
      error => raise ApiError::Plan(error)
    }
    for delta in deltas {
      match deltas_by_partition.get(delta.target_partition()) {
        Some(current) => current.push(delta)
        None => deltas_by_partition[delta.target_partition()] = [delta]
      }
    }
  }
  let works : Array[@runtime.SupervisorWork] = []
  for partition in 0..<self.spec.partition_count() {
    guard assignments.get(partition) is Some(assignment) else {
      raise ApiError::Cluster("missing local assignment")
    }
    let records = match grouped.get(partition) {
      None => []
      Some(values) => values.map(event => event.record)
    }
    let batches : Array[@protocol.EventBatch] = []
    if !records.is_empty() {
      batches.push(
        @protocol.EventBatch::new(
          partition,
          assignment.epoch(),
          assignment.start_offset(),
          records,
        ) catch {
          error => raise cluster_error_at("build batch", error)
        },
      )
    }
    let worker_id = assignment.worker_id()
    let deltas = deltas_by_partition.get(partition).unwrap_or([])
    works.push(
      @runtime.SupervisorWork::new(
        worker_id,
        self.definition.job_id + "-api-p" + partition.to_string(),
        partition,
        batches,
        deltas~,
        required_capability=descriptor.capability(),
      ),
    )
  }
  let output_manifest = match self.definition.sink {
    SinkSpec::Discard => None
    SinkSpec::Durable(_) =>
      Some(
        @runtime.JobOutputManifest::open(
          config.storage_dir + "/manifest.wal",
          self.definition.job_id,
          self.definition.operator.id(),
          self.spec.partition_count(),
        ) catch {
          error => raise cluster_error_at("open output manifest", error)
        },
      )
  }
  defer (match output_manifest {
    None => ()
    Some(manifest) => manifest.close()
  })
  let address = @socket.Addr::parse("127.0.0.1:0") catch {
    error => raise cluster_error_at("parse address", error)
  }
  let server = @socket.TcpServer(address, reuse_addr=true) catch {
    error => raise cluster_error_at("open tcp server", error)
  }
  defer server.close()
  let outputs : Array[Output] = []
  let supervisor_result : Result[@runtime.SupervisorRunSummary, String] = (@async_runtime.with_task_group <| group => {
    let supervisor : @async_runtime.Task[
      Result[@runtime.SupervisorRunSummary, String],
    ] = group.spawn(allow_failure=true) <| () => {
      let result : Result[@runtime.SupervisorRunSummary, String] = Ok(
        @runtime.run_durable_job_supervisor(
          server,
          coordinator,
          job_store,
          works,
          output_manifest?,
        ),
      ) catch {
        error => Err(@debug.to_string(error))
      }
      result
    }
    let worker_tasks : Array[
      @async_runtime.Task[Result[@runtime.WorkerRunSummary, String]],
    ] = []
    for partition in 0..<self.spec.partition_count() {
      guard assignments.get(partition) is Some(assignment) else { continue }
      guard incarnations.get(assignment.worker_id()) is Some(incarnation) else {
        continue
      }
      let connection = @socket.Tcp::connect(server.addr)
      let task = descriptor.create_task(
        self.definition.job_id +
        "/p" +
        partition.to_string() +
        "/e" +
        assignment.epoch().to_string(),
        partition,
        assignment.epoch(),
        assignment.start_offset(),
      ) catch {
        error => raise ApiError::TaskFailed(@debug.to_string(error))
      }
      let worker_id = assignment.worker_id()
      let worker_wal = config.storage_dir +
        "/worker-p" +
        partition.to_string() +
        ".wal"
      let delta_wal_path = config.storage_dir +
        "/delta-p" +
        partition.to_string() +
        ".wal"
      let output_log_path = match self.definition.sink {
        SinkSpec::Discard => None
        SinkSpec::Durable(_) =>
          Some(
            config.storage_dir + "/output-p" + partition.to_string() + ".wal",
          )
      }
      let worker_task : @async_runtime.Task[
        Result[@runtime.WorkerRunSummary, String],
      ] = group.spawn(allow_failure=true) <| () => {
        let handler : (@task.TaskOutput) -> Unit raise = output => {
          outputs.push(local_output(output))
        }
        let result : Result[@runtime.WorkerRunSummary, String] = Ok(
          @runtime.run_worker_task(
            connection,
            config.cluster_id,
            config.coordinator_id,
            worker_id,
            incarnation.value(),
            capabilities,
            worker_wal,
            task,
            handler,
            initial_credit_events=128L,
            initial_credit_bytes=4L * 1024L * 1024L,
            delta_wal_path~,
            output_log_path?,
          ),
        ) catch {
          error => Err(@debug.to_string(error))
        }
        result
      }
      worker_tasks.push(worker_task)
    }
    let supervisor_outcome : Result[@runtime.SupervisorRunSummary, String] = supervisor.wait() catch {
      error => Err(@debug.to_string(error))
    }
    for worker_task in worker_tasks {
      let _worker_outcome : Result[@runtime.WorkerRunSummary, String] = worker_task.wait() catch {
        error => Err(@debug.to_string(error))
      }
    }
    supervisor_outcome
  }) catch {
    error => Err(@debug.to_string(error))
  }
  let summary = match supervisor_result {
    Ok(value) => value
    Err(reason) => raise ApiError::Cluster(reason)
  }
  outputs.sort()
  {
    plan_digest: self.plan_digest,
    completed_partitions: summary.completed_workers(),
    failed_partitions: summary.failed_workers(),
    sent_batches: summary.sent_batches(),
    outputs,
    failures: summary
    .failures()
    .map(failure => {
      let partition = match failure.partition() {
        Some(value) => value.to_string()
        None => "?"
      }
      failure.worker_id() + "#" + partition + ":" + failure.reason()
    }),
  }
}
