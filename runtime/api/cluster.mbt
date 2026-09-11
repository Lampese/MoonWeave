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
/// Run a finite Job through real local Coordinator/Worker TCP sessions. When
/// the Job declares an external source, the supplied events are ignored. A
/// Kafka source contributes one bounded poll batch; a live runner is required
/// for continuous Kafka consumption.
pub async fn CompiledJob::run_local_cluster(
  self : CompiledJob,
  config : ClusterConfig,
  events : ArrayView[Event],
) -> ClusterRunSummary raise ApiError {
  let mut outcome : Result[ClusterRunSummary, ApiError]? = None
  (@async_runtime.with_task_group <| group => {
    outcome = Some(
      Ok(self.run_local_cluster_in_group(group, config, events)) catch {
        error => Err(error)
      },
    )
  }) catch {
    error =>
      raise ApiError::Cluster("local Kafka runtime: " + @debug.to_string(error))
  }
  match outcome {
    Some(Ok(summary)) => summary
    Some(Err(error)) => raise error
    None => raise ApiError::Cluster("local cluster runtime did not start")
  }
}

///|
async fn CompiledJob::run_local_cluster_in_group(
  self : CompiledJob,
  group : @async_runtime.TaskGroup[Unit],
  config : ClusterConfig,
  events : ArrayView[Event],
) -> ClusterRunSummary raise ApiError {
  let kafka_runtime = config.kafka_runtime.unwrap_or(KafkaRuntimeConfig::new())
  let external_source : JsonLinesSource? = match self.definition.source {
    SourceSpec::Events => None
    SourceSpec::JsonLines(path) => Some(JsonLinesSource::open(path))
    SourceSpec::Kafka(_) => None
  }
  let kafka_source : KafkaSource? = match self.definition.source {
    SourceSpec::Kafka(spec) =>
      Some(KafkaSource::connect(group~, kafka_runtime.source_config(spec)))
    _ => None
  }
  defer (match external_source {
    None => ()
    Some(source) => source.close()
  })
  defer (match kafka_source {
    None => ()
    Some(source) => source.close()
  })
  let kafka_sink : KafkaSink? = match self.definition.sink {
    SinkSpec::Kafka(spec) =>
      Some(KafkaSink::connect(group~, kafka_runtime.sink_config(spec)))
    _ => None
  }
  defer (match kafka_sink {
    None => ()
    Some(sink) => sink.close()
  })
  let (input_events, kafka_next_offset) : (Array[Event], Int64?) = match
    (external_source, kafka_source) {
    (None, None) => (events.to_owned(), None)
    (Some(source), None) => {
      let result : Array[Event] = []
      let mut finished = false
      while !finished {
        let before = source.polled_offset()
        let (batch, next_offset) = source.poll_with_offset(maximum_events=128)
        if next_offset == before {
          finished = true
        } else {
          for event in batch {
            result.push(event)
          }
        }
      }
      (result, None)
    }
    (None, Some(source)) => {
      let batch = source.poll()
      (batch.events().to_owned(), Some(batch.next_offset()))
    }
    _ => raise ApiError::Cluster("multiple external sources")
  }
  for event in input_events {
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
  let grouped = cluster_event_groups(self, input_events)
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
    SinkSpec::Durable(_) | SinkSpec::JsonLines(_) | SinkSpec::Kafka(_) =>
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
  let mut supervisor_summary : @runtime.SupervisorRunSummary? = None
  let mut supervisor_failure : String? = None
  let mut worker_failure : String? = None
  // Keep the finite Coordinator/Worker exchange in a nested group. Any
  // terminal session failure cancels peer sessions immediately; the captured
  // error is returned after structured cleanup completes.
  (@async_runtime.with_task_group <| execution_group => {
    execution_group.spawn_bg(allow_failure=true) <| () => {
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
      match result {
        Ok(summary) => supervisor_summary = Some(summary)
        Err(reason) => {
          supervisor_failure = Some(reason)
          execution_group.return_immediately(())
        }
      }
    }
    for partition in 0..<self.spec.partition_count() {
      guard assignments.get(partition) is Some(assignment) else { continue }
      guard incarnations.get(assignment.worker_id()) is Some(incarnation) else {
        continue
      }
      let connection = @socket.Tcp::connect(server.addr) catch {
        error => raise cluster_error_at("connect local Worker", error)
      }
      let task = descriptor.create_task(
        self.definition.job_id +
        "/p" +
        partition.to_string() +
        "/e" +
        assignment.epoch().to_string(),
        partition,
        assignment.epoch(),
        assignment.start_offset(),
        self.definition.max_state_events.unwrap_or(0),
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
        SinkSpec::Durable(_) | SinkSpec::JsonLines(_) | SinkSpec::Kafka(_) =>
          Some(
            config.storage_dir + "/output-p" + partition.to_string() + ".wal",
          )
      }
      execution_group.spawn_bg(allow_failure=true) <| () => {
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
            output_delivery?=match kafka_sink {
              Some(sink) =>
                Some(record => {
                  sink.write(record)
                  outputs.push(local_output(record.output()))
                })
              None => None
            },
            output_consumer_checkpoint_prefix?=kafka_sink.map(_ => {
              config.storage_dir + "/sink-kafka-p" + partition.to_string()
            }),
            output_consumer_id?=kafka_sink.map(sink => {
              self.definition.job_id + "-kafka-" + sink.topic()
            }),
          ),
        ) catch {
          error => Err(@debug.to_string(error))
        }
        match result {
          Ok(_) => ()
          Err(reason) => {
            if worker_failure is None {
              worker_failure = Some(reason)
            }
            execution_group.return_immediately(())
          }
        }
      }
    }
  }) catch {
    error =>
      raise ApiError::Cluster(
        "local execution runtime: " + @debug.to_string(error),
      )
  }
  match worker_failure {
    Some(reason) => raise ApiError::Cluster("local Worker failed: " + reason)
    None => ()
  }
  match supervisor_failure {
    Some(reason) => raise ApiError::Cluster(reason)
    None => ()
  }
  let summary = match supervisor_summary {
    Some(value) => value
    None => raise ApiError::Cluster("local supervisor did not finish")
  }
  match self.definition.sink {
    SinkSpec::JsonLines(path) => {
      let sink = @runtime.JsonLinesSink::open(path) catch {
        error => raise ApiError::Sink(@debug.to_string(error))
      }
      defer sink.close()
      for partition in 0..<self.spec.partition_count() {
        guard assignments.get(partition) is Some(assignment) else {
          raise ApiError::Cluster("missing output assignment")
        }
        let task_id = self.definition.job_id +
          "/p" +
          partition.to_string() +
          "/e" +
          assignment.epoch().to_string()
        let output_log = @runtime.TaskOutputLog::open(
          config.storage_dir + "/output-p" + partition.to_string() + ".wal",
        ) catch {
          error => raise ApiError::Sink(@debug.to_string(error))
        }
        let consumer = @runtime.TaskOutputConsumer::open(
          output_log,
          config.storage_dir + "/sink-p" + partition.to_string() + ".wal",
          self.definition.job_id + "-json-lines",
          task_id,
          partition,
          assignment.epoch(),
        ) catch {
          error => {
            output_log.close()
            raise ApiError::Sink(@debug.to_string(error))
          }
        }
        let mut done = false
        while !done {
          let count = sink.consume(consumer, maximum_records=128) catch {
            error => {
              consumer.close()
              output_log.close()
              raise ApiError::Sink(@debug.to_string(error))
            }
          }
          if count == 0 {
            done = true
          }
        }
        consumer.close()
        output_log.close()
      }
    }
    _ => ()
  }
  if summary.failed_workers() == 0 {
    match (external_source, kafka_source, kafka_next_offset) {
      (Some(source), _, _) => source.commit(source.polled_offset())
      (_, Some(source), Some(next_offset)) => source.commit(next_offset)
      _ => ()
    }
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
