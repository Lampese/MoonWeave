///|
let assignment_query_capability = "moonweave:assignment-query:v1"

///|
struct WorkerAllocation {
  job_id : String
  phase : JobRunPhase
  assignments : Array[@protocol.AssignmentSpec]
  submission : JobSubmission
  control_endpoint : NodeEndpoint
  targets : Array[(GraphEdge, JobSubmission)]
  parallelism : Int
  migration : WorkerMigrationRequest?
} derive(ToJson, FromJson)

///|
fn CoordinatorService::worker_withdrawn(
  self : CoordinatorService,
  child : DriverCoordinator,
) -> Bool {
  if child.rebalancing || child.store.is_paused() {
    return true
  }
  for graph in self.graphs.values() {
    if graph.stages
      .values()
      .any(stage => {
        stage.store.submission.map(value => value.submission_id()) ==
        child.store.submission.map(value => value.submission_id())
      }) {
      return graph.parent.store.is_paused()
    }
  }
  false
}

///|
async fn CoordinatorService::reply_worker_allocation(
  self : CoordinatorService,
  child : DriverCoordinator,
  connection : @socket.Tcp,
  first : @protocol.Frame,
  hello : @protocol.Hello,
) -> Unit {
  if hello.capabilities().contains(migration_result_capability) {
    self.receive_worker_migration_report(connection, first, hello)
  }
  let assignments = []
  if !self.worker_withdrawn(child) {
    match child.live_execution {
      Some(execution) =>
        for value in execution.coordinator.snapshot().assignments() {
          if value.worker_id() == hello.worker_id() &&
            value.incarnation == hello.incarnation() {
            assignments.push(
              @protocol.AssignmentSpec::new(
                value.partition(),
                value.epoch(),
                value.incarnation,
                start_offset=value.start_offset(),
              ),
            )
          }
        }
      None => ()
    }
  }
  let targets : Array[(GraphEdge, JobSubmission)] = []
  for graph in self.graphs.values() {
    for (id, job) in graph.job.graph_stages {
      if job.job_id() == hello.job_id().unwrap() {
        for edge in graph.job.definition.graph.unwrap().edges {
          if edge.from == id {
            targets.push(
              (edge, graph.stages[edge.to].store.submission().unwrap()),
            )
          }
        }
      }
    }
  }
  let allocation : WorkerAllocation = {
    job_id: hello.job_id().unwrap(),
    phase: child.store.phase(),
    assignments,
    submission: child.store.submission().unwrap(),
    control_endpoint: self.control_endpoint(),
    targets,
    parallelism: child.store.parallelism,
    migration: self.migrations
    .get((hello.job_id().unwrap(), hello.worker_id()))
    .bind(exchange => {
      if exchange.result is None {
        Some(exchange.request)
      } else {
        None
      }
    }),
  }
  let config = self.data_listener.config
  let outbound = @protocol.FrameBuilder::new(
    config.cluster_id,
    config.coordinator_id,
    hello.incarnation(),
    session_id=first.message_id(),
  )
  @runtime.write_frame(
    connection,
    outbound.build(
      @protocol.FrameKind::WorkerAssignments,
      @json.to_json(allocation).stringify(),
    ),
    maximum_frame_bytes=migration_frame_bytes,
  )
}

///|
async fn CompiledJob::worker_allocation(
  self : CompiledJob,
  config : WorkerNodeConfig,
  seed : Int,
  report? : WorkerMigrationReport,
) -> WorkerAllocation {
  let connection = connect_worker_coordinator(config, seed)
  defer connection.close()
  let hello = @protocol.Hello::new(
    config.worker_id,
    config.incarnation,
    if report is Some(_) {
      [assignment_query_capability, migration_result_capability]
    } else {
      [assignment_query_capability]
    },
    auth_token?=config.auth_token,
    job_id=self.job_id(),
  )
  let frame = @protocol.FrameBuilder::new(
    config.cluster_id,
    config.worker_id,
    config.incarnation,
  ).build(@protocol.FrameKind::Hello, hello.to_payload())
  @runtime.write_frame(connection, frame)
  if report is Some(report) {
    let outbound = @protocol.FrameBuilder::new(
      config.cluster_id,
      config.worker_id,
      config.incarnation,
      session_id=frame.message_id(),
    )
    @runtime.write_frame(
      connection,
      outbound.build(
        @protocol.FrameKind::WorkerMigrationResult,
        @json.to_json(report).stringify(),
      ),
      maximum_frame_bytes=migration_frame_bytes,
    )
  }
  let response = @runtime.read_frame(
    connection,
    maximum_frame_bytes=migration_frame_bytes,
  )
  if response.kind() != @protocol.FrameKind::WorkerAssignments ||
    response.cluster_id() != config.cluster_id ||
    response.sender_id() != config.coordinator_id ||
    response.worker_incarnation() != config.incarnation ||
    response.session_id() != frame.message_id() {
    raise ApiError::InvalidWorkerAssignment("invalid allocation response")
  }
  let allocation : WorkerAllocation = @json.from_json(
    @json.parse(response.payload()),
  )
  if allocation.job_id != self.job_id() {
    raise ApiError::InvalidWorkerAssignment("foreign allocation response")
  }
  if allocation.parallelism <= 0 ||
    allocation.parallelism > self.partition_count() {
    raise ApiError::InvalidWorkerAssignment("invalid allocation parallelism")
  }
  let seen : Set[Int] = Set([])
  for assignment in allocation.assignments {
    if !seen.add_and_check(assignment.partition()) ||
      assignment.partition() < 0 ||
      assignment.partition() >= self.partition_count() ||
      assignment.epoch() < 0 ||
      assignment.start_offset() < 0L ||
      assignment.incarnation() != config.incarnation {
      raise ApiError::InvalidWorkerAssignment("invalid allocation")
    }
  }
  allocation
}

///|
/// Discover current leases and run their tasks in this Worker process. A
/// rebalance withdraws allocations before state capture; the next allocation
/// starts only after the Coordinator publishes the installed generation.
pub async fn CompiledJob::run_worker_service(
  self : CompiledJob,
  config : WorkerNodeConfig,
) -> WorkerNodeSetSummary raise ApiError {
  self.run_worker_service_registered(config, self.worker_registry())
}

///|
fn CompiledJob::worker_registry(
  self : CompiledJob,
) -> OperatorRegistry raise ApiError {
  let registry = OperatorRegistry::builtins()
  if self.definition.transform is None &&
    self.descriptor.implementation is Some(implementation) {
    registry.register_custom(implementation)
  }
  for (_, stage) in self.graph_stages {
    if stage.definition.transform is None &&
      stage.descriptor.implementation is Some(implementation) {
      registry.register_custom(implementation)
    }
  }
  registry
}

///|
async fn CompiledJob::run_managed_partition(
  self : CompiledJob,
  config : WorkerNodeConfig,
  allocation : WorkerAllocation,
  registry : OperatorRegistry,
  lane : @runtime.ExecutionLane,
) -> WorkerNodeSummary raise ApiError {
  if allocation.targets.is_empty() {
    return self.run_worker_node(config, execution_lane=lane)
  }
  let sink = GraphOutputSink::{
    transformed: self.definition.transform is Some(_),
    targets: [],
  }
  defer sink.close()
  for (edge, submission) in allocation.targets {
    let driver_config = DriverConfig::new(
      config.cluster_id,
      config.coordinator_id,
      "edge/" +
      config.worker_id +
      "/" +
      edge.identity() +
      "/" +
      config.partition.to_string(),
      allocation.control_endpoint,
    )
    let client = DriverClient::connect(
      { ..driver_config, auth_token: config.auth_token, },
      registry~,
    )
    sink.targets.push({ edge, client, })
    ignore(client.submit(submission))
  }
  self.run_worker_node_with_sink(
    config,
    Some(WorkerOutputSink::Graph(sink)),
    execution_lane=lane,
  )
}

///|
async fn CompiledJob::run_worker_service_registered(
  self : CompiledJob,
  config : WorkerNodeConfig,
  registry : OperatorRegistry,
) -> WorkerNodeSetSummary raise ApiError {
  if self.is_graph() {
    raise ApiError::Cluster("graph jobs require run_graph_worker")
  }
  ensure_live_storage(config.storage_dir)
  (@async_runtime.with_task_group <| group => {
    let lanes : Map[Int, @runtime.ExecutionLane] = Map([])
    let tasks : Map[
      Int,
      (
        @protocol.AssignmentSpec,
        String,
        @async_runtime.Task[Result[WorkerNodeSummary, String]],
      ),
    ] = Map([])
    let finished : Set[(Int, Int)] = Set([])
    let mut completed_tasks = 0
    let mut failed_tasks = 0
    let mut replayed_batches = 0
    let mut live_batches = 0
    let mut failures = 0
    let mut seed = 0
    let mut migration_report : WorkerMigrationReport? = None
    for ;; {
      let allocation = @async_runtime.with_timeout(3000, () => {
        self.worker_allocation(config, seed, report?=migration_report)
      }) catch {
        error => {
          if @async_runtime.is_being_cancelled() {
            raise error
          }
          failures += 1
          if failures > 100 {
            raise error
          }
          seed += 1
          @async_runtime.sleep(100)
          continue
        }
      }
      failures = 0
      migration_report = None
      let current = allocation.submission.job().compile(registry~)
      let generation = @json.to_json(allocation.submission).stringify() +
        @json.to_json(allocation.targets).stringify() +
        @json.to_json(allocation.control_endpoint).stringify() +
        "/" +
        allocation.parallelism.to_string()
      // Withdraw old attempts before inspecting their result. A control
      // barrier deliberately closes their transport; that is not a job fault.
      for partition, (assignment, previous, task) in tasks.copy() {
        if previous != generation ||
          !allocation.assignments.contains(assignment) {
          task.cancel()
          ignore(task.wait() catch { _ => Err("allocation withdrawn") })
          tasks.remove(partition)
        }
      }
      for partition, (assignment, _, task) in tasks.copy() {
        match task.try_wait() {
          Some(Ok(summary)) => {
            finished.add((partition, assignment.epoch()))
            tasks.remove(partition)
            completed_tasks += 1
            replayed_batches += summary.replayed_batches()
            live_batches += summary.live_batches()
          }
          Some(Err(reason)) => {
            tasks.remove(partition)
            failed_tasks += 1
            if allocation.phase == Running &&
              allocation.assignments.contains(assignment) {
              raise ApiError::Cluster("Worker task failed: " + reason)
            }
          }
          None => ()
        }
      }
      if allocation.migration is Some(request) {
        if !allocation.assignments.is_empty() || !tasks.is_empty() {
          raise ApiError::InvalidWorkerAssignment(
            "migration requires withdrawn tasks",
          )
        }
        migration_report = Some(
          current.perform_worker_migration(config, request),
        )
        continue
      }
      if allocation.phase == Cancelled ||
        allocation.phase == Failed ||
        allocation.phase == Succeeded {
        for (_, _, task) in tasks.values() {
          task.cancel()
        }
        break
      }
      for assignment in allocation.assignments {
        let partition = assignment.partition()
        if !tasks.contains(partition) &&
          !finished.contains((partition, assignment.epoch())) {
          let config = { ..config, partition, assignment_known: false, }
          let lane_id = partition % allocation.parallelism
          let lane = match lanes.get(lane_id) {
            Some(lane) => lane
            None => {
              let lane = @runtime.ExecutionLane::new(lane_id)
              lanes[lane_id] = lane
              lane
            }
          }
          tasks[partition] = (
            assignment,
            generation,
            group.spawn(allow_failure=true) <| () => {
              Ok(
                current.run_managed_partition(
                  config, allocation, registry, lane,
                ),
              ) catch {
                error => Err(@debug.to_string(error))
              }
            },
          )
        }
      }
      @async_runtime.sleep(25)
    }
    {
      worker_id: config.worker_id,
      completed_tasks,
      failed_tasks,
      replayed_batches,
      live_batches,
    }
  }) catch {
    error => raise driver_error("Worker service", error)
  }
}
