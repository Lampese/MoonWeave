///|
/// A host and TCP port used by a Coordinator or Worker node. Port zero is
/// allowed only for a Coordinator bind endpoint so the OS can choose a free
/// port before a Worker connects.
pub(all) struct NodeEndpoint {
  host : String
  port : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn NodeEndpoint::new(
  host : String,
  port : Int,
) -> NodeEndpoint raise ApiError {
  if host.is_empty() {
    raise ApiError::InvalidEndpoint("host is empty")
  }
  if port < 0 || port > 65535 {
    raise ApiError::InvalidPort(port)
  }
  { host, port, }
}

///|
pub fn NodeEndpoint::host(self : NodeEndpoint) -> String {
  self.host
}

///|
pub fn NodeEndpoint::port(self : NodeEndpoint) -> Int {
  self.port
}

///|
pub fn NodeEndpoint::to_string(self : NodeEndpoint) -> String {
  if self.host.contains(":") && !self.host.has_prefix("[") {
    "[" + self.host + "]:" + self.port.to_string()
  } else {
    self.host + ":" + self.port.to_string()
  }
}

///|
fn NodeEndpoint::is_wildcard(self : NodeEndpoint) -> Bool {
  self.host == "0.0.0.0" || self.host == "::" || self.host == "[::]"
}

///|
fn NodeEndpoint::to_addr(self : NodeEndpoint) -> @socket.Addr raise ApiError {
  @socket.Addr::parse(self.to_string()) catch {
    error => raise ApiError::InvalidEndpoint(@debug.to_string(error))
  }
}

///|
fn NodeEndpoint::validate_connect(self : NodeEndpoint) -> Unit raise ApiError {
  if self.port == 0 {
    raise ApiError::InvalidEndpoint("a Worker cannot connect to port zero")
  }
}

///|
/// A Coordinator-side Worker identity. The incarnation must match the value
/// supplied by the corresponding WorkerNodeConfig.
pub(all) struct WorkerSlot {
  worker_id : String
  incarnation : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn WorkerSlot::new(
  worker_id : String,
  incarnation : String,
) -> WorkerSlot raise ApiError {
  if worker_id.is_empty() {
    raise ApiError::EmptyWorkerId
  }
  ignore(
    @protocol.Incarnation::new(incarnation) catch {
      _ => raise ApiError::Cluster("worker incarnation is empty")
    },
  )
  { worker_id, incarnation, }
}

///|
pub fn WorkerSlot::worker_id(self : WorkerSlot) -> String {
  self.worker_id
}

///|
pub fn WorkerSlot::incarnation(self : WorkerSlot) -> String {
  self.incarnation
}

///|
/// Configuration kept by the Coordinator process. Storage is local to the
/// Coordinator host; Worker storage is configured independently below.
pub struct CoordinatorNodeConfig {
  cluster_id : String
  coordinator_id : String
  bind : NodeEndpoint
  advertised : NodeEndpoint?
  storage_dir : String
  workers : Array[WorkerSlot]
} derive(Debug, Eq)

///|
pub fn CoordinatorNodeConfig::new(
  cluster_id : String,
  coordinator_id : String,
  bind : NodeEndpoint,
  storage_dir : String,
  workers : ArrayView[WorkerSlot],
) -> CoordinatorNodeConfig raise ApiError {
  if cluster_id.is_empty() || coordinator_id.is_empty() {
    raise ApiError::Cluster("cluster and coordinator ids are required")
  }
  if storage_dir.is_empty() {
    raise ApiError::Cluster("Coordinator storage directory is required")
  }
  if workers.is_empty() {
    raise ApiError::EmptyWorkerList
  }
  let seen : Set[String] = Set([])
  let copied : Array[WorkerSlot] = []
  for worker in workers {
    if !seen.add_and_check(worker.worker_id()) {
      raise ApiError::DuplicateWorker(worker.worker_id())
    }
    copied.push(worker)
  }
  copied.sort_by((left, right) => left.worker_id().compare(right.worker_id()))
  {
    cluster_id,
    coordinator_id,
    bind,
    advertised: None,
    storage_dir,
    workers: copied,
  }
}

///|
pub fn CoordinatorNodeConfig::cluster_id(
  self : CoordinatorNodeConfig,
) -> String {
  self.cluster_id
}

///|
pub fn CoordinatorNodeConfig::coordinator_id(
  self : CoordinatorNodeConfig,
) -> String {
  self.coordinator_id
}

///|
pub fn CoordinatorNodeConfig::bind(
  self : CoordinatorNodeConfig,
) -> NodeEndpoint {
  self.bind
}

///|
/// Set the endpoint that remote Workers should use. Port zero reuses the
/// actual port selected by the operating system.
pub fn CoordinatorNodeConfig::with_advertised_endpoint(
  self : CoordinatorNodeConfig,
  endpoint : NodeEndpoint,
) -> CoordinatorNodeConfig {
  { ..self, advertised: Some(endpoint), }
}

///|
pub fn CoordinatorNodeConfig::advertised_endpoint(
  self : CoordinatorNodeConfig,
) -> NodeEndpoint? {
  self.advertised
}

///|
pub fn CoordinatorNodeConfig::storage_dir(
  self : CoordinatorNodeConfig,
) -> String {
  self.storage_dir
}

///|
pub fn CoordinatorNodeConfig::workers(
  self : CoordinatorNodeConfig,
) -> ArrayView[WorkerSlot] {
  self.workers[:]
}

///|
/// Configuration kept by one Worker process. `partition` is a static
/// assignment hint for this first multi-machine runner; the Coordinator still
/// fences it with the assignment epoch during the handshake.
pub struct WorkerNodeConfig {
  cluster_id : String
  coordinator_id : String
  coordinator : NodeEndpoint
  worker_id : String
  incarnation : String
  storage_dir : String
  partition : Int
  epoch : Int
  start_offset : Int64
  assignment_known : Bool
} derive(Debug, Eq)

///|
pub fn WorkerNodeConfig::new(
  cluster_id : String,
  coordinator_id : String,
  coordinator : NodeEndpoint,
  worker_id : String,
  incarnation : String,
  storage_dir : String,
  partition : Int,
) -> WorkerNodeConfig raise ApiError {
  if cluster_id.is_empty() || coordinator_id.is_empty() {
    raise ApiError::Cluster("cluster and coordinator ids are required")
  }
  coordinator.validate_connect()
  if worker_id.is_empty() {
    raise ApiError::EmptyWorkerId
  }
  ignore(
    @protocol.Incarnation::new(incarnation) catch {
      _ => raise ApiError::Cluster("worker incarnation is empty")
    },
  )
  if storage_dir.is_empty() {
    raise ApiError::Cluster("Worker storage directory is required")
  }
  if partition < 0 {
    raise ApiError::InvalidPartitionCount(partition)
  }
  {
    cluster_id,
    coordinator_id,
    coordinator,
    worker_id,
    incarnation,
    storage_dir,
    partition,
    epoch: 0,
    start_offset: 0L,
    assignment_known: true,
  }
}

///|
/// Let the compiled Job derive the partition owned by this Worker. This is
/// valid when the topology assigns exactly one partition to the Worker; jobs
/// with multiple partitions per Worker must use the TaskSet API explicitly.
pub fn WorkerNodeConfig::new_auto(
  cluster_id : String,
  coordinator_id : String,
  coordinator : NodeEndpoint,
  worker_id : String,
  incarnation : String,
  storage_dir : String,
) -> WorkerNodeConfig raise ApiError {
  let config = WorkerNodeConfig::new(
    cluster_id, coordinator_id, coordinator, worker_id, incarnation, storage_dir,
    0,
  )
  { ..config, partition: -1, assignment_known: false, }
}

///|
/// Dynamic assignment with a known partition hint. The Coordinator still
/// supplies the authoritative epoch and start offset during the handshake.
pub fn WorkerNodeConfig::new_auto_for_partition(
  cluster_id : String,
  coordinator_id : String,
  coordinator : NodeEndpoint,
  worker_id : String,
  incarnation : String,
  storage_dir : String,
  partition : Int,
) -> WorkerNodeConfig raise ApiError {
  let config = WorkerNodeConfig::new_auto(
    cluster_id, coordinator_id, coordinator, worker_id, incarnation, storage_dir,
  )
  if partition < 0 {
    raise ApiError::InvalidPartitionCount(partition)
  }
  { ..config, partition, }
}

///|
pub fn WorkerNodeConfig::with_assignment(
  self : WorkerNodeConfig,
  epoch : Int,
  start_offset : Int64,
) -> WorkerNodeConfig raise ApiError {
  if epoch < 0 {
    raise ApiError::Cluster("worker epoch cannot be negative")
  }
  if start_offset < 0L {
    raise ApiError::Cluster("worker start offset cannot be negative")
  }
  { ..self, epoch, start_offset, assignment_known: true, }
}

///|
pub fn WorkerNodeConfig::cluster_id(self : WorkerNodeConfig) -> String {
  self.cluster_id
}

///|
pub fn WorkerNodeConfig::coordinator_id(self : WorkerNodeConfig) -> String {
  self.coordinator_id
}

///|
pub fn WorkerNodeConfig::coordinator(self : WorkerNodeConfig) -> NodeEndpoint {
  self.coordinator
}

///|
pub fn WorkerNodeConfig::worker_id(self : WorkerNodeConfig) -> String {
  self.worker_id
}

///|
pub fn WorkerNodeConfig::incarnation(self : WorkerNodeConfig) -> String {
  self.incarnation
}

///|
pub fn WorkerNodeConfig::storage_dir(self : WorkerNodeConfig) -> String {
  self.storage_dir
}

///|
pub fn WorkerNodeConfig::partition(self : WorkerNodeConfig) -> Int {
  self.partition
}

///|
pub fn WorkerNodeConfig::epoch(self : WorkerNodeConfig) -> Int {
  self.epoch
}

///|
pub fn WorkerNodeConfig::start_offset(self : WorkerNodeConfig) -> Int64 {
  self.start_offset
}

///|
/// A Coordinator listener bound to a concrete endpoint. The listener can be
/// started before a Worker process is launched, which makes port zero useful
/// for tests and supervised local deployments.
pub struct CoordinatorListener {
  config : CoordinatorNodeConfig
  server : @socket.TcpServer
}

///|
pub enum CoordinatorNode {
  Instance
}

///|
pub fn CoordinatorNode::new() -> CoordinatorNode {
  CoordinatorNode::Instance
}

///|
pub async fn CoordinatorNode::bind(
  config : CoordinatorNodeConfig,
) -> CoordinatorListener raise ApiError {
  if config.bind.is_wildcard() && config.advertised is None {
    raise ApiError::InvalidEndpoint(
      "wildcard Coordinator bind requires an advertised endpoint",
    )
  }
  let server = @socket.TcpServer(config.bind.to_addr(), reuse_addr=true) catch {
    error =>
      raise ApiError::Cluster("bind Coordinator: " + @debug.to_string(error))
  }
  { config, server, }
}

///|
pub fn CoordinatorListener::close(self : CoordinatorListener) -> Unit {
  self.server.close()
}

///|
pub fn CoordinatorListener::endpoint(
  self : CoordinatorListener,
) -> NodeEndpoint raise ApiError {
  let actual_port = self.server.addr.port()
  match self.config.advertised {
    Some(endpoint) =>
      NodeEndpoint::new(
        endpoint.host(),
        if endpoint.port() == 0 {
          actual_port
        } else {
          endpoint.port()
        },
      )
    None => NodeEndpoint::new(self.config.bind.host(), actual_port)
  }
}

///|
pub(all) struct CoordinatorNodeSummary {
  endpoint : NodeEndpoint
  plan_digest : String
  worker_count : Int
  completed_partitions : Int
  failed_partitions : Int
  sent_batches : Int
  failures : Array[String]
} derive(Debug, Eq)

///|
pub fn CoordinatorNodeSummary::endpoint(
  self : CoordinatorNodeSummary,
) -> NodeEndpoint {
  self.endpoint
}

///|
pub fn CoordinatorNodeSummary::plan_digest(
  self : CoordinatorNodeSummary,
) -> String {
  self.plan_digest
}

///|
pub fn CoordinatorNodeSummary::worker_count(
  self : CoordinatorNodeSummary,
) -> Int {
  self.worker_count
}

///|
pub fn CoordinatorNodeSummary::completed_partitions(
  self : CoordinatorNodeSummary,
) -> Int {
  self.completed_partitions
}

///|
pub fn CoordinatorNodeSummary::failed_partitions(
  self : CoordinatorNodeSummary,
) -> Int {
  self.failed_partitions
}

///|
pub fn CoordinatorNodeSummary::sent_batches(
  self : CoordinatorNodeSummary,
) -> Int {
  self.sent_batches
}

///|
pub fn CoordinatorNodeSummary::failures(
  self : CoordinatorNodeSummary,
) -> ArrayView[String] {
  self.failures[:]
}

///|
pub(all) struct WorkerNodeSummary {
  worker_id : String
  partition : Int
  replayed_batches : Int
  replayed_deltas : Int
  live_batches : Int
  persisted_offset : Int64
  applied_offset : Int64
} derive(Debug, Eq)

///|
pub fn WorkerNodeSummary::worker_id(self : WorkerNodeSummary) -> String {
  self.worker_id
}

///|
pub fn WorkerNodeSummary::partition(self : WorkerNodeSummary) -> Int {
  self.partition
}

///|
pub fn WorkerNodeSummary::replayed_batches(self : WorkerNodeSummary) -> Int {
  self.replayed_batches
}

///|
pub fn WorkerNodeSummary::replayed_deltas(self : WorkerNodeSummary) -> Int {
  self.replayed_deltas
}

///|
pub fn WorkerNodeSummary::live_batches(self : WorkerNodeSummary) -> Int {
  self.live_batches
}

///|
pub fn WorkerNodeSummary::persisted_offset(self : WorkerNodeSummary) -> Int64 {
  self.persisted_offset
}

///|
pub fn WorkerNodeSummary::applied_offset(self : WorkerNodeSummary) -> Int64 {
  self.applied_offset
}

///|
fn validate_coordinator_workers(
  job : CompiledJob,
  config : CoordinatorNodeConfig,
) -> Unit raise ApiError {
  let expected = job.definition.workers.copy()
  expected.sort()
  let actual = config.workers.map(worker => worker.worker_id())
  actual.sort()
  if expected != actual {
    raise ApiError::InvalidWorkerAssignment(
      "Coordinator workers do not match the compiled Job workers",
    )
  }
}

///|
fn coordinator_failures(
  failures : ArrayView[@runtime.SupervisorFailure],
) -> Array[String] {
  failures.map(failure => {
    let partition = match failure.partition() {
      Some(value) => value.to_string()
      None => "?"
    }
    failure.worker_id() + "#" + partition + ":" + failure.reason()
  })
}

///|
fn build_node_works(
  job : CompiledJob,
  events : ArrayView[Event],
  assignments : Map[Int, @control.PartitionAssignment],
) -> Array[@runtime.SupervisorWork] raise ApiError {
  let grouped = cluster_event_groups(job, events)
  let deltas_by_partition : Map[Int, Array[@protocol.DeltaBatch]] = Map([])
  for source_partition in 0..<job.spec.partition_count() {
    guard grouped.get(source_partition) is Some(source_events) else { continue }
    guard assignments.get(source_partition) is Some(source_assignment) else {
      raise ApiError::Cluster("missing source assignment")
    }
    let deltas = job.spec.route_batch(
      source_partition,
      source_assignment.epoch(),
      0,
      source_events.map(event => event.record),
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
  for partition in 0..<job.spec.partition_count() {
    guard assignments.get(partition) is Some(assignment) else {
      raise ApiError::Cluster("missing partition assignment")
    }
    let records = match grouped.get(partition) {
      Some(values) => values.map(event => event.record)
      None => []
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
          error => raise ApiError::InvalidEvent(@debug.to_string(error))
        },
      )
    }
    works.push(
      @runtime.SupervisorWork::new(
        assignment.worker_id(),
        job.definition.job_id + "-node-p" + partition.to_string(),
        partition,
        batches,
        deltas=deltas_by_partition.get(partition).unwrap_or([]),
        required_capability=job.descriptor.capability(),
      ),
    )
  }
  works
}

///|
fn replacement_for_failed_partition(
  config : CoordinatorNodeConfig,
  assignments : Map[Int, @control.PartitionAssignment],
  partition : Int,
) -> String? {
  guard assignments.get(partition) is Some(failed) else { return None }
  let owned_count = assignments
    .values()
    .filter(value => value.worker_id() == failed.worker_id())
    .length()
  if owned_count != 1 {
    return None
  }
  for worker in config.workers {
    if worker.worker_id() != failed.worker_id() {
      return Some(worker.worker_id())
    }
  }
  None
}

///|
async fn recover_node_partition(
  coordinator : @runtime.CoordinatorStore,
  job_store : @runtime.JobStore,
  config : CoordinatorNodeConfig,
  assignments : Map[Int, @control.PartitionAssignment],
  partition : Int,
) -> @control.PartitionAssignment raise ApiError {
  let old = match assignments.get(partition) {
    Some(value) => value
    None => raise ApiError::InvalidWorkerAssignment(partition.to_string())
  }
  let replacement = match
    replacement_for_failed_partition(config, assignments, partition) {
    Some(value) => value
    None =>
      raise ApiError::InvalidWorkerAssignment(
        "automatic recovery requires one partition per failed Worker and a replacement",
      )
  }
  let moved = coordinator.fence_and_reassign(
    old.worker_id(),
    old.incarnation(),
    replacement,
  ) catch {
    error =>
      raise ApiError::Cluster(
        "fence/reassign Worker: " + @debug.to_string(error),
      )
  }
  guard moved.length() == 1 && moved[0].partition() == partition else {
    raise ApiError::InvalidWorkerAssignment(
      "recovery moved an unexpected lease",
    )
  }
  ignore(
    job_store.reassign(
      partition,
      replacement,
      moved[0].epoch(),
      moved[0].start_offset(),
    ) catch {
      error =>
        raise ApiError::Cluster(
          "persist Worker reassign: " + @debug.to_string(error),
        )
    },
  )
  assignments[partition] = moved[0]
  moved[0]
}

///|
/// Run a Job on a Coordinator node. Input records stay on the Coordinator
/// host; Workers connect to the bound endpoint from their own machines.
pub async fn CoordinatorListener::run(
  self : CoordinatorListener,
  job : CompiledJob,
  events : ArrayView[Event],
) -> CoordinatorNodeSummary raise ApiError {
  validate_coordinator_workers(job, self.config)
  for event in events {
    job.descriptor.validate_event(event)
  }
  let endpoint = self.endpoint()
  defer self.server.close()
  let storage_exists = @fs.exists(self.config.storage_dir) catch {
    error =>
      raise ApiError::Cluster(
        "check Coordinator storage: " + @debug.to_string(error),
      )
  }
  if !storage_exists {
    @fs.mkdir(self.config.storage_dir, recursive=true) catch {
      error =>
        raise ApiError::Cluster(
          "mkdir Coordinator storage: " + @debug.to_string(error),
        )
    }
  }
  let coordinator = @runtime.CoordinatorStore::open(
    self.config.cluster_id,
    self.config.coordinator_id,
    self.config.storage_dir + "/coordinator.wal",
    checkpoint_path=self.config.storage_dir + "/coordinator.json",
  ) catch {
    error =>
      raise ApiError::Cluster(
        "open Coordinator store: " + @debug.to_string(error),
      )
  }
  defer coordinator.close()
  let job_store = @runtime.JobStore::open(
    job.spec,
    self.config.storage_dir + "/job.wal",
    checkpoint_path=self.config.storage_dir + "/job.json",
  ) catch {
    error =>
      raise ApiError::Cluster("open Job store: " + @debug.to_string(error))
  }
  defer job_store.close()
  let descriptor = job.descriptor
  let capabilities = [
    job.definition.operator.id(),
    descriptor.capability(),
    "causal-frontier",
    descriptor.code_digest(),
  ]
  let assignments : Map[Int, @control.PartitionAssignment] = Map([])
  for worker in self.config.workers {
    let incarnation = @protocol.Incarnation::new(worker.incarnation()) catch {
      error =>
        raise ApiError::Cluster(
          "invalid Worker incarnation: " + @debug.to_string(error),
        )
    }
    ignore(
      coordinator.register_worker(worker.worker_id(), incarnation, capabilities) catch {
        error =>
          raise ApiError::Cluster("register Worker: " + @debug.to_string(error))
      },
    )
  }
  for partition in 0..<job.spec.partition_count() {
    let worker_id = job.spec.owner(partition) catch {
      error =>
        raise ApiError::Cluster(
          "resolve partition owner: " + @debug.to_string(error),
        )
    }
    let assignment = coordinator.assign(partition, worker_id) catch {
      error =>
        raise ApiError::Cluster("assign partition: " + @debug.to_string(error))
    }
    ignore(
      job_store.assign(
        partition,
        worker_id,
        assignment.epoch(),
        assignment.start_offset(),
      ) catch {
        error =>
          raise ApiError::Cluster(
            "persist partition assignment: " + @debug.to_string(error),
          )
      },
    )
    assignments[partition] = assignment
  }
  let works = build_node_works(job, events, assignments)
  let output_manifest = match job.definition.sink {
    SinkSpec::Discard => None
    SinkSpec::Durable(_) =>
      Some(
        @runtime.JobOutputManifest::open(
          self.config.storage_dir + "/manifest.wal",
          job.definition.job_id,
          job.definition.operator.id(),
          job.spec.partition_count(),
        ) catch {
          error =>
            raise ApiError::Cluster(
              "open output manifest: " + @debug.to_string(error),
            )
        },
      )
  }
  defer (match output_manifest {
    None => ()
    Some(manifest) => manifest.close()
  })
  let mut summary = @runtime.run_durable_job_supervisor(
    self.server,
    coordinator,
    job_store,
    works,
    output_manifest?,
  ) catch {
    error =>
      raise ApiError::Cluster(
        "run Coordinator supervisor: " + @debug.to_string(error),
      )
  }
  if summary.failed_workers() == 1 {
    guard summary.failures()[0].partition() is Some(failed_partition) else {
      raise ApiError::Cluster("failed Worker did not identify a partition")
    }
    if replacement_for_failed_partition(
        self.config,
        assignments,
        failed_partition,
      )
      is Some(_) {
      ignore(
        recover_node_partition(
          coordinator,
          job_store,
          self.config,
          assignments,
          failed_partition,
        ),
      )
      let retry_works = build_node_works(job, events, assignments)
      summary = @runtime.run_durable_job_supervisor(
        self.server,
        coordinator,
        job_store,
        retry_works,
        output_manifest?,
      ) catch {
        error =>
          raise ApiError::Cluster(
            "run recovered Coordinator supervisor: " + @debug.to_string(error),
          )
      }
    }
  }
  {
    endpoint,
    plan_digest: job.plan_digest,
    worker_count: self.config.workers.length(),
    completed_partitions: summary.completed_workers(),
    failed_partitions: summary.failed_workers(),
    sent_batches: summary.sent_batches(),
    failures: coordinator_failures(summary.failures()),
  }
}

///|
pub async fn CompiledJob::run_coordinator_node(
  self : CompiledJob,
  config : CoordinatorNodeConfig,
  events : ArrayView[Event],
) -> CoordinatorNodeSummary raise ApiError {
  let listener = CoordinatorNode::bind(config)
  listener.run(self, events)
}

///|
/// Run one Worker process against a remote Coordinator. The operator is a
/// registered implementation already compiled into this Worker binary.
pub async fn CompiledJob::run_worker_node(
  self : CompiledJob,
  config : WorkerNodeConfig,
) -> WorkerNodeSummary raise ApiError {
  let partition = if config.partition < 0 {
    let owned : Array[Int] = []
    for candidate in 0..<self.spec.partition_count() {
      let worker = self.spec.owner(candidate) catch {
        error => raise ApiError::Plan(error)
      }
      if worker == config.worker_id {
        owned.push(candidate)
      }
    }
    if owned.length() != 1 {
      raise ApiError::InvalidWorkerAssignment(
        "automatic assignment requires exactly one owned partition",
      )
    }
    owned[0]
  } else {
    config.partition
  }
  if partition < 0 || partition >= self.spec.partition_count() {
    raise ApiError::InvalidWorkerAssignment(config.partition.to_string())
  }
  if !self.definition.workers.any(worker => worker == config.worker_id) {
    raise ApiError::InvalidWorkerAssignment(config.worker_id)
  }
  let storage_exists = @fs.exists(config.storage_dir) catch {
    error =>
      raise ApiError::Cluster(
        "check Worker storage: " + @debug.to_string(error),
      )
  }
  if !storage_exists {
    @fs.mkdir(config.storage_dir, recursive=true) catch {
      error =>
        raise ApiError::Cluster(
          "mkdir Worker storage: " + @debug.to_string(error),
        )
    }
  }
  let connection = @socket.Tcp::connect_to_host(
    config.coordinator.host,
    port=config.coordinator.port,
  ) catch {
    error =>
      raise ApiError::Cluster("connect Coordinator: " + @debug.to_string(error))
  }
  let task = self.descriptor.create_task(
    self.definition.job_id + "/dynamic/" + config.worker_id,
    partition,
    config.epoch,
    config.start_offset,
  )
  let capabilities = [
    self.definition.operator.id(),
    self.descriptor.capability(),
    "causal-frontier",
    self.descriptor.code_digest(),
  ]
  let output_log_path = match self.definition.sink {
    SinkSpec::Discard => None
    SinkSpec::Durable(_) =>
      Some(config.storage_dir + "/output-p" + partition.to_string() + ".wal")
  }
  let summary = @runtime.run_worker_task(
    connection,
    config.cluster_id,
    config.coordinator_id,
    config.worker_id,
    config.incarnation,
    capabilities,
    config.storage_dir + "/worker-p" + partition.to_string() + ".wal",
    task,
    _ => (),
    checkpoint_path=config.storage_dir +
      "/worker-p" +
      partition.to_string() +
      ".json",
    delta_wal_path=config.storage_dir +
      "/delta-p" +
      partition.to_string() +
      ".wal",
    output_log_path?,
    dynamic_assignment=!config.assignment_known,
  ) catch {
    error =>
      raise ApiError::Cluster("run Worker task: " + @debug.to_string(error))
  }
  {
    worker_id: config.worker_id,
    partition,
    replayed_batches: summary.replayed_batches(),
    replayed_deltas: summary.replayed_deltas(),
    live_batches: summary.live_batches(),
    persisted_offset: summary.persisted_offset(),
    applied_offset: summary.applied_offset(),
  }
}
