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
  storage_dir : String?
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
  { worker_id, incarnation, storage_dir: None, }
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
/// Attach the Worker-local state directory used by automatic cross-epoch
/// recovery. The directory must be reachable by the Coordinator process.
pub fn WorkerSlot::with_storage_dir(
  self : WorkerSlot,
  storage_dir : String,
) -> WorkerSlot raise ApiError {
  if storage_dir.is_empty() {
    raise ApiError::Cluster("Worker storage directory is required")
  }
  { ..self, storage_dir: Some(storage_dir), }
}

///|
pub fn WorkerSlot::storage_dir(self : WorkerSlot) -> String? {
  self.storage_dir
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
  migration_required : Bool
  auth_token : String?
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
    migration_required: false,
    auth_token: None,
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
/// Require automatic recovery to transfer durable Worker state. This is
/// intended for deployments that configure shared Worker storage directories.
pub fn CoordinatorNodeConfig::with_migration_required(
  self : CoordinatorNodeConfig,
  required? : Bool = true,
) -> CoordinatorNodeConfig {
  { ..self, migration_required: required, }
}

///|
pub fn CoordinatorNodeConfig::migration_required(
  self : CoordinatorNodeConfig,
) -> Bool {
  self.migration_required
}

///|
/// Configure the shared secret required on every Worker handshake.
pub fn CoordinatorNodeConfig::with_auth_token(
  self : CoordinatorNodeConfig,
  token : String,
) -> CoordinatorNodeConfig raise ApiError {
  if token.is_empty() {
    raise ApiError::Cluster("Coordinator authentication token is required")
  }
  { ..self, auth_token: Some(token), }
}

///|
pub fn CoordinatorNodeConfig::auth_token(
  self : CoordinatorNodeConfig,
) -> String? {
  self.auth_token
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
  auth_token : String?
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
    auth_token: None,
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
/// Configure the shared secret used to authenticate this Worker connection.
pub fn WorkerNodeConfig::with_auth_token(
  self : WorkerNodeConfig,
  token : String,
) -> WorkerNodeConfig raise ApiError {
  if token.is_empty() {
    raise ApiError::Cluster("Worker authentication token is required")
  }
  { ..self, auth_token: Some(token), }
}

///|
pub fn WorkerNodeConfig::auth_token(self : WorkerNodeConfig) -> String? {
  self.auth_token
}

///|
/// Identity and storage configuration for one Worker process that hosts every
/// partition assigned to it by the compiled Job. Per-partition Task sessions
/// are created by `run_worker_node_set`.
pub struct WorkerNodeSetConfig {
  cluster_id : String
  coordinator_id : String
  coordinator : NodeEndpoint
  worker_id : String
  incarnation : String
  storage_dir : String
  partitions : Array[Int]?
  auth_token : String?
} derive(Debug, Eq)

///|
pub fn WorkerNodeSetConfig::new(
  cluster_id : String,
  coordinator_id : String,
  coordinator : NodeEndpoint,
  worker_id : String,
  incarnation : String,
  storage_dir : String,
) -> WorkerNodeSetConfig raise ApiError {
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
  {
    cluster_id,
    coordinator_id,
    coordinator,
    worker_id,
    incarnation,
    storage_dir,
    partitions: None,
    auth_token: None,
  }
}

///|
/// Override static JobSpec placement when this Worker is a recovery target.
/// Each listed partition still receives its authoritative epoch/start offset
/// from the Coordinator during the handshake.
pub fn WorkerNodeSetConfig::with_partitions(
  self : WorkerNodeSetConfig,
  partitions : ArrayView[Int],
) -> WorkerNodeSetConfig raise ApiError {
  if partitions.is_empty() {
    raise ApiError::InvalidWorkerAssignment("partition list is empty")
  }
  let seen : Set[Int] = Set([])
  let copied : Array[Int] = []
  for partition in partitions {
    if partition < 0 || !seen.add_and_check(partition) {
      raise ApiError::InvalidWorkerAssignment(partition.to_string())
    }
    copied.push(partition)
  }
  copied.sort()
  { ..self, partitions: Some(copied), }
}

///|
/// Return the explicit partition hint, or an empty view when static placement
/// from the compiled JobSpec is used.
pub fn WorkerNodeSetConfig::partitions(
  self : WorkerNodeSetConfig,
) -> ArrayView[Int] {
  match self.partitions {
    Some(value) => value[:]
    None => []
  }
}

///|
pub fn WorkerNodeSetConfig::with_auth_token(
  self : WorkerNodeSetConfig,
  token : String,
) -> WorkerNodeSetConfig raise ApiError {
  if token.is_empty() {
    raise ApiError::Cluster("Worker authentication token is required")
  }
  { ..self, auth_token: Some(token), }
}

///|
pub fn WorkerNodeSetConfig::auth_token(self : WorkerNodeSetConfig) -> String? {
  self.auth_token
}

///|
pub fn WorkerNodeSetConfig::cluster_id(self : WorkerNodeSetConfig) -> String {
  self.cluster_id
}

///|
pub fn WorkerNodeSetConfig::coordinator_id(
  self : WorkerNodeSetConfig,
) -> String {
  self.coordinator_id
}

///|
pub fn WorkerNodeSetConfig::coordinator(
  self : WorkerNodeSetConfig,
) -> NodeEndpoint {
  self.coordinator
}

///|
pub fn WorkerNodeSetConfig::worker_id(self : WorkerNodeSetConfig) -> String {
  self.worker_id
}

///|
pub fn WorkerNodeSetConfig::incarnation(self : WorkerNodeSetConfig) -> String {
  self.incarnation
}

///|
pub fn WorkerNodeSetConfig::storage_dir(self : WorkerNodeSetConfig) -> String {
  self.storage_dir
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
  state_events : Int
  pending_events : Int
  state_schema : String
} derive(Debug, Eq)

///|
pub fn WorkerNodeSummary::worker_id(self : WorkerNodeSummary) -> String {
  self.worker_id
}

///|
pub(all) struct WorkerNodeSetSummary {
  worker_id : String
  completed_tasks : Int
  failed_tasks : Int
  replayed_batches : Int
  live_batches : Int
} derive(Debug, Eq)

///|
pub fn WorkerNodeSetSummary::worker_id(self : WorkerNodeSetSummary) -> String {
  self.worker_id
}

///|
pub fn WorkerNodeSetSummary::completed_tasks(
  self : WorkerNodeSetSummary,
) -> Int {
  self.completed_tasks
}

///|
pub fn WorkerNodeSetSummary::failed_tasks(self : WorkerNodeSetSummary) -> Int {
  self.failed_tasks
}

///|
pub fn WorkerNodeSetSummary::replayed_batches(
  self : WorkerNodeSetSummary,
) -> Int {
  self.replayed_batches
}

///|
pub fn WorkerNodeSetSummary::live_batches(self : WorkerNodeSetSummary) -> Int {
  self.live_batches
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
pub fn WorkerNodeSummary::state_events(self : WorkerNodeSummary) -> Int {
  self.state_events
}

///|
pub fn WorkerNodeSummary::pending_events(self : WorkerNodeSummary) -> Int {
  self.pending_events
}

///|
pub fn WorkerNodeSummary::state_schema(self : WorkerNodeSummary) -> String {
  self.state_schema
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
fn replacement_for_failed_worker(
  config : CoordinatorNodeConfig,
  worker_id : String,
) -> String? {
  for worker in config.workers {
    if worker.worker_id() != worker_id {
      return Some(worker.worker_id())
    }
  }
  None
}

///|
fn failed_worker_partitions(
  summary : @runtime.SupervisorRunSummary,
  assignments : Map[Int, @control.PartitionAssignment],
) -> (Array[String], Map[String, Array[Int]]) raise ApiError {
  let workers : Array[String] = []
  let partitions : Map[String, Array[Int]] = Map([])
  for failure in summary.failures() {
    let worker_id = failure.worker_id()
    guard !worker_id.is_empty() else {
      raise ApiError::Cluster("failed Worker identity is missing")
    }
    guard failure.partition() is Some(partition) else {
      raise ApiError::Cluster("failed Worker did not identify a partition")
    }
    guard assignments.get(partition) is Some(assignment) else {
      raise ApiError::InvalidWorkerAssignment(partition.to_string())
    }
    if assignment.worker_id() != worker_id {
      raise ApiError::InvalidWorkerAssignment(
        "failure partition does not belong to failed Worker",
      )
    }
    if !workers.any(value => value == worker_id) {
      workers.push(worker_id)
    }
    match partitions.get(worker_id) {
      Some(values) =>
        if !values.any(value => value == partition) {
          values.push(partition)
        }
      None => partitions[worker_id] = [partition]
    }
  }
  (workers, partitions)
}

///|
/// Durable phase record for a cross-epoch Worker state transfer. The
/// reconciliation WAL protects the two control stores; this WAL protects the
/// bundle/pointer side of the same operation so a Coordinator restart can
/// resume it before accepting Worker sessions.
priv struct NodeMigrationIntent {
  version : Int
  phase : String
  transaction_id : String
  cluster_id : String
  coordinator_id : String
  job_id : String
  partition : Int
  source_worker_id : String
  source_incarnation : String
  source_epoch : Int
  start_offset : Int64
  target_worker_id : String
  target_incarnation : String
  target_epoch : Int
  source_storage_dir : String
  target_storage_dir : String
  bundle_path : String
} derive(ToJson, FromJson)

///|
priv struct NodeMigrationIntentEntry {
  version : Int
  kind : String
  transaction_id : String
  intent : NodeMigrationIntent?
} derive(ToJson, FromJson)

///|
priv struct NodeMigrationLog {
  wal : @runtime.Wal
  pending : Map[String, NodeMigrationIntent]
}

///|
fn node_migration_id(
  job_id : String,
  partition : Int,
  source_epoch : Int,
  target_worker_id : String,
) -> String {
  job_id +
  ":p" +
  partition.to_string() +
  ":e" +
  source_epoch.to_string() +
  ":" +
  target_worker_id
}

///|
fn validate_node_migration_intent(
  intent : NodeMigrationIntent,
) -> Unit raise ApiError {
  if intent.version != 1 ||
    (
      intent.phase != "prepared" &&
      intent.phase != "fenced" &&
      intent.phase != "installed" &&
      intent.phase != "committed"
    ) ||
    intent.transaction_id.is_empty() ||
    intent.cluster_id.is_empty() ||
    intent.coordinator_id.is_empty() ||
    intent.job_id.is_empty() ||
    intent.partition < 0 ||
    intent.source_worker_id.is_empty() ||
    intent.source_incarnation.is_empty() ||
    intent.source_epoch < 0 ||
    intent.start_offset < 0L ||
    intent.target_worker_id.is_empty() ||
    intent.target_incarnation.is_empty() ||
    intent.target_epoch != intent.source_epoch + 1 ||
    intent.source_worker_id == intent.target_worker_id ||
    intent.source_storage_dir.is_empty() ||
    intent.target_storage_dir.is_empty() ||
    intent.bundle_path.is_empty() {
    raise ApiError::Cluster("invalid Worker migration intent")
  }
  ignore(
    @protocol.Incarnation::new(intent.source_incarnation) catch {
      _ =>
        raise ApiError::Cluster("invalid Worker migration source incarnation")
    },
  )
  ignore(
    @protocol.Incarnation::new(intent.target_incarnation) catch {
      _ =>
        raise ApiError::Cluster("invalid Worker migration target incarnation")
    },
  )
}

///|
async fn NodeMigrationLog::open(
  path : String,
) -> NodeMigrationLog raise ApiError {
  let (wal, replay) = @runtime.Wal::open(path) catch {
    error =>
      raise ApiError::Cluster(
        "open Worker migration intent WAL: " + @debug.to_string(error),
      )
  }
  errdefer wal.close()
  let pending : Map[String, NodeMigrationIntent] = Map([])
  for encoded in replay.records() {
    let entry : NodeMigrationIntentEntry = @json.from_json(
      @json.parse(@utf8.decode(encoded)) catch {
        _ => raise ApiError::Cluster("malformed Worker migration intent")
      },
    ) catch {
      _ => raise ApiError::Cluster("malformed Worker migration intent")
    }
    if entry.version != 1 || entry.transaction_id.is_empty() {
      raise ApiError::Cluster("unsupported Worker migration intent")
    }
    match entry.kind {
      "phase" => {
        guard entry.intent is Some(intent) else {
          raise ApiError::Cluster("missing Worker migration intent payload")
        }
        validate_node_migration_intent(intent)
        if intent.transaction_id != entry.transaction_id {
          raise ApiError::Cluster("Worker migration intent id mismatch")
        }
        pending[entry.transaction_id] = intent
      }
      "complete" => {
        if entry.intent is Some(_) {
          raise ApiError::Cluster("malformed Worker migration completion")
        }
        pending.remove(entry.transaction_id)
      }
      _ => raise ApiError::Cluster("unknown Worker migration intent record")
    }
  }
  { wal, pending, }
}

///|
async fn NodeMigrationLog::phase(
  self : NodeMigrationLog,
  intent : NodeMigrationIntent,
) -> Unit raise ApiError {
  validate_node_migration_intent(intent)
  let entry = NodeMigrationIntentEntry::{
    version: 1,
    kind: "phase",
    transaction_id: intent.transaction_id,
    intent: Some(intent),
  }
  ignore(
    self.wal.append(@utf8.encode(@json.to_json(entry).stringify())) catch {
      error =>
        raise ApiError::Cluster(
          "persist Worker migration intent: " + @debug.to_string(error),
        )
    },
  )
  self.pending[intent.transaction_id] = intent
}

///|
async fn NodeMigrationLog::complete(
  self : NodeMigrationLog,
  transaction_id : String,
) -> Unit raise ApiError {
  let entry = NodeMigrationIntentEntry::{
    version: 1,
    kind: "complete",
    transaction_id,
    intent: None,
  }
  ignore(
    self.wal.append(@utf8.encode(@json.to_json(entry).stringify())) catch {
      error =>
        raise ApiError::Cluster(
          "persist Worker migration completion: " + @debug.to_string(error),
        )
    },
  )
  self.pending.remove(transaction_id)
}

///|
async fn NodeMigrationLog::compact(
  self : NodeMigrationLog,
) -> Unit raise ApiError {
  let entries : Array[Bytes] = []
  for transaction_id, intent in self.pending {
    ignore(transaction_id)
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
  self.wal.rewrite(entries[:]) catch {
    error =>
      raise ApiError::Cluster(
        "compact Worker migration intents: " + @debug.to_string(error),
      )
  }
}

///|
fn node_migration_slot(
  config : CoordinatorNodeConfig,
  worker_id : String,
) -> WorkerSlot? {
  for worker in config.workers {
    if worker.worker_id() == worker_id {
      return Some(worker)
    }
  }
  None
}

///|
fn snapshot_assignment(
  snapshot : @control.CoordinatorSnapshot,
  partition : Int,
) -> @control.PartitionAssignment? {
  for assignment in snapshot.assignments() {
    if assignment.partition() == partition {
      let value = @control.PartitionAssignment::from_snapshot(assignment) catch {
        _ => return None
      }
      return Some(value)
    }
  }
  None
}

///|
fn node_migration_intent(
  config : CoordinatorNodeConfig,
  job : CompiledJob,
  source : WorkerSlot,
  target : WorkerSlot,
  assignment : @control.PartitionAssignment,
  partition : Int,
) -> NodeMigrationIntent raise ApiError {
  let source_dir = match source.storage_dir() {
    Some(value) => value
    None =>
      raise ApiError::Cluster("Worker migration source storage is missing")
  }
  let target_dir = match target.storage_dir() {
    Some(value) => value
    None =>
      raise ApiError::Cluster("Worker migration target storage is missing")
  }
  let transaction_id = node_migration_id(
    job.definition.job_id,
    partition,
    assignment.epoch(),
    target.worker_id(),
  )
  {
    version: 1,
    phase: "prepared",
    transaction_id,
    cluster_id: config.cluster_id,
    coordinator_id: config.coordinator_id,
    job_id: job.definition.job_id,
    partition,
    source_worker_id: source.worker_id(),
    source_incarnation: source.incarnation(),
    source_epoch: assignment.epoch(),
    start_offset: assignment.start_offset(),
    target_worker_id: target.worker_id(),
    target_incarnation: target.incarnation(),
    target_epoch: assignment.epoch() + 1,
    source_storage_dir: source_dir,
    target_storage_dir: target_dir,
    bundle_path: config.storage_dir +
    "/migration-p" +
    partition.to_string() +
    ".json",
  }
}

///|
fn node_migration_matches(
  left : NodeMigrationIntent,
  right : NodeMigrationIntent,
) -> Bool {
  left.transaction_id == right.transaction_id &&
  left.cluster_id == right.cluster_id &&
  left.coordinator_id == right.coordinator_id &&
  left.job_id == right.job_id &&
  left.partition == right.partition &&
  left.source_worker_id == right.source_worker_id &&
  left.source_incarnation == right.source_incarnation &&
  left.source_epoch == right.source_epoch &&
  left.start_offset == right.start_offset &&
  left.target_worker_id == right.target_worker_id &&
  left.target_incarnation == right.target_incarnation &&
  left.target_epoch == right.target_epoch &&
  left.source_storage_dir == right.source_storage_dir &&
  left.target_storage_dir == right.target_storage_dir &&
  left.bundle_path == right.bundle_path
}

///|
async fn resume_pending_node_migrations(
  coordinator : @runtime.CoordinatorStore,
  job_store : @runtime.JobStore,
  job : CompiledJob,
  config : CoordinatorNodeConfig,
  assignments : Map[Int, @control.PartitionAssignment],
) -> Unit raise ApiError {
  let log = NodeMigrationLog::open(config.storage_dir + "/node-migrations.wal")
  defer log.wal.close()
  let pending : Array[NodeMigrationIntent] = []
  for intent in log.pending.values() {
    validate_node_migration_intent(intent)
    if intent.job_id != job.definition.job_id ||
      intent.cluster_id != config.cluster_id ||
      intent.coordinator_id != config.coordinator_id {
      raise ApiError::Cluster("Worker migration intent belongs to another Job")
    }
    pending.push(intent)
  }
  if pending.is_empty() {
    return
  }
  for intent in pending {
    guard node_migration_slot(config, intent.source_worker_id) is Some(source) &&
      node_migration_slot(config, intent.target_worker_id) is Some(target) else {
      raise ApiError::Cluster(
        "Worker migration intent references unknown Worker",
      )
    }
    guard source.storage_dir() is Some(source_dir) &&
      target.storage_dir() is Some(target_dir) &&
      source_dir == intent.source_storage_dir &&
      target_dir == intent.target_storage_dir else {
      raise ApiError::Cluster("Worker migration storage does not match intent")
    }
    guard assignments.get(intent.partition) is Some(current) else {
      raise ApiError::Cluster(
        "Worker migration partition assignment is missing",
      )
    }
    if current.start_offset() != intent.start_offset {
      raise ApiError::Cluster("Worker migration start offset changed")
    }
    if current.worker_id() == intent.source_worker_id &&
      current.epoch() == intent.source_epoch {
      let moved = @runtime.recover_worker_leases(
        coordinator,
        job_store,
        intent.source_worker_id,
        @protocol.Incarnation::new(intent.source_incarnation),
        intent.target_worker_id,
        reconcile_path=node_reconciliation_path(config),
      ) catch {
        error =>
          raise ApiError::Cluster(
            "recover Worker leases from intent: " + @debug.to_string(error),
          )
      }
      for lease in moved {
        assignments[lease.partition()] = lease
      }
      if !moved.any(value => value.partition() == intent.partition) {
        raise ApiError::Cluster("Worker migration lease was not moved")
      }
    } else if current.worker_id() != intent.target_worker_id ||
      current.epoch() != intent.target_epoch {
      raise ApiError::Cluster("Worker migration lease identity changed")
    }
    if intent.phase == "prepared" {
      let fenced = { ..intent, phase: "fenced", }
      log.phase(fenced)
    }
  }
  for intent in pending {
    let current_intent = log.pending.get(intent.transaction_id).unwrap()
    let source = node_migration_slot(config, current_intent.source_worker_id).unwrap()
    let target = node_migration_slot(config, current_intent.target_worker_id).unwrap()
    let source_dir = source.storage_dir().unwrap()
    let target_dir = target.storage_dir().unwrap()
    let old_config = WorkerNodeConfig::new(
      config.cluster_id,
      config.coordinator_id,
      NodeEndpoint::new("127.0.0.1", 1),
      current_intent.source_worker_id,
      current_intent.source_incarnation,
      source_dir,
      current_intent.partition,
    ) catch {
      error => raise ApiError::Cluster(@debug.to_string(error))
    }
    let old_config = old_config.with_assignment(
      current_intent.source_epoch,
      current_intent.start_offset,
    )
    if current_intent.phase == "fenced" || current_intent.phase == "installed" {
      let bundle_exists = @fs.exists(current_intent.bundle_path) catch {
        error => raise ApiError::Cluster(@debug.to_string(error))
      }
      if !bundle_exists {
        job.capture_worker_migration(
          old_config,
          current_intent.target_worker_id,
          current_intent.target_incarnation,
          current_intent.target_epoch,
          current_intent.bundle_path,
        )
      } else {
        let bundle = @runtime.WorkerMigration::load(current_intent.bundle_path) catch {
          error => raise ApiError::Cluster(@debug.to_string(error))
        }
        if bundle.identity().source_epoch() != current_intent.source_epoch ||
          bundle.identity().target_epoch() != current_intent.target_epoch ||
          bundle.identity().partition() != current_intent.partition {
          raise ApiError::Cluster("Worker migration bundle identity changed")
        }
      }
      let target_config = WorkerNodeConfig::new(
        config.cluster_id,
        config.coordinator_id,
        NodeEndpoint::new("127.0.0.1", 1),
        current_intent.target_worker_id,
        current_intent.target_incarnation,
        target_dir,
        current_intent.partition,
      ) catch {
        error => raise ApiError::Cluster(@debug.to_string(error))
      }
      let target_config = target_config.with_assignment(
        current_intent.target_epoch,
        current_intent.start_offset,
      )
      job.install_worker_migration(target_config, current_intent.bundle_path)
      log.phase({ ..current_intent, phase: "installed", })
    }
    let after_install = log.pending.get(intent.transaction_id).unwrap()
    if after_install.phase == "installed" {
      let bundle = @runtime.WorkerMigration::load(after_install.bundle_path) catch {
        error => raise ApiError::Cluster(@debug.to_string(error))
      }
      @runtime.commit_worker_migration(
        coordinator,
        job_store,
        bundle,
        reconcile_path=config.storage_dir +
          "/node-migration-p" +
          after_install.partition.to_string() +
          ".wal",
      ) catch {
        error =>
          raise ApiError::Cluster(
            "commit Worker migration intent: " + @debug.to_string(error),
          )
      }
      log.phase({ ..after_install, phase: "committed", })
      log.complete(after_install.transaction_id)
    } else if after_install.phase == "committed" {
      log.complete(after_install.transaction_id)
    }
  }
  log.compact()
}

///|
async fn recover_node_worker(
  coordinator : @runtime.CoordinatorStore,
  job_store : @runtime.JobStore,
  job : CompiledJob,
  config : CoordinatorNodeConfig,
  assignments : Map[Int, @control.PartitionAssignment],
  worker_id : String,
  failed_partitions : ArrayView[Int],
) -> Array[@control.PartitionAssignment] raise ApiError {
  let owned = assignments
    .values()
    .filter(value => value.worker_id() == worker_id)
    .map(value => value.partition())
    .to_array()
  owned.sort()
  let failed = failed_partitions.to_owned()
  failed.sort()
  if owned != failed {
    raise ApiError::InvalidWorkerAssignment(
      "automatic recovery requires every lease of a failed Worker to fail",
    )
  }
  let old_assignments : Map[Int, @control.PartitionAssignment] = Map([])
  for partition in failed {
    guard assignments.get(partition) is Some(previous) else {
      raise ApiError::InvalidWorkerAssignment(partition.to_string())
    }
    old_assignments[partition] = previous
  }
  guard assignments.get(failed[0]) is Some(old) else {
    raise ApiError::InvalidWorkerAssignment("failed Worker has no lease")
  }
  let replacement = match replacement_for_failed_worker(config, worker_id) {
    Some(value) => value
    None =>
      raise ApiError::InvalidWorkerAssignment(
        "automatic recovery requires a replacement Worker",
      )
  }
  let mut source_slot : WorkerSlot? = None
  let mut target_slot : WorkerSlot? = None
  for worker in config.workers {
    if worker.worker_id() == worker_id {
      source_slot = Some(worker)
    }
    if worker.worker_id() == replacement {
      target_slot = Some(worker)
    }
  }
  let migration_enabled = source_slot is Some(source) &&
    target_slot is Some(target) &&
    source.storage_dir() is Some(_) &&
    target.storage_dir() is Some(_)
  if config.migration_required && !migration_enabled {
    raise ApiError::Cluster(
      "automatic Worker recovery requires source and target storage directories",
    )
  }
  if migration_enabled {
    let source = source_slot.unwrap()
    let target = target_slot.unwrap()
    let log = NodeMigrationLog::open(
      config.storage_dir + "/node-migrations.wal",
    )
    for partition in failed {
      let expected = node_migration_intent(
        config,
        job,
        source,
        target,
        old_assignments.get(partition).unwrap(),
        partition,
      )
      match log.pending.get(expected.transaction_id) {
        Some(previous) if !node_migration_matches(previous, expected) => {
          log.wal.close()
          raise ApiError::Cluster(
            "Worker migration intent conflicts with retry",
          )
        }
        Some(_) => ()
        None => log.phase(expected)
      }
    }
    log.wal.close()
    resume_pending_node_migrations(
      coordinator, job_store, job, config, assignments,
    )
    let moved : Array[@control.PartitionAssignment] = []
    for partition in failed {
      guard assignments.get(partition) is Some(lease) else {
        raise ApiError::InvalidWorkerAssignment(partition.to_string())
      }
      assignments[partition] = lease
      moved.push(lease)
    }
    moved
  } else {
    let moved = @runtime.recover_worker_leases(
      coordinator,
      job_store,
      worker_id,
      old.incarnation(),
      replacement,
      reconcile_path=node_reconciliation_path(config),
    ) catch {
      error =>
        raise ApiError::Cluster(
          "recover Worker leases: " + @debug.to_string(error),
        )
    }
    guard moved.length() == owned.length() else {
      raise ApiError::InvalidWorkerAssignment(
        "recovery moved an unexpected lease",
      )
    }
    for lease in moved {
      assignments[lease.partition()] = lease
    }
    moved
  }
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
  for worker in self.config.workers {
    let incarnation = @protocol.Incarnation::new(worker.incarnation()) catch {
      error =>
        raise ApiError::Cluster(
          "invalid Worker incarnation: " + @debug.to_string(error),
        )
    }
    ignore(
      coordinator.register_worker(worker.worker_id(), incarnation, capabilities) catch {
        @runtime.CoordinatorStoreError::Coordinator(
          @control.CoordinatorError::StaleIncarnation
        ) => {
          let fenced = coordinator
            .snapshot()
            .workers()
            .any(value => {
              value.worker_id() == worker.worker_id() &&
              value.lifecycle() == @control.WorkerLifecycle::Fenced
            })
          if fenced {
            @control.RegisterStatus::Duplicate
          } else {
            raise ApiError::Cluster("register Worker: stale incarnation")
          }
        }
        error =>
          raise ApiError::Cluster("register Worker: " + @debug.to_string(error))
      },
    )
  }
  let assignments = restore_node_assignments(
    coordinator,
    job_store,
    job,
    self.config,
  )
  for partition in 0..<job.spec.partition_count() {
    let worker_id = job.spec.owner(partition) catch {
      error =>
        raise ApiError::Cluster(
          "resolve partition owner: " + @debug.to_string(error),
        )
    }
    let assignment = match
      snapshot_assignment(coordinator.snapshot(), partition) {
      Some(existing) => existing
      None =>
        coordinator.assign(partition, worker_id) catch {
          error =>
            raise ApiError::Cluster(
              "assign partition: " + @debug.to_string(error),
            )
        }
    }
    let saved = job_store.snapshot().partitions()[partition]
    if saved.phase() == @control.JobPartitionPhase::Unassigned {
      ignore(
        job_store.assign(
          partition,
          assignment.worker_id(),
          assignment.epoch(),
          assignment.start_offset(),
        ) catch {
          error =>
            raise ApiError::Cluster(
              "persist partition assignment: " + @debug.to_string(error),
            )
        },
      )
    } else if saved.worker_id() != assignment.worker_id() ||
      saved.epoch() != assignment.epoch() ||
      saved.start_offset() != assignment.start_offset() {
      raise ApiError::Cluster(
        "restored partition assignment does not match Coordinator",
      )
    }
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
    reconcile_path=node_reconciliation_path(self.config),
    auth_token?=self.config.auth_token,
  ) catch {
    error =>
      raise ApiError::Cluster(
        "run Coordinator supervisor: " + @debug.to_string(error),
      )
  }
  if summary.failed_workers() > 0 {
    let (failed_workers, failed_partitions) = failed_worker_partitions(
      summary, assignments,
    )
    if failed_workers.length() == 1 {
      let worker_id = failed_workers[0]
      guard failed_partitions.get(worker_id) is Some(partitions) else {
        raise ApiError::Cluster("failed Worker partitions are missing")
      }
      match replacement_for_failed_worker(self.config, worker_id) {
        Some(_) => {
          ignore(
            recover_node_worker(
              coordinator,
              job_store,
              job,
              self.config,
              assignments,
              worker_id,
              partitions[:],
            ) catch {
              error =>
                raise ApiError::Cluster(
                  "recover Worker " + worker_id + ": " + @debug.to_string(error),
                )
            },
          )
          let retry_works = build_node_works(job, events, assignments)
          summary = @runtime.run_durable_job_supervisor(
            self.server,
            coordinator,
            job_store,
            retry_works,
            output_manifest?,
            reconcile_path=node_reconciliation_path(self.config),
            auth_token?=self.config.auth_token,
          ) catch {
            error =>
              raise ApiError::Cluster(
                "run recovered Coordinator supervisor: " +
                @debug.to_string(error),
              )
          }
        }
        None => ()
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
  let paths = worker_node_storage_paths(
    config.storage_dir,
    partition,
    self.definition.sink,
  )
  let wal_path = paths.wal_path()
  let capabilities = [
    self.definition.operator.id(),
    self.descriptor.capability(),
    "causal-frontier",
    self.descriptor.code_digest(),
  ]
  let output_log_path = paths.output_log_path()
  let mut migration_retries = 0
  for ;; {
    let connection = @socket.Tcp::connect_to_host(
      config.coordinator.host,
      port=config.coordinator.port,
    ) catch {
      error =>
        raise ApiError::Cluster(
          "connect Coordinator: " + @debug.to_string(error),
        )
    }
    let task_id = @runtime.migrated_task_id(wal_path) catch {
      error =>
        raise ApiError::Cluster(
          "read Worker migration pointer: " + @debug.to_string(error),
        )
    }
    let task_id = task_id.unwrap_or(
      self.definition.job_id + "/dynamic/" + config.worker_id,
    )
    let task = self.descriptor.create_task(
      task_id,
      partition,
      config.epoch,
      config.start_offset,
      self.definition.max_state_events.unwrap_or(0),
    )
    let outcome : Result[@runtime.WorkerRunSummary, @runtime.FrameIoError] = Ok(
      @runtime.run_worker_task(
        connection,
        config.cluster_id,
        config.coordinator_id,
        config.worker_id,
        config.incarnation,
        capabilities,
        wal_path,
        task,
        _ => (),
        checkpoint_path=paths.checkpoint_path(),
        delta_wal_path?=paths.delta_wal_path(),
        output_log_path?,
        dynamic_assignment=!config.assignment_known,
        auth_token?=config.auth_token,
      ),
    ) catch {
      error => Err(error)
    }
    match outcome {
      Ok(summary) =>
        return {
          worker_id: config.worker_id,
          partition,
          replayed_batches: summary.replayed_batches(),
          replayed_deltas: summary.replayed_deltas(),
          live_batches: summary.live_batches(),
          persisted_offset: summary.persisted_offset(),
          applied_offset: summary.applied_offset(),
          state_events: task.event_count(),
          pending_events: task.pending_count(),
          state_schema: self.descriptor.state_schema(),
        }
      Err(
        @runtime.FrameIoError::Migration(
          @runtime.WorkerMigrationError::InvalidIdentity
        )
      ) if !config.assignment_known && migration_retries < 300 => {
        migration_retries += 1
        @async_runtime.sleep(10) catch {
          error => raise ApiError::Cluster(@debug.to_string(error))
        }
      }
      Err(error) =>
        raise ApiError::Cluster("run Worker task: " + @debug.to_string(error))
    }
  }
}

///|
/// Capture one stopped Worker Task into a portable transfer bundle. The source
/// WAL/checkpoint/output files are locked for the duration of the capture.
pub async fn CompiledJob::capture_worker_migration(
  self : CompiledJob,
  config : WorkerNodeConfig,
  target_worker_id : String,
  target_incarnation : String,
  target_epoch : Int,
  bundle_path : String,
) -> Unit raise ApiError {
  if config.partition < 0 {
    raise ApiError::InvalidWorkerAssignment(
      "migration capture requires an explicit partition",
    )
  }
  let partition = config.partition
  let paths = worker_node_storage_paths(
    config.storage_dir,
    partition,
    self.definition.sink,
  )
  let identity = @runtime.WorkerMigrationIdentity::new(
    config.cluster_id,
    config.coordinator_id,
    self.definition.job_id,
    partition,
    config.start_offset,
    config.worker_id,
    config.incarnation,
    config.epoch,
    target_worker_id,
    target_incarnation,
    target_epoch,
  ) catch {
    error =>
      raise ApiError::Cluster(
        "build Worker migration identity: " + @debug.to_string(error),
      )
  }
  let bundle = @runtime.WorkerMigration::capture(identity, paths) catch {
    error =>
      raise ApiError::Cluster(
        "capture Worker migration: " + @debug.to_string(error),
      )
  }
  bundle.save(bundle_path) catch {
    error =>
      raise ApiError::Cluster(
        "save Worker migration: " + @debug.to_string(error),
      )
  }
}

///|
/// Install a previously captured bundle. Call this after fencing/reassigning
/// the lease and before starting the replacement Worker.
pub async fn CompiledJob::install_worker_migration(
  self : CompiledJob,
  config : WorkerNodeConfig,
  bundle_path : String,
) -> Unit raise ApiError {
  if config.partition < 0 {
    raise ApiError::InvalidWorkerAssignment(
      "migration install requires an explicit partition",
    )
  }
  let bundle = @runtime.WorkerMigration::load(bundle_path) catch {
    error =>
      raise ApiError::Cluster(
        "load Worker migration: " + @debug.to_string(error),
      )
  }
  let identity = bundle.identity()
  if identity.job_id() != self.definition.job_id ||
    identity.cluster_id() != config.cluster_id ||
    identity.coordinator_id() != config.coordinator_id ||
    identity.target_worker_id() != config.worker_id ||
    identity.target_incarnation() != config.incarnation ||
    identity.partition() != config.partition {
    raise ApiError::InvalidWorkerAssignment(
      "Worker migration target does not match Worker config",
    )
  }
  let storage_exists = @fs.exists(config.storage_dir) catch {
    error =>
      raise ApiError::Cluster(
        "check Worker migration storage: " + @debug.to_string(error),
      )
  }
  if !storage_exists {
    @fs.mkdir(config.storage_dir, recursive=true) catch {
      error =>
        raise ApiError::Cluster(
          "mkdir Worker migration storage: " + @debug.to_string(error),
        )
    }
  }
  let paths = worker_node_storage_paths(
    config.storage_dir,
    config.partition,
    self.definition.sink,
  )
  bundle.install(identity, paths) catch {
    error =>
      raise ApiError::Cluster(
        "install Worker migration: " + @debug.to_string(error),
      )
  }
}

///|
/// Complete replacement-side recovery: install the verified bundle, fence the
/// old lease in both durable control stores, and commit the transferred
/// cursors. Start the replacement Worker only after this returns.
pub async fn CompiledJob::recover_worker_migration(
  self : CompiledJob,
  config : WorkerNodeConfig,
  coordinator : @runtime.CoordinatorStore,
  job : @runtime.JobStore,
  bundle_path : String,
  reconcile_path? : String,
) -> Unit raise ApiError {
  if config.partition < 0 {
    raise ApiError::InvalidWorkerAssignment(
      "migration recovery requires an explicit partition",
    )
  }
  let bundle = @runtime.WorkerMigration::load(bundle_path) catch {
    error =>
      raise ApiError::Cluster(
        "load Worker migration: " + @debug.to_string(error),
      )
  }
  let identity = bundle.identity()
  if identity.job_id() != self.definition.job_id ||
    identity.cluster_id() != config.cluster_id ||
    identity.coordinator_id() != config.coordinator_id ||
    identity.target_worker_id() != config.worker_id ||
    identity.target_incarnation() != config.incarnation ||
    identity.partition() != config.partition {
    raise ApiError::InvalidWorkerAssignment(
      "Worker migration target does not match Worker config",
    )
  }
  self.install_worker_migration(config, bundle_path)
  ignore(
    @runtime.recover_worker_leases(
      coordinator,
      job,
      identity.source_worker_id(),
      @protocol.Incarnation::new(identity.source_incarnation()),
      config.worker_id,
      reconcile_path?,
    ) catch {
      error =>
        raise ApiError::Cluster(
          "recover Worker lease: " + @debug.to_string(error),
        )
    },
  )
  let cursor_path = reconcile_path.map(path => path + ".migration-cursors")
  @runtime.commit_worker_migration(
    coordinator,
    job,
    bundle,
    reconcile_path?=cursor_path,
  ) catch {
    error =>
      raise ApiError::Cluster(
        "commit Worker migration: " + @debug.to_string(error),
      )
  }
}

///|
/// Run every statically owned partition inside one Worker process. Each
/// partition retains independent WAL/checkpoint/output paths and a distinct
/// Task identity; the existing TaskSet runtime owns concurrent sessions.
pub async fn CompiledJob::run_worker_node_set(
  self : CompiledJob,
  config : WorkerNodeSetConfig,
) -> WorkerNodeSetSummary raise ApiError {
  if !self.definition.workers.any(worker => worker == config.worker_id) {
    raise ApiError::InvalidWorkerAssignment(config.worker_id)
  }
  let partitions = match config.partitions {
    Some(value) => value.copy()
    None => {
      let result : Array[Int] = []
      for partition in 0..<self.spec.partition_count() {
        let owner = self.spec.owner(partition) catch {
          error => raise ApiError::Plan(error)
        }
        if owner == config.worker_id {
          result.push(partition)
        }
      }
      result
    }
  }
  if partitions.any(partition => partition >= self.spec.partition_count()) {
    raise ApiError::InvalidWorkerAssignment("partition is outside JobSpec")
  }
  if partitions.is_empty() {
    raise ApiError::InvalidWorkerAssignment("Worker has no partitions")
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
  let works : Array[@runtime.WorkerTaskWork] = []
  for partition in partitions {
    let connection = @socket.Tcp::connect_to_host(
      config.coordinator.host(),
      port=config.coordinator.port(),
    ) catch {
      error =>
        raise ApiError::Cluster(
          "connect Coordinator: " + @debug.to_string(error),
        )
    }
    let paths = worker_node_storage_paths(
      config.storage_dir,
      partition,
      self.definition.sink,
    )
    let migrated_id = @runtime.migrated_task_id(paths.wal_path()) catch {
      error =>
        raise ApiError::Cluster(
          "read Worker migration pointer: " + @debug.to_string(error),
        )
    }
    let task_id = migrated_id.unwrap_or(
      self.definition.job_id + "/p" + partition.to_string() + "/e0",
    )
    let task = self.descriptor.create_task(
      task_id,
      partition,
      0,
      0L,
      self.definition.max_state_events.unwrap_or(0),
    )
    let output_log_path = paths.output_log_path()
    works.push(
      @runtime.WorkerTaskWork::new(
        connection,
        config.cluster_id,
        config.coordinator_id,
        config.worker_id,
        config.incarnation,
        [
          self.definition.operator.id(),
          self.descriptor.capability(),
          "causal-frontier",
          self.descriptor.code_digest(),
        ],
        paths.wal_path(),
        task,
        checkpoint_path=paths.checkpoint_path(),
        delta_wal_path?=paths.delta_wal_path(),
        output_log_path?,
        dynamic_assignment=config.partitions is Some(_),
        auth_token?=config.auth_token,
      ),
    )
  }
  let summary = @runtime.run_worker_task_set(works, (_, _, _) => ()) catch {
    error =>
      raise ApiError::Cluster("run Worker TaskSet: " + @debug.to_string(error))
  }
  {
    worker_id: config.worker_id,
    completed_tasks: summary.completed_tasks(),
    failed_tasks: summary.failed_tasks(),
    replayed_batches: summary.replayed_batches(),
    live_batches: summary.live_batches(),
  }
}
