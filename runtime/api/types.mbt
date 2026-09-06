///|
/// Errors raised before a high-level Job is lowered into the runtime plan.
pub(all) suberror ApiError {
  EmptyJobId
  EmptyOperatorId
  InvalidOperatorVersion(Int)
  UnsupportedOperator(String)
  UnsupportedOperatorVersion(id~ : String, version~ : Int)
  DuplicateOperator(String)
  InvalidPartitionCount(Int)
  EmptyWorkerList
  EmptyWorkerId
  DuplicateWorker(String)
  EmptySinkId
  InvalidRoute(source~ : Int, target~ : Int)
  InvalidArtifact(String)
  ArtifactMismatch(String)
  SubmissionMismatch(String)
  LocalModeUnsupported
  InvalidEvent(String)
  TaskFailed(String)
  Closed
  Cluster(String)
  Plan(@plan.PlanError)
} derive(Debug, Eq)

///|
/// The first source adapter accepts protocol EventRecord values. Source
/// connectors are deliberately separate from the distributed transport.
pub(all) enum SourceSpec {
  Events
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn SourceSpec::events() -> SourceSpec {
  SourceSpec::Events
}

///|
/// Deterministic partitioning selected by the job compiler.
pub(all) enum Partitioning {
  ByKey
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Partitioning::by_key() -> Partitioning {
  Partitioning::ByKey
}

///|
/// A sink declaration carries delivery intent without exposing the output WAL
/// or consumer checkpoint implementation.
pub(all) enum SinkSpec {
  Discard
  Durable(String)
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn SinkSpec::discard() -> SinkSpec {
  SinkSpec::Discard
}

///|
pub fn SinkSpec::durable(name : String) -> SinkSpec raise ApiError {
  if name.is_empty() {
    raise ApiError::EmptySinkId
  }
  SinkSpec::Durable(name)
}

///|
/// User-facing, transport-free description of a MoonWeave Job.
pub struct JobDefinition {
  job_id : String
  operator : OperatorRef
  partition_count : Int
  workers : Array[String]
  routes : Array[@plan.ExchangeRoute]
  source : SourceSpec
  partitioning : Partitioning
  sink : SinkSpec
  artifact : OperatorArtifact?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobDefinition::new(
  job_id : String,
  operator : OperatorRef,
) -> JobDefinition raise ApiError {
  if job_id.is_empty() {
    raise ApiError::EmptyJobId
  }
  {
    job_id,
    operator,
    partition_count: 1,
    workers: ["local"],
    routes: [],
    source: SourceSpec::Events,
    partitioning: Partitioning::ByKey,
    sink: SinkSpec::Discard,
    artifact: None,
  }
}

///|
pub fn JobDefinition::with_partition_count(
  self : JobDefinition,
  partition_count : Int,
) -> JobDefinition raise ApiError {
  if partition_count <= 0 {
    raise ApiError::InvalidPartitionCount(partition_count)
  }
  { ..self, partition_count, }
}

///|
pub fn JobDefinition::with_workers(
  self : JobDefinition,
  workers : ArrayView[String],
) -> JobDefinition raise ApiError {
  if workers.is_empty() {
    raise ApiError::EmptyWorkerList
  }
  let seen : Set[String] = Set([])
  let copied = []
  for worker in workers {
    if worker.is_empty() {
      raise ApiError::EmptyWorkerId
    }
    if !seen.add_and_check(worker) {
      raise ApiError::DuplicateWorker(worker)
    }
    copied.push(worker)
  }
  copied.sort()
  { ..self, workers: copied, }
}

///|
pub fn JobDefinition::with_routes(
  self : JobDefinition,
  routes : ArrayView[@plan.ExchangeRoute],
) -> JobDefinition {
  { ..self, routes: routes.to_owned(), }
}

///|
pub fn JobDefinition::with_source(
  self : JobDefinition,
  source : SourceSpec,
) -> JobDefinition {
  { ..self, source, }
}

///|
pub fn JobDefinition::with_partitioning(
  self : JobDefinition,
  partitioning : Partitioning,
) -> JobDefinition {
  { ..self, partitioning, }
}

///|
pub fn JobDefinition::with_sink(
  self : JobDefinition,
  sink : SinkSpec,
) -> JobDefinition {
  { ..self, sink, }
}

///|
pub fn JobDefinition::with_artifact(
  self : JobDefinition,
  artifact : OperatorArtifact,
) -> JobDefinition {
  { ..self, artifact: Some(artifact), }
}

///|
pub fn JobDefinition::job_id(self : JobDefinition) -> String {
  self.job_id
}

///|
pub fn JobDefinition::operator(self : JobDefinition) -> OperatorRef {
  self.operator
}

///|
pub fn JobDefinition::partition_count(self : JobDefinition) -> Int {
  self.partition_count
}

///|
pub fn JobDefinition::workers(self : JobDefinition) -> ArrayView[String] {
  self.workers[:]
}

///|
pub fn JobDefinition::routes(
  self : JobDefinition,
) -> ArrayView[@plan.ExchangeRoute] {
  self.routes[:]
}

///|
pub fn JobDefinition::source(self : JobDefinition) -> SourceSpec {
  self.source
}

///|
pub fn JobDefinition::partitioning(self : JobDefinition) -> Partitioning {
  self.partitioning
}

///|
pub fn JobDefinition::sink(self : JobDefinition) -> SinkSpec {
  self.sink
}

///|
pub fn JobDefinition::artifact(self : JobDefinition) -> OperatorArtifact? {
  self.artifact
}

///|
/// A user-facing causal input. The protocol EventRecord remains an internal
/// representation of the local/cluster runner.
pub struct Event {
  record : @protocol.EventRecord
} derive(Debug, Eq)

///|
pub fn Event::new(
  id : @causal.EventId,
  parents : ArrayView[@causal.EventId],
  key : String,
  payload : Json,
) -> Event raise ApiError {
  let record = @protocol.EventRecord::new(id, parents, key, payload) catch {
    error => raise ApiError::InvalidEvent(@debug.to_string(error))
  }
  { record, }
}

///|
pub fn Event::id(self : Event) -> @causal.EventId {
  self.record.id()
}

///|
pub fn Event::key(self : Event) -> String {
  self.record.key()
}

///|
pub fn Event::payload(self : Event) -> Json {
  self.record.payload()
}

///|
/// User-facing built-in outputs. The operator-specific payload types remain
/// typed, but Task/transport types do not leak through this API.
pub(all) enum Output {
  Reachable(@reachability.Reachable)
  SecurityAlert(@security.SecurityAlert)
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn Output::reachable(value : @reachability.Reachable) -> Output {
  Output::Reachable(value)
}

///|
pub fn Output::security_alert(value : @security.SecurityAlert) -> Output {
  Output::SecurityAlert(value)
}

///|
pub fn Output::as_reachable(self : Output) -> @reachability.Reachable? {
  match self {
    Output::Reachable(value) => Some(value)
    Output::SecurityAlert(_) => None
  }
}

///|
pub fn Output::as_security_alert(self : Output) -> @security.SecurityAlert? {
  match self {
    Output::Reachable(_) => None
    Output::SecurityAlert(value) => Some(value)
  }
}

///|
pub(all) enum LocalRunPhase {
  Created
  Running
  Draining
  Stopped
  Failed
  Mixed
} derive(Debug, Eq, Compare)

///|
pub(all) struct LocalProgress {
  job_id : String
  task_id : String
  partition : Int
  epoch : Int
  offset : Int64
  frontier_digest : String
  emitted_outputs : Int
  phase : LocalRunPhase
} derive(Debug, Eq)

///|
pub fn LocalProgress::job_id(self : LocalProgress) -> String {
  self.job_id
}

///|
pub fn LocalProgress::task_id(self : LocalProgress) -> String {
  self.task_id
}

///|
pub fn LocalProgress::partition(self : LocalProgress) -> Int {
  self.partition
}

///|
pub fn LocalProgress::epoch(self : LocalProgress) -> Int {
  self.epoch
}

///|
pub fn LocalProgress::offset(self : LocalProgress) -> Int64 {
  self.offset
}

///|
pub fn LocalProgress::frontier_digest(self : LocalProgress) -> String {
  self.frontier_digest
}

///|
pub fn LocalProgress::emitted_outputs(self : LocalProgress) -> Int {
  self.emitted_outputs
}

///|
pub fn LocalProgress::phase(self : LocalProgress) -> LocalRunPhase {
  self.phase
}

///|
/// Configuration for the finite local-cluster runner. It derives worker
/// placement from JobDefinition::with_workers and owns no open I/O handles.
pub struct ClusterConfig {
  cluster_id : String
  coordinator_id : String
  storage_dir : String
} derive(Debug, Eq)

///|
pub fn ClusterConfig::new(
  cluster_id : String,
  coordinator_id : String,
  storage_dir : String,
) -> ClusterConfig raise ApiError {
  if cluster_id.is_empty() || coordinator_id.is_empty() {
    raise ApiError::Cluster("cluster and coordinator ids are required")
  }
  if storage_dir.is_empty() {
    raise ApiError::Cluster("storage directory is required")
  }
  { cluster_id, coordinator_id, storage_dir, }
}

///|
pub fn ClusterConfig::cluster_id(self : ClusterConfig) -> String {
  self.cluster_id
}

///|
pub fn ClusterConfig::coordinator_id(self : ClusterConfig) -> String {
  self.coordinator_id
}

///|
pub fn ClusterConfig::storage_dir(self : ClusterConfig) -> String {
  self.storage_dir
}

///|
/// High-level result of a finite ClusterRunner execution.
pub(all) struct ClusterRunSummary {
  plan_digest : String
  completed_partitions : Int
  failed_partitions : Int
  sent_batches : Int
  outputs : Array[Output]
  failures : Array[String]
} derive(Debug, Eq)

///|
pub fn ClusterRunSummary::plan_digest(self : ClusterRunSummary) -> String {
  self.plan_digest
}

///|
pub fn ClusterRunSummary::completed_partitions(self : ClusterRunSummary) -> Int {
  self.completed_partitions
}

///|
pub fn ClusterRunSummary::failed_partitions(self : ClusterRunSummary) -> Int {
  self.failed_partitions
}

///|
pub fn ClusterRunSummary::sent_batches(self : ClusterRunSummary) -> Int {
  self.sent_batches
}

///|
pub fn ClusterRunSummary::outputs(
  self : ClusterRunSummary,
) -> ArrayView[Output] {
  self.outputs[:]
}

///|
pub fn ClusterRunSummary::failures(
  self : ClusterRunSummary,
) -> ArrayView[String] {
  self.failures[:]
}

///|
/// Serialized handoff between a high-level Driver and a Cluster coordinator.
/// It contains plan and operator identity, not executable closure bytes.
pub(all) struct ClusterSubmission {
  definition : JobDefinition
  plan_digest : String
  operator : OperatorArtifact
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn ClusterSubmission::definition(self : ClusterSubmission) -> JobDefinition {
  self.definition
}

///|
pub fn ClusterSubmission::plan_digest(self : ClusterSubmission) -> String {
  self.plan_digest
}

///|
pub fn ClusterSubmission::operator(
  self : ClusterSubmission,
) -> OperatorArtifact {
  self.operator
}
