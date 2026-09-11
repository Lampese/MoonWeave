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
  EmptyKey
  InvalidWindowSize(Int)
  InvalidStateLimit(Int)
  InvalidEndpoint(String)
  InvalidPort(Int)
  InvalidWorkerAssignment(String)
  EmptySubmissionId
  InvalidJobPhase(expected~ : JobRunPhase, actual~ : JobRunPhase)
  UnknownSubmission(String)
  RequestConflict(String)
  InvalidDriverCommand(String)
  DriverClosed
  StateLimitExceeded(limit~ : Int, actual~ : Int)
  TaskFailed(String)
  Source(String)
  Sink(String)
  Closed
  Cluster(String)
  Plan(@plan.PlanError)
} derive(Debug, Eq)

///|
/// The first source adapter accepts protocol EventRecord values. Source
/// connectors are deliberately separate from the distributed transport.
pub(all) enum SourceSpec {
  Events
  JsonLines(String)
  Kafka(KafkaSourceSpec)
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn SourceSpec::events() -> SourceSpec {
  SourceSpec::Events
}

///|
/// Read one JSON-encoded `Event` per physical line from an external file.
pub fn SourceSpec::json_lines(path : String) -> SourceSpec raise ApiError {
  if path.is_empty() {
    raise ApiError::Source("source path is required")
  }
  SourceSpec::JsonLines(path)
}

///|
/// Read complete MoonWeave `Event` values from Kafka. A source partition of
/// `-1` follows consumer-group assignment across all topic partitions;
/// non-negative values keep an explicit partition. Credentials are injected
/// at execution time.
pub fn SourceSpec::kafka(config : KafkaSourceSpec) -> SourceSpec {
  SourceSpec::Kafka(config)
}

///|
pub fn SourceSpec::path(self : SourceSpec) -> String? {
  match self {
    SourceSpec::Events => None
    SourceSpec::JsonLines(path) => Some(path)
    SourceSpec::Kafka(_) => None
  }
}

///|
/// The built-in event source is backed by a durable Coordinator SourceLog.
pub(all) enum RecoveryCapability {
  Replayable
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn RecoveryCapability::replayable() -> RecoveryCapability {
  RecoveryCapability::Replayable
}

///|
pub fn SourceSpec::recovery_capability(self : SourceSpec) -> RecoveryCapability {
  match self {
    SourceSpec::Events => RecoveryCapability::Replayable
    SourceSpec::JsonLines(_) => RecoveryCapability::Replayable
    SourceSpec::Kafka(_) => RecoveryCapability::Replayable
  }
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
  JsonLines(String)
  Kafka(KafkaSinkSpec)
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Delivery guarantees are part of the connector contract. The current
/// durable sink is explicitly at-least-once; exactly-once requires a
/// transactional external sink and is not implied by this local WAL.
pub(all) enum DeliveryGuarantee {
  AtLeastOnce
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn DeliveryGuarantee::at_least_once() -> DeliveryGuarantee {
  DeliveryGuarantee::AtLeastOnce
}

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
/// Append one JSON-encoded `TaskOutputRecord` per line to an external sink.
pub fn SinkSpec::json_lines(path : String) -> SinkSpec raise ApiError {
  if path.is_empty() {
    raise ApiError::Sink("sink path is required")
  }
  SinkSpec::JsonLines(path)
}

///|
/// Send each durable task output to a Kafka topic after the local output WAL
/// has recorded it. The runtime credentials are supplied separately through
/// `KafkaRuntimeConfig` and never enter this serializable Job definition.
pub fn SinkSpec::kafka(config : KafkaSinkSpec) -> SinkSpec {
  SinkSpec::Kafka(config)
}

///|
pub fn SinkSpec::path(self : SinkSpec) -> String? {
  match self {
    SinkSpec::Discard => None
    SinkSpec::Durable(name) => Some(name)
    SinkSpec::JsonLines(path) => Some(path)
    SinkSpec::Kafka(_) => None
  }
}

///|
pub fn SinkSpec::delivery_guarantee(self : SinkSpec) -> DeliveryGuarantee {
  match self {
    SinkSpec::Discard
    | SinkSpec::Durable(_)
    | SinkSpec::JsonLines(_)
    | SinkSpec::Kafka(_) => DeliveryGuarantee::AtLeastOnce
  }
}

///|
/// Durable output consumers must use the stable output idempotency key when
/// retrying after a crash. The engine does not claim an external transaction.
pub fn SinkSpec::requires_idempotent_consumer(self : SinkSpec) -> Bool {
  match self {
    SinkSpec::Discard => false
    SinkSpec::Durable(_) | SinkSpec::JsonLines(_) | SinkSpec::Kafka(_) => true
  }
}

///|
/// Explicit source/sink contract exposed by the compiler and run handles.
pub(all) struct ConnectorContract {
  source_recovery : RecoveryCapability
  sink_delivery : DeliveryGuarantee
  idempotent_sink_required : Bool
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn ConnectorContract::source_recovery(
  self : ConnectorContract,
) -> RecoveryCapability {
  self.source_recovery
}

///|
pub fn ConnectorContract::sink_delivery(
  self : ConnectorContract,
) -> DeliveryGuarantee {
  self.sink_delivery
}

///|
pub fn ConnectorContract::idempotent_sink_required(
  self : ConnectorContract,
) -> Bool {
  self.idempotent_sink_required
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
  max_state_events : Int?
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
    max_state_events: None,
  }
}

///|
/// Bound the number of operator and causally pending events retained by each
/// Task. Leave unset only for an intentionally unbounded local experiment.
pub fn JobDefinition::with_state_limit(
  self : JobDefinition,
  max_events : Int,
) -> JobDefinition raise ApiError {
  if max_events <= 0 {
    raise ApiError::InvalidStateLimit(max_events)
  }
  { ..self, max_state_events: Some(max_events), }
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
pub fn JobDefinition::max_state_events(self : JobDefinition) -> Int? {
  self.max_state_events
}

///|
/// A user-facing causal input. The protocol EventRecord remains an internal
/// representation of the local/cluster runner.
pub struct Event {
  record : @protocol.EventRecord
} derive(Debug, Eq, ToJson, FromJson)

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
pub fn Event::keyed_sum(
  id : @causal.EventId,
  key : String,
  value : Int64,
) -> Event raise ApiError {
  Event::new(
    id,
    [],
    key,
    Json::object({ "value": Json::string(value.to_string()) }),
  )
}

///|
pub fn Event::id(self : Event) -> @causal.EventId {
  self.record.id()
}

///|
/// Causal parents carried by this event.
pub fn Event::parents(self : Event) -> ArrayView[@causal.EventId] {
  self.record.parents()
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
pub(all) enum JobRunPhase {
  Created
  Running
  Draining
  Succeeded
  Failed
  Cancelled
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// A serializable handoff envelope with a stable submission identity. The
/// embedded ClusterSubmission still carries plan and operator compatibility.
pub(all) struct JobSubmission {
  submission_id : String
  job : ClusterSubmission
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobSubmission::new(
  submission_id : String,
  job : ClusterSubmission,
) -> JobSubmission raise ApiError {
  if submission_id.is_empty() {
    raise ApiError::EmptySubmissionId
  }
  { submission_id, job, }
}

///|
pub fn JobSubmission::submission_id(self : JobSubmission) -> String {
  self.submission_id
}

///|
pub fn JobSubmission::job(self : JobSubmission) -> ClusterSubmission {
  self.job
}

///|
/// Commands share one schema between a local handle and a future remote
/// Driver/Coordinator control session.
pub(all) enum JobCommand {
  Submit(JobSubmission)
  Input(Array[Event])
  Status
  Drain
  Cancel
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobCommand::submit(value : JobSubmission) -> JobCommand {
  JobCommand::Submit(value)
}

///|
pub fn JobCommand::input(events : ArrayView[Event]) -> JobCommand {
  JobCommand::Input(events.to_owned())
}

///|
pub fn JobCommand::status() -> JobCommand {
  JobCommand::Status
}

///|
pub fn JobCommand::drain() -> JobCommand {
  JobCommand::Drain
}

///|
pub fn JobCommand::cancel() -> JobCommand {
  JobCommand::Cancel
}

///|
/// Durable identity and lifecycle view returned by a Job handle.
pub(all) struct JobRunStatus {
  submission_id : String
  job_id : String
  plan_digest : String
  phase : JobRunPhase
  accepted_events : Int64
  failure : String?
  idempotent_sink_required : Bool
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn JobRunStatus::new(
  submission_id : String,
  job_id : String,
  plan_digest : String,
  phase : JobRunPhase,
  accepted_events : Int64,
  failure : String?,
  idempotent_sink_required? : Bool = true,
) -> JobRunStatus {
  {
    submission_id,
    job_id,
    plan_digest,
    phase,
    accepted_events,
    failure,
    idempotent_sink_required,
  }
}

///|
pub fn JobRunStatus::submission_id(self : JobRunStatus) -> String {
  self.submission_id
}

///|
pub fn JobRunStatus::job_id(self : JobRunStatus) -> String {
  self.job_id
}

///|
pub fn JobRunStatus::plan_digest(self : JobRunStatus) -> String {
  self.plan_digest
}

///|
pub fn JobRunStatus::phase(self : JobRunStatus) -> JobRunPhase {
  self.phase
}

///|
pub fn JobRunStatus::accepted_events(self : JobRunStatus) -> Int64 {
  self.accepted_events
}

///|
pub fn JobRunStatus::failure(self : JobRunStatus) -> String? {
  self.failure
}

///|
pub fn JobRunStatus::delivery_guarantee(
  _self : JobRunStatus,
) -> DeliveryGuarantee {
  DeliveryGuarantee::AtLeastOnce
}

///|
pub fn JobRunStatus::recovery_capability(
  _self : JobRunStatus,
) -> RecoveryCapability {
  RecoveryCapability::Replayable
}

///|
pub fn JobRunStatus::idempotent_sink_required(self : JobRunStatus) -> Bool {
  self.idempotent_sink_required
}

///|
/// User-facing built-in outputs. The operator-specific payload types remain
/// typed, but Task/transport types do not leak through this API.
pub(all) enum Output {
  KeyedSum(@aggregation.KeyedSum)
  Reachable(@reachability.Reachable)
  SecurityAlert(@security.SecurityAlert)
  Custom(Json)
} derive(Debug, Eq, ToJson, FromJson)

///|
pub impl Compare for Output with fn compare(self, other) {
  match (self, other) {
    (Output::KeyedSum(left), Output::KeyedSum(right)) =>
      Compare::compare(left, right)
    (Output::Reachable(left), Output::Reachable(right)) =>
      Compare::compare(left, right)
    (Output::SecurityAlert(left), Output::SecurityAlert(right)) =>
      Compare::compare(left, right)
    (Output::Custom(left), Output::Custom(right)) =>
      @json.to_json(left).stringify().compare(@json.to_json(right).stringify())
    (Output::KeyedSum(_), _) => -1
    (Output::Reachable(_), Output::KeyedSum(_)) => 1
    (Output::Reachable(_), _) => -1
    (Output::SecurityAlert(_), Output::Custom(_)) => -1
    (Output::SecurityAlert(_), _) => 1
    (Output::Custom(_), _) => 1
  }
}

///|
pub fn Output::reachable(value : @reachability.Reachable) -> Output {
  Output::Reachable(value)
}

///|
pub fn Output::keyed_sum(value : @aggregation.KeyedSum) -> Output {
  Output::KeyedSum(value)
}

///|
pub fn Output::security_alert(value : @security.SecurityAlert) -> Output {
  Output::SecurityAlert(value)
}

///|
pub fn Output::as_reachable(self : Output) -> @reachability.Reachable? {
  match self {
    Output::KeyedSum(_) | Output::Custom(_) => None
    Output::Reachable(value) => Some(value)
    Output::SecurityAlert(_) => None
  }
}

///|
pub fn Output::as_security_alert(self : Output) -> @security.SecurityAlert? {
  match self {
    Output::KeyedSum(_) | Output::Custom(_) => None
    Output::Reachable(_) => None
    Output::SecurityAlert(value) => Some(value)
  }
}

///|
pub fn Output::as_keyed_sum(self : Output) -> @aggregation.KeyedSum? {
  match self {
    Output::KeyedSum(value) => Some(value)
    Output::Reachable(_) | Output::SecurityAlert(_) | Output::Custom(_) => None
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
  storage_budget_bytes : Int64
  kafka_runtime : KafkaRuntimeConfig?
}

///|
/// Default per-WAL physical budget. A deployment can override this per
/// process; zero is reserved for explicitly unbounded development runs.
const DEFAULT_STORAGE_BUDGET_BYTES : Int64 = 256L * 1024L * 1024L

///|
/// A production runner must not silently create an unbounded in-memory Task.
/// The explicit JobDefinition limit still overrides this conservative default;
/// zero/unset remains available to direct in-memory Task tests and experiments.
const DEFAULT_RUNTIME_STATE_LIMIT : Int = 100000

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
  {
    cluster_id,
    coordinator_id,
    storage_dir,
    storage_budget_bytes: DEFAULT_STORAGE_BUDGET_BYTES,
    kafka_runtime: None,
  }
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
/// Set the physical budget applied to every WAL opened by this local runner.
/// A zero value disables the guard and is intended only for development.
pub fn ClusterConfig::with_storage_budget(
  self : ClusterConfig,
  maximum_bytes : Int64,
) -> ClusterConfig raise ApiError {
  if maximum_bytes < 0L {
    raise ApiError::Cluster("storage budget cannot be negative")
  }
  { ..self, storage_budget_bytes: maximum_bytes, }
}

///|
pub fn ClusterConfig::storage_budget_bytes(self : ClusterConfig) -> Int64 {
  self.storage_budget_bytes
}

///|
/// Supply non-serializable Kafka TLS/SASL handles for this local execution.
/// They are deliberately kept on the runtime configuration rather than the
/// Job definition, submission envelope, or WAL.
pub fn ClusterConfig::with_kafka_runtime(
  self : ClusterConfig,
  runtime : KafkaRuntimeConfig,
) -> ClusterConfig {
  { ..self, kafka_runtime: Some(runtime), }
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
