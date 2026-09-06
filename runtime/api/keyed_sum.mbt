///|
/// A typed input record for the built-in keyed-sum operator.
pub(all) struct KeyedSumEvent {
  event : Event
  value : Int64
} derive(Debug, Eq)

///|
pub fn KeyedSumEvent::new(
  id : @causal.EventId,
  key : String,
  value : Int64,
) -> KeyedSumEvent raise ApiError {
  { event: Event::keyed_sum(id, key, value), value, }
}

///|
/// Construct an input with an explicit causal dependency without exposing the
/// JSON payload used by the transport boundary.
pub fn KeyedSumEvent::new_with_parents(
  id : @causal.EventId,
  parents : ArrayView[@causal.EventId],
  key : String,
  value : Int64,
) -> KeyedSumEvent raise ApiError {
  {
    event: Event::new(
      id,
      parents,
      key,
      Json::object({ "value": Json::string(value.to_string()) }),
    ),
    value,
  }
}

///|
pub fn KeyedSumEvent::id(self : KeyedSumEvent) -> @causal.EventId {
  self.event.id()
}

///|
pub fn KeyedSumEvent::key(self : KeyedSumEvent) -> String {
  self.event.key()
}

///|
pub fn KeyedSumEvent::value(self : KeyedSumEvent) -> Int64 {
  self.value
}

///|
/// A typed running total emitted after one or more input records are admitted.
pub(all) struct KeyedSumUpdate {
  key : String
  total : Int64
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn KeyedSumUpdate::new(key : String, total : Int64) -> KeyedSumUpdate {
  { key, total, }
}

///|
pub fn KeyedSumUpdate::key(self : KeyedSumUpdate) -> String {
  self.key
}

///|
pub fn KeyedSumUpdate::total(self : KeyedSumUpdate) -> Int64 {
  self.total
}

///|
fn keyed_sum_update_from_output(
  output : Output,
) -> KeyedSumUpdate raise ApiError {
  match output {
    Output::KeyedSum(value) => KeyedSumUpdate::new(value.key(), value.total())
    _ =>
      raise ApiError::TaskFailed(
        "keyed-sum facade received an incompatible operator output",
      )
  }
}

///|
fn keyed_sum_updates_from_outputs(
  outputs : ArrayView[Output],
) -> Array[KeyedSumUpdate] raise ApiError {
  let result : Array[KeyedSumUpdate] = []
  for output in outputs {
    result.push(keyed_sum_update_from_output(output))
  }
  result
}

///|
fn keyed_sum_final_totals(
  updates : ArrayView[KeyedSumUpdate],
) -> Array[KeyedSumUpdate] {
  let totals : Map[String, Int64] = Map([])
  for update in updates {
    totals[update.key()] = update.total()
  }
  let result = totals
    .to_array()
    .map(pair => KeyedSumUpdate::new(pair.0, pair.1))
  result.sort_by((left, right) => left.key().compare(right.key()))
  result
}

///|
/// User-facing definition for a keyed numeric aggregation job. The operator
/// identity, input schema, and runtime output representation remain internal.
pub struct KeyedSumJob {
  definition : JobDefinition
} derive(Debug, Eq)

///|
pub fn KeyedSumJob::new(job_id : String) -> KeyedSumJob raise ApiError {
  { definition: JobDefinition::new(job_id, OperatorRef::keyed_sum()), }
}

///|
pub fn KeyedSumJob::with_partition_count(
  self : KeyedSumJob,
  partition_count : Int,
) -> KeyedSumJob raise ApiError {
  { definition: self.definition.with_partition_count(partition_count), }
}

///|
pub fn KeyedSumJob::with_workers(
  self : KeyedSumJob,
  workers : ArrayView[String],
) -> KeyedSumJob raise ApiError {
  { definition: self.definition.with_workers(workers), }
}

///|
pub fn KeyedSumJob::with_durable_sink(
  self : KeyedSumJob,
  sink_id : String,
) -> KeyedSumJob raise ApiError {
  { definition: self.definition.with_sink(SinkSpec::durable(sink_id)), }
}

///|
pub fn KeyedSumJob::without_sink(self : KeyedSumJob) -> KeyedSumJob {
  { definition: self.definition.with_sink(SinkSpec::discard()), }
}

///|
pub fn KeyedSumJob::job_id(self : KeyedSumJob) -> String {
  self.definition.job_id()
}

///|
pub fn KeyedSumJob::compile(self : KeyedSumJob) -> KeyedSumPlan raise ApiError {
  { compiled: self.definition.compile(), }
}

///|
pub fn KeyedSumJob::run_local(self : KeyedSumJob) -> KeyedSumRun raise ApiError {
  self.compile().run_local()
}

///|
pub async fn KeyedSumJob::run_local_cluster(
  self : KeyedSumJob,
  config : ClusterConfig,
  events : ArrayView[KeyedSumEvent],
) -> KeyedSumRunSummary raise ApiError {
  self.compile().run_local_cluster(config, events)
}

///|
/// A compiled keyed-sum plan. It exposes only deterministic placement and
/// execution entry points, not the underlying JobSpec or TaskSpec.
pub struct KeyedSumPlan {
  compiled : CompiledJob
}

///|
pub fn KeyedSumPlan::job_id(self : KeyedSumPlan) -> String {
  self.compiled.job_id()
}

///|
pub fn KeyedSumPlan::plan_digest(self : KeyedSumPlan) -> String {
  self.compiled.plan_digest()
}

///|
pub fn KeyedSumPlan::partition_count(self : KeyedSumPlan) -> Int {
  self.compiled.partition_count()
}

///|
pub fn KeyedSumPlan::partition_for_key(
  self : KeyedSumPlan,
  key : String,
) -> Int {
  self.compiled.partition_for_key(key)
}

///|
pub fn KeyedSumPlan::run_local(
  self : KeyedSumPlan,
) -> KeyedSumRun raise ApiError {
  { run: self.compiled.run_local(), updates: [], }
}

///|
pub async fn KeyedSumPlan::run_local_cluster(
  self : KeyedSumPlan,
  config : ClusterConfig,
  events : ArrayView[KeyedSumEvent],
) -> KeyedSumRunSummary raise ApiError {
  let records = events.map(event => event.event)
  {
    job_id: self.job_id(),
    summary: self.compiled.run_local_cluster(config, records),
  }
}

///|
/// A local typed keyed-sum run. It retains typed updates for callers that need
/// to inspect the stream as well as the final total per key.
pub struct KeyedSumRun {
  run : LocalRun
  updates : Array[KeyedSumUpdate]
}

///|
pub fn KeyedSumRun::job_id(self : KeyedSumRun) -> String {
  self.run.job_id()
}

///|
pub fn KeyedSumRun::plan_digest(self : KeyedSumRun) -> String {
  self.run.plan_digest()
}

///|
pub fn KeyedSumRun::phase(self : KeyedSumRun) -> LocalRunPhase {
  self.run.phase()
}

///|
pub fn KeyedSumRun::progresses(self : KeyedSumRun) -> Array[LocalProgress] {
  self.run.progresses()
}

///|
pub fn KeyedSumRun::submit(
  self : KeyedSumRun,
  events : ArrayView[KeyedSumEvent],
) -> Array[KeyedSumUpdate] raise ApiError {
  let records = events.map(event => event.event)
  let outputs = self.run.submit(records)
  let updates = keyed_sum_updates_from_outputs(outputs)
  self.updates.push_iter(updates.iter())
  updates
}

///|
pub fn KeyedSumRun::updates(self : KeyedSumRun) -> ArrayView[KeyedSumUpdate] {
  self.updates[:]
}

///|
pub fn KeyedSumRun::totals(self : KeyedSumRun) -> Array[KeyedSumUpdate] {
  keyed_sum_final_totals(self.updates[:])
}

///|
pub fn KeyedSumRun::close(self : KeyedSumRun) -> Unit raise ApiError {
  self.run.close()
}

///|
/// Typed result of a finite local-cluster keyed-sum execution.
pub struct KeyedSumRunSummary {
  job_id : String
  summary : ClusterRunSummary
} derive(Debug, Eq)

///|
pub fn KeyedSumRunSummary::job_id(self : KeyedSumRunSummary) -> String {
  self.job_id
}

///|
pub fn KeyedSumRunSummary::plan_digest(self : KeyedSumRunSummary) -> String {
  self.summary.plan_digest()
}

///|
pub fn KeyedSumRunSummary::completed_partitions(
  self : KeyedSumRunSummary,
) -> Int {
  self.summary.completed_partitions()
}

///|
pub fn KeyedSumRunSummary::failed_partitions(self : KeyedSumRunSummary) -> Int {
  self.summary.failed_partitions()
}

///|
pub fn KeyedSumRunSummary::sent_batches(self : KeyedSumRunSummary) -> Int {
  self.summary.sent_batches()
}

///|
pub fn KeyedSumRunSummary::updates(
  self : KeyedSumRunSummary,
) -> Array[KeyedSumUpdate] raise ApiError {
  keyed_sum_updates_from_outputs(self.summary.outputs())
}

///|
pub fn KeyedSumRunSummary::totals(
  self : KeyedSumRunSummary,
) -> Array[KeyedSumUpdate] raise ApiError {
  let updates = self.updates()
  keyed_sum_final_totals(updates[:])
}

///|
pub fn KeyedSumRunSummary::failures(
  self : KeyedSumRunSummary,
) -> ArrayView[String] {
  self.summary.failures()
}
