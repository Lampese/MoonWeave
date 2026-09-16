///|
/// A validated, deterministic job plan ready for a runner. The underlying
/// JobSpec stays private to the API package so users do not need to construct
/// assignments or transport frames.
pub struct CompiledJob {
  definition : JobDefinition
  spec : @plan.JobSpec
  descriptor : OperatorDescriptor
  plan_digest : String
  graph_stages : Array[(String, CompiledJob)]
}

///|
fn validate_sink(sink : SinkSpec) -> Unit raise ApiError {
  match sink {
    SinkSpec::Discard => ()
    SinkSpec::Durable(name) if name.is_empty() => raise ApiError::EmptySinkId
    SinkSpec::Durable(_) => ()
    SinkSpec::JsonLines(path) if path.is_empty() =>
      raise ApiError::Sink("sink path is required")
    SinkSpec::JsonLines(_) => ()
    SinkSpec::Kafka(spec) => spec.validate()
  }
}

///|
fn validate_source(source : SourceSpec) -> Unit raise ApiError {
  match source {
    SourceSpec::Events => ()
    SourceSpec::JsonLines(path) if path.is_empty() =>
      raise ApiError::Source("source path is required")
    SourceSpec::JsonLines(_) => ()
    SourceSpec::Kafka(spec) => spec.validate()
    SourceSpec::Inputs(inputs) => {
      if inputs.is_empty() {
        raise ApiError::Source("at least one input is required")
      }
      let seen : Set[String] = Set([])
      let file_paths : Set[String] = Set([])
      let kafka_topics : Set[String] = Set([])
      for input in inputs {
        if !seen.add_and_check(input.input_id) {
          raise ApiError::Source("duplicate input identity")
        }
        ignore(NamedSource::new(input.input_id, input.source))
        match input.source {
          JsonLines(path) =>
            if !file_paths.add_and_check(path) {
              raise ApiError::Source(
                "named file inputs must own distinct cursor paths",
              )
            }
          Kafka(spec) =>
            if !kafka_topics.add_and_check(spec.topic()) {
              raise ApiError::Source(
                "named Kafka inputs must use distinct topics for unambiguous event identities",
              )
            }
          _ => ()
        }
      }
    }
  }
}

///|
fn api_digest(value : String) -> String {
  let mut hash : UInt64 = 14695981039346656037UL
  for byte in @utf8.encode(value) {
    hash = (hash ^ byte.to_uint64()) * 1099511628211UL
  }
  "plan-" + hash.to_string(radix=16)
}

///|
/// Compile a user-facing JobDefinition into the existing deterministic plan.
/// This is the only public entry point that turns user configuration into a
/// runtime topology.
pub fn JobDefinition::compile(
  self : JobDefinition,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> CompiledJob raise ApiError {
  let self = self.compile_graph(registry)
  self.operator.validate()
  let descriptor = if self.transform is Some(_) {
    transform_descriptor(self)
  } else {
    match self.artifact {
      None => registry.resolve(self.operator)
      Some(artifact) => {
        if artifact.reference != self.operator {
          raise ApiError::ArtifactMismatch(self.operator.id())
        }
        registry.resolve_artifact(artifact)
      }
    }
  }
  if self.transform is Some(_) &&
    self.artifact is Some(artifact) &&
    !descriptor.artifact().identity_equal(artifact) {
    raise ApiError::ArtifactMismatch("transform artifact")
  }
  descriptor.validate()
  match self.event_time_config {
    None => ()
    Some(config) => {
      config.validate() catch {
        error => raise ApiError::InvalidEvent(@debug.to_string(error))
      }
      if config.window_size_ms() > 0L &&
        descriptor.operator_kind != @task.OperatorKind::KeyedSum {
        raise ApiError::InvalidEvent("event-time windows require keyed-sum")
      }
    }
  }
  if self.watermark_idle_timeout_ms.unwrap_or(0L) < 0L {
    raise ApiError::InvalidEvent("negative idle timeout")
  }
  validate_source(self.source)
  if self.source is SourceSpec::Inputs(inputs) &&
    self.event_time_config is Some(config) {
    let actual = inputs.map(input => input.input_id)
    actual.sort()
    let expected = if config.window_size_ms() > 0L {
      ["main"]
    } else {
      ["left", "right"]
    }
    if actual != expected {
      raise ApiError::Source(
        "event-time input identities do not match the operator",
      )
    }
  }
  validate_sink(self.sink)
  let routes = self.routes.copy()
  routes.sort()
  let workers = self.workers.copy()
  workers.sort()
  // Normalize explicit artifacts and default built-in references to the same
  // implementation identity; signatures do not affect the execution plan.
  let canonical = JobDefinition::{
    ..self,
    workers,
    routes,
    artifact: Some(descriptor.artifact()),
  }
  let spec = @plan.JobSpec::new(
    canonical.job_id,
    canonical.operator.id(),
    canonical.partition_count,
    canonical.workers[:],
    canonical.routes[:],
    parallelism=canonical.parallelism(),
  ) catch {
    error => raise ApiError::Plan(error)
  }
  {
    definition: canonical,
    spec,
    descriptor,
    graph_stages: match canonical.graph {
      None => []
      Some(graph) =>
        graph.stages.map(stage => {
          (stage.id, stage.definition.compile(registry~))
        })
    },
    plan_digest: api_digest(
      Json::array([
        @json.to_json(canonical),
        Json::string(descriptor.capability()),
      ]).stringify(),
    ),
  }
}

///|
/// Compile and start the local runner in one step. This is the shortest
/// development path; ClusterRunner remains a separate explicit mode.
pub fn JobDefinition::run_local(
  self : JobDefinition,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> LocalRun raise ApiError {
  self.compile(registry~).run_local()
}

///|
/// Compile and execute a finite job through the local real-TCP cluster runner.
pub async fn JobDefinition::run_local_cluster(
  self : JobDefinition,
  config : ClusterConfig,
  events : ArrayView[Event],
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> ClusterRunSummary raise ApiError {
  self.compile(registry~).run_local_cluster(config, events)
}

///|
pub fn CompiledJob::definition(self : CompiledJob) -> JobDefinition {
  {
    ..self.definition,
    workers: self.definition.workers.copy(),
    routes: self.definition.routes.copy(),
  }
}

///|
pub fn CompiledJob::job_id(self : CompiledJob) -> String {
  self.definition.job_id
}

///|
pub fn CompiledJob::operator(self : CompiledJob) -> OperatorRef {
  self.definition.operator
}

///|
pub fn CompiledJob::operator_descriptor(
  self : CompiledJob,
) -> OperatorDescriptor {
  self.descriptor
}

///|
/// Build a serializable submission envelope for a Driver/Coordinator handoff.
pub fn CompiledJob::submission(self : CompiledJob) -> ClusterSubmission {
  ClusterSubmission::{
    definition: self.definition(),
    plan_digest: self.plan_digest,
    operator: self.descriptor.artifact(),
  }
}

///|
pub fn CompiledJob::submission_with_id(
  self : CompiledJob,
  submission_id : String,
) -> JobSubmission raise ApiError {
  JobSubmission::new(submission_id, self.submission())
}

///|
/// Recompile and verify a submission before accepting it into a runtime. The
/// registry is the static Worker build's implementation catalog; it must carry
/// the same custom callback that was compiled into the submitting Worker.
pub fn ClusterSubmission::compile(
  self : ClusterSubmission,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> CompiledJob raise ApiError {
  self.operator.validate()
  let compiled = self.definition.compile(registry~)
  if compiled.plan_digest() != self.plan_digest {
    raise ApiError::SubmissionMismatch("plan digest")
  }
  let expected = compiled.operator_descriptor().artifact()
  if !expected.identity_equal(self.operator) {
    raise ApiError::SubmissionMismatch("operator artifact")
  }
  compiled
}

///|
pub fn CompiledJob::partition_count(self : CompiledJob) -> Int {
  self.spec.partition_count()
}

///|
/// Expose deterministic placement without exposing JobSpec or RouteTarget.
pub fn CompiledJob::partition_for_key(self : CompiledJob, key : String) -> Int {
  self.spec.partition_for_key(key)
}

///|
pub fn CompiledJob::plan_digest(self : CompiledJob) -> String {
  self.plan_digest
}

///|
pub fn CompiledJob::state_schema(self : CompiledJob) -> String {
  self.descriptor.state_schema()
}

///|
pub fn CompiledJob::delivery_guarantee(self : CompiledJob) -> DeliveryGuarantee {
  self.definition.sink.delivery_guarantee()
}

///|
pub fn CompiledJob::recovery_capability(
  self : CompiledJob,
) -> RecoveryCapability {
  self.definition.source.recovery_capability()
}

///|
pub fn CompiledJob::connector_contract(self : CompiledJob) -> ConnectorContract {
  {
    source_recovery: self.recovery_capability(),
    sink_delivery: self.delivery_guarantee(),
    idempotent_sink_required: self.definition.sink.requires_idempotent_consumer(),
  }
}

///|
pub fn CompiledJob::sink(self : CompiledJob) -> SinkSpec {
  self.definition.sink
}

///|
pub fn CompiledJob::partitioning(self : CompiledJob) -> Partitioning {
  self.definition.partitioning
}
