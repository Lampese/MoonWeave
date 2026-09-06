///|
/// A validated, deterministic job plan ready for a runner. The underlying
/// JobSpec stays private to the API package so users do not need to construct
/// assignments or transport frames.
pub struct CompiledJob {
  definition : JobDefinition
  spec : @plan.JobSpec
  descriptor : OperatorDescriptor
  plan_digest : String
}

///|
fn validate_sink(sink : SinkSpec) -> Unit raise ApiError {
  match sink {
    SinkSpec::Discard => ()
    SinkSpec::Durable(name) if name.is_empty() => raise ApiError::EmptySinkId
    SinkSpec::Durable(_) => ()
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
  self.operator.validate()
  let descriptor = match self.artifact {
    None => registry.resolve(self.operator)
    Some(artifact) => {
      if artifact.reference != self.operator {
        raise ApiError::ArtifactMismatch(self.operator.id())
      }
      registry.resolve_artifact(artifact)
    }
  }
  descriptor.validate()
  validate_sink(self.sink)
  let routes = self.routes.copy()
  routes.sort()
  let workers = self.workers.copy()
  workers.sort()
  // 显式 artifact 与默认内置引用统一成相同实现身份；签名不参与执行计划。
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
  ) catch {
    error => raise ApiError::Plan(error)
  }
  {
    definition: canonical,
    spec,
    descriptor,
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
) -> LocalRun raise ApiError {
  self.compile().run_local()
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
/// Recompile and verify a submission before accepting it into a runtime. This
/// catches plan drift and operator artifact mismatch without loading code.
pub fn ClusterSubmission::compile(
  self : ClusterSubmission,
) -> CompiledJob raise ApiError {
  self.operator.validate()
  let compiled = self.definition.compile()
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
pub fn CompiledJob::sink(self : CompiledJob) -> SinkSpec {
  self.definition.sink
}

///|
pub fn CompiledJob::partitioning(self : CompiledJob) -> Partitioning {
  self.definition.partitioning
}
