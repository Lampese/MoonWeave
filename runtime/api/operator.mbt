///|
/// A stable identity for code that can run on a Worker. v1 ships the built-in
/// operators with the Worker binary; later artifact execution can add a code
/// digest without changing JobDefinition.
pub struct OperatorRef {
  id : String
  version : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn OperatorRef::new(
  id : String,
  version : Int,
) -> OperatorRef raise ApiError {
  if id.is_empty() {
    raise ApiError::EmptyOperatorId
  }
  if version <= 0 {
    raise ApiError::InvalidOperatorVersion(version)
  }
  { id, version, }
}

///|
pub fn OperatorRef::reachability() -> OperatorRef {
  { id: "reachability", version: 1, }
}

///|
pub fn OperatorRef::keyed_sum() -> OperatorRef {
  { id: "keyed-sum", version: 1, }
}

///|
pub fn OperatorRef::security_chain() -> OperatorRef {
  { id: "security-chain", version: 1, }
}

///|
pub fn OperatorRef::id(self : OperatorRef) -> String {
  self.id
}

///|
pub fn OperatorRef::version(self : OperatorRef) -> Int {
  self.version
}

///|
fn OperatorRef::validate(self : OperatorRef) -> Unit raise ApiError {
  ignore(OperatorRef::new(self.id, self.version))
}

///|
/// Metadata advertised by a Worker for one registered operator. `code_digest`
/// is an identity for the compiled implementation in v1; artifact loading can
/// replace it with a signed package digest later.
pub struct OperatorDescriptor {
  reference : OperatorRef
  code_digest : String
  state_schema : String
  input_schema : String
  output_schema : String
  runtime_abi : String
  target : String
  dependency_digest : String
  deterministic : Bool
  supports_proof : Bool
  operator_kind : @task.OperatorKind
} derive(Debug, Eq)

///|
pub fn OperatorDescriptor::reference(self : OperatorDescriptor) -> OperatorRef {
  self.reference
}

///|
pub fn OperatorDescriptor::code_digest(self : OperatorDescriptor) -> String {
  self.code_digest
}

///|
pub fn OperatorDescriptor::state_schema(self : OperatorDescriptor) -> String {
  self.state_schema
}

///|
pub fn OperatorDescriptor::input_schema(self : OperatorDescriptor) -> String {
  self.input_schema
}

///|
pub fn OperatorDescriptor::output_schema(self : OperatorDescriptor) -> String {
  self.output_schema
}

///|
pub fn OperatorDescriptor::runtime_abi(self : OperatorDescriptor) -> String {
  self.runtime_abi
}

///|
pub fn OperatorDescriptor::target(self : OperatorDescriptor) -> String {
  self.target
}

///|
pub fn OperatorDescriptor::dependency_digest(
  self : OperatorDescriptor,
) -> String {
  self.dependency_digest
}

///|
pub fn OperatorDescriptor::deterministic(self : OperatorDescriptor) -> Bool {
  self.deterministic
}

///|
pub fn OperatorDescriptor::supports_proof(self : OperatorDescriptor) -> Bool {
  self.supports_proof
}

///|
pub fn OperatorDescriptor::artifact(
  self : OperatorDescriptor,
) -> OperatorArtifact {
  OperatorArtifact::{
    reference: self.reference,
    code_digest: self.code_digest,
    runtime_abi: self.runtime_abi,
    target: self.target,
    state_schema: self.state_schema,
    dependency_digest: self.dependency_digest,
    signature: None,
  }
}

///|
pub struct OperatorRegistry {
  descriptors : Map[String, OperatorDescriptor]
}

///|
fn operator_registry_key(reference : OperatorRef) -> String {
  reference.id() + "#" + reference.version().to_string()
}

///|
/// Create an empty Worker allow-list. Registered descriptors must correspond to
/// implementations in this binary.
pub fn OperatorRegistry::new() -> OperatorRegistry {
  { descriptors: Map([]), }
}

///|
fn builtin_descriptor(kind : @task.OperatorKind) -> OperatorDescriptor {
  {
    reference: { id: kind.id(), version: kind.version(), },
    code_digest: kind.code_digest(),
    state_schema: kind.state_schema(),
    input_schema: kind.input_schema(),
    output_schema: kind.output_schema(),
    runtime_abi: kind.runtime_abi(),
    // This labels the native user-layer executor; it does not constrain the
    // backend used by the pure Task package.
    target: "native",
    dependency_digest: kind.dependency_digest(),
    deterministic: true,
    supports_proof: kind.supports_proof(),
    operator_kind: kind,
  }
}

///|
/// Register a precompiled implementation descriptor. This does not load
/// foreign machine code or serialize functions.
pub fn OperatorRegistry::register(
  self : OperatorRegistry,
  descriptor : OperatorDescriptor,
) -> Unit raise ApiError {
  descriptor.validate()
  let key = operator_registry_key(descriptor.reference)
  if self.descriptors.contains(key) {
    raise ApiError::DuplicateOperator(key)
  }
  self.descriptors[key] = descriptor
}

///|
pub fn OperatorRegistry::builtins() -> OperatorRegistry {
  let registry = OperatorRegistry::new()
  for
    kind in [
      @task.OperatorKind::KeyedSum,
      @task.OperatorKind::Reachability,
      @task.OperatorKind::SecurityChain,
    ] {
    let descriptor = builtin_descriptor(kind)
    registry.descriptors[operator_registry_key(descriptor.reference)] = descriptor
  }
  registry
}

///|
/// Return an independent array whose order is independent of registration
/// order.
pub fn OperatorRegistry::entries(
  self : OperatorRegistry,
) -> Array[OperatorDescriptor] {
  let result = self.descriptors.values().to_array()
  result.sort_by((left, right) => {
    let order = left.reference.id.compare(right.reference.id)
    if order == 0 {
      left.reference.version.compare(right.reference.version)
    } else {
      order
    }
  })
  result
}

///|
pub fn OperatorRegistry::capabilities(self : OperatorRegistry) -> Array[String] {
  self.entries().map(descriptor => descriptor.capability())
}

///|
fn OperatorDescriptor::validate(
  self : OperatorDescriptor,
) -> Unit raise ApiError {
  self.reference.validate()
  let expected = builtin_descriptor(self.operator_kind)
  if self.reference != expected.reference ||
    self.code_digest != expected.code_digest ||
    self.state_schema != expected.state_schema ||
    self.input_schema != expected.input_schema ||
    self.output_schema != expected.output_schema ||
    self.runtime_abi != expected.runtime_abi ||
    self.target != expected.target ||
    self.dependency_digest != expected.dependency_digest ||
    self.deterministic != expected.deterministic ||
    self.supports_proof != expected.supports_proof {
    raise ApiError::ArtifactMismatch(self.reference.id)
  }
}

///|
pub fn OperatorDescriptor::capability(self : OperatorDescriptor) -> String {
  self.operator_kind.capability()
}

///|
/// Validate input structure with the operator parser; a schema name alone is
/// not sufficient to claim compatibility.
pub fn OperatorDescriptor::validate_event(
  self : OperatorDescriptor,
  event : Event,
) -> Unit raise ApiError {
  self.validate()
  self.operator_kind.validate_payload(event.payload()) catch {
    error => raise ApiError::InvalidEvent(@debug.to_string(error))
  }
}

///|
fn OperatorDescriptor::create_task(
  self : OperatorDescriptor,
  task_id : String,
  partition : Int,
  epoch : Int,
  start_offset : Int64,
) -> @task.Task raise ApiError {
  self.validate()
  @task.Task::new(
    task_id,
    self.reference.id,
    self.operator_kind,
    partition,
    epoch,
    start_offset~,
  ) catch {
    error => raise ApiError::TaskFailed(@debug.to_string(error))
  }
}

///|
pub fn OperatorRegistry::resolve(
  self : OperatorRegistry,
  reference : OperatorRef,
) -> OperatorDescriptor raise ApiError {
  reference.validate()
  match self.descriptors.get(operator_registry_key(reference)) {
    Some(value) => value
    None if self.descriptors
      .values()
      .any(value => value.reference.id == reference.id) =>
      raise ApiError::UnsupportedOperatorVersion(
        id=reference.id(),
        version=reference.version(),
      )
    None => raise ApiError::UnsupportedOperator(reference.id())
  }
}

///|
/// Validate an artifact envelope against a registered implementation. This
/// does not execute or dynamically load code; it establishes the identity and
/// compatibility contract needed by a future sandboxed loader.
pub fn OperatorRegistry::resolve_artifact(
  self : OperatorRegistry,
  artifact : OperatorArtifact,
) -> OperatorDescriptor raise ApiError {
  artifact.validate()
  let descriptor = self.resolve(artifact.reference)
  if descriptor.code_digest() != artifact.code_digest ||
    descriptor.runtime_abi() != artifact.runtime_abi ||
    descriptor.target() != artifact.target ||
    descriptor.state_schema() != artifact.state_schema ||
    descriptor.dependency_digest() != artifact.dependency_digest {
    raise ApiError::ArtifactMismatch(artifact.reference.id())
  }
  descriptor
}
