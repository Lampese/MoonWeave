///|
/// A compile-time registered custom operator. The callback is process-local;
/// only the identity and causal input history are persisted.
pub(all) struct CustomOperator {
  kind : OperatorKind
  validate : (Json) -> Unit raise TaskError
  process : (TaskInput) -> Array[Json] raise TaskError
}

///|
pub fn CustomOperator::new(
  id : String,
  version : Int,
  code_digest : String,
  state_schema : String,
  input_schema : String,
  output_schema : String,
  runtime_abi : String,
  dependency_digest : String,
  deterministic : Bool,
  supports_proof : Bool,
  validate : (Json) -> Unit raise TaskError,
  process : (TaskInput) -> Array[Json] raise TaskError,
) -> CustomOperator raise TaskError {
  let kind = OperatorKind::custom(
    id, version, code_digest, state_schema, input_schema, output_schema, runtime_abi,
    dependency_digest, deterministic, supports_proof,
  )
  { kind, validate, process, }
}

///|
pub fn CustomOperator::kind(self : CustomOperator) -> OperatorKind {
  self.kind
}

///|
pub fn CustomOperator::id(self : CustomOperator) -> String {
  self.kind.id()
}

///|
pub fn CustomOperator::version(self : CustomOperator) -> Int {
  self.kind.version()
}

///|
pub fn CustomOperator::code_digest(self : CustomOperator) -> String {
  self.kind.code_digest()
}

///|
pub fn CustomOperator::state_schema(self : CustomOperator) -> String {
  self.kind.state_schema()
}

///|
pub fn CustomOperator::input_schema(self : CustomOperator) -> String {
  self.kind.input_schema()
}

///|
pub fn CustomOperator::output_schema(self : CustomOperator) -> String {
  self.kind.output_schema()
}

///|
pub fn CustomOperator::runtime_abi(self : CustomOperator) -> String {
  self.kind.runtime_abi()
}

///|
pub fn CustomOperator::dependency_digest(self : CustomOperator) -> String {
  self.kind.dependency_digest()
}

///|
pub fn CustomOperator::deterministic(self : CustomOperator) -> Bool {
  self.kind.deterministic()
}

///|
pub fn CustomOperator::supports_proof(self : CustomOperator) -> Bool {
  self.kind.supports_proof()
}

///|
pub fn CustomOperator::validate_payload(
  self : CustomOperator,
  payload : Json,
) -> Unit raise TaskError {
  (self.validate)(payload)
}

///|
pub fn CustomOperator::apply(
  self : CustomOperator,
  input : TaskInput,
) -> Array[Json] raise TaskError {
  (self.process)(input)
}
