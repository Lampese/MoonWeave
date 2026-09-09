///|
/// Semantic identity of a built-in operator. This pure Task contract is
/// independent of a particular MoonBit backend.
pub(all) enum OperatorKind {
  KeyedSum
  Reachability
  SecurityChain
  Custom(
    String,
    Int,
    String,
    String,
    String,
    String,
    String,
    String,
    Bool,
    Bool
  )
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn OperatorKind::id(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum"
    Reachability => "reachability"
    SecurityChain => "security-chain"
    Custom(id, _, _, _, _, _, _, _, _, _) => id
  }
}

///|
pub fn OperatorKind::version(self : OperatorKind) -> Int {
  match self {
    KeyedSum | Reachability | SecurityChain => 1
    Custom(_, version, _, _, _, _, _, _, _, _) => version
  }
}

///|
/// Explicit implementation revision, not a cryptographic binary hash or a
/// signature. Bump it whenever replay semantics change.
pub fn OperatorKind::code_digest(self : OperatorKind) -> String {
  match self {
    KeyedSum | Reachability | SecurityChain => "builtin:" + self.id() + ":v1"
    Custom(_, _, code_digest, _, _, _, _, _, _, _) => code_digest
  }
}

///|
pub fn OperatorKind::state_schema(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum-state:v1"
    Reachability => "reachability-state:v1"
    SecurityChain => "security-chain-state:v1"
    Custom(_, _, _, state_schema, _, _, _, _, _, _) => state_schema
  }
}

///|
pub fn OperatorKind::input_schema(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum-contribution:v1"
    Reachability => "reachability-edge:v1"
    SecurityChain => "security-event:v1"
    Custom(_, _, _, _, input_schema, _, _, _, _, _) => input_schema
  }
}

///|
pub fn OperatorKind::output_schema(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum-total:v1"
    Reachability => "reachable:v1"
    SecurityChain => "security-alert:v1"
    Custom(_, _, _, _, _, output_schema, _, _, _, _) => output_schema
  }
}

///|
/// MoonWeave logical data contract, not a MoonBit native function/closure ABI.
pub fn OperatorKind::runtime_abi(self : OperatorKind) -> String {
  match self {
    KeyedSum | Reachability | SecurityChain => "moonweave-task-json-v1"
    Custom(_, _, _, _, _, _, runtime_abi, _, _, _) => runtime_abi
  }
}

///|
pub fn OperatorKind::dependency_digest(self : OperatorKind) -> String {
  match self {
    KeyedSum | Reachability | SecurityChain => "builtin-core-v1"
    Custom(_, _, _, _, _, _, _, dependency_digest, _, _) => dependency_digest
  }
}

///|
pub fn OperatorKind::supports_proof(self : OperatorKind) -> Bool {
  match self {
    KeyedSum | Reachability | SecurityChain => true
    Custom(_, _, _, _, _, _, _, _, _, supports_proof) => supports_proof
  }
}

///|
pub fn OperatorKind::deterministic(self : OperatorKind) -> Bool {
  match self {
    KeyedSum | Reachability | SecurityChain => true
    Custom(_, _, _, _, _, _, _, _, deterministic, _) => deterministic
  }
}

///|
/// Construct the serializable identity for a compile-time custom operator.
pub fn OperatorKind::custom(
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
) -> OperatorKind raise TaskError {
  if id.is_empty() {
    raise TaskError::EmptyOperatorId
  }
  if version <= 0 ||
    code_digest.is_empty() ||
    state_schema.is_empty() ||
    input_schema.is_empty() ||
    output_schema.is_empty() ||
    runtime_abi.is_empty() ||
    dependency_digest.is_empty() {
    raise TaskError::Custom("custom operator metadata is incomplete")
  }
  Custom(
    id, version, code_digest, state_schema, input_schema, output_schema, runtime_abi,
    dependency_digest, deterministic, supports_proof,
  )
}

///|
/// Transport the complete revision and schema identity instead of comparing
/// only the operator name. The token checks compatibility in a trusted
/// cluster; it does not authenticate a Worker or prove that it is honest.
pub fn OperatorKind::capability(self : OperatorKind) -> String {
  let determinism = if self.deterministic() {
    "deterministic"
  } else {
    "nondeterministic"
  }
  let proof = if self.supports_proof() {
    "task-replay-proof"
  } else {
    "no-task-replay-proof"
  }
  "moonweave-operator:" +
  @json.to_json([
    self.id(),
    self.version().to_string(),
    self.code_digest(),
    self.input_schema(),
    self.output_schema(),
    self.state_schema(),
    self.runtime_abi(),
    self.dependency_digest(),
    determinism,
    "exchange",
    proof,
  ]).stringify()
}

///|
/// Validate payload structure only. Missing parents are not invalid input;
/// causal constraints remain the Task's responsibility.
pub fn OperatorKind::validate_payload(
  self : OperatorKind,
  payload : Json,
) -> Unit raise TaskError {
  match self {
    KeyedSum =>
      ignore(
        @aggregation.Contribution::from_json("validation-key", payload) catch {
          error => raise TaskError::Aggregation(error)
        },
      )
    Reachability =>
      ignore(
        @reachability.Edge::from_json(payload) catch {
          error =>
            raise TaskError::Reachability(
              @reachability.EngineError::Reachability(error),
            )
        },
      )
    SecurityChain =>
      ignore(
        @security.SecurityEvent::from_json(payload) catch {
          error => raise TaskError::Security(error)
        },
      )
    Custom(_, _, _, _, _, _, _, _, _, _) => ()
  }
}
