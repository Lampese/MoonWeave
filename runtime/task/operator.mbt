///|
/// Semantic identity of a built-in operator. This pure Task contract is
/// independent of a particular MoonBit backend.
pub(all) enum OperatorKind {
  KeyedSum
  Reachability
  SecurityChain
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn OperatorKind::id(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum"
    Reachability => "reachability"
    SecurityChain => "security-chain"
  }
}

///|
pub fn OperatorKind::version(_self : OperatorKind) -> Int {
  1
}

///|
/// Explicit implementation revision, not a cryptographic binary hash or a
/// signature. Bump it whenever replay semantics change.
pub fn OperatorKind::code_digest(self : OperatorKind) -> String {
  "builtin:" + self.id() + ":v1"
}

///|
pub fn OperatorKind::state_schema(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum-state:v1"
    Reachability => "reachability-state:v1"
    SecurityChain => "security-chain-state:v1"
  }
}

///|
pub fn OperatorKind::input_schema(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum-contribution:v1"
    Reachability => "reachability-edge:v1"
    SecurityChain => "security-event:v1"
  }
}

///|
pub fn OperatorKind::output_schema(self : OperatorKind) -> String {
  match self {
    KeyedSum => "keyed-sum-total:v1"
    Reachability => "reachable:v1"
    SecurityChain => "security-alert:v1"
  }
}

///|
/// MoonWeave logical data contract, not a MoonBit native function/closure ABI.
pub fn OperatorKind::runtime_abi(_self : OperatorKind) -> String {
  "moonweave-task-json-v1"
}

///|
pub fn OperatorKind::dependency_digest(_self : OperatorKind) -> String {
  "builtin-core-v1"
}

///|
pub fn OperatorKind::supports_proof(_self : OperatorKind) -> Bool {
  true
}

///|
/// Transport the complete revision and schema identity instead of comparing
/// only the operator name. The token checks compatibility in a trusted
/// cluster; it does not authenticate a Worker or prove that it is honest.
pub fn OperatorKind::capability(self : OperatorKind) -> String {
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
    "deterministic",
    "exchange",
    "task-replay-proof",
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
  }
}
