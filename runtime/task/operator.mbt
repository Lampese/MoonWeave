///|
/// 内置算子的语义身份。这里只描述纯 Task 契约，不绑定 MoonBit 的某个后端。
pub(all) enum OperatorKind {
  Reachability
  SecurityChain
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn OperatorKind::id(self : OperatorKind) -> String {
  match self {
    Reachability => "reachability"
    SecurityChain => "security-chain"
  }
}

///|
pub fn OperatorKind::version(_self : OperatorKind) -> Int {
  1
}

///|
/// 显式维护的实现修订标识，不是代码二进制的加密哈希，也不是签名。
/// 修改会影响重放结果的算法时，必须同时升级此标识。
pub fn OperatorKind::code_digest(self : OperatorKind) -> String {
  "builtin:" + self.id() + ":v1"
}

///|
pub fn OperatorKind::state_schema(self : OperatorKind) -> String {
  match self {
    Reachability => "reachability-state:v1"
    SecurityChain => "security-chain-state:v1"
  }
}

///|
pub fn OperatorKind::input_schema(self : OperatorKind) -> String {
  match self {
    Reachability => "reachability-edge:v1"
    SecurityChain => "security-event:v1"
  }
}

///|
pub fn OperatorKind::output_schema(self : OperatorKind) -> String {
  match self {
    Reachability => "reachable:v1"
    SecurityChain => "security-alert:v1"
  }
}

///|
/// MoonWeave 的逻辑数据契约，不是 MoonBit native 函数/闭包 ABI。
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
/// 传输完整的修订与 schema 身份，避免只比较算子名。
/// token 用于可信集群中的兼容性检查，不认证 Worker 是否诚实。
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
/// 只检查 payload 的结构，不把缺失父事件等同于输入非法；因果约束仍由 Task 处理。
pub fn OperatorKind::validate_payload(
  self : OperatorKind,
  payload : Json,
) -> Unit raise TaskError {
  match self {
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
