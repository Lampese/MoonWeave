///|
/// Portable identity for a compiled operator artifact. This metadata is
/// checked for compatibility; it contains no executable bytes. The signature
/// field is opaque, unverified metadata, NOT proof of authenticity.
pub struct OperatorArtifact {
  reference : OperatorRef
  code_digest : String
  runtime_abi : String
  target : String
  state_schema : String
  dependency_digest : String
  signature : String?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn OperatorArtifact::new(
  reference : OperatorRef,
  code_digest : String,
  runtime_abi : String,
  target : String,
  state_schema : String,
  dependency_digest : String,
  signature? : String,
) -> OperatorArtifact raise ApiError {
  reference.validate()
  if code_digest.is_empty() ||
    runtime_abi.is_empty() ||
    target.is_empty() ||
    state_schema.is_empty() ||
    dependency_digest.is_empty() {
    raise ApiError::InvalidArtifact("artifact metadata is incomplete")
  }
  match signature {
    Some(value) if value.is_empty() =>
      raise ApiError::InvalidArtifact("artifact signature is empty")
    _ => ()
  }
  {
    reference,
    code_digest,
    runtime_abi,
    target,
    state_schema,
    dependency_digest,
    signature,
  }
}

///|
pub fn OperatorArtifact::reference(self : OperatorArtifact) -> OperatorRef {
  self.reference
}

///|
pub fn OperatorArtifact::code_digest(self : OperatorArtifact) -> String {
  self.code_digest
}

///|
pub fn OperatorArtifact::runtime_abi(self : OperatorArtifact) -> String {
  self.runtime_abi
}

///|
pub fn OperatorArtifact::target(self : OperatorArtifact) -> String {
  self.target
}

///|
pub fn OperatorArtifact::state_schema(self : OperatorArtifact) -> String {
  self.state_schema
}

///|
pub fn OperatorArtifact::dependency_digest(self : OperatorArtifact) -> String {
  self.dependency_digest
}

///|
pub fn OperatorArtifact::signature(self : OperatorArtifact) -> String? {
  self.signature
}

///|
fn OperatorArtifact::validate(self : OperatorArtifact) -> Unit raise ApiError {
  self.reference.validate()
  if self.code_digest.is_empty() ||
    self.runtime_abi.is_empty() ||
    self.target.is_empty() ||
    self.state_schema.is_empty() ||
    self.dependency_digest.is_empty() {
    raise ApiError::InvalidArtifact("artifact metadata is incomplete")
  }
  match self.signature {
    Some(value) if value.is_empty() =>
      raise ApiError::InvalidArtifact("artifact signature is empty")
    _ => ()
  }
}

///|
/// Compare executable identity fields while deliberately ignoring the
/// transport/authentication signature.
fn OperatorArtifact::identity_equal(
  self : OperatorArtifact,
  other : OperatorArtifact,
) -> Bool {
  self.reference == other.reference &&
  self.code_digest == other.code_digest &&
  self.runtime_abi == other.runtime_abi &&
  self.target == other.target &&
  self.state_schema == other.state_schema &&
  self.dependency_digest == other.dependency_digest
}
