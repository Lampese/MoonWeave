///|
/// Failures rejected at the causal log boundary.
pub(all) suberror CausalError {
  EmptyOrigin
  NegativeSequence(Int64)
  EmptyKey
  SelfDependency(EventId)
  DuplicateParent(EventId)
  IdentityConflict(EventId)
  InvalidPendingLimit(Int)
  PendingLimitExceeded(limit~ : Int)
  InvalidRetirement(String)
} derive(Debug, Eq)
