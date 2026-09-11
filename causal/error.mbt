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
} derive(Debug, Eq)
