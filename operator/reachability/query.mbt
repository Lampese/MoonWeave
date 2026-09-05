///|
/// Knowledge state exposed by a reachability query.
pub(all) enum QueryStatus {
  Tentative
  Stable
  ResolvedNoMatch
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) suberror QueryError {
  EmptyQueryId
  EmptyNode
  EmptyDigest
  MissingQuery
  NotResolved
  InvalidPartition(Int)
} derive(Debug, Eq)

///|
/// One partition that can still change the answer to a query.
pub(all) struct CoordinationObligation {
  query_id : String
  partition : Int
  reason : String
} derive(Debug, Eq, Compare, Hash)

///|
pub fn CoordinationObligation::new(
  query_id : String,
  partition : Int,
  reason : String,
) -> CoordinationObligation {
  { query_id, partition, reason, }
}

///|
pub fn CoordinationObligation::query_id(
  self : CoordinationObligation,
) -> String {
  self.query_id
}

///|
pub fn CoordinationObligation::partition(self : CoordinationObligation) -> Int {
  self.partition
}

///|
pub fn CoordinationObligation::reason(self : CoordinationObligation) -> String {
  self.reason
}

///|
/// A targeted reachability query with explicit open-world obligations.
///
/// `matched` is monotone, while obligations close only when the caller has a
/// verified frontier for that partition. Therefore an empty current result is
/// never reported as a definitive negative while any obligation remains open.
pub struct Query {
  query_id : String
  source : String
  target : String
  reason : String
  mut matched : Bool
  obligations : Set[Int]
  open : Set[Int]
}

///|
pub fn Query::new(
  query_id : String,
  source : String,
  target : String,
  partitions : ArrayView[Int],
  reason? : String = "frontier-open",
) -> Query raise QueryError {
  if query_id.is_empty() {
    raise QueryError::EmptyQueryId
  }
  if source.is_empty() || target.is_empty() {
    raise QueryError::EmptyNode
  }
  let obligations : Set[Int] = Set([])
  for partition in partitions {
    if partition < 0 {
      raise QueryError::InvalidPartition(partition)
    }
    obligations.add(partition)
  }
  {
    query_id,
    source,
    target,
    reason,
    matched: false,
    obligations,
    open: obligations.copy(),
  }
}

///|
pub fn Query::copy(self : Query) -> Query {
  {
    query_id: self.query_id,
    source: self.source,
    target: self.target,
    reason: self.reason,
    matched: self.matched,
    obligations: self.obligations.copy(),
    open: self.open.copy(),
  }
}

///|
pub fn Query::query_id(self : Query) -> String {
  self.query_id
}

///|
pub fn Query::source(self : Query) -> String {
  self.source
}

///|
pub fn Query::target(self : Query) -> String {
  self.target
}

///|
pub fn Query::matched(self : Query) -> Bool {
  self.matched
}

///|
pub fn Query::status(self : Query) -> QueryStatus {
  if !self.open.is_empty() {
    Tentative
  } else if self.matched {
    Stable
  } else {
    ResolvedNoMatch
  }
}

///|
/// Return currently open obligations in deterministic partition order.
pub fn Query::obligations(self : Query) -> Array[CoordinationObligation] {
  let partitions = self.open.to_array()
  partitions.sort()
  partitions.map(partition => {
    CoordinationObligation::new(self.query_id, partition, self.reason)
  })
}

///|
/// Observe a matching derived fact. Nonmatching facts are ignored.
pub fn Query::observe(self : Query, fact : Reachable) -> Bool {
  if fact.source() == self.source && fact.target() == self.target {
    self.matched = true
    true
  } else {
    false
  }
}

///|
/// Close one partition's obligation after its target frontier is verified.
/// Repeated closes are idempotent, which is required for at-least-once probes.
pub fn Query::close_partition(self : Query, partition : Int) -> Bool {
  if self.open.contains(partition) {
    self.open.remove(partition)
    true
  } else {
    false
  }
}

///|
/// Construct a deterministic certificate for a resolved query. The digest is
/// supplied by the runtime's verified ProbeReply; this package does not assume
/// a particular hashing algorithm.
pub fn Query::certificate(
  self : Query,
  frontier_digest : String,
  supporting_events : ArrayView[@causal.EventId],
) -> ResultCertificate raise QueryError {
  if frontier_digest.is_empty() {
    raise QueryError::EmptyDigest
  }
  let status = self.status()
  if status is QueryStatus::Tentative {
    raise QueryError::NotResolved
  }
  ResultCertificate::new(
    self.query_id,
    self.source,
    self.target,
    status,
    frontier_digest,
    supporting_events,
  )
}

///|
/// A portable proof record for a resolved reachability query.
pub(all) struct ResultCertificate {
  query_id : String
  source : String
  target : String
  status : QueryStatus
  result_digest : String
  frontier_digest : String
  supporting_events : Array[@causal.EventId]
} derive(Debug, Eq, ToJson, FromJson)

///|
fn result_digest(
  status : QueryStatus,
  source : String,
  target : String,
) -> String {
  match status {
    QueryStatus::Stable => "match:" + source + "->" + target
    QueryStatus::ResolvedNoMatch => "no-match:" + source + "->" + target
    QueryStatus::Tentative => "tentative"
  }
}

///|
fn ResultCertificate::new(
  query_id : String,
  source : String,
  target : String,
  status : QueryStatus,
  frontier_digest : String,
  supporting_events : ArrayView[@causal.EventId],
) -> ResultCertificate raise QueryError {
  if query_id.is_empty() {
    raise QueryError::EmptyQueryId
  }
  if source.is_empty() || target.is_empty() {
    raise QueryError::EmptyNode
  }
  if frontier_digest.is_empty() {
    raise QueryError::EmptyDigest
  }
  if status is QueryStatus::Tentative {
    raise QueryError::NotResolved
  }
  let events = supporting_events.to_owned()
  events.sort()
  {
    query_id,
    source,
    target,
    status,
    result_digest: result_digest(status, source, target),
    frontier_digest,
    supporting_events: events,
  }
}

///|
pub fn ResultCertificate::query_id(self : ResultCertificate) -> String {
  self.query_id
}

///|
pub fn ResultCertificate::source(self : ResultCertificate) -> String {
  self.source
}

///|
pub fn ResultCertificate::target(self : ResultCertificate) -> String {
  self.target
}

///|
pub fn ResultCertificate::status(self : ResultCertificate) -> QueryStatus {
  self.status
}

///|
pub fn ResultCertificate::result_digest(self : ResultCertificate) -> String {
  self.result_digest
}

///|
pub fn ResultCertificate::frontier_digest(self : ResultCertificate) -> String {
  self.frontier_digest
}

///|
pub fn ResultCertificate::supporting_events(
  self : ResultCertificate,
) -> ArrayView[@causal.EventId] {
  self.supporting_events[:]
}

///|
/// Verify the certificate against the current query state and a separately
/// obtained frontier digest. A false result never mutates query state.
pub fn ResultCertificate::verify(
  self : ResultCertificate,
  query : Query,
  frontier_digest : String,
) -> Bool {
  self.query_id == query.query_id() &&
  self.source == query.source() &&
  self.target == query.target() &&
  self.status == query.status() &&
  self.status != QueryStatus::Tentative &&
  self.frontier_digest == frontier_digest &&
  self.result_digest == result_digest(self.status, self.source, self.target)
}
