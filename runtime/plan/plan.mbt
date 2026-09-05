///|
/// Errors raised while constructing a deterministic job topology.
pub(all) suberror PlanError {
  EmptyJobId
  EmptyOperatorId
  EmptyWorkerId
  DuplicateWorker(String)
  InvalidPartitionCount(Int)
  InvalidPartition(Int)
  InvalidOffset(Int64)
  InvalidExchangeRoute(source~ : Int, target~ : Int)
  MissingExchangeRoute(source~ : Int, target~ : Int)
} derive(Debug, Eq)

///|
/// A deterministic source partition lease before an assignment epoch is
/// installed by the Coordinator control plane.
pub(all) struct PartitionPlan {
  partition : Int
  worker_id : String
  start_offset : Int64
} derive(Debug, Eq)

///|
pub fn PartitionPlan::partition(self : PartitionPlan) -> Int {
  self.partition
}

///|
/// Result of routing one input key through a JobSpec.
pub(all) struct RouteTarget {
  partition : Int
  worker_id : String
} derive(Debug, Eq)

///|
pub fn RouteTarget::partition(self : RouteTarget) -> Int {
  self.partition
}

///|
pub fn RouteTarget::worker_id(self : RouteTarget) -> String {
  self.worker_id
}

///|
pub fn PartitionPlan::worker_id(self : PartitionPlan) -> String {
  self.worker_id
}

///|
pub fn PartitionPlan::start_offset(self : PartitionPlan) -> Int64 {
  self.start_offset
}

///|
/// One explicit cross-partition route. Routes are data, not implicit remote
/// state access, so a job can be audited before any Worker is started.
pub(all) struct ExchangeRoute {
  source_partition : Int
  target_partition : Int
} derive(Debug, Eq, Compare, Hash, ToJson, FromJson)

///|
pub fn ExchangeRoute::new(
  source_partition : Int,
  target_partition : Int,
) -> ExchangeRoute raise PlanError {
  if source_partition < 0 ||
    target_partition < 0 ||
    source_partition == target_partition {
    raise PlanError::InvalidExchangeRoute(
      source=source_partition,
      target=target_partition,
    )
  }
  { source_partition, target_partition, }
}

///|
pub fn ExchangeRoute::source_partition(self : ExchangeRoute) -> Int {
  self.source_partition
}

///|
pub fn ExchangeRoute::target_partition(self : ExchangeRoute) -> Int {
  self.target_partition
}

///|
/// The pure, deterministic part of a distributed job plan.
pub(all) struct JobSpec {
  job_id : String
  operator_id : String
  partition_count : Int
  workers : ReadOnlyArray[String]
  routes : ReadOnlyArray[ExchangeRoute]
} derive(Debug, Eq)

///|
pub fn JobSpec::new(
  job_id : String,
  operator_id : String,
  partition_count : Int,
  worker_ids : ArrayView[String],
  routes : ArrayView[ExchangeRoute],
) -> JobSpec raise PlanError {
  if job_id.is_empty() {
    raise PlanError::EmptyJobId
  }
  if operator_id.is_empty() {
    raise PlanError::EmptyOperatorId
  }
  if partition_count <= 0 {
    raise PlanError::InvalidPartitionCount(partition_count)
  }
  let unique : Set[String] = Set([])
  for worker_id in worker_ids {
    if worker_id.is_empty() {
      raise PlanError::EmptyWorkerId
    }
    if !unique.add_and_check(worker_id) {
      raise PlanError::DuplicateWorker(worker_id)
    }
  }
  if unique.is_empty() {
    raise PlanError::EmptyWorkerId
  }
  let sorted_workers = unique.to_array()
  sorted_workers.sort()
  let sorted_routes = routes.to_owned()
  sorted_routes.sort()
  let seen_routes : Set[ExchangeRoute] = Set([])
  for route in sorted_routes {
    if route.source_partition() >= partition_count ||
      route.target_partition() >= partition_count {
      raise PlanError::InvalidExchangeRoute(
        source=route.source_partition(),
        target=route.target_partition(),
      )
    }
    if !seen_routes.add_and_check(route) {
      raise PlanError::InvalidExchangeRoute(
        source=route.source_partition(),
        target=route.target_partition(),
      )
    }
  }
  {
    job_id,
    operator_id,
    partition_count,
    workers: ReadOnlyArray::from_array(sorted_workers),
    routes: ReadOnlyArray::from_array(sorted_routes),
  }
}

///|
pub fn JobSpec::job_id(self : JobSpec) -> String {
  self.job_id
}

///|
pub fn JobSpec::operator_id(self : JobSpec) -> String {
  self.operator_id
}

///|
pub fn JobSpec::partition_count(self : JobSpec) -> Int {
  self.partition_count
}

///|
pub fn JobSpec::workers(self : JobSpec) -> ArrayView[String] {
  self.workers[:]
}

///|
pub fn JobSpec::routes(self : JobSpec) -> ArrayView[ExchangeRoute] {
  self.routes[:]
}

///|
pub fn JobSpec::owner(
  self : JobSpec,
  partition : Int,
) -> String raise PlanError {
  if partition < 0 || partition >= self.partition_count {
    raise PlanError::InvalidPartition(partition)
  }
  self.workers[partition % self.workers.length()]
}

///|
/// Stable FNV-1a routing. This is a placement function, not a security hash.
pub fn JobSpec::partition_for_key(self : JobSpec, key : String) -> Int {
  let mut hash : UInt = 2166136261U
  for byte in @utf8.encode(key) {
    hash = (hash ^ byte.to_int().reinterpret_as_uint()) * 16777619U
  }
  (hash % self.partition_count.reinterpret_as_uint()).reinterpret_as_int()
}

///|
pub fn JobSpec::route_key(self : JobSpec, key : String) -> RouteTarget {
  let partition = self.partition_for_key(key)
  { partition, worker_id: self.workers[partition % self.workers.length()], }
}

///|
pub fn JobSpec::route_event(
  self : JobSpec,
  event : @protocol.EventRecord,
) -> RouteTarget {
  self.route_key(event.key())
}

///|
/// Return all explicitly declared targets for one source partition.
pub fn JobSpec::exchange_targets(
  self : JobSpec,
  source_partition : Int,
) -> Array[RouteTarget] raise PlanError {
  if source_partition < 0 || source_partition >= self.partition_count {
    raise PlanError::InvalidPartition(source_partition)
  }
  let targets = self.routes
    .filter(route => route.source_partition() == source_partition)
    .map(route => {
      let target = route.target_partition()
      RouteTarget::{
        partition: target,
        worker_id: self.workers[target % self.workers.length()],
      }
    })
    .view()
    .to_owned()
  targets.sort_by((left, right) => left.partition.compare(right.partition))
  targets
}

///|
pub fn JobSpec::plan_partitions(
  self : JobSpec,
  start_offset? : Int64 = 0L,
) -> Array[PartitionPlan] raise PlanError {
  if start_offset < 0L {
    raise PlanError::InvalidOffset(start_offset)
  }
  let result = []
  for partition in 0..<self.partition_count {
    result.push(PartitionPlan::{
      partition,
      worker_id: self.owner(partition),
      start_offset,
    })
  }
  result
}

///|
fn JobSpec::has_route(
  self : JobSpec,
  source_partition : Int,
  target_partition : Int,
) -> Bool {
  self.routes.any(route => {
    route.source_partition() == source_partition &&
    route.target_partition() == target_partition
  })
}

///|
fn stable_delta_id(
  job_id : String,
  operator_id : String,
  source_partition : Int,
  source_epoch : Int,
  target_partition : Int,
  target_epoch : Int,
  events : ArrayView[@protocol.EventRecord],
) -> String {
  let material = job_id +
    ":" +
    operator_id +
    ":" +
    source_partition.to_string() +
    ":" +
    source_epoch.to_string() +
    ":" +
    target_partition.to_string() +
    ":" +
    target_epoch.to_string() +
    ":" +
    events.map(event => @json.to_json(event).stringify()).join(",")
  let mut hash : UInt64 = 14695981039346656037UL
  for byte in @utf8.encode(material) {
    hash = (hash ^ byte.to_uint64()) * 1099511628211UL
  }
  "delta-" + hash.to_string(radix=16)
}

///|
/// Build an explicitly routed DeltaBatch with a stable identity. The route
/// must be declared in the job topology before data can cross partitions.
pub fn JobSpec::delta(
  self : JobSpec,
  source_partition : Int,
  source_epoch : Int,
  target_partition : Int,
  target_epoch : Int,
  events : ArrayView[@protocol.EventRecord],
) -> @protocol.DeltaBatch raise PlanError {
  if source_partition < 0 || source_partition >= self.partition_count {
    raise PlanError::InvalidPartition(source_partition)
  }
  if target_partition < 0 || target_partition >= self.partition_count {
    raise PlanError::InvalidPartition(target_partition)
  }
  if source_epoch < 0 || target_epoch < 0 {
    raise PlanError::InvalidOffset(
      if source_epoch < 0 {
        source_epoch.to_int64()
      } else {
        target_epoch.to_int64()
      },
    )
  }
  if events.is_empty() {
    raise PlanError::InvalidOffset(0L)
  }
  if !self.has_route(source_partition, target_partition) {
    raise PlanError::MissingExchangeRoute(
      source=source_partition,
      target=target_partition,
    )
  }
  @protocol.DeltaBatch::new(
    stable_delta_id(
      self.job_id,
      self.operator_id,
      source_partition,
      source_epoch,
      target_partition,
      target_epoch,
      events,
    ),
    source_partition,
    source_epoch,
    target_partition,
    target_epoch,
    events.to_owned(),
  ) catch {
    _ => raise PlanError::InvalidOffset(0L)
  }
}

///|
/// Expand one operator output batch to every explicitly declared target. The
/// returned order is deterministic by target partition.
pub fn JobSpec::route_batch(
  self : JobSpec,
  source_partition : Int,
  source_epoch : Int,
  target_epoch : Int,
  events : ArrayView[@protocol.EventRecord],
) -> Array[@protocol.DeltaBatch] raise PlanError {
  let targets = self.exchange_targets(source_partition)
  let result = []
  for target in targets {
    result.push(
      self.delta(
        source_partition,
        source_epoch,
        target.partition(),
        target_epoch,
        events,
      ),
    )
  }
  result
}
