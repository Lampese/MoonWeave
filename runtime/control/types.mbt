///|
/// Lifecycle observed by the static Coordinator membership table.
pub(all) enum WorkerLifecycle {
  Active
  Suspect
  Fenced
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) enum RegisterStatus {
  Added
  Duplicate
  Refreshed
} derive(Debug, Eq)

///|
/// Failures raised by the synchronous Coordinator control state.
pub(all) suberror CoordinatorError {
  EmptyClusterId
  EmptyCoordinatorId
  EmptyWorkerId
  InvalidPartition(Int)
  InvalidOffset(Int64)
  InvalidEventCount(Int64)
  UnknownWorker(String)
  EmptyWorkerSet
  InvalidSnapshotVersion(Int)
  InvalidTick(Int64)
  InvalidHeartbeatTimeout(Int64)
  HeartbeatRegression(expected_at_least~ : Int64, actual~ : Int64)
  UnknownPartition(Int)
  WorkerNotActive(String)
  IncarnationMismatch
  IncarnationConflict
  StaleIncarnation
  StaleEpoch(expected~ : Int, actual~ : Int)
  AssignmentConflict(Int)
  InvalidSendOffset(expected~ : Int64, actual~ : Int64)
  OffsetRegression(expected_at_least~ : Int64, actual~ : Int64)
  AppliedAheadOfPersisted
  ProgressAheadOfSent
} derive(Debug, Eq)

///|
/// Immutable view of one registered Worker.
pub struct WorkerInfo {
  worker_id : String
  incarnation : @protocol.Incarnation
  capabilities : ReadOnlyArray[String]
  lifecycle : WorkerLifecycle
  last_heartbeat_tick : Int64?
} derive(Debug, Eq)

///|
pub fn WorkerInfo::worker_id(self : WorkerInfo) -> String {
  self.worker_id
}

///|
pub fn WorkerInfo::incarnation(self : WorkerInfo) -> @protocol.Incarnation {
  self.incarnation
}

///|
pub fn WorkerInfo::capabilities(self : WorkerInfo) -> ArrayView[String] {
  self.capabilities[:]
}

///|
pub fn WorkerInfo::lifecycle(self : WorkerInfo) -> WorkerLifecycle {
  self.lifecycle
}

///|
pub fn WorkerInfo::last_heartbeat_tick(self : WorkerInfo) -> Int64? {
  self.last_heartbeat_tick
}

///|
/// A partition lease and its monotone source/application cursors.
pub struct PartitionAssignment {
  partition : Int
  epoch : Int
  worker_id : String
  incarnation : @protocol.Incarnation
  start_offset : Int64
  mut next_offset : Int64
  mut persisted_offset : Int64
  mut applied_offset : Int64
} derive(Debug, Eq)

///|
pub fn PartitionAssignment::partition(self : PartitionAssignment) -> Int {
  self.partition
}

///|
pub fn PartitionAssignment::epoch(self : PartitionAssignment) -> Int {
  self.epoch
}

///|
pub fn PartitionAssignment::worker_id(self : PartitionAssignment) -> String {
  self.worker_id
}

///|
pub fn PartitionAssignment::incarnation(
  self : PartitionAssignment,
) -> @protocol.Incarnation {
  self.incarnation
}

///|
pub fn PartitionAssignment::start_offset(self : PartitionAssignment) -> Int64 {
  self.start_offset
}

///|
pub fn PartitionAssignment::next_offset(self : PartitionAssignment) -> Int64 {
  self.next_offset
}

///|
pub fn PartitionAssignment::persisted_offset(
  self : PartitionAssignment,
) -> Int64 {
  self.persisted_offset
}

///|
pub fn PartitionAssignment::applied_offset(self : PartitionAssignment) -> Int64 {
  self.applied_offset
}

///|
/// Durable membership record used by the async Coordinator store. The control
/// package exposes snapshots as data, while file and journal concerns remain
/// in `runtime/async`.
pub(all) struct WorkerSnapshot {
  worker_id : String
  incarnation : String
  capabilities : Array[String]
  lifecycle : WorkerLifecycle
  last_heartbeat_tick : Int64?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct AssignmentSnapshot {
  partition : Int
  epoch : Int
  worker_id : String
  incarnation : String
  start_offset : Int64
  next_offset : Int64
  persisted_offset : Int64
  applied_offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct EpochSnapshot {
  partition : Int
  epoch : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Complete deterministic view of Coordinator control state. Epoch counters
/// are included even for revoked partitions so fencing survives a restart.
pub(all) struct CoordinatorSnapshot {
  version : Int
  cluster_id : String
  coordinator_id : String
  workers : Array[WorkerSnapshot]
  assignments : Array[AssignmentSnapshot]
  epochs : Array[EpochSnapshot]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn CoordinatorSnapshot::workers(
  self : CoordinatorSnapshot,
) -> ArrayView[WorkerSnapshot] {
  self.workers[:]
}

///|
pub fn CoordinatorSnapshot::assignments(
  self : CoordinatorSnapshot,
) -> ArrayView[AssignmentSnapshot] {
  self.assignments[:]
}

///|
pub fn CoordinatorSnapshot::epochs(
  self : CoordinatorSnapshot,
) -> ArrayView[EpochSnapshot] {
  self.epochs[:]
}

///|
pub fn WorkerSnapshot::worker_id(self : WorkerSnapshot) -> String {
  self.worker_id
}

///|
pub fn WorkerSnapshot::lifecycle(self : WorkerSnapshot) -> WorkerLifecycle {
  self.lifecycle
}

///|
pub fn AssignmentSnapshot::partition(self : AssignmentSnapshot) -> Int {
  self.partition
}

///|
pub fn AssignmentSnapshot::epoch(self : AssignmentSnapshot) -> Int {
  self.epoch
}

///|
pub fn AssignmentSnapshot::worker_id(self : AssignmentSnapshot) -> String {
  self.worker_id
}

///|
pub fn AssignmentSnapshot::start_offset(self : AssignmentSnapshot) -> Int64 {
  self.start_offset
}

///|
pub fn AssignmentSnapshot::next_offset(self : AssignmentSnapshot) -> Int64 {
  self.next_offset
}

///|
pub fn AssignmentSnapshot::persisted_offset(self : AssignmentSnapshot) -> Int64 {
  self.persisted_offset
}

///|
pub fn AssignmentSnapshot::applied_offset(self : AssignmentSnapshot) -> Int64 {
  self.applied_offset
}
