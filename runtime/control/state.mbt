///|
/// Synchronous static Coordinator control plane.
///
/// This type owns membership, assignment epochs, source send cursors, and
/// worker progress. It deliberately has no sockets, timers, persistence, or
/// retry loop; those are async adapters and policy above this state machine.
pub struct CoordinatorState {
  cluster_id : String
  coordinator_id : String
  workers : Map[String, WorkerRecord]
  assignments : Map[Int, PartitionAssignment]
  epochs : Map[Int, Int]
}

///|
struct WorkerRecord {
  worker_id : String
  incarnation : @protocol.Incarnation
  mut capabilities : ReadOnlyArray[String]
  mut lifecycle : WorkerLifecycle
  mut last_heartbeat_tick : Int64?
}

///|
fn WorkerRecord::info(self : WorkerRecord) -> WorkerInfo {
  {
    worker_id: self.worker_id,
    incarnation: self.incarnation,
    capabilities: self.capabilities,
    lifecycle: self.lifecycle,
    last_heartbeat_tick: self.last_heartbeat_tick,
  }
}

///|
pub fn CoordinatorState::new(
  cluster_id : String,
  coordinator_id : String,
) -> CoordinatorState raise CoordinatorError {
  if cluster_id.is_empty() {
    raise CoordinatorError::EmptyClusterId
  }
  if coordinator_id.is_empty() {
    raise CoordinatorError::EmptyCoordinatorId
  }
  {
    cluster_id,
    coordinator_id,
    workers: Map([]),
    assignments: Map([]),
    epochs: Map([]),
  }
}

///|
pub fn CoordinatorState::cluster_id(self : CoordinatorState) -> String {
  self.cluster_id
}

///|
pub fn CoordinatorState::coordinator_id(self : CoordinatorState) -> String {
  self.coordinator_id
}

///|
pub fn CoordinatorState::worker_count(self : CoordinatorState) -> Int {
  self.workers.length()
}

///|
pub fn CoordinatorState::partition_count(self : CoordinatorState) -> Int {
  self.assignments.length()
}

///|
pub fn CoordinatorState::worker(
  self : CoordinatorState,
  worker_id : String,
) -> WorkerInfo? {
  self.workers.get(worker_id).map(worker => worker.info())
}

///|
pub fn CoordinatorState::workers(self : CoordinatorState) -> Array[WorkerInfo] {
  let result = self.workers.values().map(worker => worker.info()).to_array()
  result.sort_by((left, right) => left.worker_id().compare(right.worker_id()))
  result
}

///|
/// Return current leases in partition order. The returned values are immutable
/// views, so callers can build a transport plan without mutating control state.
pub fn CoordinatorState::assignments(
  self : CoordinatorState,
) -> Array[PartitionAssignment] {
  let result = self.assignments.values().to_array()
  result.sort_by((left, right) => left.partition().compare(right.partition()))
  result
}

///|
/// Export a deterministic snapshot suitable for an async durable store.
pub fn CoordinatorState::snapshot(
  self : CoordinatorState,
) -> CoordinatorSnapshot {
  let workers = self.workers
    .values()
    .map(worker => WorkerSnapshot::{
      worker_id: worker.worker_id,
      incarnation: worker.incarnation.value(),
      capabilities: worker.capabilities[:].to_owned(),
      lifecycle: worker.lifecycle,
      last_heartbeat_tick: worker.last_heartbeat_tick,
    })
    .to_array()
  workers.sort_by((left, right) => left.worker_id.compare(right.worker_id))
  let assignments = self.assignments
    .values()
    .map(assignment => AssignmentSnapshot::{
      partition: assignment.partition,
      epoch: assignment.epoch,
      worker_id: assignment.worker_id,
      incarnation: assignment.incarnation.value(),
      start_offset: assignment.start_offset,
      next_offset: assignment.next_offset,
      persisted_offset: assignment.persisted_offset,
      applied_offset: assignment.applied_offset,
    })
    .to_array()
  assignments.sort_by((left, right) => left.partition.compare(right.partition))
  let epochs = self.epochs
    .to_array()
    .map(entry => {
      let (partition, epoch) = entry
      EpochSnapshot::{ partition, epoch, }
    })
  epochs.sort_by((left, right) => left.partition.compare(right.partition))
  {
    version: 1,
    cluster_id: self.cluster_id,
    coordinator_id: self.coordinator_id,
    workers,
    assignments,
    epochs,
  }
}

///|
/// Restore control state after validating every snapshot invariant. This is a
/// synchronous constructor; the async store is responsible for reading bytes.
pub fn CoordinatorState::from_snapshot(
  snapshot : CoordinatorSnapshot,
) -> CoordinatorState raise CoordinatorError {
  if snapshot.version != 1 {
    raise CoordinatorError::InvalidSnapshotVersion(snapshot.version)
  }
  let state = CoordinatorState::new(
    snapshot.cluster_id,
    snapshot.coordinator_id,
  )
  for epoch in snapshot.epochs {
    if epoch.partition < 0 || epoch.epoch < 0 {
      raise CoordinatorError::InvalidPartition(epoch.partition)
    }
    state.epochs[epoch.partition] = epoch.epoch
  }
  for worker in snapshot.workers {
    let incarnation = @protocol.Incarnation::new(worker.incarnation) catch {
      _ => raise CoordinatorError::StaleIncarnation
    }
    if worker.worker_id.is_empty() {
      raise CoordinatorError::EmptyWorkerId
    }
    guard !state.workers.contains(worker.worker_id) else {
      raise CoordinatorError::IncarnationConflict
    }
    state.workers[worker.worker_id] = WorkerRecord::{
      worker_id: worker.worker_id,
      incarnation,
      capabilities: ReadOnlyArray::from_array(worker.capabilities),
      lifecycle: worker.lifecycle,
      last_heartbeat_tick: worker.last_heartbeat_tick,
    }
  }
  for assignment in snapshot.assignments {
    if assignment.partition < 0 {
      raise CoordinatorError::InvalidPartition(assignment.partition)
    }
    if assignment.epoch < 0 ||
      assignment.start_offset < 0L ||
      assignment.next_offset < assignment.start_offset ||
      assignment.persisted_offset < assignment.start_offset - 1L ||
      assignment.applied_offset < assignment.start_offset - 1L ||
      assignment.applied_offset > assignment.persisted_offset ||
      assignment.persisted_offset >= assignment.next_offset ||
      assignment.applied_offset >= assignment.next_offset {
      raise CoordinatorError::InvalidOffset(assignment.next_offset)
    }
    let worker = state.checked_worker(assignment.worker_id)
    let incarnation = @protocol.Incarnation::new(assignment.incarnation) catch {
      _ => raise CoordinatorError::StaleIncarnation
    }
    if worker.incarnation != incarnation {
      raise CoordinatorError::StaleIncarnation
    }
    state.assignments[assignment.partition] = PartitionAssignment::{
      partition: assignment.partition,
      epoch: assignment.epoch,
      worker_id: assignment.worker_id,
      incarnation,
      start_offset: assignment.start_offset,
      next_offset: assignment.next_offset,
      persisted_offset: assignment.persisted_offset,
      applied_offset: assignment.applied_offset,
    }
    match state.epochs.get(assignment.partition) {
      Some(epoch) if epoch >= assignment.epoch => ()
      _ => state.epochs[assignment.partition] = assignment.epoch
    }
  }
  state
}

///|
fn CoordinatorState::checked_worker(
  self : CoordinatorState,
  worker_id : String,
) -> WorkerRecord raise CoordinatorError {
  match self.workers.get(worker_id) {
    Some(worker) => worker
    None => raise CoordinatorError::UnknownWorker(worker_id)
  }
}

///|
/// Register a worker. Same active incarnation registration is idempotent; a
/// new incarnation is accepted only after the old one was explicitly fenced.
pub fn CoordinatorState::register_worker(
  self : CoordinatorState,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  capabilities : ArrayView[String],
) -> RegisterStatus raise CoordinatorError {
  if worker_id.is_empty() {
    raise CoordinatorError::EmptyWorkerId
  }
  match self.workers.get(worker_id) {
    None => {
      self.workers[worker_id] = WorkerRecord::{
        worker_id,
        incarnation,
        capabilities: ReadOnlyArray::from_array(capabilities.to_owned()),
        lifecycle: WorkerLifecycle::Active,
        last_heartbeat_tick: None,
      }
      Added
    }
    Some(previous) =>
      if previous.incarnation == incarnation {
        if previous.lifecycle is WorkerLifecycle::Fenced {
          raise CoordinatorError::StaleIncarnation
        }
        previous.lifecycle = WorkerLifecycle::Active
        previous.capabilities = ReadOnlyArray::from_array(
          capabilities.to_owned(),
        )
        Duplicate
      } else {
        if previous.lifecycle != WorkerLifecycle::Fenced {
          raise CoordinatorError::IncarnationConflict
        }
        self.workers[worker_id] = WorkerRecord::{
          worker_id,
          incarnation,
          capabilities: ReadOnlyArray::from_array(capabilities.to_owned()),
          lifecycle: WorkerLifecycle::Active,
          last_heartbeat_tick: None,
        }
        Refreshed
      }
  }
}

///|
pub fn CoordinatorState::set_suspect(
  self : CoordinatorState,
  worker_id : String,
) -> Unit raise CoordinatorError {
  let worker = self.checked_worker(worker_id)
  if worker.lifecycle is WorkerLifecycle::Fenced {
    return
  }
  worker.lifecycle = WorkerLifecycle::Suspect
}

///|
/// Record a heartbeat using a caller-owned monotone clock. A heartbeat from a
/// suspect worker reactivates observation, but never resurrects a fenced one.
pub fn CoordinatorState::record_heartbeat(
  self : CoordinatorState,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  tick : Int64,
) -> Unit raise CoordinatorError {
  if tick < 0L {
    raise CoordinatorError::InvalidTick(tick)
  }
  let worker = self.checked_worker(worker_id)
  if worker.incarnation != incarnation {
    raise CoordinatorError::StaleIncarnation
  }
  if worker.lifecycle is WorkerLifecycle::Fenced {
    raise CoordinatorError::WorkerNotActive(worker_id)
  }
  match worker.last_heartbeat_tick {
    Some(previous) if tick < previous =>
      raise CoordinatorError::HeartbeatRegression(
        expected_at_least=previous,
        actual=tick,
      )
    _ => ()
  }
  worker.last_heartbeat_tick = Some(tick)
  if worker.lifecycle is WorkerLifecycle::Suspect {
    worker.lifecycle = WorkerLifecycle::Active
  }
}

///|
/// Mark workers whose last heartbeat exceeded `timeout` as suspect. This method
/// never fences or revokes an assignment; the caller must make that decision.
pub fn CoordinatorState::expire_workers(
  self : CoordinatorState,
  now_tick : Int64,
  timeout : Int64,
) -> Array[String] raise CoordinatorError {
  if now_tick < 0L {
    raise CoordinatorError::InvalidTick(now_tick)
  }
  if timeout <= 0L {
    raise CoordinatorError::InvalidHeartbeatTimeout(timeout)
  }
  let stale = []
  for worker in self.workers.values() {
    match worker.last_heartbeat_tick {
      Some(last) if now_tick >= last && now_tick - last >= timeout =>
        if worker.lifecycle is WorkerLifecycle::Active {
          worker.lifecycle = WorkerLifecycle::Suspect
          stale.push(worker.worker_id)
        }
      _ => ()
    }
  }
  stale.sort()
  stale
}

///|
pub fn CoordinatorState::fence_worker(
  self : CoordinatorState,
  worker_id : String,
  incarnation : @protocol.Incarnation,
) -> Unit raise CoordinatorError {
  let worker = self.checked_worker(worker_id)
  if worker.incarnation != incarnation {
    raise CoordinatorError::StaleIncarnation
  }
  worker.lifecycle = WorkerLifecycle::Fenced
  let to_revoke = self.assignments
    .values()
    .filter(assignment => assignment.worker_id() == worker_id)
    .map(assignment => assignment.partition())
    .to_array()
  for partition in to_revoke {
    self.revoke(partition)
  }
}

///|
fn CoordinatorState::next_epoch(
  self : CoordinatorState,
  partition : Int,
) -> Int {
  match self.epochs.get(partition) {
    None => 0
    Some(previous) => previous + 1
  }
}

///|
/// Install or idempotently recover a partition lease. Replacing a lease always
/// advances its epoch, fencing frames from the previous owner.
pub fn CoordinatorState::assign(
  self : CoordinatorState,
  partition : Int,
  worker_id : String,
  start_offset? : Int64 = 0L,
) -> PartitionAssignment raise CoordinatorError {
  if partition < 0 {
    raise CoordinatorError::InvalidPartition(partition)
  }
  if start_offset < 0L {
    raise CoordinatorError::InvalidOffset(start_offset)
  }
  let worker = self.checked_worker(worker_id)
  if worker.lifecycle != WorkerLifecycle::Active {
    raise CoordinatorError::WorkerNotActive(worker_id)
  }
  match self.assignments.get(partition) {
    Some(current) => {
      if current.worker_id() == worker_id &&
        current.incarnation() == worker.incarnation &&
        current.start_offset() == start_offset {
        return current
      }
      raise CoordinatorError::AssignmentConflict(partition)
    }
    None => ()
  }
  let epoch = self.next_epoch(partition)
  self.epochs[partition] = epoch
  let assignment : PartitionAssignment = {
    partition,
    epoch,
    worker_id,
    incarnation: worker.incarnation,
    start_offset,
    next_offset: start_offset,
    persisted_offset: start_offset - 1L,
    applied_offset: start_offset - 1L,
  }
  self.assignments[partition] = assignment
  assignment
}

///|
/// Deterministically assign partitions to active workers in sorted
/// round-robin order. Duplicate worker IDs are collapsed before planning.
pub fn CoordinatorState::assign_round_robin(
  self : CoordinatorState,
  partition_count : Int,
  worker_ids : ArrayView[String],
  start_offset? : Int64 = 0L,
) -> Array[PartitionAssignment] raise CoordinatorError {
  if partition_count < 0 {
    raise CoordinatorError::InvalidPartition(partition_count)
  }
  if start_offset < 0L {
    raise CoordinatorError::InvalidOffset(start_offset)
  }
  let unique : Set[String] = Set([])
  for worker_id in worker_ids {
    if worker_id.is_empty() {
      raise CoordinatorError::EmptyWorkerId
    }
    unique.add(worker_id)
  }
  if partition_count > 0 && unique.is_empty() {
    raise CoordinatorError::EmptyWorkerSet
  }
  let sorted_workers = unique.to_array()
  sorted_workers.sort()
  let result = []
  for partition in 0..<partition_count {
    let worker_id = sorted_workers[partition % sorted_workers.length()]
    result.push(self.assign(partition, worker_id, start_offset~))
  }
  result
}

///|
pub fn CoordinatorState::assignment(
  self : CoordinatorState,
  partition : Int,
) -> PartitionAssignment? {
  self.assignments.get(partition)
}

///|
/// Return the current source cursor after validating assignment ownership.
pub fn CoordinatorState::next_offset(
  self : CoordinatorState,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
) -> Int64 raise CoordinatorError {
  let assignment = self.check_assignment(
    partition, worker_id, incarnation, epoch,
  )
  assignment.next_offset()
}

///|
/// Admit a worker resume cursor. The cursor must refer to data already sent.
/// The source cursor is rewound to the first input after that persisted offset,
/// so a Coordinator that crashed before recording an ACK can safely resend the
/// unconfirmed suffix.
pub fn CoordinatorState::record_resume(
  self : CoordinatorState,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  persisted_offset : Int64,
) -> Unit raise CoordinatorError {
  let assignment = self.check_assignment(
    partition, worker_id, incarnation, epoch,
  )
  if persisted_offset < -1L {
    raise CoordinatorError::InvalidOffset(persisted_offset)
  }
  if persisted_offset < assignment.persisted_offset() {
    raise CoordinatorError::OffsetRegression(
      expected_at_least=assignment.persisted_offset(),
      actual=persisted_offset,
    )
  }
  if persisted_offset < assignment.applied_offset() {
    raise CoordinatorError::OffsetRegression(
      expected_at_least=assignment.applied_offset(),
      actual=persisted_offset,
    )
  }
  if persisted_offset >= assignment.next_offset() {
    raise CoordinatorError::ProgressAheadOfSent
  }
  assignment.next_offset = persisted_offset + 1L
  if persisted_offset > assignment.persisted_offset() {
    assignment.persisted_offset = persisted_offset
  }
}

///|
/// Revoke a lease. The next assignment advances the partition epoch fence.
pub fn CoordinatorState::revoke(
  self : CoordinatorState,
  partition : Int,
) -> Unit raise CoordinatorError {
  if partition < 0 {
    raise CoordinatorError::InvalidPartition(partition)
  }
  guard self.assignments.contains(partition) else {
    raise CoordinatorError::UnknownPartition(partition)
  }
  self.assignments.remove(partition)
}

///|
/// Replace a revoked partition lease with an active Worker. The old lease must
/// already be absent, so every reassignment advances the epoch fence exactly
/// once and stale output cannot be accepted by accident.
pub fn CoordinatorState::reassign(
  self : CoordinatorState,
  partition : Int,
  worker_id : String,
  start_offset? : Int64 = 0L,
) -> PartitionAssignment raise CoordinatorError {
  if self.assignments.contains(partition) {
    raise CoordinatorError::AssignmentConflict(partition)
  }
  self.assign(partition, worker_id, start_offset~)
}

///|
/// Fence a failed Worker and reassign all of its leases to one active
/// replacement. Reassignment replays from each lease's original start offset;
/// at-least-once delivery and operator idempotence make this conservative.
pub fn CoordinatorState::fence_and_reassign(
  self : CoordinatorState,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  replacement_worker_id : String,
) -> Array[PartitionAssignment] raise CoordinatorError {
  let replacement = self.checked_worker(replacement_worker_id)
  if replacement.lifecycle != WorkerLifecycle::Active {
    raise CoordinatorError::WorkerNotActive(replacement_worker_id)
  }
  if worker_id == replacement_worker_id {
    raise CoordinatorError::AssignmentConflict(-1)
  }
  let leases = self.assignments
    .values()
    .filter(assignment => {
      assignment.worker_id() == worker_id &&
      assignment.incarnation() == incarnation
    })
    .to_array()
  self.fence_worker(worker_id, incarnation)
  leases.sort_by((left, right) => left.partition().compare(right.partition()))
  let reassigned = []
  for lease in leases {
    reassigned.push(
      self.reassign(
        lease.partition(),
        replacement_worker_id,
        start_offset=lease.start_offset(),
      ),
    )
  }
  reassigned
}

///|
fn CoordinatorState::check_assignment(
  self : CoordinatorState,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
) -> PartitionAssignment raise CoordinatorError {
  guard partition >= 0 else {
    raise CoordinatorError::InvalidPartition(partition)
  }
  let assignment = match self.assignments.get(partition) {
    Some(value) => value
    None => raise CoordinatorError::UnknownPartition(partition)
  }
  if assignment.epoch() != epoch {
    raise CoordinatorError::StaleEpoch(
      expected=assignment.epoch(),
      actual=epoch,
    )
  }
  if assignment.worker_id() != worker_id {
    raise CoordinatorError::IncarnationMismatch
  }
  if assignment.incarnation() != incarnation {
    raise CoordinatorError::StaleIncarnation
  }
  assignment
}

///|
/// Record a batch reservation before it is written to the socket.
pub fn CoordinatorState::record_batch_sent(
  self : CoordinatorState,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  first_offset : Int64,
  event_count : Int64,
) -> Int64 raise CoordinatorError {
  if event_count <= 0L {
    raise CoordinatorError::InvalidEventCount(event_count)
  }
  let assignment = self.check_assignment(
    partition, worker_id, incarnation, epoch,
  )
  if first_offset != assignment.next_offset() {
    raise CoordinatorError::InvalidSendOffset(
      expected=assignment.next_offset(),
      actual=first_offset,
    )
  }
  let end_offset = first_offset + event_count - 1L
  assignment.next_offset += event_count
  end_offset
}

///|
/// Record monotone worker progress, rejecting claims beyond the sent cursor.
pub fn CoordinatorState::record_progress(
  self : CoordinatorState,
  partition : Int,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise CoordinatorError {
  let assignment = self.check_assignment(
    partition, worker_id, incarnation, epoch,
  )
  if persisted_offset < -1L || applied_offset < -1L {
    raise CoordinatorError::InvalidOffset(
      if persisted_offset < -1L {
        persisted_offset
      } else {
        applied_offset
      },
    )
  }
  if persisted_offset < assignment.persisted_offset() {
    raise CoordinatorError::OffsetRegression(
      expected_at_least=assignment.persisted_offset(),
      actual=persisted_offset,
    )
  }
  if applied_offset < assignment.applied_offset() {
    raise CoordinatorError::OffsetRegression(
      expected_at_least=assignment.applied_offset(),
      actual=applied_offset,
    )
  }
  if applied_offset > persisted_offset {
    raise CoordinatorError::AppliedAheadOfPersisted
  }
  if persisted_offset >= assignment.next_offset() ||
    applied_offset >= assignment.next_offset() {
    raise CoordinatorError::ProgressAheadOfSent
  }
  assignment.persisted_offset = persisted_offset
  assignment.applied_offset = applied_offset
}
