///|
/// Errors raised by the replicated Coordinator control store.
pub(all) suberror CoordinatorHaError {
  InvalidReplicaCount(Int)
  InvalidQuorum(Int)
  EmptyReplicaId
  DuplicateReplica(String)
  DuplicatePath(String)
  NoQuorum(required~ : Int, available~ : Int)
  Closed
  Replica(index~ : Int, reason~ : String)
  Store(CoordinatorStoreError)
} derive(Debug, Eq)

///|
/// Durable identity and paths for one independent Coordinator replica. Replica
/// directories must be on independent failure domains in a deployment; the
/// quorum layer does not pretend that two files on one disk are HA.
pub(all) struct CoordinatorReplicaConfig {
  replica_id : String
  journal_path : String
  checkpoint_path : String?
} derive(Debug, Eq)

///|
pub fn CoordinatorReplicaConfig::new(
  replica_id : String,
  journal_path : String,
  checkpoint_path? : String,
) -> CoordinatorReplicaConfig raise CoordinatorHaError {
  if replica_id.is_empty() {
    raise CoordinatorHaError::EmptyReplicaId
  }
  if journal_path.is_empty() {
    raise CoordinatorHaError::DuplicatePath(journal_path)
  }
  { replica_id, journal_path, checkpoint_path, }
}

///|
pub fn CoordinatorReplicaConfig::replica_id(
  self : CoordinatorReplicaConfig,
) -> String {
  self.replica_id
}

///|
pub fn CoordinatorReplicaConfig::journal_path(
  self : CoordinatorReplicaConfig,
) -> String {
  self.journal_path
}

///|
pub fn CoordinatorReplicaConfig::checkpoint_path(
  self : CoordinatorReplicaConfig,
) -> String? {
  self.checkpoint_path
}

///|
/// A small synchronous view used by operators and readiness checks.
pub(all) struct CoordinatorReplicaStatus {
  replica_id : String
  available : Bool
} derive(Debug, Eq)

///|
pub fn CoordinatorReplicaStatus::replica_id(
  self : CoordinatorReplicaStatus,
) -> String {
  self.replica_id
}

///|
pub fn CoordinatorReplicaStatus::available(
  self : CoordinatorReplicaStatus,
) -> Bool {
  self.available
}

///|
/// Quorum-replicated control state. Every accepted mutation is written to at
/// least `quorum` independent CoordinatorStore WALs before the caller sees
/// success. A failed replica is fenced from subsequent writes until it is
/// explicitly reopened and caught up from its own WAL.
pub struct CoordinatorHa {
  cluster_id : String
  coordinator_id : String
  configs : Array[CoordinatorReplicaConfig]
  stores : Array[CoordinatorStore]
  active : Array[Bool]
  quorum : Int
  mut leader : Int
  mut closed : Bool
}

///|
pub async fn CoordinatorHa::open(
  cluster_id : String,
  coordinator_id : String,
  replicas : ArrayView[CoordinatorReplicaConfig],
  quorum? : Int,
  maximum_bytes? : Int64,
) -> CoordinatorHa raise CoordinatorHaError {
  let configs = replicas.to_owned()
  if configs.length() < 3 {
    raise CoordinatorHaError::InvalidReplicaCount(configs.length())
  }
  let required = quorum.unwrap_or(configs.length() / 2 + 1)
  if required <= configs.length() / 2 || required > configs.length() {
    raise CoordinatorHaError::InvalidQuorum(required)
  }
  let ids : Set[String] = Set([])
  let paths : Set[String] = Set([])
  for config in configs {
    if !ids.add_and_check(config.replica_id) {
      raise CoordinatorHaError::DuplicateReplica(config.replica_id)
    }
    if !paths.add_and_check(config.journal_path) {
      raise CoordinatorHaError::DuplicatePath(config.journal_path)
    }
  }
  let stores : Array[CoordinatorStore] = []
  for config in configs {
    let store = CoordinatorStore::open(
      cluster_id,
      coordinator_id,
      config.journal_path,
      checkpoint_path?=config.checkpoint_path,
      maximum_bytes?,
    ) catch {
      error => {
        for opened in stores {
          opened.close()
        }
        raise CoordinatorHaError::Store(error)
      }
    }
    stores.push(store)
  }
  let active : Array[Bool] = []
  for _ in stores {
    active.push(true)
  }
  {
    cluster_id,
    coordinator_id,
    configs,
    stores,
    active,
    quorum: required,
    leader: 0,
    closed: false,
  }
}

///|
pub fn CoordinatorHa::cluster_id(self : CoordinatorHa) -> String {
  self.cluster_id
}

///|
pub fn CoordinatorHa::coordinator_id(self : CoordinatorHa) -> String {
  self.coordinator_id
}

///|
pub fn CoordinatorHa::quorum(self : CoordinatorHa) -> Int {
  self.quorum
}

///|
pub fn CoordinatorHa::available_replicas(self : CoordinatorHa) -> Int {
  self.active.filter(value => value).length()
}

///|
/// Return the currently elected local leader. This is a local election over
/// the admitted stores; it is deliberately not presented as a replacement for
/// a cross-process term/fencing protocol.
pub fn CoordinatorHa::leader_index(
  self : CoordinatorHa,
) -> Int raise CoordinatorHaError {
  self.check_open()
  self.ensure_leader()
  self.leader
}

///|
pub fn CoordinatorHa::leader_id(
  self : CoordinatorHa,
) -> String raise CoordinatorHaError {
  self.check_open()
  self.ensure_leader()
  self.configs[self.leader].replica_id
}

///|
pub fn CoordinatorHa::statuses(
  self : CoordinatorHa,
) -> Array[CoordinatorReplicaStatus] {
  let result : Array[CoordinatorReplicaStatus] = []
  for index in 0..<self.configs.length() {
    let config = self.configs[index]
    result.push({
      replica_id: config.replica_id,
      available: self.active[index],
    })
  }
  result
}

///|
pub fn CoordinatorHa::snapshot(
  self : CoordinatorHa,
) -> @control.CoordinatorSnapshot raise CoordinatorHaError {
  self.check_open()
  self.ensure_leader()
  self.stores[self.leader].snapshot()
}

///|
fn CoordinatorHa::check_open(
  self : CoordinatorHa,
) -> Unit raise CoordinatorHaError {
  if self.closed {
    raise CoordinatorHaError::Closed
  }
}

///|
fn CoordinatorHa::check_quorum(
  self : CoordinatorHa,
) -> Unit raise CoordinatorHaError {
  self.check_open()
  self.ensure_leader()
  let available = self.available_replicas()
  if available < self.quorum {
    raise CoordinatorHaError::NoQuorum(required=self.quorum, available~)
  }
}

///|
/// Choose the first admitted replica when the previous leader was fenced. The
/// ordering is stable, so every process with the same membership view makes a
/// deterministic choice. A real deployment still needs a replicated term and
/// fencing token before this can be used across processes.
fn CoordinatorHa::ensure_leader(
  self : CoordinatorHa,
) -> Unit raise CoordinatorHaError {
  if self.active[self.leader] {
    return
  }
  for index in 0..<self.active.length() {
    if self.active[index] {
      self.leader = index
      return
    }
  }
  raise CoordinatorHaError::NoQuorum(required=self.quorum, available=0)
}

///|
/// Restore the last committed view after a mutation failed to reach quorum.
/// Stores whose WAL has become uncertain are fenced and must be recovered from
/// an admitted peer before they can accept another write.
async fn CoordinatorHa::rollback_failed_write(
  self : CoordinatorHa,
  snapshot : @control.CoordinatorSnapshot,
) -> Unit noraise {
  for index in 0..<self.stores.length() {
    if !self.active[index] {
      continue
    }
    self.stores[index].replace_snapshot(snapshot) catch {
      _ => self.active[index] = false
    }
  }
  self.ensure_leader() catch {
    _ => ()
  }
}

///|
/// Reopen one unavailable replica. Its existing WAL is replayed before it is
/// admitted back into the quorum; a replica with divergent state remains down.
pub async fn CoordinatorHa::recover_replica(
  self : CoordinatorHa,
  index : Int,
  maximum_bytes? : Int64,
) -> Unit raise CoordinatorHaError {
  self.check_open()
  if index < 0 || index >= self.stores.length() {
    raise CoordinatorHaError::Replica(index~, reason="invalid replica index")
  }
  if self.active[index] {
    return
  }
  self.ensure_leader()
  let config = self.configs[index]
  let reopened = CoordinatorStore::open(
    self.cluster_id,
    self.coordinator_id,
    config.journal_path,
    checkpoint_path?=config.checkpoint_path,
    maximum_bytes?,
  ) catch {
    error => raise CoordinatorHaError::Store(error)
  }
  let snapshot = self.stores[self.leader].snapshot()
  reopened.replace_snapshot(snapshot) catch {
    error => {
      reopened.close()
      raise CoordinatorHaError::Store(error)
    }
  }
  self.stores[index] = reopened
  self.active[index] = true
}

///|
/// Fence a replica after a transport, disk, or process failure. It is excluded
/// from all future writes until `recover_replica` completes the durable
/// snapshot catch-up.
pub fn CoordinatorHa::deactivate_replica(
  self : CoordinatorHa,
  index : Int,
) -> Unit raise CoordinatorHaError {
  self.check_open()
  if index < 0 || index >= self.stores.length() {
    raise CoordinatorHaError::Replica(index~, reason="invalid replica index")
  }
  if self.active[index] {
    self.stores[index].close()
    self.active[index] = false
    if index == self.leader {
      for candidate in 0..<self.active.length() {
        if self.active[candidate] {
          self.leader = candidate
          break
        }
      }
    }
  }
}

///|
pub async fn CoordinatorHa::register_worker(
  self : CoordinatorHa,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  capabilities : ArrayView[String],
) -> @control.RegisterStatus raise CoordinatorHaError {
  self.check_quorum()
  let before = self.stores[self.leader].snapshot()
  let mut result : @control.RegisterStatus? = None
  let mut successes = 0
  for index in 0..<self.stores.length() {
    if !self.active[index] {
      continue
    }
    let outcome : Result[@control.RegisterStatus, CoordinatorStoreError] = Ok(
      self.stores[index].register_worker(worker_id, incarnation, capabilities),
    ) catch {
      error => Err(error)
    }
    match outcome {
      Ok(value) => {
        successes += 1
        if index == self.leader || result is None {
          result = Some(value)
        }
      }
      Err(_) => {
        self.stores[index].close()
        self.active[index] = false
      }
    }
  } nobreak {
    ()
  }
  if successes < self.quorum {
    self.rollback_failed_write(before)
    raise CoordinatorHaError::NoQuorum(
      required=self.quorum,
      available=successes,
    )
  }
  result.unwrap()
}

///|
pub async fn CoordinatorHa::assign(
  self : CoordinatorHa,
  partition : Int,
  worker_id : String,
  start_offset? : Int64 = 0L,
) -> @control.PartitionAssignment raise CoordinatorHaError {
  self.check_quorum()
  let before = self.stores[self.leader].snapshot()
  let mut result : @control.PartitionAssignment? = None
  let mut successes = 0
  for index in 0..<self.stores.length() {
    if !self.active[index] {
      continue
    }
    let outcome : Result[@control.PartitionAssignment, CoordinatorStoreError] = Ok(
      self.stores[index].assign(partition, worker_id, start_offset~),
    ) catch {
      error => Err(error)
    }
    match outcome {
      Ok(value) => {
        successes += 1
        if index == self.leader || result is None {
          result = Some(value)
        }
      }
      Err(_) => {
        self.stores[index].close()
        self.active[index] = false
      }
    }
  } nobreak {
    ()
  }
  if successes < self.quorum {
    self.rollback_failed_write(before)
    raise CoordinatorHaError::NoQuorum(
      required=self.quorum,
      available=successes,
    )
  }
  result.unwrap()
}

///|
pub async fn CoordinatorHa::fence_and_reassign(
  self : CoordinatorHa,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  replacement_worker_id : String,
) -> Array[@control.PartitionAssignment] raise CoordinatorHaError {
  self.check_quorum()
  let before = self.stores[self.leader].snapshot()
  let mut result : Array[@control.PartitionAssignment]? = None
  let mut successes = 0
  for index in 0..<self.stores.length() {
    if !self.active[index] {
      continue
    }
    let outcome : Result[
      Array[@control.PartitionAssignment],
      CoordinatorStoreError,
    ] = Ok(
      self.stores[index].fence_and_reassign(
        worker_id, incarnation, replacement_worker_id,
      ),
    ) catch {
      error => Err(error)
    }
    match outcome {
      Ok(value) => {
        successes += 1
        if index == self.leader || result is None {
          result = Some(value)
        }
      }
      Err(_) => {
        self.stores[index].close()
        self.active[index] = false
      }
    }
  } nobreak {
    ()
  }
  if successes < self.quorum {
    self.rollback_failed_write(before)
    raise CoordinatorHaError::NoQuorum(
      required=self.quorum,
      available=successes,
    )
  }
  result.unwrap()
}

///|
/// Persist the same checkpoint on every currently admitted replica. Compaction
/// is only reported successful once a majority has completed it.
pub async fn CoordinatorHa::checkpoint(
  self : CoordinatorHa,
  compact? : Bool = false,
) -> Unit raise CoordinatorHaError {
  self.check_quorum()
  let before = self.stores[self.leader].snapshot()
  let mut successes = 0
  for index in 0..<self.stores.length() {
    if !self.active[index] {
      continue
    }
    let result : Result[Unit, CoordinatorStoreError] = try {
      if compact {
        self.stores[index].checkpoint_and_compact()
      } else {
        self.stores[index].checkpoint()
      }
      Ok(())
    } catch {
      error => Err(error)
    }
    match result {
      Ok(_) => successes += 1
      Err(_) => {
        self.stores[index].close()
        self.active[index] = false
      }
    }
  } nobreak {
    ()
  }
  if successes < self.quorum {
    self.rollback_failed_write(before)
    raise CoordinatorHaError::NoQuorum(
      required=self.quorum,
      available=successes,
    )
  }
}

///|
pub fn CoordinatorHa::close(self : CoordinatorHa) -> Unit {
  if !self.closed {
    self.closed = true
    for store in self.stores {
      store.close()
    }
  }
}
