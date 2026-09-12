///|
/// Errors raised by the cross-process Coordinator quorum protocol.
pub(all) suberror CoordinatorHaNodeError {
  InvalidCluster
  InvalidNode
  InvalidReplicaCount(Int)
  DuplicateNode(String)
  InvalidLease(Int64)
  Io(String)
  Malformed
  Store(CoordinatorStoreError)
  Closed
  NotLeader(expected~ : String, actual~ : String)
  StaleTerm(expected~ : Int64, actual~ : Int64)
  NoQuorum(required~ : Int, available~ : Int)
  CommandMissing(Int64)
  CommandGap(expected~ : Int64, actual~ : Int64)
} derive(Debug, Eq)

///|
/// Configuration for one process in a file-backed Coordinator quorum. The
/// root directory is shared by the members for this local protocol; each
/// member still owns a separate CoordinatorStore WAL below that directory.
pub(all) struct CoordinatorHaNodeConfig {
  cluster_id : String
  coordinator_id : String
  node_id : String
  node_ids : Array[String]
  root : String
  lease_ms : Int64
  maximum_bytes : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn CoordinatorHaNodeConfig::new(
  cluster_id : String,
  coordinator_id : String,
  node_id : String,
  node_ids : ArrayView[String],
  root : String,
  lease_ms? : Int64 = 1000L,
  maximum_bytes? : Int64 = 268435456L,
) -> CoordinatorHaNodeConfig raise CoordinatorHaNodeError {
  if cluster_id.is_empty() || coordinator_id.is_empty() {
    raise CoordinatorHaNodeError::InvalidCluster
  }
  if node_id.is_empty() || node_id.contains("/") || root.is_empty() {
    raise CoordinatorHaNodeError::InvalidNode
  }
  if lease_ms <= 0L {
    raise CoordinatorHaNodeError::InvalidLease(lease_ms)
  }
  let ids = node_ids.to_owned()
  if ids.length() < 3 {
    raise CoordinatorHaNodeError::InvalidReplicaCount(ids.length())
  }
  let seen : Set[String] = Set([])
  let mut contains_self = false
  for id in ids {
    if id.is_empty() || id.contains("/") {
      raise CoordinatorHaNodeError::InvalidNode
    }
    if !seen.add_and_check(id) {
      raise CoordinatorHaNodeError::DuplicateNode(id)
    }
    if id == node_id {
      contains_self = true
    }
  }
  if !contains_self {
    raise CoordinatorHaNodeError::InvalidNode
  }
  if maximum_bytes < 0L {
    raise CoordinatorHaNodeError::Io("negative storage budget")
  }
  {
    cluster_id,
    coordinator_id,
    node_id,
    node_ids: ids,
    root,
    lease_ms,
    maximum_bytes,
  }
}

///|
/// The current-schema durable state owned by one Coordinator deployment. The
/// file protocol treats these values as an atomic commit unit: a follower may
/// apply none of them until the command index is committed.
pub(all) struct CoordinatorHaStateBundle {
  coordinator : Json
  job : Json
  driver : Json
  source : Json
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn CoordinatorHaNodeConfig::cluster_id(
  self : CoordinatorHaNodeConfig,
) -> String {
  self.cluster_id
}

///|
pub fn CoordinatorHaNodeConfig::coordinator_id(
  self : CoordinatorHaNodeConfig,
) -> String {
  self.coordinator_id
}

///|
pub fn CoordinatorHaNodeConfig::node_id(
  self : CoordinatorHaNodeConfig,
) -> String {
  self.node_id
}

///|
pub fn CoordinatorHaNodeConfig::node_ids(
  self : CoordinatorHaNodeConfig,
) -> ArrayView[String] {
  self.node_ids[:]
}

///|
pub fn CoordinatorHaNodeConfig::root(self : CoordinatorHaNodeConfig) -> String {
  self.root
}

///|
pub fn CoordinatorHaNodeConfig::lease_ms(
  self : CoordinatorHaNodeConfig,
) -> Int64 {
  self.lease_ms
}

///|
struct CoordinatorHaTerm {
  version : Int
  mut term : Int64
  mut leader : String
  mut lease_until : Int64
  mut commit_index : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
struct CoordinatorHaHeartbeat {
  version : Int
  node_id : String
  term : Int64
  timestamp : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
struct CoordinatorHaCommand {
  version : Int
  term : Int64
  index : Int64
  leader : String
  snapshot : @control.CoordinatorSnapshot
  state : CoordinatorHaStateBundle?
} derive(Debug, Eq, ToJson, FromJson)

///|
struct CoordinatorHaAck {
  version : Int
  node_id : String
  term : Int64
  index : Int64
  kind : String
} derive(Debug, Eq, ToJson, FromJson)

///|
/// A process-local handle to one member of the file-backed quorum.
pub struct CoordinatorHaNode {
  config : CoordinatorHaNodeConfig
  store : CoordinatorStore
  mut term : Int64
  mut applied_index : Int64
  mut state_bundle : CoordinatorHaStateBundle?
  mut closed : Bool
}

///|
fn ha_node_error(stage : String, error : Error) -> CoordinatorHaNodeError {
  CoordinatorHaNodeError::Io(stage + ": " + @debug.to_string(error))
}

///|
fn ha_term_path(config : CoordinatorHaNodeConfig) -> String {
  config.root + "/term.json"
}

///|
fn ha_term_lock_path(config : CoordinatorHaNodeConfig) -> String {
  config.root + "/term.lock"
}

///|
fn ha_command_path(config : CoordinatorHaNodeConfig, index : Int64) -> String {
  config.root + "/commands/" + index.to_string() + ".json"
}

///|
fn ha_ack_path(
  config : CoordinatorHaNodeConfig,
  index : Int64,
  node_id : String,
  kind : String,
) -> String {
  config.root + "/acks/" + index.to_string() + "." + node_id + "." + kind
}

///|
fn ha_heartbeat_path(
  config : CoordinatorHaNodeConfig,
  node_id : String,
) -> String {
  config.root + "/heartbeats/" + node_id + ".json"
}

///|
fn ha_applied_path(config : CoordinatorHaNodeConfig) -> String {
  config.root + "/replicas/" + config.node_id + ".applied"
}

///|
fn ha_state_path(config : CoordinatorHaNodeConfig) -> String {
  config.root + "/replicas/" + config.node_id + ".state.json"
}

///|
async fn ha_write_text(
  path : String,
  text : String,
) -> Unit raise CoordinatorHaNodeError {
  @fs.write_file(
    path + ".tmp",
    @utf8.encode(text),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise ha_node_error("write " + path, error)
  }
  @fs.rename(path + ".tmp", path, replace=true) catch {
    error => raise ha_node_error("install " + path, error)
  }
}

///|
async fn[T : FromJson] ha_read_json(
  path : String,
) -> T raise CoordinatorHaNodeError {
  let text = @fs.read_file(path).text() catch {
    error => raise ha_node_error("read " + path, error)
  }
  @json.from_json(
    @json.parse(text) catch {
      _ => raise CoordinatorHaNodeError::Malformed
    },
  ) catch {
    _ => raise CoordinatorHaNodeError::Malformed
  }
}

///|
async fn ha_exists(path : String) -> Bool raise CoordinatorHaNodeError {
  @fs.exists(path) catch {
    error => raise ha_node_error("check " + path, error)
  }
}

///|
async fn ha_mkdir(path : String) -> Unit raise CoordinatorHaNodeError {
  let already_exists = ha_exists(path)
  if already_exists {
    return
  }
  @fs.mkdir(path, recursive=true) catch {
    error => {
      let appeared = ha_exists(path)
      if !appeared {
        raise ha_node_error("mkdir " + path, error)
      }
    }
  }
}

///|
async fn ha_sleep(milliseconds : Int) -> Unit raise CoordinatorHaNodeError {
  @async.sleep(milliseconds) catch {
    error => raise ha_node_error("sleep", error)
  }
}

///|
async fn ha_open_lock(path : String) -> @fs.File? raise CoordinatorHaNodeError {
  let file = @fs.open(
    path,
    mode=@fs.ReadWrite,
    create_mode=@fs.OpenOrCreate,
    sync=@fs.NoSync,
  ) catch {
    error => raise ha_node_error("open lock", error)
  }
  let acquired = file.try_lock(@fs.Exclusive) catch {
    error => {
      file.close()
      raise ha_node_error("lock", error)
    }
  }
  if acquired {
    Some(file)
  } else {
    file.close()
    None
  }
}

///|
fn ha_close_lock(lock : @fs.File?) -> Unit {
  match lock {
    Some(file) => {
      file.unlock()
      file.close()
    }
    None => ()
  }
}

///|
async fn CoordinatorHaNode::read_term(
  self : CoordinatorHaNode,
) -> CoordinatorHaTerm raise CoordinatorHaNodeError {
  let path = ha_term_path(self.config)
  if !ha_exists(path) {
    return {
      version: 1,
      term: 0L,
      leader: "",
      lease_until: 0L,
      commit_index: 0L,
    }
  }
  let term : CoordinatorHaTerm = ha_read_json(path)
  if term.version != 1 || term.term < 0L || term.commit_index < 0L {
    raise CoordinatorHaNodeError::Malformed
  }
  term
}

///|
async fn CoordinatorHaNode::write_term(
  self : CoordinatorHaNode,
  term : CoordinatorHaTerm,
) -> Unit raise CoordinatorHaNodeError {
  ha_write_text(ha_term_path(self.config), @json.to_json(term).stringify())
}

///|
async fn CoordinatorHaNode::heartbeat(
  self : CoordinatorHaNode,
  term : Int64,
) -> Unit raise CoordinatorHaNodeError {
  let heartbeat = CoordinatorHaHeartbeat::{
    version: 1,
    node_id: self.config.node_id,
    term,
    timestamp: @async.now(),
  }
  ha_write_text(
    ha_heartbeat_path(self.config, self.config.node_id),
    @json.to_json(heartbeat).stringify(),
  )
}

///|
async fn CoordinatorHaNode::alive(
  self : CoordinatorHaNode,
  node_id : String,
  now : Int64,
) -> Bool raise CoordinatorHaNodeError {
  let path = ha_heartbeat_path(self.config, node_id)
  if !ha_exists(path) {
    return false
  }
  let heartbeat : CoordinatorHaHeartbeat = ha_read_json(path) catch {
    _ => return false
  }
  heartbeat.version == 1 &&
  heartbeat.node_id == node_id &&
  now - heartbeat.timestamp <= self.config.lease_ms * 2L &&
  now >= heartbeat.timestamp
}

///|
async fn CoordinatorHaNode::alive_nodes(
  self : CoordinatorHaNode,
  now : Int64,
) -> Array[String] raise CoordinatorHaNodeError {
  let alive : Array[String] = []
  for node_id in self.config.node_ids {
    if self.alive(node_id, now) {
      alive.push(node_id)
    }
  }
  alive.sort()
  alive
}

///|
async fn CoordinatorHaNode::read_applied(
  self : CoordinatorHaNode,
) -> Int64 raise CoordinatorHaNodeError {
  let path = ha_applied_path(self.config)
  if !ha_exists(path) {
    return 0L
  }
  let text = @fs.read_file(path).text() catch {
    error => raise ha_node_error("read applied index", error)
  }
  @string.parse_int64(text.trim()) catch {
    _ => raise CoordinatorHaNodeError::Malformed
  }
}

///|
async fn CoordinatorHaNode::write_applied(
  self : CoordinatorHaNode,
) -> Unit raise CoordinatorHaNodeError {
  ha_write_text(ha_applied_path(self.config), self.applied_index.to_string())
}

///|
async fn CoordinatorHaNode::write_state_bundle(
  self : CoordinatorHaNode,
  bundle : CoordinatorHaStateBundle,
) -> Unit raise CoordinatorHaNodeError {
  ha_write_text(ha_state_path(self.config), @json.to_json(bundle).stringify())
  self.state_bundle = Some(bundle)
}

///|
async fn CoordinatorHaNode::write_ack(
  self : CoordinatorHaNode,
  command : CoordinatorHaCommand,
  kind : String,
) -> Unit raise CoordinatorHaNodeError {
  let ack = CoordinatorHaAck::{
    version: 1,
    node_id: self.config.node_id,
    term: command.term,
    index: command.index,
    kind,
  }
  ha_write_text(
    ha_ack_path(self.config, command.index, self.config.node_id, ack.kind),
    @json.to_json(ack).stringify(),
  )
}

///|
async fn CoordinatorHaNode::ack_count(
  self : CoordinatorHaNode,
  command : CoordinatorHaCommand,
  kind : String,
) -> Int raise CoordinatorHaNodeError {
  let mut count = 0
  for node_id in self.config.node_ids {
    let path = ha_ack_path(self.config, command.index, node_id, kind)
    if !ha_exists(path) {
      continue
    }
    let ack : CoordinatorHaAck = ha_read_json(path) catch { _ => continue }
    if ack.version == 1 &&
      ack.node_id == node_id &&
      ack.term == command.term &&
      ack.index == command.index &&
      ack.kind == kind {
      count += 1
    }
  }
  count
}

///|
async fn CoordinatorHaNode::apply_committed(
  self : CoordinatorHaNode,
) -> Unit raise CoordinatorHaNodeError {
  let term = self.read_term()
  while self.applied_index < term.commit_index {
    let next = self.applied_index + 1L
    let path = ha_command_path(self.config, next)
    if !ha_exists(path) {
      raise CoordinatorHaNodeError::CommandMissing(next)
    }
    let command : CoordinatorHaCommand = ha_read_json(path)
    if command.version != 1 || command.index != next || command.term > term.term {
      raise CoordinatorHaNodeError::Malformed
    }
    self.store.replace_snapshot(command.snapshot) catch {
      error => raise CoordinatorHaNodeError::Store(error)
    }
    self.applied_index = next
    self.write_applied()
    match command.state {
      Some(bundle) => self.write_state_bundle(bundle)
      None => ()
    }
    self.write_ack(command, "commit")
  }
}

///|
/// A replica durably acknowledges the next command before the leader can
/// advance commit_index.  The command is not applied here; application still
/// requires the commit marker in term.json.
async fn CoordinatorHaNode::prepare_next(
  self : CoordinatorHaNode,
) -> Unit raise CoordinatorHaNodeError {
  let term = self.read_term()
  let next = self.applied_index + 1L
  let path = ha_command_path(self.config, next)
  if term.leader.is_empty() || !ha_exists(path) {
    return
  }
  let command : CoordinatorHaCommand = ha_read_json(path)
  if command.version != 1 ||
    command.index != next ||
    command.term != term.term ||
    command.leader != term.leader {
    return
  }
  self.write_ack(command, "prepare")
}

///|
/// Open one process member. The local WAL is never opened by another process;
/// only the term, command and acknowledgement files are shared.
pub async fn CoordinatorHaNode::open(
  config : CoordinatorHaNodeConfig,
) -> CoordinatorHaNode raise CoordinatorHaNodeError {
  ha_mkdir(config.root + "/commands")
  ha_mkdir(config.root + "/acks")
  ha_mkdir(config.root + "/heartbeats")
  ha_mkdir(config.root + "/replicas")
  let store = CoordinatorStore::open(
    config.cluster_id,
    config.coordinator_id,
    config.root + "/replicas/" + config.node_id + ".wal",
    checkpoint_path=config.root + "/replicas/" + config.node_id + ".checkpoint",
    maximum_bytes=config.maximum_bytes,
  ) catch {
    error => raise CoordinatorHaNodeError::Store(error)
  }
  let node = {
    config,
    store,
    term: 0L,
    applied_index: 0L,
    state_bundle: None,
    closed: false,
  }
  node.applied_index = node.read_applied()
  node.term = node.read_term().term
  node.heartbeat(node.term)
  node
}

///|
pub fn CoordinatorHaNode::node_id(self : CoordinatorHaNode) -> String {
  self.config.node_id
}

///|
pub fn CoordinatorHaNode::term(self : CoordinatorHaNode) -> Int64 {
  self.term
}

///|
pub async fn CoordinatorHaNode::leader_id(
  self : CoordinatorHaNode,
) -> String raise CoordinatorHaNodeError {
  self.read_term().leader
}

///|
pub async fn CoordinatorHaNode::is_leader(
  self : CoordinatorHaNode,
) -> Bool raise CoordinatorHaNodeError {
  self.read_term().leader == self.config.node_id
}

///|
pub fn CoordinatorHaNode::snapshot(
  self : CoordinatorHaNode,
) -> @control.CoordinatorSnapshot {
  self.store.snapshot()
}

///|
/// Heartbeat, deterministic election, and committed-command replay. Each
/// process calls this frequently from its service loop.
pub async fn CoordinatorHaNode::tick(
  self : CoordinatorHaNode,
) -> Unit raise CoordinatorHaNodeError {
  if self.closed {
    raise CoordinatorHaNodeError::Closed
  }
  let current = self.read_term()
  self.term = current.term
  self.heartbeat(current.term)
  let now = @async.now()
  let lock = ha_open_lock(ha_term_lock_path(self.config))
  match lock {
    Some(_) => {
      let latest = self.read_term()
      if latest.leader == self.config.node_id && latest.lease_until > now {
        latest.lease_until = now + self.config.lease_ms
        self.write_term(latest)
      } else if latest.leader.is_empty() || !self.alive(latest.leader, now) {
        let candidates = self.alive_nodes(now)
        if candidates.get(0) == Some(self.config.node_id) {
          latest.term += 1L
          latest.leader = self.config.node_id
          latest.lease_until = now + self.config.lease_ms
          self.write_term(latest)
        }
      }
      ha_close_lock(lock)
    }
    None => ()
  }
  self.term = self.read_term().term
  self.prepare_next()
  self.apply_committed()
}

///|
/// Propose a complete current-schema Coordinator snapshot. The command is
/// prepared by members, committed only after a majority has durable command
/// files, and applied by each member after the committed index is durable.
pub async fn CoordinatorHaNode::propose_snapshot(
  self : CoordinatorHaNode,
  snapshot : @control.CoordinatorSnapshot,
  state? : CoordinatorHaStateBundle? = None,
) -> Unit raise CoordinatorHaNodeError {
  if self.closed {
    raise CoordinatorHaNodeError::Closed
  }
  let before = self.read_term()
  if before.leader != self.config.node_id {
    raise CoordinatorHaNodeError::NotLeader(
      expected=self.config.node_id,
      actual=before.leader,
    )
  }
  let lock = ha_open_lock(ha_term_lock_path(self.config))
  guard lock is Some(_) else {
    raise CoordinatorHaNodeError::NotLeader(
      expected=self.config.node_id,
      actual=self.read_term().leader,
    )
  }
  let latest = self.read_term()
  if latest.term != before.term ||
    latest.leader != self.config.node_id ||
    latest.lease_until <= @async.now() {
    ha_close_lock(lock)
    raise CoordinatorHaNodeError::StaleTerm(
      expected=before.term,
      actual=latest.term,
    )
  }
  let command = CoordinatorHaCommand::{
    version: 1,
    term: latest.term,
    index: latest.commit_index + 1L,
    leader: self.config.node_id,
    snapshot,
    state,
  }
  ha_write_text(
    ha_command_path(self.config, command.index),
    @json.to_json(command).stringify(),
  )
  self.write_ack(command, "prepare")
  ha_close_lock(lock)
  let deadline = @async.now() + self.config.lease_ms * 3L
  let mut prepared = false
  while !prepared {
    self.tick()
    let current = self.read_term()
    if current.term != command.term || current.leader != self.config.node_id {
      raise CoordinatorHaNodeError::StaleTerm(
        expected=command.term,
        actual=current.term,
      )
    }
    prepared = self.ack_count(command, "prepare") >=
      self.config.node_ids.length() / 2 + 1
    if !prepared && @async.now() >= deadline {
      raise CoordinatorHaNodeError::NoQuorum(
        required=self.config.node_ids.length() / 2 + 1,
        available=self.ack_count(command, "prepare"),
      )
    }
    if !prepared {
      ha_sleep(10)
    }
  }
  let commit_lock = ha_open_lock(ha_term_lock_path(self.config))
  guard commit_lock is Some(_) else {
    raise CoordinatorHaNodeError::StaleTerm(
      expected=command.term,
      actual=self.read_term().term,
    )
  }
  let committed_term = self.read_term()
  if committed_term.term != command.term ||
    committed_term.leader != self.config.node_id {
    ha_close_lock(commit_lock)
    raise CoordinatorHaNodeError::StaleTerm(
      expected=command.term,
      actual=committed_term.term,
    )
  }
  let committed = committed_term
  committed.commit_index = command.index
  self.write_term(committed)
  ha_close_lock(commit_lock)
  let commit_deadline = @async.now() + self.config.lease_ms * 3L
  while self.ack_count(command, "commit") <
        self.config.node_ids.length() / 2 + 1 {
    self.tick()
    if @async.now() >= commit_deadline {
      raise CoordinatorHaNodeError::NoQuorum(
        required=self.config.node_ids.length() / 2 + 1,
        available=self.ack_count(command, "commit"),
      )
    }
    ha_sleep(10)
  }
}

///|
/// Commit the complete current-schema state bundle together with the
/// Coordinator snapshot. All four durable authorities share one index and
/// quorum decision.
pub async fn CoordinatorHaNode::propose_state(
  self : CoordinatorHaNode,
  state : CoordinatorHaStateBundle,
) -> Unit raise CoordinatorHaNodeError {
  self.propose_snapshot(self.store.snapshot(), state=Some(state))
}

///|
/// Mutate one worker registration under the quorum protocol.
pub async fn CoordinatorHaNode::register_worker(
  self : CoordinatorHaNode,
  worker_id : String,
  incarnation : @protocol.Incarnation,
  capabilities : ArrayView[String],
) -> @control.RegisterStatus raise CoordinatorHaNodeError {
  let before = self.store.snapshot()
  let result = self.store.register_worker(worker_id, incarnation, capabilities) catch {
    error => raise CoordinatorHaNodeError::Store(error)
  }
  errdefer (self.store.replace_snapshot(before) catch { _ => () })
  self.propose_snapshot(self.store.snapshot())
  result
}

///|
/// Mutate one partition assignment under the quorum protocol.
pub async fn CoordinatorHaNode::assign(
  self : CoordinatorHaNode,
  partition : Int,
  worker_id : String,
  start_offset? : Int64 = 0L,
) -> @control.PartitionAssignment raise CoordinatorHaNodeError {
  let before = self.store.snapshot()
  let result = self.store.assign(partition, worker_id, start_offset~) catch {
    error => raise CoordinatorHaNodeError::Store(error)
  }
  errdefer (self.store.replace_snapshot(before) catch { _ => () })
  self.propose_snapshot(self.store.snapshot())
  result
}

///|
pub fn CoordinatorHaNode::close(self : CoordinatorHaNode) -> Unit {
  if !self.closed {
    self.closed = true
    self.store.close()
  }
}
