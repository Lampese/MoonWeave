///|
/// A networked Coordinator quorum. Each member owns its own directory and
/// communicates only through TCP; no shared authority directory is used.
/// State mutations are accepted after a majority has durably prepared and
/// committed the complete state payload.
pub(all) suberror NetworkHaError {
  InvalidConfig
  InvalidPeer(String)
  DuplicatePeer(String)
  Closed
  NotLeader(expected~ : String, actual~ : String)
  NoQuorum(required~ : Int, available~ : Int)
  StaleTerm(expected~ : Int64, actual~ : Int64)
  CommitGap(expected~ : Int64, actual~ : Int64)
  Malformed
  Io(String)
} derive(Debug, Eq)

///|
/// An endpoint advertised by one quorum member.
pub(all) struct NetworkHaEndpoint {
  host : String
  port : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn NetworkHaEndpoint::new(
  host : String,
  port : Int,
) -> NetworkHaEndpoint raise NetworkHaError {
  if host.is_empty() || port <= 0 || port > 65535 {
    raise NetworkHaError::InvalidConfig
  }
  { host, port, }
}

///|
pub fn NetworkHaEndpoint::host(self : NetworkHaEndpoint) -> String {
  self.host
}

///|
pub fn NetworkHaEndpoint::port(self : NetworkHaEndpoint) -> Int {
  self.port
}

///|
/// Independent durable peer identity. `storage_dir` must be local to this
/// process and must not be shared with another member.
pub(all) struct NetworkHaPeer {
  node_id : String
  endpoint : NetworkHaEndpoint
  storage_dir : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn NetworkHaPeer::new(
  node_id : String,
  endpoint : NetworkHaEndpoint,
  storage_dir : String,
) -> NetworkHaPeer raise NetworkHaError {
  if node_id.is_empty() || node_id.contains("/") || storage_dir.is_empty() {
    raise NetworkHaError::InvalidConfig
  }
  { node_id, endpoint, storage_dir, }
}

///|
pub fn NetworkHaPeer::node_id(self : NetworkHaPeer) -> String {
  self.node_id
}

///|
pub fn NetworkHaPeer::endpoint(self : NetworkHaPeer) -> NetworkHaEndpoint {
  self.endpoint
}

///|
pub fn NetworkHaPeer::storage_dir(self : NetworkHaPeer) -> String {
  self.storage_dir
}

///|
pub(all) struct NetworkHaConfig {
  cluster_id : String
  coordinator_id : String
  node_id : String
  bind : NetworkHaEndpoint
  peers : Array[NetworkHaPeer]
  storage_dir : String
  lease_ms : Int64
  maximum_bytes : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn NetworkHaConfig::new(
  cluster_id : String,
  coordinator_id : String,
  node_id : String,
  bind : NetworkHaEndpoint,
  peers : ArrayView[NetworkHaPeer],
  storage_dir : String,
  lease_ms? : Int64 = 1000L,
  maximum_bytes? : Int64 = 268435456L,
) -> NetworkHaConfig raise NetworkHaError {
  if cluster_id.is_empty() ||
    coordinator_id.is_empty() ||
    node_id.is_empty() ||
    storage_dir.is_empty() ||
    lease_ms <= 0L ||
    maximum_bytes < 0L {
    raise NetworkHaError::InvalidConfig
  }
  let copied = peers.to_owned()
  if copied.length() < 3 {
    raise NetworkHaError::InvalidConfig
  }
  let seen : Set[String] = Set([])
  let mut found = false
  for peer in copied {
    if !seen.add_and_check(peer.node_id()) {
      raise NetworkHaError::DuplicatePeer(peer.node_id())
    }
    if peer.node_id() == node_id {
      found = true
    }
    if peer.endpoint().port() <= 0 || peer.storage_dir().is_empty() {
      raise NetworkHaError::InvalidPeer(peer.node_id())
    }
  }
  if !found {
    raise NetworkHaError::InvalidConfig
  }
  {
    cluster_id,
    coordinator_id,
    node_id,
    bind,
    peers: copied,
    storage_dir,
    lease_ms,
    maximum_bytes,
  }
}

///|
pub fn NetworkHaConfig::cluster_id(self : NetworkHaConfig) -> String {
  self.cluster_id
}

///|
pub fn NetworkHaConfig::coordinator_id(self : NetworkHaConfig) -> String {
  self.coordinator_id
}

///|
pub fn NetworkHaConfig::node_id(self : NetworkHaConfig) -> String {
  self.node_id
}

///|
pub fn NetworkHaConfig::bind(self : NetworkHaConfig) -> NetworkHaEndpoint {
  self.bind
}

///|
pub fn NetworkHaConfig::peers(
  self : NetworkHaConfig,
) -> ArrayView[NetworkHaPeer] {
  self.peers[:]
}

///|
pub fn NetworkHaConfig::storage_dir(self : NetworkHaConfig) -> String {
  self.storage_dir
}

///|
pub fn NetworkHaConfig::lease_ms(self : NetworkHaConfig) -> Int64 {
  self.lease_ms
}

///|
struct NetworkHaMeta {
  version : Int
  term : Int64
  leader : String
  commit_index : Int64
  lease_until : Int64
  voted_for : String
} derive(ToJson, FromJson, Eq)

///|
struct NetworkHaCommand {
  version : Int
  term : Int64
  index : Int64
  leader : String
  state : Json
} derive(ToJson, FromJson, Eq)

///|
struct NetworkHaRequest {
  version : Int
  cluster_id : String
  coordinator_id : String
  kind : String
  node_id : String
  term : Int64
  index : Int64
  leader : String
  state : Json?
  state_term : Int64?
} derive(ToJson, FromJson, Eq)

///|
struct NetworkHaReply {
  version : Int
  ok : Bool
  node_id : String
  term : Int64
  index : Int64
  leader : String
  error : String?
  state : Json?
  state_term : Int64
  voted_for : String
} derive(ToJson, FromJson, Eq)

///|
pub(all) struct NetworkHaStatus {
  node_id : String
  term : Int64
  leader : String
  commit_index : Int64
  available_peers : Int
  quorum : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn NetworkHaStatus::node_id(self : NetworkHaStatus) -> String {
  self.node_id
}

///|
pub fn NetworkHaStatus::term(self : NetworkHaStatus) -> Int64 {
  self.term
}

///|
pub fn NetworkHaStatus::leader(self : NetworkHaStatus) -> String {
  self.leader
}

///|
pub fn NetworkHaStatus::commit_index(self : NetworkHaStatus) -> Int64 {
  self.commit_index
}

///|
pub fn NetworkHaStatus::available_peers(self : NetworkHaStatus) -> Int {
  self.available_peers
}

///|
pub fn NetworkHaStatus::quorum(self : NetworkHaStatus) -> Int {
  self.quorum
}

///|
async fn network_mkdir(path : String) -> Unit raise NetworkHaError {
  let exists = @fs.exists(path) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  if !exists {
    @fs.mkdir(path, recursive=true) catch {
      error => raise NetworkHaError::Io(@debug.to_string(error))
    }
  }
}

///|
async fn network_write(
  path : String,
  text : String,
) -> Unit raise NetworkHaError {
  @fs.write_file(
    path + ".tmp",
    @utf8.encode(text),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  @fs.rename(path + ".tmp", path, replace=true) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
}

///|
async fn[T : FromJson] network_read(path : String) -> T raise NetworkHaError {
  let text = @fs.read_file(path).text() catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  @json.from_json(
    @json.parse(text) catch {
      _ => raise NetworkHaError::Malformed
    },
  ) catch {
    _ => raise NetworkHaError::Malformed
  }
}

///|
fn network_encode(value : NetworkHaRequest) -> Bytes {
  let body = @utf8.encode(@json.to_json(value).stringify())
  let buffer = Buffer()
  buffer.write_uint_be(body.length().reinterpret_as_uint())
  buffer.write_bytes(body[:])
  buffer.to_bytes()
}

///|
async fn network_read_request(
  connection : @socket.Tcp,
) -> NetworkHaRequest raise NetworkHaError {
  let header = connection.read_exactly(4) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  let length = (header[0].to_int() << 24) +
    (header[1].to_int() << 16) +
    (header[2].to_int() << 8) +
    header[3].to_int()
  if length <= 0 || length > 64 * 1024 * 1024 {
    raise NetworkHaError::Malformed
  }
  let body = connection.read_exactly(length) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  @json.from_json(
    @json.parse(@utf8.decode(body)) catch {
      _ => raise NetworkHaError::Malformed
    },
  ) catch {
    _ => raise NetworkHaError::Malformed
  }
}

///|
async fn network_write_reply(
  connection : @socket.Tcp,
  reply : NetworkHaReply,
) -> Unit raise NetworkHaError {
  let body = @utf8.encode(@json.to_json(reply).stringify())
  let buffer = Buffer()
  buffer.write_uint_be(body.length().reinterpret_as_uint())
  buffer.write_bytes(body[:])
  connection.write(buffer.to_bytes()) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
}

///|
async fn network_rpc(
  peer : NetworkHaPeer,
  request : NetworkHaRequest,
) -> NetworkHaReply raise NetworkHaError {
  let connection = @socket.Tcp::connect_to_host(
    peer.endpoint().host(),
    port=peer.endpoint().port(),
  ) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  defer connection.close()
  connection.write(network_encode(request)) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  let header = connection.read_exactly(4) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  let length = (header[0].to_int() << 24) +
    (header[1].to_int() << 16) +
    (header[2].to_int() << 8) +
    header[3].to_int()
  if length <= 0 || length > 64 * 1024 * 1024 {
    raise NetworkHaError::Malformed
  }
  let body = connection.read_exactly(length) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  @json.from_json(
    @json.parse(@utf8.decode(body)) catch {
      _ => raise NetworkHaError::Malformed
    },
  ) catch {
    _ => raise NetworkHaError::Malformed
  }
}

///|
pub struct NetworkHaNode {
  config : NetworkHaConfig
  server : @socket.TcpServer
  wal : Wal
  mut term : Int64
  mut leader : String
  mut commit_index : Int64
  mut lease_until : Int64
  mut state : Json?
  mut state_term : Int64
  mut voted_for : String
  mut closed : Bool
  state_lock : @async.Mutex
  reachable : Set[String]
}

///|
fn network_meta_path(config : NetworkHaConfig) -> String {
  config.storage_dir() + "/ha.meta.json"
}

///|
fn network_pending_path(config : NetworkHaConfig, index : Int64) -> String {
  config.storage_dir() + "/pending-" + index.to_string() + ".json"
}

///|
fn network_majority(config : NetworkHaConfig) -> Int {
  config.peers().length() / 2 + 1
}

///|
async fn NetworkHaNode::persist_meta(
  self : NetworkHaNode,
) -> Unit raise NetworkHaError {
  network_write(
    network_meta_path(self.config),
    @json.to_json(NetworkHaMeta::{
      version: 1,
      term: self.term,
      leader: self.leader,
      commit_index: self.commit_index,
      lease_until: self.lease_until,
      voted_for: self.voted_for,
    }).stringify(),
  )
}

///|
async fn NetworkHaNode::append_command(
  self : NetworkHaNode,
  command : NetworkHaCommand,
) -> Unit raise NetworkHaError {
  // Every command is a complete state image. Atomic replacement bounds the
  // quorum history without discarding any information required for recovery.
  self.wal.rewrite([@utf8.encode(@json.to_json(command).stringify())]) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  self.commit_index = command.index
  self.state = Some(command.state)
  self.state_term = command.term
  self.persist_meta()
}

///|
async fn NetworkHaNode::install_snapshot(
  self : NetworkHaNode,
  term : Int64,
  leader : String,
  index : Int64,
  state : Json,
  state_term? : Int64 = term,
) -> Unit raise NetworkHaError {
  if state_term < self.state_term && index >= self.commit_index {
    raise NetworkHaError::Malformed
  }
  if index < self.commit_index ||
    (index == self.commit_index && state_term <= self.state_term) {
    return
  }
  let command = NetworkHaCommand::{
    version: 1,
    term: state_term,
    index,
    leader,
    state,
  }
  self.wal.rewrite([@utf8.encode(@json.to_json(command).stringify())]) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  if term >= self.term {
    if term > self.term {
      self.voted_for = ""
    }
    self.term = term
    self.leader = leader
  }
  self.commit_index = index
  self.state = Some(state)
  self.state_term = state_term
  self.persist_meta()
  self.remove_committed_prepares()
}

///|
async fn NetworkHaNode::remove_committed_prepares(
  self : NetworkHaNode,
) -> Unit raise NetworkHaError {
  let names = @fs.readdir(self.config.storage_dir()) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  for name in names {
    if name.has_prefix("pending-") && name.has_suffix(".json") {
      let path = self.config.storage_dir() + "/" + name
      let pending : NetworkHaCommand = network_read(path)
      if pending.index <= self.commit_index {
        @fs.remove(path) catch {
          _ => ()
        }
      }
    }
  }
}

///|
pub async fn NetworkHaNode::open(
  config : NetworkHaConfig,
) -> NetworkHaNode raise NetworkHaError {
  network_mkdir(config.storage_dir())
  let (wal, replay) = Wal::open(
    config.storage_dir() + "/commands.wal",
    maximum_bytes=config.maximum_bytes,
  ) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  errdefer wal.close()
  let mut term = 0L
  let mut leader = ""
  let mut commit_index = 0L
  let mut lease_until = 0L
  let mut state : Json? = None
  let mut state_term = 0L
  let mut voted_for = ""
  let meta_path = network_meta_path(config)
  let meta_exists = @fs.exists(meta_path) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  if meta_exists {
    let meta : NetworkHaMeta = network_read(meta_path)
    if meta.version != 1 || meta.term < 0L || meta.commit_index < 0L {
      raise NetworkHaError::Malformed
    }
    term = meta.term
    leader = meta.leader
    commit_index = meta.commit_index
    lease_until = meta.lease_until
    voted_for = meta.voted_for
  }
  let mut replay_index = 0L
  let mut replay_first = true
  for encoded in replay.records() {
    let command : NetworkHaCommand = @json.from_json(
      @json.parse(@utf8.decode(encoded)) catch {
        _ => raise NetworkHaError::Malformed
      },
    ) catch {
      _ => raise NetworkHaError::Malformed
    }
    let valid_index = if replay_first {
      command.index > 0L
    } else {
      command.index == replay_index + 1L
    }
    if command.version != 1 || !valid_index || command.term < 0L {
      raise NetworkHaError::Malformed
    }
    replay_index = command.index
    replay_first = false
    if command.term > term {
      term = command.term
      voted_for = command.leader
    }
    if command.index >= commit_index {
      if command.term >= term {
        leader = command.leader
      }
      commit_index = command.index
    }
    state = Some(command.state)
    state_term = command.term
  }
  if replay_index < commit_index && !replay_first {
    raise NetworkHaError::Malformed
  }
  let server = @socket.TcpServer(
    @socket.Addr::parse(
      config.bind().host() + ":" + config.bind().port().to_string(),
    ),
    reuse_addr=true,
  ) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  let node = {
    config,
    server,
    wal,
    term,
    leader,
    commit_index,
    lease_until,
    state,
    state_term,
    voted_for,
    closed: false,
    state_lock: @async.Mutex::Mutex(),
    reachable: Set([]),
  }
  node.persist_meta()
  node
}

///|
pub fn NetworkHaNode::node_id(self : NetworkHaNode) -> String {
  self.config.node_id()
}

///|
pub fn NetworkHaNode::endpoint(self : NetworkHaNode) -> NetworkHaEndpoint {
  NetworkHaEndpoint::new(self.config.bind().host(), self.server.addr.port()) catch {
    _ =>
      NetworkHaEndpoint::{
        host: self.config.bind().host(),
        port: self.server.addr.port(),
      }
  }
}

///|
pub fn NetworkHaNode::state(self : NetworkHaNode) -> Json? {
  self.state
}

///|
pub fn NetworkHaNode::commit_index(self : NetworkHaNode) -> Int64 {
  self.commit_index
}

///|
pub fn NetworkHaNode::state_term(self : NetworkHaNode) -> Int64 {
  self.state_term
}

///|
async fn NetworkHaNode::handle_unlocked(
  self : NetworkHaNode,
  request : NetworkHaRequest,
) -> NetworkHaReply raise NetworkHaError {
  let base = (ok : Bool, error : String?, state : Json?) => NetworkHaReply::{
    version: 1,
    ok,
    node_id: self.node_id(),
    term: self.term,
    index: self.commit_index,
    leader: self.leader,
    error,
    state,
    state_term: self.state_term,
    voted_for: self.voted_for,
  }
  if request.version != 1 ||
    request.cluster_id != self.config.cluster_id() ||
    request.coordinator_id != self.config.coordinator_id() {
    return base(false, Some("identity mismatch"), self.state)
  }
  match request.kind {
    "ping" | "leader" => base(true, None, self.state)
    "vote" => {
      if !self.config.peers().any(peer => peer.node_id() == request.leader) ||
        request.term < self.term {
        return base(false, Some("invalid election candidate"), self.state)
      }
      if request.term > self.term {
        self.term = request.term
        self.voted_for = ""
        self.leader = ""
        self.persist_meta()
      }
      if (!self.voted_for.is_empty() && self.voted_for != request.leader) ||
        request.index < self.commit_index ||
        request.state_term.unwrap_or(0L) < self.state_term {
        return base(
          false,
          Some("candidate is stale or term already promised"),
          self.state,
        )
      }
      self.voted_for = request.leader
      self.leader = request.leader
      self.persist_meta()
      base(true, None, self.state)
    }
    "read" => {
      // `handle` already owns `state_lock`; use the unlocked implementation
      // here to avoid recursively acquiring the same mutex.
      let status = self.tick_unlocked()
      if status.leader != self.node_id() ||
        status.available_peers < status.quorum {
        return base(false, Some("leader has no quorum"), self.state)
      }
      base(true, None, self.state)
    }
    "prepare" => {
      if !self.config.peers().any(peer => peer.node_id() == request.leader) {
        return base(false, Some("unknown leader"), self.state)
      }
      if request.term < self.term {
        return base(false, Some("stale term"), self.state)
      }
      if request.term > self.term {
        self.term = request.term
        self.voted_for = request.leader
        self.leader = request.leader
        self.lease_until = @async.now() + self.config.lease_ms()
        self.persist_meta()
      }
      if (!self.voted_for.is_empty() && self.voted_for != request.leader) ||
        self.leader != request.leader ||
        request.index < self.commit_index + 1L {
        return base(false, Some("fenced leader or stale index"), self.state)
      }
      guard request.state is Some(state) else {
        return base(false, Some("missing state"), self.state)
      }
      if request.index > self.commit_index + 1L {
        return base(false, Some("command gap"), self.state)
      }
      let command = NetworkHaCommand::{
        version: 1,
        term: request.term,
        index: request.index,
        leader: request.leader,
        state,
      }
      let pending_path = network_pending_path(self.config, request.index)
      let pending_exists = @fs.exists(pending_path) catch {
        error => return base(false, Some(@debug.to_string(error)), self.state)
      }
      if pending_exists {
        let previous : NetworkHaCommand = network_read(pending_path) catch {
          error => return base(false, Some(@debug.to_string(error)), self.state)
        }
        if previous != command && previous.term >= command.term {
          return base(false, Some("conflicting prepare"), self.state)
        }
        if previous == command {
          return base(true, None, self.state)
        }
      }
      network_write(pending_path, @json.to_json(command).stringify()) catch {
        error => return base(false, Some(@debug.to_string(error)), self.state)
      }
      base(true, None, self.state)
    }
    "commit" => {
      if !self.config.peers().any(peer => peer.node_id() == request.leader) {
        return base(false, Some("unknown leader"), self.state)
      }
      if request.term < self.term || request.leader != self.leader {
        return base(false, Some("fenced commit"), self.state)
      }
      // A heartbeat can install the exact committed image between prepare
      // and this RPC. A lost commit reply can also be retried. Both are the
      // same durable acknowledgement, rather than a command gap.
      if request.index == self.commit_index {
        if request.state != self.state || request.term != self.state_term {
          return base(false, Some("conflicting committed state"), self.state)
        }
        self.remove_committed_prepares()
        return base(true, None, self.state)
      }
      if request.index != self.commit_index + 1L {
        return base(false, Some("commit gap"), self.state)
      }
      let path = network_pending_path(self.config, request.index)
      let command : NetworkHaCommand = network_read(path) catch {
        _ => return base(false, Some("missing prepare"), self.state)
      }
      if command.term != request.term ||
        command.index != request.index ||
        command.leader != request.leader ||
        request.state != Some(command.state) {
        return base(false, Some("commit mismatch"), self.state)
      }
      self.append_command(command) catch {
        error => return base(false, Some(@debug.to_string(error)), self.state)
      }
      @fs.remove(path) catch {
        _ => ()
      }
      base(true, None, self.state)
    }
    "sync" => {
      if !self.config.peers().any(peer => peer.node_id() == request.leader) {
        return base(false, Some("unknown leader"), self.state)
      }
      if request.term < self.term {
        return base(false, Some("stale term"), self.state)
      }
      if request.term == self.term &&
        !self.leader.is_empty() &&
        self.leader != request.leader {
        return base(false, Some("fenced leader"), self.state)
      }
      if request.term > self.term {
        self.term = request.term
        self.voted_for = ""
        self.leader = request.leader
        self.persist_meta()
      }
      match request.state {
        Some(state) =>
          self.install_snapshot(
            request.term,
            request.leader,
            request.index,
            state,
            state_term?=request.state_term,
          )
        None => ()
      }
      base(true, None, self.state)
    }
    "propose" => {
      guard request.state is Some(state) else {
        return base(false, Some("missing state"), self.state)
      }
      self.propose_unlocked(state) catch {
        error => return base(false, Some(@debug.to_string(error)), self.state)
      }
      base(true, None, self.state)
    }
    _ => base(false, Some("unknown request"), self.state)
  }
}

///|
async fn NetworkHaNode::handle(
  self : NetworkHaNode,
  request : NetworkHaRequest,
) -> NetworkHaReply raise NetworkHaError {
  // Heartbeats only read the current identity and never mutate durable state.
  // Serving them without the mutation lock avoids a lock cycle when two nodes
  // heartbeat each other while each is electing or renewing its lease.
  if request.kind == "ping" || request.kind == "leader" {
    return self.handle_unlocked(request)
  }
  self.state_lock.acquire() catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  defer self.state_lock.release()
  self.handle_unlocked(request)
}

///|
async fn NetworkHaNode::serve_connection(
  self : NetworkHaNode,
  connection : @socket.Tcp,
) -> Unit raise NetworkHaError {
  defer connection.close()
  let request = network_read_request(connection) catch { _ => return }
  let reply = self.handle(request)
  network_write_reply(connection, reply) catch {
    _ => ()
  }
}

///|
/// Serve RPCs until close. Supervisors should run this in a background task.
pub async fn NetworkHaNode::serve(
  self : NetworkHaNode,
) -> Unit raise NetworkHaError {
  (self.server.run_forever(allow_failure=true, max_connections=128) <| ((
    connection,
    _,
  ) => self.serve_connection(connection))) catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
}

///|
async fn NetworkHaNode::peer_rpc(
  self : NetworkHaNode,
  peer : NetworkHaPeer,
  kind : String,
  index : Int64,
  state : Json?,
  timeout_ms? : Int = (self.config.lease_ms() / 4L).max(25L).to_int(),
) -> NetworkHaReply? raise NetworkHaError {
  let request = NetworkHaRequest::{
    version: 1,
    cluster_id: self.config.cluster_id(),
    coordinator_id: self.config.coordinator_id(),
    kind,
    node_id: self.node_id(),
    term: self.term,
    index,
    leader: self.node_id(),
    state,
    state_term: Some(self.state_term),
  }
  let result : NetworkHaReply? = @async.with_timeout_opt(timeout_ms, () => {
    network_rpc(peer, request)
  }) catch {
    _ => None
  }
  result
}

///|
/// Refresh liveness and elect a deterministic leader when the previous lease
/// has expired. A candidate only self-elects after observing a majority.
pub async fn NetworkHaNode::tick(
  self : NetworkHaNode,
) -> NetworkHaStatus raise NetworkHaError {
  self.state_lock.acquire() catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  defer self.state_lock.release()
  self.tick_unlocked()
}

///|
async fn NetworkHaNode::tick_unlocked(
  self : NetworkHaNode,
) -> NetworkHaStatus raise NetworkHaError {
  if self.closed {
    raise NetworkHaError::Closed
  }
  let now = @async.now()
  self.reachable.clear()
  let mut alive = 1
  let mut observed_leader = self.leader
  // A lower-term peer must never overwrite a leader learned from a newer
  // term. Keeping the leader and term coupled prevents transient startup
  // split-brain when nodes elect concurrently.
  let mut observed_term = self.term
  let replies : Array[NetworkHaReply] = []
  for peer in self.config.peers() {
    if peer.node_id() == self.node_id() {
      continue
    }
    match self.peer_rpc(peer, "ping", self.commit_index, None) {
      Some(reply) if reply.ok => {
        replies.push(reply)
        self.reachable.add(peer.node_id())
        alive += 1
        if reply.term > observed_term {
          observed_term = reply.term
          observed_leader = reply.leader
        } else if reply.term == observed_term &&
          !reply.leader.is_empty() &&
          (observed_leader.is_empty() || reply.leader < observed_leader) {
          observed_leader = reply.leader
        }
        if (
            reply.index > self.commit_index ||
            (
              reply.index == self.commit_index &&
              reply.state_term > self.state_term
            )
          ) &&
          reply.state is Some(state) &&
          !reply.leader.is_empty() {
          self.install_snapshot(
            reply.term,
            reply.leader,
            reply.index,
            state,
            state_term=reply.state_term,
          ) catch {
            _ => ()
          }
        }
      }
      _ => ()
    }
  }
  if observed_term > self.term {
    self.term = observed_term
    self.voted_for = ""
    self.leader = observed_leader
    self.persist_meta()
  }
  let mut leader_alive = false
  if !self.leader.is_empty() {
    if self.leader == self.node_id() {
      leader_alive = self.lease_until > now
    } else {
      for peer in self.config.peers() {
        if peer.node_id() == self.leader {
          leader_alive = self.peer_rpc(peer, "ping", self.commit_index, None)
            is Some(_)
        }
      }
    }
  }
  if (!leader_alive || self.leader.is_empty()) &&
    alive >= network_majority(self.config) {
    let mut lowest = self.node_id()
    for peer in self.config.peers() {
      if peer.node_id() < lowest &&
        self.peer_rpc(peer, "ping", self.commit_index, None) is Some(_) {
        lowest = peer.node_id()
      }
    }
    if lowest == self.node_id() {
      self.term += 1L
      self.leader = self.node_id()
      self.voted_for = self.node_id()
      self.lease_until = @async.now() + self.config.lease_ms()
      self.persist_meta()
      for peer in self.config.peers() {
        if peer.node_id() == self.node_id() ||
          !self.reachable.contains(peer.node_id()) {
          continue
        }
        if self.peer_rpc(
            peer,
            "vote",
            self.commit_index,
            None,
            timeout_ms=self.config.lease_ms().to_int(),
          )
          is Some(reply) {
          replies.push(reply)
        }
      }
    }
  } else if self.leader == self.node_id() &&
    alive >= network_majority(self.config) {
    self.lease_until = @async.now() + self.config.lease_ms()
    self.persist_meta()
  }
  if self.leader == self.node_id() {
    let supporters : Set[String] = Set([self.node_id()])
    for reply in replies {
      if reply.ok &&
        reply.term == self.term &&
        reply.voted_for == self.node_id() {
        supporters.add(reply.node_id)
      }
    }
    alive = supporters.length()
  }
  {
    node_id: self.node_id(),
    term: self.term,
    leader: self.leader,
    commit_index: self.commit_index,
    available_peers: alive,
    quorum: network_majority(self.config),
  }
}

///|
/// Propose a complete authoritative state payload. The payload is not
/// acknowledged until a majority has persisted both prepare and commit.
async fn NetworkHaNode::propose_unlocked(
  self : NetworkHaNode,
  state : Json,
  expected_index? : Int64,
  expected_term? : Int64,
) -> Unit raise NetworkHaError {
  if self.closed {
    raise NetworkHaError::Closed
  }
  // A client may lose the reply after a quorum commit and retry the same
  // snapshot. Treat an identical committed payload as an idempotent success so
  // retries do not create unbounded no-op log entries or consume the index
  // sequence during failover.
  let status = self.tick_unlocked()
  if expected_index is Some(index) && index != self.commit_index {
    raise NetworkHaError::Malformed
  }
  if expected_term is Some(term) && term != self.state_term {
    raise NetworkHaError::Malformed
  }
  if status.leader != self.node_id() {
    raise NetworkHaError::NotLeader(
      expected=self.node_id(),
      actual=status.leader,
    )
  }
  if status.available_peers < network_majority(self.config) {
    raise NetworkHaError::NoQuorum(
      required=network_majority(self.config),
      available=status.available_peers,
    )
  }
  match self.state {
    Some(previous) if previous == state => {
      let mut confirmed = 1
      for peer in self.config.peers() {
        if peer.node_id() == self.node_id() ||
          !self.reachable.contains(peer.node_id()) {
          continue
        }
        if self.peer_rpc(peer, "ping", self.commit_index, None) is Some(reply) &&
          reply.ok &&
          reply.term == self.term &&
          reply.leader == self.node_id() &&
          reply.index == self.commit_index &&
          reply.state == Some(state) {
          confirmed += 1
        }
      }
      if confirmed >= network_majority(self.config) {
        return
      }
    }
    _ => ()
  }
  let index = self.commit_index + 1L
  let command = NetworkHaCommand::{
    version: 1,
    term: self.term,
    index,
    leader: self.node_id(),
    state,
  }
  network_write(
    network_pending_path(self.config, index),
    @json.to_json(command).stringify(),
  )
  let mut prepared = 1
  for peer in self.config.peers() {
    if peer.node_id() == self.node_id() ||
      !self.reachable.contains(peer.node_id()) {
      continue
    }
    match
      self.peer_rpc(
        peer,
        "prepare",
        index,
        Some(state),
        timeout_ms=self.config.lease_ms().to_int(),
      ) {
      Some(reply) if reply.ok && reply.term == self.term => prepared += 1
      _ => ()
    }
  }
  if prepared < network_majority(self.config) {
    raise NetworkHaError::NoQuorum(
      required=network_majority(self.config),
      available=prepared,
    )
  }
  self.append_command(command)
  @fs.remove(network_pending_path(self.config, index)) catch {
    _ => ()
  }
  let mut committed = 1
  for peer in self.config.peers() {
    if peer.node_id() == self.node_id() ||
      !self.reachable.contains(peer.node_id()) {
      continue
    }
    match
      self.peer_rpc(
        peer,
        "commit",
        index,
        Some(state),
        timeout_ms=self.config.lease_ms().to_int(),
      ) {
      Some(reply) if reply.ok && reply.term == self.term => committed += 1
      _ => ()
    }
  }
  if committed < network_majority(self.config) {
    raise NetworkHaError::NoQuorum(
      required=network_majority(self.config),
      available=committed,
    )
  }
}

///|
pub async fn NetworkHaNode::propose(
  self : NetworkHaNode,
  state : Json,
  expected_index? : Int64,
  expected_term? : Int64,
) -> Unit raise NetworkHaError {
  self.state_lock.acquire() catch {
    error => raise NetworkHaError::Io(@debug.to_string(error))
  }
  defer self.state_lock.release()
  self.propose_unlocked(state, expected_index?, expected_term?)
}

///|
pub fn NetworkHaNode::close(self : NetworkHaNode) -> Unit {
  if !self.closed {
    self.closed = true
    self.server.close()
    self.wal.close()
  }
}

///|
/// A reconnecting client for Workers/Drivers. It discovers the current leader
/// from any seed and retries once after a term change.
pub struct NetworkHaClient {
  cluster_id : String
  coordinator_id : String
  seeds : Array[NetworkHaPeer]
}

///|
pub fn NetworkHaClient::new(
  cluster_id : String,
  coordinator_id : String,
  seeds : ArrayView[NetworkHaPeer],
) -> NetworkHaClient raise NetworkHaError {
  if cluster_id.is_empty() || coordinator_id.is_empty() || seeds.length() < 3 {
    raise NetworkHaError::InvalidConfig
  }
  { cluster_id, coordinator_id, seeds: seeds.to_owned(), }
}

///|
async fn NetworkHaClient::rpc(
  self : NetworkHaClient,
  peer : NetworkHaPeer,
  request : NetworkHaRequest,
) -> NetworkHaReply? {
  Some(network_rpc(peer, request)) catch {
    _ => None
  }
}

///|
async fn NetworkHaClient::find_leader(
  self : NetworkHaClient,
) -> NetworkHaPeer? raise NetworkHaError {
  for seed in self.seeds {
    let request = NetworkHaRequest::{
      version: 1,
      cluster_id: self.cluster_id,
      coordinator_id: self.coordinator_id,
      kind: "leader",
      node_id: "client",
      term: 0L,
      index: 0L,
      leader: "",
      state: None,
      state_term: None,
    }
    match (self.rpc(seed, request) catch { _ => None }) {
      Some(reply) if reply.ok =>
        for peer in self.seeds {
          if peer.node_id() == reply.leader {
            return Some(peer)
          }
        }
      _ => ()
    }
  }
  None
}

///|
pub async fn NetworkHaClient::propose(
  self : NetworkHaClient,
  state : Json,
) -> Unit raise NetworkHaError {
  let mut attempts = 0
  while attempts < self.seeds.length() + 1 {
    attempts += 1
    guard self.find_leader() is Some(peer) else { continue }
    let request = NetworkHaRequest::{
      version: 1,
      cluster_id: self.cluster_id,
      coordinator_id: self.coordinator_id,
      kind: "propose",
      node_id: "client",
      term: 0L,
      index: 0L,
      leader: "",
      state: Some(state),
      state_term: None,
    }
    match (self.rpc(peer, request) catch { _ => None }) {
      Some(reply) if reply.ok => return
      _ => ()
    }
  }
  raise NetworkHaError::NoQuorum(
    required=self.seeds.length() / 2 + 1,
    available=0,
  )
}

///|
/// Read the state committed by the current leader after a quorum heartbeat.
pub async fn NetworkHaClient::read(
  self : NetworkHaClient,
) -> Json? raise NetworkHaError {
  let mut attempts = 0
  while attempts < self.seeds.length() + 1 {
    attempts += 1
    guard self.find_leader() is Some(peer) else { continue }
    let request = NetworkHaRequest::{
      version: 1,
      cluster_id: self.cluster_id,
      coordinator_id: self.coordinator_id,
      kind: "read",
      node_id: "client",
      term: 0L,
      index: 0L,
      leader: "",
      state: None,
      state_term: None,
    }
    match (self.rpc(peer, request) catch { _ => None }) {
      Some(reply) if reply.ok => return reply.state
      _ => ()
    }
  }
  raise NetworkHaError::NoQuorum(
    required=self.seeds.length() / 2 + 1,
    available=0,
  )
}
