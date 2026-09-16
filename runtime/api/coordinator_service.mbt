///|
/// A durable multi-job Coordinator. Each submission owns an independent
/// DriverStore, execution, input log, and set of partition sessions. Public
/// Driver connections bind to one submission; Worker Hello selects a job.
pub struct CoordinatorService {
  data_listener : CoordinatorListener
  control_listener : DriverControlListener
  catalog : @runtime.Wal
  registry : OperatorRegistry
  maximum_events : Int
  entries : Array[JobSubmission]
  jobs : Map[String, String]
  children : Map[String, DriverCoordinator]
  graphs : Map[String, GraphExecution]
  migrations : Map[(String, String), WorkerMigrationExchange]
  network_ha : @runtime.NetworkHaNode?
  mut authority_index : Int64
  mut authority_term : Int64
  mut authority_active : Bool
  mut last_authority_sync : Int64
  catalog_lock : @async_runtime.Mutex
  authority_lock : @async_runtime.Mutex
  mut running : Bool
  mut closed : Bool
  mut shutting_down : Bool
}

///|
struct NetworkJobSnapshot {
  submission : JobSubmission
  driver : DriverAuthoritySnapshot
  execution : ExecutionAuthoritySnapshot?
  stages : Array[NetworkStageSnapshot]
} derive(ToJson, FromJson)

///|
struct NetworkStageSnapshot {
  id : String
  driver : DriverAuthoritySnapshot
  execution : ExecutionAuthoritySnapshot?
} derive(ToJson, FromJson)

///|
struct NetworkServiceSnapshot {
  version : String
  entries : Array[JobSubmission]
  jobs : Array[NetworkJobSnapshot]
} derive(ToJson, FromJson)

///|
pub async fn CoordinatorService::bind(
  data_config : CoordinatorNodeConfig,
  control_config : DriverControlConfig,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
  maximum_events? : Int = 100000,
) -> CoordinatorService raise ApiError {
  if data_config.cluster_id != control_config.cluster_id ||
    data_config.coordinator_id != control_config.coordinator_id ||
    maximum_events < 0 {
    raise ApiError::Cluster("invalid multi-job Coordinator configuration")
  }
  if !(@fs.exists(data_config.storage_dir) catch {
      error => raise driver_error("check service storage", error)
    }) {
    @fs.mkdir(data_config.storage_dir, recursive=true) catch {
      error => raise driver_error("create service storage", error)
    }
  }
  let data_listener = CoordinatorNode::bind(data_config)
  errdefer data_listener.close()
  let control_listener = DriverControlListener::bind(control_config)
  errdefer control_listener.close()
  let (catalog, replay) = @runtime.Wal::open(
    data_config.storage_dir + "/catalog.wal",
    maximum_bytes=data_config.storage_budget_bytes,
  ) catch {
    error => raise driver_error("open job catalog", error)
  }
  errdefer catalog.close()
  let network_ha = match data_config.network_ha {
    None => None
    Some(ha_config) =>
      Some(
        @runtime.NetworkHaNode::open(ha_config) catch {
          error => raise driver_error("open network Coordinator quorum", error)
        },
      )
  }
  let service = CoordinatorService::{
    data_listener,
    control_listener,
    catalog,
    registry,
    maximum_events,
    entries: [],
    jobs: Map([]),
    children: Map([]),
    graphs: Map([]),
    migrations: Map([]),
    network_ha,
    authority_index: 0L,
    authority_term: 0L,
    authority_active: false,
    last_authority_sync: 0L,
    catalog_lock: @async_runtime.Mutex::Mutex(),
    authority_lock: @async_runtime.Mutex::Mutex(),
    running: false,
    closed: false,
    shutting_down: false,
  }
  errdefer service.release()
  for bytes in replay.records() {
    let submission : JobSubmission = @json.from_json(
      @json.parse(@utf8.decode(bytes)),
    ) catch {
      error => raise driver_error("decode job catalog", error)
    }
    service.validate_new_submission(submission)
    service.entries.push(submission)
    service.jobs[submission.job().definition.job_id] = submission.submission_id()
  }
  match service.network_ha {
    Some(ha) =>
      match ha.state() {
        None => {
          service.catalog.rewrite([]) catch {
            error => raise driver_error("clear unconfirmed catalog", error)
          }
          service.entries.clear()
          service.jobs.clear()
        }
        Some(encoded) => {
          service.restore_network_authority(encoded)
          service.authority_index = ha.commit_index()
          service.authority_term = ha.state_term()
        }
      }
    None =>
      for index, submission in service.entries {
        ignore(service.open_child(submission, index))
      }
  }
  service
}

///|
/// Install the entire committed service generation before accepting work.
/// The quorum WAL remains the recovery point if any local installation stops.
async fn CoordinatorService::restore_network_authority(
  self : CoordinatorService,
  encoded : Json,
) -> Unit raise ApiError {
  self.catalog_lock.acquire() catch {
    error => raise driver_error("lock authority catalog installation", error)
  }
  defer self.catalog_lock.release()
  let snapshot : NetworkServiceSnapshot = @json.from_json(encoded) catch {
    error => raise driver_error("decode network Coordinator authority", error)
  }
  if snapshot.version != "1" {
    raise ApiError::Cluster("unsupported network Coordinator authority version")
  }
  if snapshot.entries != snapshot.jobs.map(item => item.submission) {
    raise ApiError::Cluster("authority catalog differs from jobs")
  }
  for child in self.children.values().to_array() {
    child.stop_for_authority()
    child.close()
  }
  for graph in self.graphs.values().to_array() {
    for child in graph.stages.values() {
      child.stop_for_authority()
    }
    graph.close()
  }
  self.children.clear()
  self.graphs.clear()
  self.entries.clear()
  self.jobs.clear()
  for item in snapshot.jobs {
    self.validate_new_submission(item.submission)
    self.entries.push(item.submission)
    self.jobs[item.submission.job().definition.job_id] = item.submission.submission_id()
  }
  self.catalog.rewrite(
    snapshot.entries.map(entry => @utf8.encode(@json.to_json(entry).stringify())),
  ) catch {
    error => raise driver_error("replace authority catalog", error)
  }
  for index, item in snapshot.jobs {
    ignore(self.open_child(item.submission, index, authority=item))
  }
}

///|
/// Sync the latest quorum-confirmed service state before scheduling children.
/// A recovering node may learn a newer command from a peer heartbeat while its
/// local child is already open. Starting that child before applying the
/// authority snapshot can expose an empty delivery plan to a Worker that is
/// resuming from durable progress.
async fn CoordinatorService::sync_network_authority(
  self : CoordinatorService,
) -> Unit raise ApiError {
  self.authority_lock.acquire() catch {
    error => raise driver_error("lock authority synchronization", error)
  }
  defer self.authority_lock.release()
  match self.network_ha {
    None => ()
    Some(ha) => {
      ignore(
        ha.tick() catch {
          error => raise driver_error("tick network Coordinator quorum", error)
        },
      )
      match ha.state() {
        Some(encoded) if ha.commit_index() > self.authority_index ||
          ha.state_term() > self.authority_term => {
          self.restore_network_authority(encoded)
          self.authority_index = ha.commit_index()
          self.authority_term = ha.state_term()
        }
        _ => ()
      }
    }
  }
}

///|
pub fn CoordinatorService::data_endpoint(
  self : CoordinatorService,
) -> NodeEndpoint raise ApiError {
  self.data_listener.endpoint()
}

///|
pub fn CoordinatorService::control_endpoint(
  self : CoordinatorService,
) -> NodeEndpoint raise ApiError {
  self.control_listener.endpoint()
}

///|
pub fn CoordinatorService::job_count(self : CoordinatorService) -> Int {
  self.entries.length()
}

///|
/// Serialize the complete Coordinator authority carried by the network
/// quorum. This includes submission identity, lifecycle state and every
/// accepted input retained by each DriverStore; no local catalog file is used
/// as an implicit shared authority.
async fn CoordinatorService::authoritative_snapshot(
  self : CoordinatorService,
) -> Json raise ApiError {
  self.catalog_lock.acquire() catch {
    error => raise driver_error("lock authority catalog snapshot", error)
  }
  defer self.catalog_lock.release()
  let jobs : Array[NetworkJobSnapshot] = []
  for entry in self.entries {
    guard self.children.get(entry.submission_id()) is Some(child) else {
      raise ApiError::Cluster("authority catalog has no execution")
    }
    let stages : Array[NetworkStageSnapshot] = []
    if self.graphs.get(entry.submission_id()) is Some(graph) {
      for (id, _) in graph.job.graph_stages {
        let stage = graph.stages[id]
        stages.push({
          id,
          driver: stage.store.authority_snapshot(),
          execution: match stage.live_execution {
            Some(execution) => Some(execution.authority_snapshot())
            None => stage.pending_authority
          },
        })
      }
    }
    jobs.push({
      submission: entry,
      driver: child.store.authority_snapshot(),
      execution: match child.live_execution {
        Some(execution) => Some(execution.authority_snapshot())
        None => child.pending_authority
      },
      stages,
    })
  }
  Json::object({
    "version": Json::string("1"),
    "entries": @json.to_json(self.entries),
    "jobs": @json.to_json(jobs),
  })
}

///|
pub fn CoordinatorService::statuses(
  self : CoordinatorService,
) -> Array[JobRunStatus] raise ApiError {
  if self.closed {
    raise ApiError::DriverClosed
  }
  let result = []
  for entry in self.entries {
    if self.children.get(entry.submission_id()) is Some(child) {
      result.push(child.store.status())
    }
  }
  result
}

///|
fn CoordinatorService::child_config(
  self : CoordinatorService,
  compiled : CompiledJob,
  index : Int,
) -> CoordinatorNodeConfig raise ApiError {
  let config = self.data_listener.config
  // Keep registered spare Worker slots in every child configuration. They
  // are idle until an online rescale moves a lease to them, but filtering
  // them here made a valid target impossible to connect to.
  let workers = config.workers.copy()
  let result = {
    ..config,
    bind: NodeEndpoint::new("127.0.0.1", 0),
    advertised: None,
    storage_dir: config.storage_dir + "/jobs/job-" + index.to_string(),
    workers,
    migration_rpc: Some((intent, bundle) => {
      self.request_worker_migration(intent, bundle)
    }),
  }
  validate_coordinator_workers(compiled, result)
  result
}

///|
fn CoordinatorService::validate_new_submission(
  self : CoordinatorService,
  submission : JobSubmission,
) -> Unit raise ApiError {
  ignore(JobSubmission::new(submission.submission_id(), submission.job()))
  let compiled = submission.job().compile(registry=self.registry)
  if self.entries.any(entry => {
      entry.submission_id() == submission.submission_id()
    }) ||
    self.jobs.contains(compiled.job_id()) {
    raise ApiError::RequestConflict(submission.submission_id())
  }
  ignore(self.child_config(compiled, self.entries.length()))
}

///|
/// Reject mutations on a fenced/old Coordinator before touching any local
/// Driver or catalog state. Followers are intentionally read-only until the
/// caller reconnects through a discovered leader.
async fn CoordinatorService::ensure_network_leader(
  self : CoordinatorService,
) -> Unit raise ApiError {
  self.sync_network_authority()
  match self.network_ha {
    None => ()
    Some(ha) => {
      let status = ha.tick() catch {
        error => raise driver_error("tick network Coordinator quorum", error)
      }
      if status.leader() != ha.node_id() {
        raise ApiError::Cluster(
          "Coordinator is not the network quorum leader (leader=" +
          status.leader() +
          ")",
        )
      }
      if status.available_peers() < status.quorum() {
        raise ApiError::Cluster(
          "Coordinator quorum is unavailable (available=" +
          status.available_peers().to_string() +
          ")",
        )
      }
    }
  }
}

///|
async fn CoordinatorService::open_child(
  self : CoordinatorService,
  submission : JobSubmission,
  index : Int,
  authority? : NetworkJobSnapshot,
) -> DriverCoordinator raise ApiError {
  if self.children.get(submission.submission_id()) is Some(child) {
    return child
  }
  let current = authority
    .map(item => item.driver.submission)
    .unwrap_or(submission)
  let compiled = current.job().compile(registry=self.registry)
  let config = self.child_config(compiled, index)
  ensure_live_storage(config.storage_dir)
  let store = DriverStore::open(
    config.storage_dir + "/driver.wal",
    registry=self.registry,
    maximum_bytes=config.storage_budget_bytes,
    maximum_events=self.maximum_events,
  )
  errdefer store.close()
  match authority {
    Some(item) => store.replace_authority_snapshot(item.driver)
    None => if store.submission() is None { ignore(store.submit(submission)) }
  }
  // The catalog records the original submission identity; a compatible
  // update lives in the Driver WAL and is the execution configuration.
  let compiled = store.compiled.unwrap()
  let child = DriverCoordinator::bind(
    config,
    {
      ..self.control_listener.config,
      bind: NodeEndpoint::new("127.0.0.1", 0),
      advertised: None,
    },
    store,
  )
  child.pending_authority = authority.bind(item => item.execution)
  child.confirm_authority = self.network_ha.map(_ => {
    () => self.replicate_authority()
  })
  if compiled.is_graph() {
    self.graphs[submission.submission_id()] = GraphExecution::open(
      compiled,
      child,
      self.registry,
      self.maximum_events,
      authority?=authority.map(item => item.stages),
    )
  }
  // Publish only after graph children are ready. The service tick must never
  // mistake a partially initialized graph for an ordinary single-stage job.
  self.children[submission.submission_id()] = child
  child
}

///|
async fn CoordinatorService::submitted_child(
  self : CoordinatorService,
  submission : JobSubmission,
) -> DriverCoordinator raise ApiError {
  self.catalog_lock.acquire() catch {
    error => raise driver_error("lock job catalog", error)
  }
  defer self.catalog_lock.release()
  if self.closed {
    raise ApiError::DriverClosed
  }
  let index = match
    self.entries.search_by(entry => {
      entry.submission_id() == submission.submission_id()
    }) {
    // A Driver reconnect may carry the candidate submission of an update
    // that was committed just before the transport failed. The catalog
    // identity remains the stable submission_id; DriverStore performs the
    // full payload/idempotency check once the child is selected.
    Some(index) => index
    None => {
      if self.shutting_down {
        raise ApiError::DriverClosed
      }
      self.validate_new_submission(submission)
      let index = self.entries.length()
      // The catalog makes the job identity durable before any per-job ACK.
      // An interruption here can leave an unacknowledged job to recover, but
      // never an acknowledged submission absent from the recovered catalog.
      ignore(
        self.catalog.append(@utf8.encode(@json.to_json(submission).stringify())) catch {
          error => raise driver_error("persist job catalog", error)
        },
      )
      self.entries.push(submission)
      self.jobs[submission.job().definition.job_id] = submission.submission_id()
      index
    }
  }
  // Always open from the catalog's original envelope. A reconnect candidate
  // must not create a second child before DriverStore validates it.
  self.open_child(self.entries[index], index)
}

///|
async fn CoordinatorService::handle_command(
  self : CoordinatorService,
  submission_id : String,
  command : JobCommand,
) -> Result[JobRunStatus, String] {
  try {
    if self.closed || (self.shutting_down && command is JobCommand::Input(_)) {
      raise ApiError::DriverClosed
    }
    self.ensure_network_leader()
    let child = match command {
      JobCommand::Submit(submission) => {
        let mut internal : DriverCoordinator? = None
        for graph in self.graphs.values() {
          if graph.submitted_child(submission) is Some(stage) {
            internal = Some(stage)
          }
        }
        match internal {
          Some(stage) => stage
          None => self.submitted_child(submission)
        }
      }
      _ =>
        match self.children.get(submission_id) {
          Some(child) => child
          None => {
            let mut internal : DriverCoordinator? = None
            for graph in self.graphs.values() {
              for (id, submission) in graph.submissions {
                if submission.submission_id() == submission_id {
                  internal = graph.stages.get(id)
                }
              }
            }
            match internal {
              Some(stage) => stage
              None => raise ApiError::UnknownSubmission(submission_id)
            }
          }
        }
    }
    let status = match (self.network_ha, command) {
      (Some(_), JobCommand::Drain) if self.graphs.get(submission_id) is None => {
        // Publish the drain intent before waiting for Worker completion. If a
        // quorum reply is lost, retrying can safely resume from Draining; the
        // old ordering let a local terminal transition outrun HA authority.
        ignore(child.apply_command(command))
        self.replicate_authority()
        while child.store.phase() == JobRunPhase::Draining {
          if self.closed {
            raise ApiError::DriverClosed
          }
          @async_runtime.sleep(5)
        }
        let terminal = child.store.status()
        self.replicate_authority()
        Ok(terminal)
      }
      (_, _) =>
        match self.graphs.get(submission_id) {
          Some(graph) => graph.handle_command(command)
          None => child.handle_command(command)
        }
    }
    if command != JobCommand::Status &&
      !(self.network_ha is Some(_) &&
      command is JobCommand::Drain &&
      self.graphs.get(submission_id) is None) {
      match self.network_ha {
        Some(_) => self.replicate_authority()
        None => ()
      }
    }
    status
  } catch {
    error => Err(@debug.to_string(error))
  }
}

///|
/// Relay with bounded buffers; blocking writes carry downstream backpressure
/// to the original socket. Either EOF closes both halves and cancels the peer.
async fn relay_job_socket(source : @socket.Tcp, target : @socket.Tcp) -> Unit {
  @async_runtime.with_task_group <| group => {
    group.spawn_bg(no_wait=true) <| () => {
      defer {
        source.close()
        target.close()
      }
      for ;; {
        guard @io.Reader::read_some(source, max_len=65536) is Some(bytes) else {
          return
        }
        @io.Writer::write(target, bytes)
      }
    }
    defer {
      source.close()
      target.close()
    }
    for ;; {
      guard @io.Reader::read_some(target, max_len=65536) is Some(bytes) else {
        return
      }
      @io.Writer::write(source, bytes)
    }
  }
}

///|
async fn CoordinatorService::serve_worker(
  self : CoordinatorService,
  connection : @socket.Tcp,
) -> Unit {
  defer connection.close()
  self.ensure_network_leader()
  let first = @async_runtime.with_timeout(5000, () => {
    @runtime.read_frame(connection)
  })
  let hello = @protocol.Hello::from_payload(first.payload())
  let config = self.data_listener.config
  let validator = @protocol.CoordinatorSession::new(
    config.cluster_id,
    config.coordinator_id,
    hello.worker_id(),
  )
  ignore(validator.accept_hello(first, auth_token?=config.auth_token))
  guard hello.job_id() is Some(job_id) else {
    raise ApiError::InvalidWorkerAssignment(
      "multi-job Worker Hello requires job_id",
    )
  }
  // Workers may arrive immediately before Driver submission, just as on the
  // single-job listener. Wait without borrowing another job's assignment.
  let child = @async_runtime.with_timeout(30000, () => {
    for ;; {
      if self.closed {
        raise ApiError::DriverClosed
      }
      if self.jobs.get(job_id) is Some(id) &&
        self.children.get(id) is Some(child) {
        return child
      }
      for graph in self.graphs.values() {
        if graph.worker_child(job_id) is Some(child) {
          return child
        }
      }
      @async_runtime.sleep(5)
    }
  })
  if !child.data_listener.config.workers.any(slot => {
      slot.worker_id() == hello.worker_id() &&
      slot.incarnation() == hello.incarnation()
    }) {
    raise ApiError::InvalidWorkerAssignment(hello.worker_id())
  }
  if hello.capabilities().contains(assignment_query_capability) {
    self.reply_worker_allocation(child, connection, first, hello)
    return
  }
  while self.worker_withdrawn(child) {
    if self.closed {
      raise ApiError::DriverClosed
    }
    @async_runtime.sleep(5)
  }
  match child.store.phase() {
    JobRunPhase::Cancelled | JobRunPhase::Failed | JobRunPhase::Succeeded => {
      // A terminal job no longer has a supervisor accepting connections.
      // Finish the authenticated handshake with no assignment, so the Worker
      // gets MissingAssignment (permanent) instead of retrying a socket EOF
      // and then waiting forever in the inactive listener's accept queue.
      ignore(
        @runtime.coordinator_handshake_after_hello(
          connection,
          first,
          config.cluster_id,
          config.coordinator_id,
          hello.worker_id(),
          "terminal-" + first.message_id(),
          auth_token?=config.auth_token,
        ),
      )
      return
    }
    _ => ()
  }
  let endpoint = child.data_endpoint()
  let target = @socket.Tcp::connect_to_host(
    endpoint.host(),
    port=endpoint.port(),
  )
  defer target.close()
  @runtime.write_frame(target, first)
  relay_job_socket(connection, target)
}

///|
fn CoordinatorService::release(self : CoordinatorService) -> Unit {
  self.data_listener.close()
  self.control_listener.close()
  for child in self.children.values() {
    child.close()
  }
  for graph in self.graphs.values() {
    graph.close()
  }
  match self.network_ha {
    Some(ha) => ha.close()
    None => ()
  }
  self.catalog.close()
}

///|
/// Stop transport without inventing terminal intents. Restart recovers the
/// independent lifecycle of every job from its durable store.
pub fn CoordinatorService::close(self : CoordinatorService) -> Unit {
  self.closed = true
  self.data_listener.close()
  self.control_listener.close()
  if !self.running {
    self.release()
  }
}

///|
pub async fn CoordinatorService::shutdown_gracefully(
  self : CoordinatorService,
) -> Unit raise ApiError {
  self.shutting_down = true
  @async_runtime.protect_from_cancel(() => {
    @async_runtime.with_task_group <| group => {
      for child in self.children.values().to_array() {
        if child.store.submission() is Some(submission) &&
          self.graphs.get(submission.submission_id()) is Some(graph) {
          group.spawn_bg() <| () => { ignore(graph.handle_command(Drain)) }
        } else {
          group.spawn_bg() <| () => { child.shutdown_gracefully() }
        }
      }
    }
  }) catch {
    error => raise driver_error("drain service jobs", error)
  }
}

///|
pub async fn CoordinatorService::run(
  self : CoordinatorService,
) -> Unit raise ApiError {
  if self.closed || self.running {
    raise ApiError::DriverClosed
  }
  self.running = true
  defer {
    self.running = false
    self.closed = true
    self.release()
  }
  try
    @async_runtime.with_task_group <| group => {
      group.spawn_bg(no_wait=true) <| () => {
        self.control_listener.server.run_forever(
          allow_failure=true,
          max_connections=128,
        ) <| ((connection, _) => {
          serve_driver_connection(connection, self.control_listener, (
            id,
            command,
          ) => self.handle_command(id, command))
        })
      }
      group.spawn_bg(no_wait=true) <| () => {
        match self.network_ha {
          Some(ha) =>
            while !self.closed {
              ignore(ha.tick())
              @async_runtime.sleep(25)
            }
          None => ()
        }
      }
      group.spawn_bg(no_wait=true) <| () => {
        match self.network_ha {
          Some(ha) => ha.serve()
          None => ()
        }
      }
      group.spawn_bg(no_wait=true) <| () => {
        self.data_listener.server.run_forever(
          allow_failure=true,
          max_connections=256,
        ) <| ((connection, _) => self.serve_worker(connection))
      }
      while !self.closed {
        // Apply peer-learned authority before any child tick can start a live
        // execution. This ordering is required during Coordinator failover:
        // Worker resume progress must only meet a plan rebuilt from the same
        // quorum-confirmed input prefix.
        self.sync_network_authority()
        if !self.network_active() {
          for child in self.children.values() {
            child.stop_for_authority()
          }
          for graph in self.graphs.values() {
            for child in graph.stages.values() {
              child.stop_for_authority()
            }
          }
          if self.authority_active {
            self.authority_index = -1L
            self.authority_active = false
          }
          @async_runtime.sleep(25)
          continue
        }
        self.authority_active = true
        for child in self.children.values().to_array() {
          try {
            if child.store.submission() is Some(submission) &&
              self.graphs.get(submission.submission_id()) is Some(graph) {
              graph.tick(group)
            } else {
              child.tick(group)
            }
          } catch {
            error => {
              if self.closed || @async_runtime.is_being_cancelled() {
                raise error
              }
              ignore(child.store.fail(@debug.to_string(error)))
            }
          }
        }
        if self.network_ha is Some(_) &&
          @async_runtime.now() - self.last_authority_sync >= 100L {
          self.replicate_authority() catch {
            _ => ()
          }
          self.last_authority_sync = @async_runtime.now()
        }
        @async_runtime.sleep(5)
      }
    }
  catch {
    error =>
      if !self.closed {
        raise driver_error("Coordinator service", error)
      }
  }
}
