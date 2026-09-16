///|
/// Configuration for the asynchronous Driver control connection. The data
/// plane remains owned by Worker/Coordinator nodes; this handle only submits
/// a validated Job and lifecycle commands.
pub(all) struct DriverConfig {
  cluster_id : String
  coordinator_id : String
  driver_id : String
  endpoint : NodeEndpoint
  coordinator_endpoints : Array[NodeEndpoint]
  incarnation : String
  auth_token : String?
} derive(Eq)

///|
pub fn DriverConfig::new(
  cluster_id : String,
  coordinator_id : String,
  driver_id : String,
  endpoint : NodeEndpoint,
) -> DriverConfig raise ApiError {
  if cluster_id.is_empty() || coordinator_id.is_empty() || driver_id.is_empty() {
    raise ApiError::Cluster("cluster, coordinator, and driver ids are required")
  }
  if driver_id == coordinator_id {
    raise ApiError::Cluster("driver id must differ from coordinator id")
  }
  if endpoint.port() == 0 {
    raise ApiError::InvalidEndpoint("Driver endpoint cannot use port zero")
  }
  {
    cluster_id,
    coordinator_id,
    driver_id,
    endpoint,
    coordinator_endpoints: [endpoint],
    incarnation: "driver-" + driver_id,
    auth_token: None,
  }
}

///|
/// Configure ordered Coordinator seeds used for reconnect and leader
/// discovery. The first reachable endpoint is selected for each retry.
pub fn DriverConfig::with_coordinator_endpoints(
  self : DriverConfig,
  endpoints : ArrayView[NodeEndpoint],
) -> DriverConfig raise ApiError {
  if endpoints.is_empty() || endpoints.any(endpoint => endpoint.port() == 0) {
    raise ApiError::InvalidEndpoint(
      "Coordinator endpoint list is empty or invalid",
    )
  }
  {
    ..self,
    coordinator_endpoints: endpoints.to_owned(),
    endpoint: endpoints[0],
  }
}

///|
pub fn DriverConfig::coordinator_endpoints(
  self : DriverConfig,
) -> ArrayView[NodeEndpoint] {
  self.coordinator_endpoints[:]
}

///|
pub fn DriverConfig::with_incarnation(
  self : DriverConfig,
  incarnation : String,
) -> DriverConfig raise ApiError {
  if incarnation.is_empty() || !incarnation.has_prefix("driver-") {
    raise ApiError::Cluster("driver incarnation must start with driver-")
  }
  { ..self, incarnation, }
}

///|
pub fn DriverConfig::cluster_id(self : DriverConfig) -> String {
  self.cluster_id
}

///|
pub fn DriverConfig::coordinator_id(self : DriverConfig) -> String {
  self.coordinator_id
}

///|
pub fn DriverConfig::driver_id(self : DriverConfig) -> String {
  self.driver_id
}

///|
pub fn DriverConfig::endpoint(self : DriverConfig) -> NodeEndpoint {
  self.endpoint
}

///|
pub fn DriverConfig::incarnation(self : DriverConfig) -> String {
  self.incarnation
}

///|
/// Configure the shared secret presented in the first Driver submit frame.
pub fn DriverConfig::with_auth_token(
  self : DriverConfig,
  token : String,
) -> DriverConfig raise ApiError {
  if token.is_empty() {
    raise ApiError::Cluster("Driver auth token must not be empty")
  }
  { ..self, auth_token: Some(token), }
}

///|
pub fn DriverConfig::auth_token(self : DriverConfig) -> String? {
  self.auth_token
}

///|
async fn connect_driver_seed(
  config : DriverConfig,
  start : Int,
) -> (@socket.Tcp, Int) raise ApiError {
  let endpoints = config.coordinator_endpoints
  let mut last_error = "no Coordinator endpoint"
  for offset in 0..<endpoints.length() {
    let index = (start + offset) % endpoints.length()
    let endpoint = endpoints[index]
    try
      @socket.Tcp::connect_to_host(endpoint.host(), port=endpoint.port())
    catch {
      error => {
        last_error = @debug.to_string(error)
        continue
      }
    } noraise {
      connection => return (connection, index)
    }
  }
  raise ApiError::Cluster("connect Driver: " + last_error)
}

///|
async fn connect_driver_seed_bounded(
  config : DriverConfig,
  start : Int,
) -> (@socket.Tcp, Int)? noraise {
  try
    @async_runtime.with_timeout_opt(2000, () => {
      connect_driver_seed(config, start)
    })
  catch {
    _ => None
  } noraise {
    value => value
  }
}

///|
/// Persistent client-side control session. Every command waits for a matching
/// status frame, so a completed command is never reported before its response
/// crossed the async socket boundary.
pub struct DriverClient {
  config : DriverConfig
  registry : OperatorRegistry
  mut connection : @socket.Tcp
  mut outbound : @protocol.FrameBuilder
  mut session : @protocol.DriverClientSession
  mut closed : Bool
  operation_lock : @async_runtime.Mutex
  mut submitted : JobSubmission?
  mut pending_update : JobSubmission?
  mut endpoint_index : Int
  mut connected : Bool
}

///|
pub async fn DriverClient::connect(
  config : DriverConfig,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> DriverClient raise ApiError {
  guard connect_driver_seed_bounded(config, 0)
    is Some((connection, endpoint_index)) else {
    raise ApiError::Cluster("connect Driver: no Coordinator endpoint available")
  }
  let outbound = @protocol.FrameBuilder::new(
    config.cluster_id,
    config.driver_id,
    config.incarnation,
  ) catch {
    error => {
      connection.close()
      raise ApiError::Cluster(
        "build Driver session: " + @debug.to_string(error),
      )
    }
  }
  let session = @protocol.DriverClientSession::new(
    config.cluster_id,
    config.coordinator_id,
    config.driver_id,
  ) catch {
    error => {
      connection.close()
      raise ApiError::Cluster(
        "create Driver session: " + @debug.to_string(error),
      )
    }
  }
  {
    config,
    registry,
    connection,
    outbound,
    session,
    closed: false,
    operation_lock: @async_runtime.Mutex::Mutex(),
    submitted: None,
    pending_update: None,
    endpoint_index,
    connected: true,
  }
}

///|
pub fn DriverClient::close(self : DriverClient) -> Unit {
  if !self.closed {
    self.closed = true
    self.session.close()
    self.connection.close()
  }
}

///|
fn DriverClient::ensure_open(self : DriverClient) -> Unit raise ApiError {
  if self.closed {
    raise ApiError::DriverClosed
  }
}

///|
fn driver_command_name(command : JobCommand) -> String {
  match command {
    JobCommand::Submit(_) => "submit"
    JobCommand::Input(_) => "input"
    JobCommand::Status => "status"
    JobCommand::Pause => "pause"
    JobCommand::Resume => "resume"
    JobCommand::Rescale(_) => "rescale"
    JobCommand::Update(_) => "update"
    JobCommand::Drain => "drain"
    JobCommand::Cancel => "cancel"
  }
}

///|
fn driver_command_payload(command : JobCommand) -> Json? {
  match command {
    JobCommand::Submit(_)
    | JobCommand::Status
    | JobCommand::Pause
    | JobCommand::Resume
    | JobCommand::Drain
    | JobCommand::Cancel => None
    JobCommand::Input(events) => Some(@json.to_json(events))
    JobCommand::Rescale(workers) => Some(@json.to_json(workers))
    JobCommand::Update(submission) => Some(@json.to_json(submission))
  }
}

///|
fn driver_submit_payload(submission : JobSubmission) -> Json {
  @json.to_json(submission)
}

///|
fn driver_status_to_api(
  status : @protocol.DriverStatus,
  idempotent_sink_required : Bool,
) -> JobRunStatus raise ApiError {
  let phase = match status.phase() {
    "Created" => JobRunPhase::Created
    "Running" => JobRunPhase::Running
    "Draining" => JobRunPhase::Draining
    "Succeeded" => JobRunPhase::Succeeded
    "Failed" => JobRunPhase::Failed
    "Cancelled" => JobRunPhase::Cancelled
    _ => raise ApiError::Cluster("invalid Driver status phase")
  }
  {
    submission_id: status.submission_id(),
    job_id: status.job_id(),
    plan_digest: status.plan_digest(),
    phase,
    accepted_events: status.accepted_events(),
    failure: status.failure(),
    idempotent_sink_required,
    paused: status.is_paused(),
    parallelism: status.parallelism,
  }
}

///|
async fn DriverClient::exchange_once(
  self : DriverClient,
  kind : @protocol.FrameKind,
  payload : String,
) -> JobRunStatus raise ApiError {
  self.ensure_open()
  let frame = self.outbound.build(kind, payload) catch {
    error =>
      raise ApiError::Cluster("build Driver frame: " + @debug.to_string(error))
  }
  @runtime.write_frame(self.connection, frame) catch {
    error => {
      self.connection.close()
      self.connected = false
      raise ApiError::Cluster("write Driver frame: " + @debug.to_string(error))
    }
  }
  let response = @runtime.read_frame(self.connection) catch {
    error => {
      self.connection.close()
      self.connected = false
      raise ApiError::Cluster("read Driver status: " + @debug.to_string(error))
    }
  }
  if response.worker_incarnation() != self.config.incarnation ||
    (
      self.session.phase() == @protocol.DriverSessionPhase::Disconnected &&
      response.session_id() != frame.message_id()
    ) {
    raise ApiError::Cluster("Driver response session identity mismatch")
  }
  let reply = self.session.accept_reply(response, frame.message_id()) catch {
    error =>
      raise ApiError::Cluster(
        "validate Driver status: " + @debug.to_string(error),
      )
  }
  if self.outbound.session_id() != self.session.session_id() {
    self.outbound.set_session_id(self.session.session_id()) catch {
      error =>
        raise ApiError::Cluster(
          "activate Driver session: " + @debug.to_string(error),
        )
    }
  }
  let status = match reply.result {
    Ok(value) => value
    Err(reason) if reason.contains("network quorum leader") ||
      reason.contains("Coordinator quorum is unavailable") ||
      reason.contains("NoQuorum") ||
      reason.contains("NotLeader") ||
      reason.contains("quorum") => {
      self.connection.close()
      self.connected = false
      raise ApiError::Cluster(reason)
    }
    Err(reason) => raise ApiError::InvalidDriverCommand(reason)
  }
  // An update response carries the new plan while the client still keeps the
  // previous submission as the reconnect envelope. Validate the response
  // against the in-flight update, but validate a replayed DriverSubmit
  // against the old envelope used to bind the new session.
  let expected_submission : JobSubmission? = if kind ==
    @protocol.FrameKind::DriverSubmit {
    self.submitted
  } else {
    match self.pending_update {
      Some(value) => Some(value)
      None => self.submitted
    }
  }
  let idempotent_sink_required = match expected_submission {
    Some(submission) =>
      submission.job().definition().sink().requires_idempotent_consumer()
    None => true
  }
  let converted = driver_status_to_api(status, idempotent_sink_required)
  match expected_submission {
    Some(submission) if converted.submission_id != submission.submission_id ||
      converted.job_id != submission.job.definition.job_id ||
      converted.plan_digest != submission.job.plan_digest =>
      raise ApiError::SubmissionMismatch("Driver response")
    _ => ()
  }
  converted
}

///|
/// Exchange a command with reconnect and ordered endpoint failover. A
/// transport failure is retried by establishing a fresh Driver session and
/// replaying the durable submission before the original command.
async fn DriverClient::exchange(
  self : DriverClient,
  kind : @protocol.FrameKind,
  payload : String,
) -> JobRunStatus raise ApiError {
  self.ensure_open()
  // Failover elections and quorum catch-up may outlive one heartbeat period.
  // Keep retrying within the CLI's bounded command timeout so a transient
  // leader handoff is not surfaced as a permanent Driver failure.
  let attempts = self.config.coordinator_endpoints.length() * 10
  let mut last_error = "Coordinator connection failed"
  for _ in 0..<attempts {
    if !self.connected {
      let next = (self.endpoint_index + 1) %
        self.config.coordinator_endpoints.length()
      guard connect_driver_seed_bounded(self.config, next)
        is Some((connection, index)) else {
        last_error = "Coordinator reconnect timed out"
        @async_runtime.sleep(100) catch {
          error => raise driver_error("backoff Driver reconnect", error)
        }
        continue
      }
      self.connection = connection
      self.endpoint_index = index
      self.connected = true
      self.session = @protocol.DriverClientSession::new(
        self.config.cluster_id,
        self.config.coordinator_id,
        self.config.driver_id,
      ) catch {
        error => {
          self.connection.close()
          self.connected = false
          last_error = @debug.to_string(error)
          continue
        }
      }
      self.outbound = @protocol.FrameBuilder::new(
        self.config.cluster_id,
        self.config.driver_id,
        self.config.incarnation,
      ) catch {
        error => {
          self.connection.close()
          self.connected = false
          last_error = @debug.to_string(error)
          continue
        }
      }
      // The server binds a connection only after Submit. Replay it before a
      // command so a new leader can reconstruct the same Driver session.
      let expected_submission : JobSubmission? = self.submitted
      match expected_submission {
        Some(submission) => {
          let submit_payload = @protocol.DriverSubmit::new(
            submission.submission_id(),
            driver_submit_payload(submission),
            auth_token?=self.config.auth_token,
          ).to_payload() catch {
            error => raise ApiError::Cluster(@debug.to_string(error))
          }
          let mut replayed = try
            @async_runtime.with_timeout_opt(5000, () => {
              self.exchange_once(
                @protocol.FrameKind::DriverSubmit,
                submit_payload,
              )
            })
          catch {
            error => {
              last_error = "Coordinator replay: " + @debug.to_string(error)
              None
            }
          } noraise {
            value => value
          }
          if replayed is None && self.pending_update is Some(candidate) {
            // The update may have been committed after the old replay was
            // received but before its reply was lost. In that case the old
            // envelope is rejected by the durable store; retry the candidate
            // envelope so the new session can bind to the installed plan.
            let previous_submission = self.submitted
            self.submitted = Some(candidate)
            let candidate_payload = @protocol.DriverSubmit::new(
              candidate.submission_id(),
              driver_submit_payload(candidate),
              auth_token?=self.config.auth_token,
            ).to_payload() catch {
              error => raise ApiError::Cluster(@debug.to_string(error))
            }
            let candidate_replayed = try
              @async_runtime.with_timeout_opt(5000, () => {
                self.exchange_once(
                  @protocol.FrameKind::DriverSubmit,
                  candidate_payload,
                )
              })
            catch {
              error => {
                last_error = "Coordinator candidate replay: " +
                  @debug.to_string(error)
                None
              }
            } noraise {
              value => value
            }
            if candidate_replayed is Some(_) {
              replayed = candidate_replayed
            } else {
              self.submitted = previous_submission
            }
          }
          guard replayed is Some(replayed_status) else {
            self.connection.close()
            self.connected = false
            if !last_error.has_prefix("Coordinator replay:") {
              last_error = "Coordinator replay timed out"
            }
            @async_runtime.sleep(100) catch {
              error => raise driver_error("backoff Driver replay", error)
            }
            continue
          }
          // Rebinding already completes an in-flight submit. A second submit
          // on the now-running session violates the Driver protocol.
          if kind == @protocol.FrameKind::DriverSubmit {
            return replayed_status
          }
        }
        None => ()
      }
    }
    try self.exchange_once(kind, payload) catch {
      error =>
        match error {
          ApiError::Cluster(reason) => {
            self.connection.close()
            self.connected = false
            last_error = reason
            @async_runtime.sleep(100) catch {
              error => raise driver_error("backoff Driver reconnect", error)
            }
            continue
          }
          _ => raise error
        }
    } noraise {
      status => return status
    }
  }
  raise ApiError::Cluster(last_error)
}

///|
pub async fn DriverClient::submit(
  self : DriverClient,
  submission : JobSubmission,
) -> JobRunStatus raise ApiError {
  self.operation_lock.acquire() catch {
    error => raise driver_error("lock Driver", error)
  }
  defer self.operation_lock.release()
  self.ensure_open()
  ignore(submission.compile(registry=self.registry))
  if self.submitted is Some(previous) {
    if previous != submission {
      raise ApiError::RequestConflict(submission.submission_id)
    }
    return self.exchange(
      @protocol.FrameKind::DriverCommand,
      @protocol.DriverCommand::new("status").to_payload() catch {
        error => raise driver_error("build Driver status", error)
      },
    )
  }
  self.submitted = Some(submission)
  self.exchange(
    @protocol.FrameKind::DriverSubmit,
    @protocol.DriverSubmit::new(
      submission.submission_id(),
      driver_submit_payload(submission),
      auth_token?=self.config.auth_token,
    ).to_payload() catch {
      error => raise ApiError::Cluster(@debug.to_string(error))
    },
  )
}

///|
pub async fn DriverClient::command(
  self : DriverClient,
  command : JobCommand,
) -> JobRunStatus raise ApiError {
  self.operation_lock.acquire() catch {
    error => raise driver_error("lock Driver", error)
  }
  defer self.operation_lock.release()
  self.command_locked(command)
}

///|
/// Execute a command while the client operation lock is already held. This
/// lets Update keep its pending submission state coherent across reconnects
/// without attempting to acquire the same non-reentrant lock twice.
async fn DriverClient::command_locked(
  self : DriverClient,
  command : JobCommand,
) -> JobRunStatus raise ApiError {
  self.ensure_open()
  if self.submitted is None {
    raise ApiError::UnknownSubmission("submit first")
  }
  if command is JobCommand::Submit(_) {
    raise ApiError::InvalidDriverCommand("Submit must use DriverClient::submit")
  }
  self.exchange(
    @protocol.FrameKind::DriverCommand,
    @protocol.DriverCommand::new(
      driver_command_name(command),
      payload?=driver_command_payload(command),
    ).to_payload() catch {
      error => raise ApiError::Cluster(@debug.to_string(error))
    },
  )
}

///|
/// Replace a paused submission after the Coordinator validates state-schema
/// compatibility and recreates the live Worker execution.
pub async fn DriverClient::update(
  self : DriverClient,
  submission : JobSubmission,
) -> JobRunStatus raise ApiError {
  self.operation_lock.acquire() catch {
    error => raise driver_error("lock Driver update", error)
  }
  defer self.operation_lock.release()
  self.ensure_open()
  guard self.submitted is Some(previous) else {
    raise ApiError::UnknownSubmission("submit first")
  }
  if previous.submission_id() != submission.submission_id() {
    raise ApiError::RequestConflict(submission.submission_id())
  }
  self.pending_update = Some(submission)
  let result = Ok(self.command_locked(JobCommand::update(submission))) catch {
    error => Err(error)
  }
  // A rejected or exhausted update must not leave the new submission as a
  // reconnect hint. The next call should retry from the durable old plan.
  self.pending_update = None
  let status = match result {
    Ok(value) => value
    Err(error) => raise error
  }
  // Keep the previous envelope during the command so reconnect replay still
  // binds the session to the installed submission. Replace it only after the
  // Coordinator has durably accepted the update.
  self.submitted = Some(submission)
  self.pending_update = None
  status
}

///|
pub async fn DriverClient::input(
  self : DriverClient,
  events : ArrayView[Event],
) -> JobRunStatus raise ApiError {
  self.command(JobCommand::input(events))
}

///|
pub async fn DriverClient::status(
  self : DriverClient,
) -> JobRunStatus raise ApiError {
  self.command(JobCommand::status())
}

///|
/// Pause/resume are durable operational controls and may be retried safely.
pub async fn DriverClient::pause(
  self : DriverClient,
) -> JobRunStatus raise ApiError {
  self.command(JobCommand::pause())
}

///|
pub async fn DriverClient::resume(
  self : DriverClient,
) -> JobRunStatus raise ApiError {
  self.command(JobCommand::resume())
}

///|
/// Request a repartition across statically registered Worker identities.
pub async fn DriverClient::rescale(
  self : DriverClient,
  workers : ArrayView[String],
  parallelism? : Int,
) -> JobRunStatus raise ApiError {
  self.command(JobCommand::rescale(workers, parallelism?))
}

///|
pub async fn DriverClient::drain(
  self : DriverClient,
) -> JobRunStatus raise ApiError {
  self.command(JobCommand::drain())
}

///|
pub async fn DriverClient::cancel(
  self : DriverClient,
) -> JobRunStatus raise ApiError {
  self.command(JobCommand::cancel())
}

///|
/// Bind configuration for the Coordinator's Driver control endpoint. It is a
/// separate listener from the Worker data plane so a Driver cannot be routed
/// into a Worker handshake by accident.
pub(all) struct DriverControlConfig {
  cluster_id : String
  coordinator_id : String
  bind : NodeEndpoint
  advertised : NodeEndpoint?
  auth_token : String?
} derive(Eq)

///|
pub fn DriverControlConfig::new(
  cluster_id : String,
  coordinator_id : String,
  bind : NodeEndpoint,
) -> DriverControlConfig raise ApiError {
  if cluster_id.is_empty() || coordinator_id.is_empty() {
    raise ApiError::Cluster("cluster and coordinator ids are required")
  }
  { cluster_id, coordinator_id, bind, advertised: None, auth_token: None, }
}

///|
pub fn DriverControlConfig::with_advertised_endpoint(
  self : DriverControlConfig,
  endpoint : NodeEndpoint,
) -> DriverControlConfig {
  { ..self, advertised: Some(endpoint), }
}

///|
pub fn DriverControlConfig::cluster_id(self : DriverControlConfig) -> String {
  self.cluster_id
}

///|
pub fn DriverControlConfig::coordinator_id(
  self : DriverControlConfig,
) -> String {
  self.coordinator_id
}

///|
pub fn DriverControlConfig::bind(self : DriverControlConfig) -> NodeEndpoint {
  self.bind
}

///|
pub fn DriverControlConfig::advertised_endpoint(
  self : DriverControlConfig,
) -> NodeEndpoint? {
  self.advertised
}

///|
/// Require a shared secret on the first Driver submit frame.
pub fn DriverControlConfig::with_auth_token(
  self : DriverControlConfig,
  token : String,
) -> DriverControlConfig raise ApiError {
  if token.is_empty() {
    raise ApiError::Cluster("Driver auth token must not be empty")
  }
  { ..self, auth_token: Some(token), }
}

///|
pub fn DriverControlConfig::auth_token(self : DriverControlConfig) -> String? {
  self.auth_token
}

///|
pub struct DriverControlListener {
  config : DriverControlConfig
  server : @socket.TcpServer
  mut active_requests : Int
}

///|
pub async fn DriverControlListener::bind(
  config : DriverControlConfig,
) -> DriverControlListener raise ApiError {
  if config.bind.is_wildcard() && config.advertised is None {
    raise ApiError::InvalidEndpoint(
      "wildcard Driver bind requires an advertised endpoint",
    )
  }
  let server = @socket.TcpServer(config.bind.to_addr(), reuse_addr=true) catch {
    error =>
      raise ApiError::Cluster("bind Driver control: " + @debug.to_string(error))
  }
  { config, server, active_requests: 0, }
}

///|
pub fn DriverControlListener::close(self : DriverControlListener) -> Unit {
  self.server.close()
}

///|
pub fn DriverControlListener::endpoint(
  self : DriverControlListener,
) -> NodeEndpoint raise ApiError {
  let actual_port = self.server.addr.port()
  match self.config.advertised {
    Some(endpoint) =>
      NodeEndpoint::new(
        endpoint.host(),
        if endpoint.port() == 0 {
          actual_port
        } else {
          endpoint.port()
        },
      )
    None => NodeEndpoint::new(self.config.bind.host(), actual_port)
  }
}

///|
fn driver_command_from_wire(
  inbound : @protocol.DriverInbound,
) -> JobCommand raise ApiError {
  match inbound {
    @protocol.DriverInbound::Submit(value) => {
      let submission : JobSubmission = @json.from_json(value.job()) catch {
        _ => raise ApiError::InvalidDriverCommand("malformed submission")
      }
      if submission.submission_id() != value.submission_id() {
        raise ApiError::RequestConflict(value.submission_id())
      }
      JobCommand::Submit(
        JobSubmission::new(submission.submission_id(), submission.job()),
      )
    }
    @protocol.DriverInbound::Command(value) =>
      match value.command() {
        "input" => {
          guard value.payload() is Some(payload) else {
            raise ApiError::InvalidDriverCommand("input requires a payload")
          }
          let events : Array[Event] = @json.from_json(payload) catch {
            _ => raise ApiError::InvalidDriverCommand("malformed input payload")
          }
          JobCommand::Input(events)
        }
        "status" if value.payload() is None => JobCommand::Status
        "pause" if value.payload() is None => JobCommand::Pause
        "resume" if value.payload() is None => JobCommand::Resume
        "rescale" => {
          guard value.payload() is Some(payload) else {
            raise ApiError::InvalidDriverCommand(
              "rescale requires a worker list",
            )
          }
          let request : RescaleRequest = @json.from_json(payload) catch {
            _ =>
              raise ApiError::InvalidDriverCommand("malformed rescale payload")
          }
          JobCommand::Rescale(request)
        }
        "update" => {
          guard value.payload() is Some(payload) else {
            raise ApiError::InvalidDriverCommand("update requires a submission")
          }
          let submission : JobSubmission = @json.from_json(payload) catch {
            _ =>
              raise ApiError::InvalidDriverCommand("malformed update payload")
          }
          JobCommand::Update(submission)
        }
        "drain" if value.payload() is None => JobCommand::Drain
        "cancel" if value.payload() is None => JobCommand::Cancel
        _ => raise ApiError::InvalidDriverCommand(value.command())
      }
  }
}

///|
fn driver_phase_name(phase : JobRunPhase) -> String {
  match phase {
    JobRunPhase::Created => "Created"
    JobRunPhase::Running => "Running"
    JobRunPhase::Draining => "Draining"
    JobRunPhase::Succeeded => "Succeeded"
    JobRunPhase::Failed => "Failed"
    JobRunPhase::Cancelled => "Cancelled"
  }
}

///|
fn driver_status_from_api(status : JobRunStatus) -> @protocol.DriverStatus {
  @protocol.DriverStatus::new(
    status.submission_id(),
    status.job_id(),
    status.plan_digest(),
    driver_phase_name(status.phase()),
    status.accepted_events(),
    status.failure(),
    paused=status.is_paused(),
    parallelism=status.parallelism(),
  )
}

///|
async fn serve_driver_connection(
  connection : @socket.Tcp,
  listener : DriverControlListener,
  handler : async (String, JobCommand) -> Result[JobRunStatus, String],
) -> Unit {
  defer connection.close()
  let cluster_id = listener.config.cluster_id
  let coordinator_id = listener.config.coordinator_id
  let first = @runtime.read_frame(connection)
  match first.kind() {
    @protocol.FrameKind::DriverSubmit => ()
    _ =>
      raise ApiError::InvalidDriverCommand("first Driver frame must be submit")
  }
  let session = @protocol.DriverSession::new(
    cluster_id,
    coordinator_id,
    first.sender_id(),
  ) catch {
    error =>
      raise ApiError::Cluster(
        "create Driver session: " + @debug.to_string(error),
      )
  }
  let inbound = session.accept_frame(first) catch {
    error =>
      raise ApiError::Cluster(
        "validate Driver frame: " + @debug.to_string(error),
      )
  }
  match listener.config.auth_token {
    None => ()
    Some(expected) => {
      guard inbound is @protocol.DriverInbound::Submit(submit) else {
        raise ApiError::InvalidDriverCommand(
          "first Driver frame must be submit",
        )
      }
      if submit.auth_token() != Some(expected) {
        // Authentication is a permanent rejection, not a broken transport.
        // Return a validated reply so reconnect logic cannot mistake it for
        // a leader failure and retry the same credentials indefinitely.
        let outbound = @protocol.FrameBuilder::new(
          cluster_id,
          coordinator_id,
          first.worker_incarnation(),
          session_id=session.session_id(),
        )
        let rejected : @protocol.DriverReply = {
          request_id: first.message_id(),
          result: Err("Driver authentication failed"),
        }
        @runtime.write_frame(
          connection,
          outbound.build(
            @protocol.FrameKind::DriverStatus,
            rejected.to_payload(),
          ),
        )
        return
      }
    }
  }
  let outbound = @protocol.FrameBuilder::new(
    cluster_id,
    coordinator_id,
    first.worker_incarnation(),
    session_id=session.session_id(),
  ) catch {
    error =>
      raise ApiError::Cluster(
        "build Driver response: " + @debug.to_string(error),
      )
  }
  let mut last_message_id = ""
  let mut last_result : Result[@protocol.DriverStatus, String] = Err(
    "not handled",
  )
  let mut bound = false
  let mut bound_submission = ""
  let mut frame = first
  let mut inbound_value = inbound
  for ;; {
    listener.active_requests += 1
    {
      defer {
        listener.active_requests -= 1
      }
      let result = if frame.message_id() == last_message_id {
        last_result
      } else {
        try {
          let command = driver_command_from_wire(inbound_value)
          if !bound && !(command is JobCommand::Submit(_)) {
            raise ApiError::UnknownSubmission("submit first")
          }
          let submission_id = match inbound_value {
            @protocol.DriverInbound::Submit(value) => value.submission_id()
            @protocol.DriverInbound::Command(_) => bound_submission
          }
          if bound && submission_id != bound_submission {
            raise ApiError::RequestConflict(submission_id)
          }
          match handler(submission_id, command) {
            Ok(status) => {
              if command is JobCommand::Submit(_) {
                bound = true
                bound_submission = submission_id
              }
              Ok(driver_status_from_api(status))
            }
            Err(reason) => Err(reason)
          }
        } catch {
          error => Err(@debug.to_string(error))
        }
      }
      let reply = @protocol.DriverReply::{
        request_id: frame.message_id(),
        result,
      }
      let response = outbound.build(
        @protocol.FrameKind::DriverStatus,
        reply.to_payload(),
      )
      @runtime.write_frame(connection, response)
      if !bound {
        return
      }
      last_message_id = frame.message_id()
      last_result = result
    }
    guard @runtime.read_optional_frame(connection) is Some(next) else { return }
    frame = next
    inbound_value = session.accept_frame(frame)
  }
}

///|
pub async fn DriverClient::await_completion(
  self : DriverClient,
  poll_interval_ms? : Int = 100,
  timeout_ms? : Int = 30000,
) -> JobRunStatus raise ApiError {
  if poll_interval_ms <= 0 || timeout_ms <= 0 {
    raise ApiError::Cluster("poll interval and timeout must be positive")
  }
  @async_runtime.with_timeout(timeout_ms, () => {
    for ;; {
      let status = self.status()
      match status.phase() {
        JobRunPhase::Succeeded | JobRunPhase::Cancelled | JobRunPhase::Failed =>
          return status
        _ => @async_runtime.sleep(poll_interval_ms)
      }
    }
  }) catch {
    error =>
      raise ApiError::Cluster(
        "Driver completion wait: " + @debug.to_string(error),
      )
  }
}

///|
/// Serve Driver control connections until the listener is closed. A handler
/// may use async durable stores; the listener itself does not invent execution
/// semantics or acknowledge an input before the handler returns.
pub async fn DriverControlListener::run(
  self : DriverControlListener,
  handler : async (JobCommand) -> Result[JobRunStatus, String],
  max_connections? : Int = 128,
) -> Unit {
  self.server.run_forever(max_connections~, allow_failure=true) <| ((
    connection,
    _,
  ) => {
    serve_driver_connection(connection, self, (_, command) => handler(command))
  })
}

///|
/// Serve exactly one Driver connection. This is useful for a supervised
/// Coordinator that owns its accept loop, and makes shutdown explicit.
pub async fn DriverControlListener::run_once(
  self : DriverControlListener,
  handler : async (JobCommand) -> Result[JobRunStatus, String],
) -> Unit {
  let (connection, _) = self.server.accept()
  serve_driver_connection(connection, self, (_, command) => handler(command))
}

///|
/// Convenience form for the normal single-Job Coordinator process. The
/// DriverStore remains the source of request intent while the caller can
/// later drain its durable input into the execution scheduler.
pub async fn DriverControlListener::run_store(
  self : DriverControlListener,
  store : DriverStore,
  max_connections? : Int = 128,
) -> Unit {
  self.run(command => store.handle(command), max_connections~)
}
