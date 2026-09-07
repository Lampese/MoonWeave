///|
/// Configuration for the asynchronous Driver control connection. The data
/// plane remains owned by Worker/Coordinator nodes; this handle only submits
/// a validated Job and lifecycle commands.
pub(all) struct DriverConfig {
  cluster_id : String
  coordinator_id : String
  driver_id : String
  endpoint : NodeEndpoint
  incarnation : String
} derive(Debug, Eq)

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
    incarnation: "driver-" + driver_id,
  }
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
/// Persistent client-side control session. Every command waits for a matching
/// status frame, so a completed command is never reported before its response
/// crossed the async socket boundary.
pub struct DriverClient {
  config : DriverConfig
  connection : @socket.Tcp
  outbound : @protocol.FrameBuilder
  session : @protocol.DriverClientSession
  mut closed : Bool
  operation_lock : @async_runtime.Mutex
  mut submitted : JobSubmission?
}

///|
pub async fn DriverClient::connect(
  config : DriverConfig,
) -> DriverClient raise ApiError {
  let connection = @socket.Tcp::connect_to_host(
    config.endpoint.host,
    port=config.endpoint.port,
  ) catch {
    error =>
      raise ApiError::Cluster("connect Driver: " + @debug.to_string(error))
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
    connection,
    outbound,
    session,
    closed: false,
    operation_lock: @async_runtime.Mutex::Mutex(),
    submitted: None,
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
    JobCommand::Drain => "drain"
    JobCommand::Cancel => "cancel"
  }
}

///|
fn driver_command_payload(command : JobCommand) -> Json? {
  match command {
    JobCommand::Submit(_)
    | JobCommand::Status
    | JobCommand::Drain
    | JobCommand::Cancel => None
    JobCommand::Input(events) => Some(@json.to_json(events))
  }
}

///|
fn driver_submit_payload(submission : JobSubmission) -> Json {
  @json.to_json(submission)
}

///|
fn driver_status_to_api(
  status : @protocol.DriverStatus,
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
  }
}

///|
async fn DriverClient::exchange(
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
      self.close()
      raise ApiError::Cluster("write Driver frame: " + @debug.to_string(error))
    }
  }
  let response = @runtime.read_frame(self.connection) catch {
    error => {
      self.close()
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
    Err(reason) => raise ApiError::InvalidDriverCommand(reason)
  }
  let converted = driver_status_to_api(status)
  match self.submitted {
    Some(submission) if converted.submission_id != submission.submission_id ||
      converted.job_id != submission.job.definition.job_id ||
      converted.plan_digest != submission.job.plan_digest =>
      raise ApiError::SubmissionMismatch("Driver response")
    _ => ()
  }
  converted
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
  ignore(submission.compile())
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
  let status = self.exchange(
    @protocol.FrameKind::DriverSubmit,
    @protocol.DriverSubmit::new(
      submission.submission_id(),
      driver_submit_payload(submission),
    ).to_payload() catch {
      error => raise ApiError::Cluster(@debug.to_string(error))
    },
  )
  self.submitted = Some(submission)
  status
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
} derive(Debug, Eq)

///|
pub fn DriverControlConfig::new(
  cluster_id : String,
  coordinator_id : String,
  bind : NodeEndpoint,
) -> DriverControlConfig raise ApiError {
  if cluster_id.is_empty() || coordinator_id.is_empty() {
    raise ApiError::Cluster("cluster and coordinator ids are required")
  }
  { cluster_id, coordinator_id, bind, advertised: None, }
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
pub struct DriverControlListener {
  config : DriverControlConfig
  server : @socket.TcpServer
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
  { config, server, }
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
  )
}

///|
async fn serve_driver_connection(
  connection : @socket.Tcp,
  cluster_id : String,
  coordinator_id : String,
  handler : async (JobCommand) -> Result[JobRunStatus, String],
) -> Unit {
  defer connection.close()
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
  let mut frame = first
  let mut inbound_value = inbound
  for ;; {
    let result = if frame.message_id() == last_message_id {
      last_result
    } else {
      try {
        let command = driver_command_from_wire(inbound_value)
        if !bound && !(command is JobCommand::Submit(_)) {
          raise ApiError::UnknownSubmission("submit first")
        }
        match handler(command) {
          Ok(status) => {
            if command is JobCommand::Submit(_) {
              bound = true
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
) -> JobRunStatus {
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
  })
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
    serve_driver_connection(
      connection,
      self.config.cluster_id,
      self.config.coordinator_id,
      handler,
    )
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
  serve_driver_connection(
    connection,
    self.config.cluster_id,
    self.config.coordinator_id,
    handler,
  )
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
