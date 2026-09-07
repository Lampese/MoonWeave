///|
/// Lifecycle signal shared by a source-backed live supervisor and its control
/// plane. The signal changes only after the Driver has durably changed its
/// own lifecycle state, so a session never drains while ingress is still
/// accepted.
enum LiveJobPhase {
  Open
  Draining
  Cancelled
} derive(Eq)

///|
struct LiveJobControl {
  mut phase : LiveJobPhase
}

///|
fn LiveJobControl::new() -> LiveJobControl {
  { phase: LiveJobPhase::Open, }
}

///|
fn LiveJobControl::phase(self : LiveJobControl) -> LiveJobPhase {
  self.phase
}

///|
fn LiveJobControl::request_drain(self : LiveJobControl) -> Unit {
  if self.phase == LiveJobPhase::Open {
    self.phase = LiveJobPhase::Draining
  }
}

///|
fn LiveJobControl::cancel(self : LiveJobControl) -> Unit {
  self.phase = LiveJobPhase::Cancelled
}

///|
fn LiveJobControl::is_draining(self : LiveJobControl) -> Bool {
  self.phase == LiveJobPhase::Draining
}

///|
/// Source-backed work for a live Job. SourceLog is the durable data fact;
/// JobStore retains append-only delivery plans so Coordinator/Job recovery can
/// reconcile cursors without guessing which input was intended.
pub struct SourceBackedJob {
  source : SourceLog
  plan : @plan.JobSpec
  coordinator : CoordinatorStore
  job_store : JobStore
  required_capability : String?
  ingress_lock : @async.Mutex
  lifecycle : LiveJobControl
  mut failed : Bool
  mut closed : Bool
  mut session_sequence : Int64
}

///|
pub async fn SourceBackedJob::open(
  source_path : String,
  plan : @plan.JobSpec,
  coordinator : CoordinatorStore,
  job_store : JobStore,
  required_capability? : String,
) -> SourceBackedJob raise FrameIoError {
  if !same_job_spec(plan, job_store.snapshot()) {
    raise FrameIoError::Handler("live source topology does not match JobStore")
  }
  let source = SourceLog::open(source_path) catch {
    error => raise FrameIoError::SourceLog(error)
  }
  {
    source,
    plan,
    coordinator,
    job_store,
    required_capability,
    ingress_lock: @async.Mutex::Mutex(),
    lifecycle: LiveJobControl::new(),
    failed: false,
    closed: false,
    session_sequence: 0L,
  }
}

///|
pub fn SourceBackedJob::close(self : SourceBackedJob) -> Unit {
  self.closed = true
  self.lifecycle.cancel()
  self.source.close()
}

///|
fn SourceBackedJob::check_open(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  if self.closed || self.failed {
    raise FrameIoError::Handler("live source is closed")
  }
}

///|
pub async fn SourceBackedJob::seal(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  if self.lifecycle.is_draining() {
    return
  }
  if self.lifecycle.phase() != LiveJobPhase::Open {
    raise FrameIoError::Handler("live source is not accepting input")
  }
  if self.job_store.phase() == @control.JobPhase::Running {
    self.job_store.drain() catch {
      error => raise FrameIoError::JobStore(error)
    }
  } else if self.job_store.phase() != @control.JobPhase::Draining {
    raise FrameIoError::Handler("live Job cannot drain in its current phase")
  }
  self.lifecycle.request_drain()
}

///|
pub async fn SourceBackedJob::cancel(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  if self.job_store.phase() != @control.JobPhase::Cancelled {
    self.job_store.cancel() catch {
      error => raise FrameIoError::JobStore(error)
    }
  }
  self.lifecycle.cancel()
}

///|
pub fn SourceBackedJob::event_count(self : SourceBackedJob) -> Int {
  self.source.event_count()
}

///|
pub fn SourceBackedJob::plan(self : SourceBackedJob) -> @plan.JobSpec {
  self.plan
}

///|
pub fn SourceBackedJob::coordinator(self : SourceBackedJob) -> CoordinatorStore {
  self.coordinator
}

///|
pub fn SourceBackedJob::job_store(self : SourceBackedJob) -> JobStore {
  self.job_store
}

///|
fn source_backed_assignment(
  control : @control.CoordinatorState,
  partition : Int,
) -> @control.PartitionAssignment raise FrameIoError {
  match control.assignment(partition) {
    Some(value) => value
    None =>
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::UnknownPartition(partition),
      )
  }
}

///|
fn SourceBackedJob::source_work_unlocked(
  self : SourceBackedJob,
  partition : Int,
) -> SupervisorWork raise FrameIoError {
  let control = self.coordinator.state()
  let assignment = source_backed_assignment(control, partition)
  let batches = self.source.batches(
    partition,
    assignment.epoch(),
    assignment.start_offset(),
    maximum_batch_events=1,
  ) catch {
    error => raise FrameIoError::SourceLog(error)
  }
  let deltas : Array[@protocol.DeltaBatch] = []
  // Source locations retain WAL insertion order, including interleaved origins.
  for location in self.source.locations.values() {
    let source_partition = location.partition
    let targets = self.plan.exchange_targets(source_partition) catch {
      error => raise FrameIoError::SourceLog(SourceLogError::Plan(error))
    }
    if !targets.any(target => target.partition() == partition) {
      continue
    }
    let source_assignment = source_backed_assignment(control, source_partition)
    if location.offset >= source_assignment.start_offset() {
      deltas.push(
        self.plan.delta(
          source_partition,
          source_assignment.epoch(),
          partition,
          assignment.epoch(),
          [location.event],
        ) catch {
          error => raise FrameIoError::SourceLog(SourceLogError::Plan(error))
        },
      )
    }
  }
  SupervisorWork::new(
    assignment.worker_id(),
    self.plan.job_id() + "-live-source-p" + partition.to_string(),
    partition,
    batches,
    deltas~,
    required_capability?=self.required_capability,
  )
}

///|
async fn SourceBackedJob::refresh_work_plans_unlocked(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  for partition in 0..<self.plan.partition_count() {
    let work = self.source_work_unlocked(partition)
    self.job_store.bind_work(work, append_only=true) catch {
      error => raise FrameIoError::JobStore(error)
    }
  }
}

///|
/// Persist a complete source snapshot as append-only work plans. This is used
/// at startup before reconciliation and after every durable ingress append.
pub async fn SourceBackedJob::refresh_work_plans(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  errdefer {
    self.failed = true
  }
  self.refresh_work_plans_unlocked()
}

///|
/// Append one durable source event, then extend every affected delivery plan
/// before releasing ingress. A live session takes the same lock before reading
/// work, so it cannot send a source entry that is absent from JobStore's
/// recovery plan.
pub async fn SourceBackedJob::ingest(
  self : SourceBackedJob,
  event : @protocol.EventRecord,
) -> Int64 raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  if self.lifecycle.phase() != LiveJobPhase::Open {
    raise FrameIoError::Handler("live source is sealed")
  }
  errdefer {
    self.failed = true
  }
  let control = self.coordinator.state()
  let target = self.plan.route_event(event)
  let assignment = source_backed_assignment(control, target.partition())
  let offset = self.source.append(
    assignment.partition(),
    assignment.start_offset(),
    event,
  ) catch {
    error => raise FrameIoError::SourceLog(error)
  }
  self.refresh_work_plans_unlocked()
  offset
}

///|
/// Reconcile all durable Driver inputs into SourceLog. Equal retransmissions
/// are idempotent at SourceLog; an identity conflict remains fail-closed.
pub async fn SourceBackedJob::ingest_all(
  self : SourceBackedJob,
  events : ArrayView[@protocol.EventRecord],
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  if self.lifecycle.phase() != LiveJobPhase::Open {
    raise FrameIoError::Handler("live source is sealed")
  }
  errdefer {
    self.failed = true
  }
  let control = self.coordinator.state()
  for event in events {
    let target = self.plan.route_event(event)
    let assignment = source_backed_assignment(control, target.partition())
    ignore(
      self.source.append(
        assignment.partition(),
        assignment.start_offset(),
        event,
      ) catch {
        error => raise FrameIoError::SourceLog(error)
      },
    )
  }
  self.refresh_work_plans_unlocked()
}

///|
async fn SourceBackedJob::work(
  self : SourceBackedJob,
  partition : Int,
) -> SupervisorWork raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  guard self.job_store.work_plans.get(partition) is Some(plan) else {
    raise FrameIoError::JobStore(JobStoreError::PlanMismatch(partition))
  }
  SupervisorWork::new(
    plan.worker_id,
    "live",
    partition,
    plan.batches,
    deltas=plan.deltas,
    required_capability?=plan.required_capability,
  )
}

///|
pub(all) struct LiveSupervisorSummary {
  completed_partitions : Int
  failed_partitions : Int
  sent_batches : Int
  sent_deltas : Int
  failures : Array[SupervisorFailure]
} derive(Debug, Eq)

///|
pub fn LiveSupervisorSummary::completed_partitions(
  self : LiveSupervisorSummary,
) -> Int {
  self.completed_partitions
}

///|
pub fn LiveSupervisorSummary::failed_partitions(
  self : LiveSupervisorSummary,
) -> Int {
  self.failed_partitions
}

///|
pub fn LiveSupervisorSummary::sent_batches(self : LiveSupervisorSummary) -> Int {
  self.sent_batches
}

///|
pub fn LiveSupervisorSummary::sent_deltas(self : LiveSupervisorSummary) -> Int {
  self.sent_deltas
}

///|
pub fn LiveSupervisorSummary::failures(
  self : LiveSupervisorSummary,
) -> ArrayView[SupervisorFailure] {
  self.failures[:]
}

///|
fn same_live_assignment(
  left : @control.PartitionAssignment,
  right : @control.PartitionAssignment,
) -> Bool {
  left.partition() == right.partition() &&
  left.worker_id() == right.worker_id() &&
  left.incarnation() == right.incarnation() &&
  left.epoch() == right.epoch() &&
  left.start_offset() == right.start_offset()
}

///|
fn live_assignment_for_hello(
  control : @control.CoordinatorState,
  plan : @plan.JobSpec,
  worker_id : String,
  requested_partition : Int?,
) -> (
  @control.WorkerInfo,
  @control.PartitionAssignment,
  @protocol.AssignmentSpec,
) raise FrameIoError {
  let worker = match control.worker(worker_id) {
    Some(value) => value
    None =>
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::UnknownWorker(worker_id),
      )
  }
  if worker.lifecycle() != @control.WorkerLifecycle::Active {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::WorkerNotActive(worker_id),
    )
  }
  let matches : Array[@control.PartitionAssignment] = []
  for partition in 0..<plan.partition_count() {
    match control.assignment(partition) {
      Some(assignment) if assignment.worker_id() == worker_id =>
        match requested_partition {
          Some(requested) if requested != partition => ()
          _ => matches.push(assignment)
        }
      _ => ()
    }
  }
  guard matches.length() == 1 else {
    raise FrameIoError::Handler(
      "live Worker must identify exactly one assigned partition",
    )
  }
  let assignment = matches[0]
  if assignment.incarnation() != worker.incarnation() {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::AssignmentConflict(assignment.partition()),
    )
  }
  let spec = @protocol.AssignmentSpec::new(
    assignment.partition(),
    assignment.epoch(),
    assignment.incarnation().value(),
    start_offset=assignment.start_offset(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  (worker, assignment, spec)
}

///|
async fn process_live_frame(
  frame : @protocol.Frame,
  session : @protocol.CoordinatorSession,
  live : SourceBackedJob,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  sent_delta_ids : Map[String, Bool],
  acked_deltas : Int,
) -> Int raise FrameIoError {
  session.accept_frame(frame) catch {
    error => raise FrameIoError::Session(error)
  }
  match frame.kind() {
    @protocol.FrameKind::Resume => {
      let resume_message = @protocol.Resume::from_payload(frame.payload()) catch {
        error => raise FrameIoError::Message(error)
      }
      let previous = job_store_partition(live.job_store, assignment.partition())
      record_job_progress(
        live.job_store,
        assignment,
        resume_message.persisted_offset(),
        previous.applied_offset(),
      )
      supervisor_record_resume(
        live.coordinator.state(),
        Some(live.coordinator),
        worker,
        assignment,
        resume_message.persisted_offset(),
      )
      acked_deltas
    }
    @protocol.FrameKind::Credit | @protocol.FrameKind::AckPersisted =>
      acked_deltas
    @protocol.FrameKind::DeltaAck => {
      let ack = @protocol.DeltaAck::from_payload(frame.payload()) catch {
        error => raise FrameIoError::Message(error)
      }
      if ack.target_partition() != assignment.partition() ||
        ack.target_epoch() != assignment.epoch() {
        raise FrameIoError::DeltaMismatch
      }
      match sent_delta_ids.get(ack.delta_id()) {
        None => raise FrameIoError::UnknownDeltaAck(ack.delta_id())
        Some(true) => acked_deltas
        Some(false) => {
          sent_delta_ids[ack.delta_id()] = true
          acked_deltas + 1
        }
      }
    }
    @protocol.FrameKind::Heartbeat => {
      let heartbeat = @protocol.Heartbeat::from_payload(frame.payload()) catch {
        error => raise FrameIoError::Message(error)
      }
      if heartbeat.partition() != assignment.partition() ||
        heartbeat.epoch() != assignment.epoch() {
        raise FrameIoError::Coordinator(
          @control.CoordinatorError::StaleEpoch(
            expected=assignment.epoch(),
            actual=heartbeat.epoch(),
          ),
        )
      }
      supervisor_record_heartbeat(
        live.coordinator.state(),
        Some(live.coordinator),
        worker,
      )
      acked_deltas
    }
    @protocol.FrameKind::Progress => {
      let progress = @protocol.Progress::from_payload(frame.payload()) catch {
        error => raise FrameIoError::Message(error)
      }
      if progress.partition() != assignment.partition() ||
        progress.epoch() != assignment.epoch() {
        raise FrameIoError::Coordinator(
          @control.CoordinatorError::StaleEpoch(
            expected=assignment.epoch(),
            actual=progress.epoch(),
          ),
        )
      }
      record_job_progress(
        live.job_store,
        assignment,
        progress.persisted_offset(),
        progress.applied_offset(),
      )
      supervisor_record_progress(
        live.coordinator.state(),
        Some(live.coordinator),
        worker,
        assignment,
        progress.persisted_offset(),
        progress.applied_offset(),
      )
      acked_deltas
    }
    _ =>
      raise FrameIoError::Session(
        @protocol.SessionError::WrongPhase(
          expected=@protocol.SessionPhase::Running,
          actual=session.phase(),
        ),
      )
  }
}

///|
async fn run_live_source_session(
  connection : @socket.Tcp,
  live : SourceBackedJob,
  lifecycle : LiveJobControl,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  maximum_frame_bytes : Int,
  poll_interval_ms : Int,
  drain_timeout_ms : Int,
  output_manifest : JobOutputManifest?,
) -> CoordinatorRunSummary raise FrameIoError {
  defer connection.close()
  let result : Result[CoordinatorRunSummary, FrameIoError] = (@async.with_task_group <| group => {
    let inbox = LiveFrameInbox::new()
    let pump = group.spawn(allow_failure=true) <| () => {
      inbox.pump(connection, maximum_frame_bytes)
    }
    let result : Result[CoordinatorRunSummary, FrameIoError] = Ok(
      run_live_source_session_loop(
        connection, live, lifecycle, worker, assignment, session, outbound, maximum_frame_bytes,
        poll_interval_ms, drain_timeout_ms, output_manifest, inbox,
      ),
    ) catch {
      error => Err(error)
    }
    pump.cancel()
    ignore(pump.wait() catch { _ => () })
    result
  }) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  match result {
    Ok(value) => value
    Err(error) => raise error
  }
}

///|
async fn run_live_source_session_loop(
  connection : @socket.Tcp,
  live : SourceBackedJob,
  lifecycle : LiveJobControl,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  maximum_frame_bytes : Int,
  poll_interval_ms : Int,
  drain_timeout_ms : Int,
  output_manifest : JobOutputManifest?,
  inbox : LiveFrameInbox,
) -> CoordinatorRunSummary raise FrameIoError {
  session.restore_send_cursor(
    assignment.partition(),
    assignment.epoch(),
    assignment.next_offset(),
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  let sent_delta_ids : Map[String, Bool] = Map([])
  let mut sent_batches = 0
  let mut sent_deltas = 0
  let mut acked_deltas = 0
  let mut resumed = false
  let mut next_batch = 0
  let mut next_delta = 0
  for ;; {
    if lifecycle.phase() == LiveJobPhase::Cancelled {
      raise FrameIoError::Handler("live job cancelled")
    }
    let incoming = inbox.poll(poll_interval_ms)
    match incoming {
      None => ()
      Some(frame) => {
        if frame.kind() == @protocol.FrameKind::Resume {
          if resumed {
            raise FrameIoError::Handler("duplicate live Resume")
          }
          resumed = true
          let resume_message = @protocol.Resume::from_payload(frame.payload()) catch {
            error => raise FrameIoError::Message(error)
          }
          let work = live.work(assignment.partition())
          next_batch = resume_batch_index(
            work.batches(),
            resume_message.persisted_offset(),
          )
        } else if !resumed {
          raise FrameIoError::Handler(
            "live Worker must Resume before progress or credit",
          )
        }
        acked_deltas = process_live_frame(
          frame, session, live, worker, assignment, sent_delta_ids, acked_deltas,
        )
      }
    }
    if !resumed {
      continue
    }
    let current = source_backed_assignment(
      live.coordinator.state(),
      assignment.partition(),
    )
    if !same_live_assignment(current, assignment) {
      raise FrameIoError::Handler(
        "live assignment changed during an active session",
      )
    }
    let draining = lifecycle.is_draining()
    let work = live.work(assignment.partition())
    let mut send_cursor = current.next_offset()
    while next_batch < work.batches().length() {
      let batch = work.batches()[next_batch]
      if batch.first_offset() != send_cursor {
        raise FrameIoError::Handler(
          "live source contains a gap at the send cursor",
        )
      }
      let events = batch.events().length().to_int64()
      let bytes = @utf8.encode(batch.to_payload()).length().to_int64()
      if events > session.credit_events() || bytes > session.credit_bytes() {
        break
      }
      record_job_send(live.job_store, assignment, batch)
      managed_send_batch(
        connection,
        live.coordinator.state(),
        Some(live.coordinator),
        session,
        outbound,
        assignment,
        batch,
        maximum_frame_bytes,
      )
      sent_batches += 1
      next_batch += 1
      send_cursor = batch.end_offset() + 1L
    }
    while next_delta < work.deltas().length() {
      let delta = work.deltas()[next_delta]
      let events = delta.events().length().to_int64()
      let bytes = @utf8.encode(delta.to_payload()).length().to_int64()
      if events > session.credit_events() || bytes > session.credit_bytes() {
        break
      }
      ignore(
        coordinator_send_delta(
          connection,
          session,
          outbound,
          delta,
          bytes,
          maximum_frame_bytes~,
        ),
      )
      sent_delta_ids[delta.delta_id()] = false
      sent_deltas += 1
      next_delta += 1
    }
    let last_offset = match work.batches().last() {
      None => assignment.start_offset() - 1L
      Some(batch) => batch.end_offset()
    }
    if draining &&
      send_cursor > last_offset &&
      session.last_applied_offset() >= last_offset &&
      sent_delta_ids.length() == work.deltas().length() &&
      acked_deltas == sent_deltas {
      confirm_job_drain(
        connection,
        live.coordinator.state(),
        worker,
        assignment,
        session,
        outbound,
        live.job_store,
        Some(live.coordinator),
        output_manifest,
        last_offset,
        maximum_frame_bytes,
        drain_timeout_ms,
        read_next=() => inbox.read(),
      )
      break
    }
  }
  {
    sent_batches,
    sent_deltas,
    persisted_offset: session.last_persisted_offset(),
    applied_offset: session.last_applied_offset(),
    probe_reply: None,
  }
}

///|
async fn supervise_live_source_connection(
  server : @socket.TcpServer,
  live : SourceBackedJob,
  lifecycle : LiveJobControl,
  claimed : Set[String],
  claim_lock : @async.Mutex,
  accept_lock : @async.Mutex,
  maximum_frame_bytes : Int,
  poll_interval_ms : Int,
  drain_timeout_ms : Int,
  output_manifest : JobOutputManifest?,
  attempt : LiveConnectionAttempt,
  reconnect_timeout_ms : Int,
) -> CoordinatorRunSummary raise FrameIoError {
  let mut claimed_id = ""
  let outcome : Result[CoordinatorRunSummary, FrameIoError] = try {
    let (connection, _) = if attempt.attempts == 1 {
      accept_supervisor_connection(server, accept_lock)
    } else {
      @async.with_timeout(reconnect_timeout_ms, () => {
        accept_supervisor_connection(server, accept_lock)
      }) catch {
        error =>
          raise FrameIoError::Handler(
            "Worker reconnect deadline: " + @debug.to_string(error),
          )
      }
    }
    defer connection.close()
    let hello_frame = @async.with_timeout(reconnect_timeout_ms, () => {
      read_frame(connection, maximum_frame_bytes~)
    }) catch {
      error =>
        raise FrameIoError::Handler(
          "Worker handshake failed: " + @debug.to_string(error),
        )
    }
    let hello = @protocol.Hello::from_payload(hello_frame.payload()) catch {
      error => raise FrameIoError::Message(error)
    }
    let (worker, assignment, spec) = live_assignment_for_hello(
      live.coordinator.state(),
      live.plan,
      hello.worker_id(),
      hello.requested_partition(),
    )
    attempt.worker_id = worker.worker_id()
    attempt.partition = Some(assignment.partition())
    if job_store_partition(live.job_store, assignment.partition()).phase() ==
      @control.JobPartitionPhase::Succeeded {
      raise FrameIoError::Handler("Worker partition is already complete")
    }
    let claim_key = hello.worker_id() + "#" + assignment.partition().to_string()
    claim_lock.acquire() catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
    let unique = claimed.add_and_check(claim_key)
    claim_lock.release()
    if !unique {
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::IncarnationConflict,
      )
    }
    claimed_id = claim_key
    if hello.incarnation() != worker.incarnation().value() {
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::StaleIncarnation,
      )
    }
    match live.required_capability {
      Some(capability) if !hello
        .capabilities()
        .any(value => value == capability) =>
        raise FrameIoError::SupervisorPlan(
          SupervisorPlanError::MissingCapability(
            worker=hello.worker_id(),
            capability~,
          ),
        )
      _ => ()
    }
    live.session_sequence += 1L
    let (session, outbound) = coordinator_handshake_after_hello(
      connection,
      hello_frame,
      live.coordinator.state().cluster_id(),
      live.coordinator.state().coordinator_id(),
      hello.worker_id(),
      live.plan.job_id() +
      "-live-p" +
      assignment.partition().to_string() +
      "-s" +
      live.session_sequence.to_string(),
      assignment=spec,
      maximum_frame_bytes~,
    )
    Ok(
      run_live_source_session(
        connection, live, lifecycle, worker, assignment, session, outbound, maximum_frame_bytes,
        poll_interval_ms, drain_timeout_ms, output_manifest,
      ),
    )
  } catch {
    error => Err(error)
  }
  if outcome is Err(_) && !claimed_id.is_empty() {
    release_supervisor_claim(claimed, claim_lock, claimed_id)
  }
  match outcome {
    Ok(value) => value
    Err(error) => raise error
  }
}

///|
priv struct LiveConnectionAttempt {
  mut worker_id : String
  mut partition : Int?
  mut attempts : Int
}

///|
/// Retry only transport failures. The durable assignment and source cursor are
/// retained; a fresh Worker Resume determines the unconfirmed suffix.
async fn supervise_live_source_connection_with_retry(
  server : @socket.TcpServer,
  live : SourceBackedJob,
  claimed : Set[String],
  claim_lock : @async.Mutex,
  accept_lock : @async.Mutex,
  maximum_frame_bytes : Int,
  poll_interval_ms : Int,
  drain_timeout_ms : Int,
  output_manifest : JobOutputManifest?,
  reconnect_attempts : Int,
  reconnect_backoff_ms : Int,
  reconnect_timeout_ms : Int,
  attempt : LiveConnectionAttempt,
) -> CoordinatorRunSummary raise FrameIoError {
  for ;; {
    let result : Result[CoordinatorRunSummary, FrameIoError] = Ok(
      supervise_live_source_connection(
        server,
        live,
        live.lifecycle,
        claimed,
        claim_lock,
        accept_lock,
        maximum_frame_bytes,
        poll_interval_ms,
        drain_timeout_ms,
        output_manifest,
        attempt,
        reconnect_timeout_ms,
      ),
    ) catch {
      error => Err(error)
    }
    match result {
      Ok(summary) => return summary
      Err(FrameIoError::Io(_)) if attempt.attempts <= reconnect_attempts => {
        attempt.attempts += 1
        if reconnect_backoff_ms > 0 {
          @async.sleep(reconnect_backoff_ms) catch {
            error => raise FrameIoError::Io(@debug.to_string(error))
          }
        }
      }
      Err(error) => raise error
    }
  }
}

///|
/// Run a source-backed job while ingress remains open. Every source entry is
/// materialized as an immutable one-event batch; this deliberately favors
/// stable recovery boundaries over throughput until segmented source batches
/// are introduced.
pub async fn run_live_source_supervisor(
  server : @socket.TcpServer,
  live : SourceBackedJob,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
  poll_interval_ms? : Int = 10,
  drain_timeout_ms? : Int = 5000,
  output_manifest? : JobOutputManifest,
  reconnect_attempts? : Int = 3,
  reconnect_backoff_ms? : Int = 10,
  reconnect_timeout_ms? : Int = 5000,
) -> LiveSupervisorSummary raise FrameIoError {
  if maximum_frame_bytes <= 0 ||
    poll_interval_ms <= 0 ||
    drain_timeout_ms <= 0 ||
    reconnect_attempts < 0 ||
    reconnect_backoff_ms < 0 ||
    reconnect_timeout_ms <= 0 {
    raise FrameIoError::Handler("invalid live supervisor configuration")
  }
  if !live.job_store.supervisor_lock.try_acquire() {
    raise FrameIoError::JobStore(JobStoreError::AlreadyRunning)
  }
  defer live.job_store.supervisor_lock.release()
  let lifecycle = live.lifecycle
  live.refresh_work_plans()
  (@async.with_task_group <| group => {
    let claimed : Set[String] = Set([])
    let claim_lock = @async.Mutex::Mutex()
    let accept_lock = @async.Mutex::Mutex()
    let tasks : Array[@async.Task[Result[CoordinatorRunSummary, FrameIoError]]] = []
    let attempts : Array[LiveConnectionAttempt] = []
    let active = live.job_store
      .snapshot()
      .partitions()
      .filter(partition => {
        partition.phase() != @control.JobPartitionPhase::Succeeded
      })
    for _ in active {
      let attempt : LiveConnectionAttempt = {
        worker_id: "",
        partition: None,
        attempts: 1,
      }
      attempts.push(attempt)
      tasks.push(
        group.spawn(allow_failure=true) <| () => {
          Ok(
            supervise_live_source_connection_with_retry(
              server, live, claimed, claim_lock, accept_lock, maximum_frame_bytes,
              poll_interval_ms, drain_timeout_ms, output_manifest, reconnect_attempts,
              reconnect_backoff_ms, reconnect_timeout_ms, attempt,
            ),
          ) catch {
            error => Err(error)
          }
        },
      )
    }
    let failures : Array[SupervisorFailure] = []
    let mut sent_batches = 0
    let mut sent_deltas = 0
    let completed : Set[Int] = Set([])
    let mut stopped = live.plan.partition_count() - active.length()
    let mut drain_started : Int64? = None
    while completed.length() < tasks.length() {
      if lifecycle.phase() == LiveJobPhase::Cancelled {
        raise FrameIoError::Handler("live job cancelled")
      }
      if lifecycle.is_draining() {
        match drain_started {
          None => drain_started = Some(@async.now())
          Some(start) if @async.now() - start >= drain_timeout_ms.to_int64() =>
            raise FrameIoError::Handler("live job drain timed out")
          _ => ()
        }
      }
      for index, task in tasks {
        if completed.contains(index) {
          continue
        }
        match task.try_wait() {
          None => ()
          Some(Ok(summary)) => {
            completed.add(index)
            stopped += 1
            sent_batches += summary.sent_batches()
            sent_deltas += summary.sent_deltas()
          }
          Some(Err(error)) => {
            failures.push({
              worker_id: attempts[index].worker_id,
              partition: attempts[index].partition,
              attempts: attempts[index].attempts,
              reason: @debug.to_string(error),
            })
            group.return_immediately(LiveSupervisorSummary::{
              completed_partitions: stopped,
              failed_partitions: live.plan.partition_count() - stopped,
              sent_batches,
              sent_deltas,
              failures,
            })
          }
        }
      }
      if completed.length() < tasks.length() {
        @async.sleep(poll_interval_ms)
      }
    }
    {
      completed_partitions: stopped,
      failed_partitions: failures.length(),
      sent_batches,
      sent_deltas,
      failures,
    }
  }) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
}
