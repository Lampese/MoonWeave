///|
/// Work assigned to one Worker connection. The supervisor routes by
/// `(worker_id, partition)` when a Worker owns multiple sessions; a missing
/// partition hint retains the legacy one-connection-per-worker behavior.
pub struct SupervisorWork {
  worker_id : String
  session_id : String
  partition : Int
  batches : Array[@protocol.EventBatch]
  deltas : Array[@protocol.DeltaBatch]
  probe : @protocol.Probe?
}

///|
/// Errors in the finite Supervisor plan. These are deterministic and should
/// be reported before the accept loop starts.
pub(all) suberror SupervisorPlanError {
  InvalidMaximumFrameBytes(Int)
  EmptyWorkerId
  EmptySessionId
  InvalidPartition(Int)
  DuplicateWorker(String)
  DuplicatePartition(Int)
  DuplicateSession(String)
  DuplicateDelta(String)
} derive(Debug, Eq)

///|
pub fn SupervisorWork::new(
  worker_id : String,
  session_id : String,
  partition : Int,
  batches : Array[@protocol.EventBatch],
  probe? : @protocol.Probe,
  deltas? : Array[@protocol.DeltaBatch] = [],
) -> SupervisorWork {
  { worker_id, session_id, partition, batches, deltas, probe, }
}

///|
pub fn SupervisorWork::worker_id(self : SupervisorWork) -> String {
  self.worker_id
}

///|
pub fn SupervisorWork::session_id(self : SupervisorWork) -> String {
  self.session_id
}

///|
pub fn SupervisorWork::partition(self : SupervisorWork) -> Int {
  self.partition
}

///|
pub fn SupervisorWork::batches(
  self : SupervisorWork,
) -> ArrayView[@protocol.EventBatch] {
  self.batches[:]
}

///|
pub fn SupervisorWork::probe(self : SupervisorWork) -> @protocol.Probe? {
  self.probe
}

///|
pub fn SupervisorWork::deltas(
  self : SupervisorWork,
) -> ArrayView[@protocol.DeltaBatch] {
  self.deltas[:]
}

///|
pub struct SupervisorRunSummary {
  completed_workers : Int
  failed_workers : Int
  retried_connections : Int
  failures : Array[SupervisorFailure]
  sent_batches : Int
  sent_deltas : Int
}

///|
/// Terminal failure retained by the Supervisor instead of cancelling unrelated
/// Worker sessions.
pub(all) struct SupervisorFailure {
  worker_id : String
  partition : Int?
  attempts : Int
  reason : String
} derive(Debug, Eq)

///|
pub fn SupervisorFailure::worker_id(self : SupervisorFailure) -> String {
  self.worker_id
}

///|
pub fn SupervisorFailure::partition(self : SupervisorFailure) -> Int? {
  self.partition
}

///|
pub fn SupervisorFailure::attempts(self : SupervisorFailure) -> Int {
  self.attempts
}

///|
pub fn SupervisorFailure::reason(self : SupervisorFailure) -> String {
  self.reason
}

///|
pub fn SupervisorRunSummary::completed_workers(
  self : SupervisorRunSummary,
) -> Int {
  self.completed_workers
}

///|
pub fn SupervisorRunSummary::failed_workers(self : SupervisorRunSummary) -> Int {
  self.failed_workers
}

///|
pub fn SupervisorRunSummary::retried_connections(
  self : SupervisorRunSummary,
) -> Int {
  self.retried_connections
}

///|
pub fn SupervisorRunSummary::failures(
  self : SupervisorRunSummary,
) -> ArrayView[SupervisorFailure] {
  self.failures[:]
}

///|
pub fn SupervisorRunSummary::sent_batches(self : SupervisorRunSummary) -> Int {
  self.sent_batches
}

///|
pub fn SupervisorRunSummary::sent_deltas(self : SupervisorRunSummary) -> Int {
  self.sent_deltas
}

///|
fn supervisor_work(
  works : ArrayView[SupervisorWork],
  worker_id : String,
  requested_partition : Int?,
) -> SupervisorWork? {
  let mut found : SupervisorWork? = None
  for work in works {
    if work.worker_id() == worker_id {
      match requested_partition {
        Some(partition) if work.partition() != partition => ()
        _ =>
          match found {
            Some(_) => return None
            None => found = Some(work)
          }
      }
    }
  }
  found
}

///|
fn supervisor_work_for_partition(
  works : ArrayView[SupervisorWork],
  partition : Int,
) -> SupervisorWork? {
  for work in works {
    if work.partition() == partition {
      return Some(work)
    }
  }
  None
}

///|
fn validate_supervisor_plan(
  control : @control.CoordinatorState,
  works : ArrayView[SupervisorWork],
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  if maximum_frame_bytes <= 0 {
    raise FrameIoError::SupervisorPlan(
      SupervisorPlanError::InvalidMaximumFrameBytes(maximum_frame_bytes),
    )
  }
  let worker_partitions : Set[String] = Set([])
  let partitions : Set[Int] = Set([])
  let sessions : Set[String] = Set([])
  let delta_ids : Set[String] = Set([])
  for work in works {
    if work.worker_id().is_empty() {
      raise FrameIoError::SupervisorPlan(SupervisorPlanError::EmptyWorkerId)
    }
    if work.session_id().is_empty() {
      raise FrameIoError::SupervisorPlan(SupervisorPlanError::EmptySessionId)
    }
    let worker_partition_key = work.worker_id() +
      "#" +
      work.partition().to_string()
    if !worker_partitions.add_and_check(worker_partition_key) {
      raise FrameIoError::SupervisorPlan(
        SupervisorPlanError::DuplicateWorker(work.worker_id()),
      )
    }
    if !partitions.add_and_check(work.partition()) {
      raise FrameIoError::SupervisorPlan(
        SupervisorPlanError::DuplicatePartition(work.partition()),
      )
    }
    if work.partition() < 0 {
      raise FrameIoError::SupervisorPlan(
        SupervisorPlanError::InvalidPartition(work.partition()),
      )
    }
    if !sessions.add_and_check(work.session_id()) {
      raise FrameIoError::SupervisorPlan(
        SupervisorPlanError::DuplicateSession(work.session_id()),
      )
    }
    for delta in work.deltas() {
      if delta.delta_id().is_empty() ||
        !delta_ids.add_and_check(delta.delta_id()) {
        raise FrameIoError::SupervisorPlan(
          SupervisorPlanError::DuplicateDelta(delta.delta_id()),
        )
      }
    }
    // Validate ownership and epoch before accepting any socket. This also
    // makes a plan with an unknown partition fail synchronously.
    let (_, assignment, spec) = validate_supervisor_work(control, work)
    validate_batch_plan(spec, work.batches())
    for delta in work.deltas() {
      if delta.target_partition() != assignment.partition() ||
        delta.target_epoch() != assignment.epoch() {
        raise FrameIoError::DeltaMismatch
      }
    }
    match work.probe() {
      None => ()
      Some(probe) =>
        if probe.partition() != assignment.partition() ||
          probe.epoch() != assignment.epoch() {
          raise FrameIoError::ProbeMismatch
        }
    }
  }
}

///|
fn validate_supervisor_work(
  control : @control.CoordinatorState,
  work : SupervisorWork,
) -> (
  @control.WorkerInfo,
  @control.PartitionAssignment,
  @protocol.AssignmentSpec,
) raise FrameIoError {
  let worker = match control.worker(work.worker_id()) {
    Some(value) => value
    None =>
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::UnknownWorker(work.worker_id()),
      )
  }
  if worker.lifecycle() != @control.WorkerLifecycle::Active {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::WorkerNotActive(work.worker_id()),
    )
  }
  let assignment = match control.assignment(work.partition()) {
    Some(value) => value
    None =>
      raise FrameIoError::Coordinator(
        @control.CoordinatorError::UnknownPartition(work.partition()),
      )
  }
  if assignment.worker_id() != worker.worker_id() ||
    assignment.incarnation() != worker.incarnation() {
    raise FrameIoError::Coordinator(
      @control.CoordinatorError::AssignmentConflict(work.partition()),
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
async fn supervisor_record_resume(
  control : @control.CoordinatorState,
  store : CoordinatorStore?,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  offset : Int64,
) -> Unit raise FrameIoError {
  match store {
    Some(durable) =>
      durable.record_resume(
        assignment.partition(),
        worker.worker_id(),
        worker.incarnation(),
        assignment.epoch(),
        offset,
      ) catch {
        error => raise FrameIoError::CoordinatorStore(error)
      }
    None =>
      control.record_resume(
        assignment.partition(),
        worker.worker_id(),
        worker.incarnation(),
        assignment.epoch(),
        offset,
      ) catch {
        error => raise FrameIoError::Coordinator(error)
      }
  }
}

///|
async fn supervisor_record_progress(
  control : @control.CoordinatorState,
  store : CoordinatorStore?,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  persisted_offset : Int64,
  applied_offset : Int64,
) -> Unit raise FrameIoError {
  match store {
    Some(durable) =>
      durable.record_progress(
        assignment.partition(),
        worker.worker_id(),
        worker.incarnation(),
        assignment.epoch(),
        persisted_offset,
        applied_offset,
      ) catch {
        error => raise FrameIoError::CoordinatorStore(error)
      }
    None =>
      control.record_progress(
        assignment.partition(),
        worker.worker_id(),
        worker.incarnation(),
        assignment.epoch(),
        persisted_offset,
        applied_offset,
      ) catch {
        error => raise FrameIoError::Coordinator(error)
      }
  }
}

///|
async fn supervisor_record_heartbeat(
  control : @control.CoordinatorState,
  store : CoordinatorStore?,
  worker : @control.WorkerInfo,
) -> Unit raise FrameIoError {
  match store {
    Some(durable) =>
      durable.record_heartbeat(
        worker.worker_id(),
        worker.incarnation(),
        @async.now(),
      ) catch {
        error => raise FrameIoError::CoordinatorStore(error)
      }
    None =>
      control.record_heartbeat(
        worker.worker_id(),
        worker.incarnation(),
        @async.now(),
      ) catch {
        error => raise FrameIoError::Coordinator(error)
      }
  }
}

///|
/// Continue a managed Coordinator run after Hello/Welcome has completed.
/// Keeping this loop separate lets the multi-connection supervisor route by
/// Hello without weakening the single-connection driver contract.
async fn run_supervised_session(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  worker : @control.WorkerInfo,
  assignment : @control.PartitionAssignment,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  batches : Array[@protocol.EventBatch],
  deltas : Array[@protocol.DeltaBatch],
  probe : @protocol.Probe?,
  maximum_frame_bytes : Int,
  store : CoordinatorStore?,
) -> CoordinatorRunSummary raise FrameIoError {
  session.restore_send_cursor(
    assignment.partition(),
    assignment.epoch(),
    assignment.next_offset(),
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  let spec = @protocol.AssignmentSpec::new(
    assignment.partition(),
    assignment.epoch(),
    assignment.incarnation().value(),
    start_offset=assignment.start_offset(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  validate_batch_plan(spec, batches[:])
  let mut next_batch = 0
  let mut sent_batches = 0
  let mut next_delta = 0
  let sent_delta_ids : Map[String, Bool] = Map([])
  let mut sent_deltas = 0
  let mut acked_deltas = 0
  let mut probe_reply : @protocol.ProbeReply? = None
  for ;; {
    let frame = read_frame(connection, maximum_frame_bytes~)
    session.accept_frame(frame) catch {
      error => raise FrameIoError::Session(error)
    }
    match frame.kind() {
      @protocol.FrameKind::Resume => {
        let resume_message = @protocol.Resume::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        next_batch = resume_batch_index(
          batches[:],
          resume_message.persisted_offset(),
        )
        supervisor_record_resume(
          control,
          store,
          worker,
          assignment,
          resume_message.persisted_offset(),
        )
      }
      @protocol.FrameKind::Credit | @protocol.FrameKind::AckPersisted => ()
      @protocol.FrameKind::DeltaAck => {
        let ack = @protocol.DeltaAck::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        if ack.target_partition() != assignment.partition() ||
          ack.target_epoch() != assignment.epoch() {
          raise FrameIoError::DeltaMismatch
        }
        match sent_delta_ids.get(ack.delta_id()) {
          Some(_) => ()
          None => raise FrameIoError::UnknownDeltaAck(ack.delta_id())
        }
        if !sent_delta_ids.get(ack.delta_id()).unwrap() {
          sent_delta_ids[ack.delta_id()] = true
          acked_deltas += 1
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
        supervisor_record_heartbeat(control, store, worker)
      }
      @protocol.FrameKind::Progress => {
        let progress = @protocol.Progress::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        supervisor_record_progress(
          control,
          store,
          worker,
          assignment,
          progress.persisted_offset(),
          progress.applied_offset(),
        )
      }
      @protocol.FrameKind::Drain => break
      _ =>
        raise FrameIoError::Session(
          @protocol.SessionError::WrongPhase(
            expected=@protocol.SessionPhase::Running,
            actual=session.phase(),
          ),
        )
    }
    while next_batch < batches.length() {
      let batch = batches[next_batch]
      if batch.events().length().to_int64() > session.credit_events() {
        break
      }
      let bytes = @utf8.encode(batch.to_payload()).length().to_int64()
      if bytes > session.credit_bytes() {
        break
      }
      managed_send_batch(
        connection, control, store, session, outbound, assignment, batch, maximum_frame_bytes,
      )
      next_batch += 1
      sent_batches += 1
    }
    while next_delta < deltas.length() {
      let delta = deltas[next_delta]
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
      next_delta += 1
      sent_deltas += 1
    }
    let last_offset = match batches.last() {
      None => assignment.start_offset() - 1L
      Some(batch) => batch.end_offset()
    }
    if next_batch == batches.length() &&
      next_delta == deltas.length() &&
      acked_deltas == sent_deltas &&
      session.last_applied_offset() >= last_offset {
      match probe {
        Some(probe_value) => {
          let reply = managed_probe_and_seal(
            connection, control, store, worker, assignment, session, outbound, probe_value,
            maximum_frame_bytes,
          )
          probe_reply = Some(reply)
        }
        None => ()
      }
      let drain = outbound.build(@protocol.FrameKind::Drain, "{}") catch {
        error => raise FrameIoError::Protocol(error)
      }
      write_frame(connection, drain, maximum_frame_bytes~)
      break
    }
  }
  {
    sent_batches,
    sent_deltas,
    persisted_offset: session.last_persisted_offset(),
    applied_offset: session.last_applied_offset(),
    probe_reply,
  }
}

///|
priv struct SupervisedAttemptResult {
  attempts : Int
  summary : CoordinatorRunSummary?
  failure : SupervisorFailure?
}

///|
async fn release_supervisor_claim(
  claimed : Set[String],
  claim_lock : @async.Mutex,
  claim_id : String,
) -> Unit raise FrameIoError {
  if claim_id.is_empty() {
    return
  }
  claim_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  claimed.remove(claim_id)
  claim_lock.release()
}

///|
async fn supervise_one_connection(
  server : @socket.TcpServer,
  control : @control.CoordinatorState,
  works : ArrayView[SupervisorWork],
  claimed : Set[String],
  claim_lock : @async.Mutex,
  maximum_frame_bytes : Int,
  store : CoordinatorStore?,
  retry_policy : RetryPolicy,
) -> SupervisedAttemptResult raise FrameIoError {
  let mut attempt = 1
  for ;; {
    let mut claimed_id = ""
    let mut claimed_worker_id = ""
    let mut claimed_partition : Int? = None
    let outcome : Result[CoordinatorRunSummary, FrameIoError] = try {
      let (connection, _) = server.accept() catch {
        error => raise FrameIoError::Io(@debug.to_string(error))
      }
      defer connection.close()
      let hello_frame = read_frame(connection, maximum_frame_bytes~)
      let hello = @protocol.Hello::from_payload(hello_frame.payload()) catch {
        error => raise FrameIoError::Message(error)
      }
      claimed_worker_id = hello.worker_id()
      let work = match
        supervisor_work(works, hello.worker_id(), hello.requested_partition()) {
        Some(value) => value
        None =>
          raise FrameIoError::Coordinator(
            @control.CoordinatorError::UnknownWorker(hello.worker_id()),
          )
      }
      claimed_partition = Some(work.partition())
      let claim_key = hello.worker_id() + "#" + work.partition().to_string()
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
      let (worker, assignment, spec) = validate_supervisor_work(control, work)
      if hello.incarnation() != worker.incarnation().value() {
        raise FrameIoError::Coordinator(
          @control.CoordinatorError::StaleIncarnation,
        )
      }
      let (session, outbound) = coordinator_handshake_after_hello(
        connection,
        hello_frame,
        control.cluster_id(),
        control.coordinator_id(),
        work.worker_id(),
        work.session_id(),
        assignment=spec,
        maximum_frame_bytes~,
      )
      let summary = run_supervised_session(
        connection,
        control,
        worker,
        assignment,
        session,
        outbound,
        work.batches().to_owned(),
        work.deltas().to_owned(),
        work.probe(),
        maximum_frame_bytes,
        store,
      )
      Ok(summary)
    } catch {
      error => Err(error)
    }
    match outcome {
      Ok(summary) =>
        return { attempts: attempt, summary: Some(summary), failure: None, }
      Err(error) => {
        release_supervisor_claim(claimed, claim_lock, claimed_id)
        let transient = error is FrameIoError::Io(_)
        if transient && attempt < retry_policy.max_attempts() {
          let delay = retry_policy.backoff_ms(attempt)
          if delay > 0 {
            @async.sleep(delay) catch {
              error => raise FrameIoError::Io(@debug.to_string(error))
            }
          }
          attempt += 1
          continue
        }
        return {
          attempts: attempt,
          summary: None,
          failure: Some({
            worker_id: claimed_worker_id,
            partition: claimed_partition,
            attempts: attempt,
            reason: @debug.to_string(error),
          }),
        }
      }
    }
  }
}

///|
/// Accept and concurrently run one planned connection per SupervisorWork.
/// The caller owns the TcpServer and chooses when to stop accepting new links.
pub async fn run_supervisor(
  server : @socket.TcpServer,
  control : @control.CoordinatorState,
  works : Array[SupervisorWork],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> SupervisorRunSummary raise FrameIoError {
  let retry_policy = RetryPolicy::new(1, 0, 0) catch {
    _ => raise FrameIoError::Handler("invalid default retry policy")
  }
  run_supervisor_impl(
    server,
    control,
    works,
    maximum_frame_bytes,
    None,
    retry_policy,
  )
}

///|
/// Supervisor with bounded reconnect attempts for transient transport I/O.
pub async fn run_supervisor_with_retry(
  server : @socket.TcpServer,
  control : @control.CoordinatorState,
  works : Array[SupervisorWork],
  retry_policy : RetryPolicy,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> SupervisorRunSummary raise FrameIoError {
  run_supervisor_impl(
    server,
    control,
    works,
    maximum_frame_bytes,
    None,
    retry_policy,
  )
}

///|
/// Durable Supervisor variant. All per-worker control events and send intents
/// share one CoordinatorStore, then a checkpoint is written after completion.
pub async fn run_durable_supervisor(
  server : @socket.TcpServer,
  store : CoordinatorStore,
  works : Array[SupervisorWork],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> SupervisorRunSummary raise FrameIoError {
  let retry_policy = RetryPolicy::new(1, 0, 0) catch {
    _ => raise FrameIoError::Handler("invalid default retry policy")
  }
  let summary = run_supervisor_impl(
    server,
    store.state(),
    works,
    maximum_frame_bytes,
    Some(store),
    retry_policy,
  )
  store.checkpoint() catch {
    error => raise FrameIoError::CoordinatorStore(error)
  }
  summary
}

///|
/// Durable Supervisor with bounded transient-I/O reconnect attempts.
pub async fn run_durable_supervisor_with_retry(
  server : @socket.TcpServer,
  store : CoordinatorStore,
  works : Array[SupervisorWork],
  retry_policy : RetryPolicy,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> SupervisorRunSummary raise FrameIoError {
  let summary = run_supervisor_impl(
    server,
    store.state(),
    works,
    maximum_frame_bytes,
    Some(store),
    retry_policy,
  )
  store.checkpoint() catch {
    error => raise FrameIoError::CoordinatorStore(error)
  }
  summary
}

///|
async fn run_supervisor_impl(
  server : @socket.TcpServer,
  control : @control.CoordinatorState,
  works : Array[SupervisorWork],
  maximum_frame_bytes : Int,
  store : CoordinatorStore?,
  retry_policy : RetryPolicy,
) -> SupervisorRunSummary raise FrameIoError {
  validate_supervisor_plan(control, works[:], maximum_frame_bytes)
  (@async.with_task_group <| group => {
    let claimed : Set[String] = Set([])
    let claim_lock = @async.Mutex::Mutex()
    let tasks : Array[@async.Task[SupervisedAttemptResult]] = []
    for _ in works {
      let task = group.spawn(allow_failure=true) <| () => {
        supervise_one_connection(
          server,
          control,
          works[:],
          claimed,
          claim_lock,
          maximum_frame_bytes,
          store,
          retry_policy,
        )
      }
      tasks.push(task)
    }
    let mut sent_batches = 0
    let mut sent_deltas = 0
    let failures : Array[SupervisorFailure] = []
    let mut retried_connections = 0
    for task in tasks {
      let outcome : SupervisedAttemptResult? = Some(task.wait()) catch {
        error => {
          failures.push({
            worker_id: "",
            partition: None,
            attempts: 1,
            reason: @debug.to_string(error),
          })
          None
        }
      }
      match outcome {
        Some(result) => {
          retried_connections += result.attempts - 1
          match result.summary {
            Some(summary) => {
              sent_batches += summary.sent_batches()
              sent_deltas += summary.sent_deltas()
            }
            None => ()
          }
          match result.failure {
            Some(failure) => failures.push(failure)
            None => ()
          }
        }
        None => ()
      }
    }
    {
      completed_workers: works.length() - failures.length(),
      failed_workers: failures.length(),
      retried_connections,
      failures,
      sent_batches,
      sent_deltas,
    }
  }) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
}

///|
fn job_partition_for(
  snapshot : @control.JobSnapshot,
  partition : Int,
) -> @control.JobPartition? {
  for value in snapshot.partitions() {
    if value.partition() == partition {
      return Some(value)
    }
  }
  None
}

///|
fn validate_job_supervisor_plan(
  control : @control.CoordinatorState,
  job_store : JobStore,
  works : ArrayView[SupervisorWork],
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  if job_store.phase() != @control.JobPhase::Created {
    raise FrameIoError::JobStore(
      JobStoreError::Job(
        @control.JobError::InvalidTransition(
          expected=@control.JobPhase::Created,
          actual=job_store.phase(),
        ),
      ),
    )
  }
  let snapshot = job_store.snapshot()
  if works.length() != snapshot.partition_count() {
    raise FrameIoError::JobStore(
      JobStoreError::Job(@control.JobError::NotFullyAssigned),
    )
  }
  validate_supervisor_plan(control, works, maximum_frame_bytes)
  for work in works {
    let (_, assignment, _) = validate_supervisor_work(control, work)
    let job_partition = match job_partition_for(snapshot, work.partition()) {
      Some(value) => value
      None =>
        raise FrameIoError::JobStore(
          JobStoreError::Job(
            @control.JobError::UnknownPartition(work.partition()),
          ),
        )
    }
    if job_partition.worker_id() != assignment.worker_id() ||
      job_partition.epoch() != assignment.epoch() ||
      job_partition.start_offset() != assignment.start_offset() {
      raise FrameIoError::JobStore(
        JobStoreError::Job(
          @control.JobError::AssignmentConflict(work.partition()),
        ),
      )
    }
  }
}

///|
async fn finalize_job_supervisor_success(
  control : @control.CoordinatorState,
  job_store : JobStore,
  works : ArrayView[SupervisorWork],
) -> Unit raise FrameIoError {
  job_store.drain() catch {
    error => raise FrameIoError::JobStore(error)
  }
  for work in works {
    let assignment = match control.assignment(work.partition()) {
      Some(value) => value
      None =>
        raise FrameIoError::Coordinator(
          @control.CoordinatorError::UnknownPartition(work.partition()),
        )
    }
    for batch in work.batches() {
      ignore(
        job_store.record_batch_sent(
          batch.partition(),
          assignment.worker_id(),
          assignment.epoch(),
          batch.first_offset(),
          batch.events().length().to_int64(),
        ) catch {
          error => raise FrameIoError::JobStore(error)
        },
      )
    }
    job_store.record_progress(
      assignment.partition(),
      assignment.worker_id(),
      assignment.epoch(),
      assignment.persisted_offset(),
      assignment.applied_offset(),
    ) catch {
      error => raise FrameIoError::JobStore(error)
    }
    job_store.stop_partition(
      assignment.partition(),
      assignment.worker_id(),
      assignment.epoch(),
      0,
    ) catch {
      error => raise FrameIoError::JobStore(error)
    }
  }
  job_store.checkpoint() catch {
    error => raise FrameIoError::JobStore(error)
  }
}

///|
async fn finalize_job_supervisor_failure(
  control : @control.CoordinatorState,
  job_store : JobStore,
  works : ArrayView[SupervisorWork],
  summary : SupervisorRunSummary,
) -> Unit raise FrameIoError {
  for failure in summary.failures() {
    if failure.worker_id().is_empty() {
      job_store.fail(failure.reason()) catch {
        error => raise FrameIoError::JobStore(error)
      }
    } else {
      let work = match failure.partition() {
        Some(partition) => supervisor_work_for_partition(works, partition)
        None => supervisor_work(works, failure.worker_id(), None)
      }
      let work = match work {
        Some(value) => value
        None => {
          job_store.fail(failure.reason()) catch {
            error => raise FrameIoError::JobStore(error)
          }
          continue
        }
      }
      let assignment = match control.assignment(work.partition()) {
        Some(value) => value
        None => {
          job_store.fail(failure.reason()) catch {
            error => raise FrameIoError::JobStore(error)
          }
          continue
        }
      }
      job_store.fail_partition(
        assignment.partition(),
        assignment.worker_id(),
        assignment.epoch(),
        failure.reason(),
      ) catch {
        error => raise FrameIoError::JobStore(error)
      }
    }
  }
  job_store.checkpoint() catch {
    error => raise FrameIoError::JobStore(error)
  }
}

///|
/// Run a Supervisor while advancing a durable Job lifecycle. The existing
/// Supervisor remains responsible for socket routing and Worker control; this
/// adapter commits Job terminal state only after those per-session results are
/// known, preserving a clear two-phase boundary between transport and Job
/// scheduling state.
pub async fn run_job_supervisor(
  server : @socket.TcpServer,
  control : @control.CoordinatorState,
  job_store : JobStore,
  works : Array[SupervisorWork],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> SupervisorRunSummary raise FrameIoError {
  validate_job_supervisor_plan(
    control,
    job_store,
    works[:],
    maximum_frame_bytes,
  )
  job_store.start() catch {
    error => raise FrameIoError::JobStore(error)
  }
  let result : Result[SupervisorRunSummary, FrameIoError] = Ok(
    run_supervisor(server, control, works, maximum_frame_bytes~),
  ) catch {
    error => Err(error)
  }
  let summary = match result {
    Ok(value) => value
    Err(error) => {
      job_store.fail(@debug.to_string(error)) catch {
        failure => raise FrameIoError::JobStore(failure)
      }
      job_store.checkpoint() catch {
        failure => raise FrameIoError::JobStore(failure)
      }
      raise error
    }
  }
  if summary.failed_workers() == 0 {
    finalize_job_supervisor_success(control, job_store, works[:])
  } else {
    finalize_job_supervisor_failure(control, job_store, works[:], summary)
  }
  summary
}
