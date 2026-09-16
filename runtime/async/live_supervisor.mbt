///|
/// Lifecycle signal shared by a source-backed live supervisor and its control
/// plane. The signal changes only after the Driver has durably changed its
/// own lifecycle state, so a session never drains while ingress is still
/// accepted.
enum LiveJobPhase {
  Open
  Paused
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
  if self.phase == LiveJobPhase::Open || self.phase == LiveJobPhase::Paused {
    self.phase = LiveJobPhase::Draining
  }
}

///|
fn LiveJobControl::request_pause(self : LiveJobControl) -> Unit {
  if self.phase == LiveJobPhase::Open {
    self.phase = LiveJobPhase::Paused
  }
}

///|
fn LiveJobControl::request_resume(self : LiveJobControl) -> Unit {
  if self.phase == LiveJobPhase::Paused {
    self.phase = LiveJobPhase::Open
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
  watermark_store : SourceWatermarkStore?
  external_watermark_store : SourceWatermarkStore?
  confirmed_watermarks : Map[Int, Int64]
  plan : @plan.JobSpec
  coordinator : CoordinatorStore
  job_store : JobStore
  required_capability : String?
  ingress_lock : @async.Mutex
  ingress_gate : @async.Mutex
  acknowledged_cuts : Map[Int, Array[Int64]]
  mut retirement : @causal.CausalRetirement?
  retirement_acks : Map[Int, String]
  lifecycle : LiveJobControl
  mut failed : Bool
  mut closed : Bool
  mut session_sequence : Int64
  mut supervising : Bool
  mut authority_barrier : (async () -> Unit raise FrameIoError)?
  mut published_cut : (Array[Int64], Int64, Int64)?
}

///|
pub async fn SourceBackedJob::open(
  source_path : String,
  plan : @plan.JobSpec,
  coordinator : CoordinatorStore,
  job_store : JobStore,
  required_capability? : String,
  maximum_bytes? : Int64,
  event_time_config? : @task.TaskEventTimeConfig,
  watermark_idle_timeout_ms? : Int64 = 0L,
  watermark_inputs? : Array[String],
  external_watermarks? : Array[String],
  authority? : LiveJobAuthoritySnapshot,
) -> SourceBackedJob raise FrameIoError {
  if !same_job_spec(plan, job_store.snapshot()) {
    raise FrameIoError::Handler("live source topology does not match JobStore")
  }
  let source = SourceLog::open(source_path, maximum_bytes?) catch {
    error => raise FrameIoError::SourceLog(error)
  }
  let input_ids = match watermark_inputs {
    Some(ids) => Some(ids)
    None =>
      event_time_config.map(config => {
        if config.window_size_ms() > 0L {
          ["main"]
        } else {
          ["left", "right"]
        }
      })
  }
  let watermark_store = match (input_ids, external_watermarks) {
    (Some(input_ids), None) =>
      Some(
        SourceWatermarkStore::open(
          source_path + ".watermark",
          input_ids,
          watermark_idle_timeout_ms,
          maximum_bytes,
        ),
      ) catch {
        error => {
          source.close()
          raise error
        }
      }
    _ => None
  }
  let external_watermark_store = match external_watermarks {
    None => None
    Some(ids) =>
      Some(
        SourceWatermarkStore::open(
          source_path + ".graph-watermark",
          ids,
          watermark_idle_timeout_ms,
          maximum_bytes,
        ),
      )
  }
  let job : SourceBackedJob = {
    source,
    watermark_store,
    external_watermark_store,
    confirmed_watermarks: Map([]),
    plan,
    coordinator,
    job_store,
    required_capability,
    ingress_lock: @async.Mutex::Mutex(),
    ingress_gate: @async.Mutex::Mutex(),
    acknowledged_cuts: Map([]),
    retirement: None,
    retirement_acks: Map([]),
    lifecycle: LiveJobControl::new(),
    failed: false,
    closed: false,
    session_sequence: 0L,
    supervising: false,
    authority_barrier: None,
    published_cut: None,
  }
  errdefer job.close()
  if authority is Some(snapshot) {
    job.restore_source_authority(snapshot)
  }
  job.finish_source_reclamation()
  // Reconcile the source of truth after a crash window between SourceLog and
  // the watermark WAL. The durable published frontier remains monotone while
  // newly discovered source events restore per-input progress.
  match job.watermark_store {
    None => ()
    Some(store) => {
      let now = @async.now()
      let candidate = store.progress.copy()
      for location in job.source.locations.values() {
        match location.event.event_time() {
          None => ()
          Some(timestamp) =>
            if candidate.inputs.any(input => {
                input.input_id == location.event.input_id() &&
                (
                  input.source_id is None ||
                  input.source_id == Some(location.event.id().origin())
                )
              }) {
              candidate.observe(
                location.event.input_id(),
                timestamp,
                now,
                source_id=location.event.id().origin(),
              )
            }
        }
      }
      ignore(candidate.advance(now))
      store.save(candidate)
    }
  }
  job
}

///|
pub fn SourceBackedJob::close(self : SourceBackedJob) -> Unit {
  self.closed = true
  self.lifecycle.cancel()
  self.source.close()
  match self.watermark_store {
    Some(store) => store.wal.close()
    None => ()
  }
  match self.external_watermark_store {
    Some(store) => store.wal.close()
    None => ()
  }
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
/// Pause ingress while retaining all Worker sessions and durable state.
pub async fn SourceBackedJob::pause(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  if self.job_store.phase() != @control.JobPhase::Running {
    raise FrameIoError::Handler("live Job cannot pause in its current phase")
  }
  self.lifecycle.request_pause()
}

///|
/// Resume a previously paused live source.
pub async fn SourceBackedJob::resume(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  if self.job_store.phase() != @control.JobPhase::Running {
    raise FrameIoError::Handler("live Job cannot resume in its current phase")
  }
  self.lifecycle.request_resume()
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
/// Retire the complete source prefix after all partitions have succeeded.
/// JobStore's checkpoint is written first by the caller, so the source log no
/// longer has a dependent replay plan. The logical offsets and retention
/// markers remain durable while the physical event prefix and indexes go away.
pub async fn SourceBackedJob::compact_completed_source(
  self : SourceBackedJob,
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  if self.job_store.phase() != @control.JobPhase::Succeeded {
    return
  }
  for partition in 0..<self.plan.partition_count() {
    let retain_from = self.source.next_offset(partition).unwrap_or(0L)
    let barrier = self.source.retention_barrier(partition, retain_from) catch {
      error => raise FrameIoError::SourceLog(error)
    }
    self.source.compact(barrier) catch {
      error => raise FrameIoError::SourceLog(error)
    }
  }
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
  let planned : Map[@causal.EventId, @protocol.DeltaBatch] = Map([])
  if self.job_store.work_plans.get(partition) is Some(previous) {
    for delta in previous.deltas {
      if delta.events().length() == 1 {
        planned[delta.events()[0].id()] = delta
      }
    }
  }
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
      // A source lease move does not create a new logical exchange. Preserve
      // the already published delta identity and source attempt; only the
      // receiving lease is rebound when this target itself migrates.
      if planned.get(location.event.id()) is Some(previous) &&
        previous.source_partition() == source_partition &&
        previous.events() == [location.event][:] {
        deltas.push({ ..previous, target_epoch: assignment.epoch(), })
        continue
      }
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
  self.acquire_ingress([event])
  defer self.ingress_gate.release()
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  if self.lifecycle.phase() != LiveJobPhase::Open {
    raise FrameIoError::Handler("live source is sealed")
  }
  let watermark_candidate = self.prepare_watermark([event])
  errdefer {
    self.failed = true
  }
  let control = self.coordinator.state()
  let target = self.plan.route_event(event)
  let assignment = source_backed_assignment(control, target.partition())
  let offset = self.source.append(
    assignment.partition(),
    self.input_floor(assignment.partition(), assignment.start_offset()),
    event,
  ) catch {
    error => raise FrameIoError::SourceLog(error)
  }
  self.refresh_work_plans_unlocked()
  match (self.watermark_store, watermark_candidate) {
    (Some(store), Some(candidate)) => store.save(candidate)
    _ => ()
  }
  offset
}

///|
/// Reconcile all durable Driver inputs into SourceLog. Equal retransmissions
/// are idempotent at SourceLog; an identity conflict remains fail-closed.
pub async fn SourceBackedJob::ingest_all(
  self : SourceBackedJob,
  events : ArrayView[@protocol.EventRecord],
) -> Unit raise FrameIoError {
  // Authority replay is allowed after a local execution has already sealed
  // its source. Equal identities are durable evidence that the replay was
  // already applied; only a genuinely new identity must be rejected.
  if self.lifecycle.phase() != LiveJobPhase::Open {
    for event in events {
      if !self.source.contains(event.id()) &&
        !self.source.retired.any(range => range.contains(event.id())) {
        raise FrameIoError::Handler("live source is sealed")
      }
    }
    return
  }
  if events.is_empty() {
    self.ingest_chunk(events)
    return
  }
  for start = 0; start < events.length(); start = start + 16 {
    let end = if start + 16 < events.length() {
      start + 16
    } else {
      events.length()
    }
    self.ingest_chunk(events[start:end])
  }
}

///|
async fn SourceBackedJob::ingest_chunk(
  self : SourceBackedJob,
  events : ArrayView[@protocol.EventRecord],
) -> Unit raise FrameIoError {
  self.acquire_ingress(events)
  defer self.ingress_gate.release()
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  if self.lifecycle.phase() != LiveJobPhase::Open {
    raise FrameIoError::Handler("live source is sealed")
  }
  let watermark_candidate = self.prepare_watermark(events)
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
        self.input_floor(assignment.partition(), assignment.start_offset()),
        event,
      ) catch {
        error => raise FrameIoError::SourceLog(error)
      },
    )
  }
  self.refresh_work_plans_unlocked()
  match (self.watermark_store, watermark_candidate) {
    (Some(store), Some(candidate)) => store.save(candidate)
    _ => ()
  }
}

///|
/// Equal source retries do not reactivate idle inputs. Validate new timestamps
/// before admitting source data and install progress after its delivery plan.
fn SourceBackedJob::prepare_watermark(
  self : SourceBackedJob,
  events : ArrayView[@protocol.EventRecord],
) -> SourceWatermarks? raise FrameIoError {
  match self.watermark_store {
    None => None
    Some(store) => {
      let candidate = store.progress.copy()
      let now = @async.now()
      let mut changed = false
      for event in events {
        if self.source.locations.contains(event.id()) ||
          self.source.retired.any(range => range.contains(event.id())) {
          continue
        }
        guard event.event_time() is Some(timestamp) else {
          raise FrameIoError::Handler("event-time input requires a timestamp")
        }
        candidate.observe(
          event.input_id(),
          timestamp,
          now,
          source_id=event.id().origin(),
        )
        changed = true
      }
      ignore(candidate.advance(now))
      if changed || candidate.watermark != store.progress.watermark {
        Some(candidate)
      } else {
        None
      }
    }
  }
}

///|
pub async fn SourceBackedJob::assign_source_partitions(
  self : SourceBackedJob,
  input_id : String,
  source_ids : ArrayView[String],
) -> Unit raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  match self.watermark_store {
    None => ()
    Some(store) => {
      let candidate = store.progress.copy()
      candidate.assign_partitions(input_id, source_ids, @async.now())
      if candidate.snapshot() != store.progress.snapshot() {
        store.save(candidate)
      }
    }
  }
}

///|
async fn SourceBackedJob::source_watermark_unlocked(
  self : SourceBackedJob,
) -> Int64 raise FrameIoError {
  if self.external_watermark_store is Some(store) {
    let candidate = store.progress.copy()
    ignore(candidate.advance(@async.now()))
    if candidate.watermark != store.progress.watermark {
      store.save(candidate)
    }
    return candidate.watermark
  }
  match self.watermark_store {
    None => -1L
    Some(store) => {
      let now = @async.now()
      let candidate = store.progress.copy()
      let before = candidate.watermark
      ignore(candidate.advance(now))
      if candidate.watermark != before ||
        candidate.inputs != store.progress.inputs {
        store.save(candidate)
      }
      candidate.watermark
    }
  }
}

///|
async fn SourceBackedJob::work(
  self : SourceBackedJob,
  partition : Int,
) -> SupervisorWork raise FrameIoError {
  self.work_cut(partition).work
}

///|
/// Capture work and progress under one lock. A concurrent append must not
/// publish a watermark ahead of the last event included in this work cut.
async fn SourceBackedJob::work_cut(
  self : SourceBackedJob,
  partition : Int,
) -> LiveWorkCut raise FrameIoError {
  let cut = self.unconfirmed_work_cut(partition)
  // Release ingress before replicating the service snapshot. The snapshot
  // includes this cut (or a later one); no Worker sees unreplicated inputs or
  // watermarks even when the originating Driver loses its connection.
  let identity = (cut.source_offsets, cut.watermark, cut.generation)
  if self.authority_barrier is Some(confirm) &&
    self.published_cut != Some(identity) {
    confirm()
    self.published_cut = Some(identity)
  }
  self.check_open()
  cut
}

///|
async fn SourceBackedJob::unconfirmed_work_cut(
  self : SourceBackedJob,
  partition : Int,
) -> LiveWorkCut raise FrameIoError {
  self.ingress_lock.acquire() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  defer self.ingress_lock.release()
  self.check_open()
  guard self.job_store.work_plans.get(partition) is Some(plan) else {
    raise FrameIoError::JobStore(JobStoreError::PlanMismatch(partition))
  }
  let work = SupervisorWork::new(
    plan.worker_id,
    "live",
    partition,
    plan.batches,
    deltas=plan.deltas,
    required_capability?=plan.required_capability,
  )
  {
    work,
    watermark: self.source_watermark_unlocked(),
    retained_from: plan.retained_from.unwrap_or(plan.start_offset),
    source_offsets: self.source_offsets(),
    generation: self.job_store.journal_generation(),
  }
}

///|
pub(all) struct LiveSupervisorSummary {
  completed_partitions : Int
  failed_partitions : Int
  sent_batches : Int
  sent_deltas : Int
  failures : Array[SupervisorFailure]
  recoverable_failures : Bool
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
pub fn LiveSupervisorSummary::recoverable_failures(
  self : LiveSupervisorSummary,
) -> Bool {
  self.recoverable_failures
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
)? raise FrameIoError {
  let worker = match control.worker(worker_id) {
    Some(value) => value
    None => return None
  }
  if worker.lifecycle() != @control.WorkerLifecycle::Active {
    return None
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
  guard matches.length() == 1 else { return None }
  let assignment = matches[0]
  if assignment.incarnation() != worker.incarnation() {
    return None
  }
  let spec = @protocol.AssignmentSpec::new(
    assignment.partition(),
    assignment.epoch(),
    assignment.incarnation().value(),
    start_offset=assignment.start_offset(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  Some((worker, assignment, spec))
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
      // A Worker can reconnect with a durable progress offset that the new
      // Coordinator has not yet journaled (for example after losing the old
      // Coordinator's local disk). Rebuild the sent prefix from the durable
      // work plan before applying the Resume progress, otherwise a valid
      // offset is incorrectly rejected as "ahead of sent".
      if resume_message.persisted_offset() >= previous.next_offset() {
        let work = live.work(assignment.partition())
        for batch in work.batches() {
          if batch.end_offset() <= resume_message.persisted_offset() {
            record_job_send(live.job_store, assignment, batch)
          }
        }
      }
      // The Coordinator cursor is a separate durable fact from JobStore's
      // delivery plan. A failover can restore the latter while losing the
      // former, so rebuild the sent prefix before recording the Worker resume.
      // `record_batch_sent` is idempotent here by cursor: batches already
      // covered by the Coordinator are skipped, while a missing contiguous
      // prefix is journaled in order. This also handles a resume offset in the
      // middle of a batch; the subsequent resume rewinds to the exact suffix.
      let work = live.work(assignment.partition())
      for batch in work.batches() {
        if batch.end_offset() > resume_message.persisted_offset() {
          break
        }
        let next_offset = match
          live.coordinator.state().assignment(assignment.partition()) {
          Some(current) => current.next_offset()
          None =>
            raise FrameIoError::CoordinatorStore(
              CoordinatorStoreError::Coordinator(
                @control.CoordinatorError::UnknownPartition(
                  assignment.partition(),
                ),
              ),
            )
        }
        if next_offset <= batch.first_offset() {
          if next_offset != batch.first_offset() {
            raise FrameIoError::CoordinatorStore(
              CoordinatorStoreError::Coordinator(
                @control.CoordinatorError::InvalidSendOffset(
                  expected=next_offset,
                  actual=batch.first_offset(),
                ),
              ),
            )
          }
          ignore(
            live.coordinator.record_batch_sent(
              assignment.partition(),
              assignment.worker_id(),
              assignment.incarnation(),
              assignment.epoch(),
              batch.first_offset(),
              batch.events().length().to_int64(),
            ) catch {
              error => raise FrameIoError::CoordinatorStore(error)
            },
          )
        }
      }
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
    @protocol.FrameKind::RetireAck => {
      let ack = @protocol.RetireAck::from_payload(frame.payload()) catch {
        error => raise FrameIoError::Message(error)
      }
      if live.retirement.map(certificate => certificate.checkpoint_id()) !=
        Some(ack.checkpoint_id) {
        raise FrameIoError::Handler("unexpected retirement acknowledgement")
      }
      live.retirement_acks[assignment.partition()] = ack.checkpoint_id
      acked_deltas
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
      if progress.watermark is Some(value) {
        let previous = live.confirmed_watermarks
          .get(assignment.partition())
          .unwrap_or(-1L)
        if value > previous {
          live.confirmed_watermarks[assignment.partition()] = value
        }
      }
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
  let mut sent_watermark = -1L
  let mut work_generation = live.job_store.journal_generation()
  let mut sent_retirement : String? = None
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
    let cut = live.work_cut(assignment.partition())
    let work = cut.work
    let watermark = cut.watermark
    if cut.generation != work_generation {
      work_generation = cut.generation
      next_batch = 0
      next_delta = 0
      sent_delta_ids.clear()
      acked_deltas = 0
      sent_deltas = 0
    }
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
      None => cut.retained_from - 1L
      Some(batch) => batch.end_offset()
    }
    let caught_up = send_cursor > last_offset &&
      session.last_applied_offset() >= last_offset &&
      sent_delta_ids.length() == work.deltas().length() &&
      acked_deltas == sent_deltas
    if caught_up {
      live.acknowledged_cuts[assignment.partition()] = cut.source_offsets
    }
    if !draining &&
      caught_up &&
      live.retirement is Some(certificate) &&
      sent_retirement != Some(certificate.checkpoint_id()) {
      let request : @protocol.Retire = {
        partition: assignment.partition(),
        epoch: assignment.epoch(),
        offset: last_offset,
        certificate,
      }
      let frame = outbound.build(
        @protocol.FrameKind::Retire,
        request.to_payload(),
      ) catch {
        error => raise FrameIoError::Protocol(error)
      }
      write_frame(connection, frame, maximum_frame_bytes~)
      sent_retirement = Some(certificate.checkpoint_id())
    }
    if !draining && caught_up && watermark > sent_watermark {
      coordinator_send_watermark(
        connection,
        session,
        outbound,
        assignment.partition(),
        assignment.epoch(),
        watermark,
        maximum_frame_bytes~,
      )
      sent_watermark = watermark
    }
    let retirement_complete = sent_retirement is None ||
      live.retirement_acks.get(assignment.partition()) == sent_retirement
    if draining && caught_up && retirement_complete {
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
  auth_token : String?,
) -> CoordinatorRunSummary raise FrameIoError {
  for ;; {
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
      guard read_supervisor_hello(
          connection,
          live.coordinator.state(),
          maximum_frame_bytes,
          reconnect_timeout_ms,
          auth_token,
        )
        is Some((hello_frame, hello)) else {
        continue
      }
      guard live_assignment_for_hello(
          live.coordinator.state(),
          live.plan,
          hello.worker_id(),
          hello.requested_partition(),
        )
        is Some((worker, assignment, spec)) else {
        continue
      }
      if job_store_partition(live.job_store, assignment.partition()).phase() ==
        @control.JobPartitionPhase::Succeeded {
        continue
      }
      if hello.incarnation() != worker.incarnation().value() {
        continue
      }
      match live.required_capability {
        Some(capability) if !hello
          .capabilities()
          .any(value => value == capability) => continue
        _ => ()
      }
      let claim_key = hello.worker_id() +
        "#" +
        assignment.partition().to_string()
      claim_lock.acquire() catch {
        error => raise FrameIoError::Io(@debug.to_string(error))
      }
      let unique = claimed.add_and_check(claim_key)
      claim_lock.release()
      if !unique {
        continue
      }
      claimed_id = claim_key
      attempt.worker_id = worker.worker_id()
      attempt.partition = Some(assignment.partition())
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
        auth_token?,
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
      Ok(value) => return value
      Err(error) => raise error
    }
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
  auth_token : String?,
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
        auth_token,
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
  auth_token? : String,
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
  live.supervising = true
  defer {
    live.supervising = false
  }
  let lifecycle = live.lifecycle
  live.refresh_work_plans()
  (@async.with_task_group <| group => {
    group.spawn_bg(no_wait=true) <| () => {
      while !live.closed && live.lifecycle.phase() != LiveJobPhase::Cancelled {
        if live.lifecycle.phase() == LiveJobPhase::Open {
          live.reclaim_running()
        }
        @async.sleep(5)
      }
    }
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
              reconnect_backoff_ms, reconnect_timeout_ms, attempt, auth_token,
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
    let mut failure_started : Int64? = None
    let failure_collection_ms = if poll_interval_ms.to_int64() * 10L > 100L {
      poll_interval_ms.to_int64() * 10L
    } else {
      100L
    }
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
            let recoverable = match error {
              FrameIoError::Io(_) => true
              FrameIoError::Handler(message) =>
                message.has_prefix("Worker reconnect deadline") ||
                message.has_prefix("Worker handshake failed")
              _ => false
            }
            if recoverable {
              if failure_started is None {
                failure_started = Some(@async.now())
              }
              completed.add(index)
              stopped += 1
              failures.push({
                worker_id: attempts[index].worker_id,
                partition: attempts[index].partition,
                attempts: attempts[index].attempts,
                reason: @debug.to_string(error),
              })
            } else {
              // Protocol and state-machine errors are deterministic. Do
              // not wait for unrelated sessions after a fail-closed error.
              group.return_immediately(LiveSupervisorSummary::{
                completed_partitions: stopped,
                failed_partitions: live.plan.partition_count() - stopped,
                sent_batches,
                sent_deltas,
                failures: failures +
                [
                  {
                    worker_id: attempts[index].worker_id,
                    partition: attempts[index].partition,
                    attempts: attempts[index].attempts,
                    reason: @debug.to_string(error),
                  },
                ],
                recoverable_failures: false,
              })
            }
          }
        }
      }
      match failure_started {
        Some(start) if @async.now() - start >= failure_collection_ms =>
          group.return_immediately(LiveSupervisorSummary::{
            completed_partitions: stopped,
            failed_partitions: failures.length(),
            sent_batches,
            sent_deltas,
            failures,
            recoverable_failures: true,
          })
        _ => ()
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
      recoverable_failures: !failures.is_empty(),
    }
  }) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
}
