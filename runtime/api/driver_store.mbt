///|
/// One durable Driver command. Input events are retained here until the
/// Coordinator has lowered them into its SourceLog; this makes a Driver retry
/// independent from the network connection that carried the first attempt.
struct DriverJournalEntry {
  version : Int
  kind : String
  submission : JobSubmission?
  events : Array[Event]
  accepted_events : Int64
  reason : String
  retired : Array[@causal.EventRange]?
  paused : Bool?
  update_submission : JobSubmission?
  rescale_workers : Array[String]?
  rescale_resume : Bool?
  parallelism : Int?
  rescale_parallelism : Int?
  graph_update : JobSubmission?
  authority : DriverAuthoritySnapshot?
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Async durable state for one submitted Job. It is intentionally separate
/// from JobStore: the Driver log records intent, while JobStore records
/// partition execution and worker progress.
pub struct DriverStore {
  path : String
  journal : @runtime.Wal
  // The registry is part of the static Coordinator build. It is deliberately
  // not persisted: a restart must be given the same compiled implementations.
  registry : OperatorRegistry
  mut submission : JobSubmission?
  mut compiled : CompiledJob?
  mut phase : JobRunPhase
  mut accepted_events : Int64
  mut failure : String?
  mut paused : Bool
  mut rescale_workers : Array[String]?
  mut rescale_resume : Bool
  mut parallelism : Int
  mut rescale_parallelism : Int?
  mut graph_update : JobSubmission?
  inputs : Array[Event]
  mut event_index : Map[@causal.EventId, Event]
  mut retired : Array[@causal.EventRange]
  maximum_events : Int
  write_lock : @async_runtime.Mutex
}

///|
fn driver_entry(kind : String) -> DriverJournalEntry {
  {
    version: 1,
    kind,
    submission: None,
    events: [],
    accepted_events: -1L,
    reason: "",
    retired: None,
    paused: None,
    update_submission: None,
    rescale_workers: None,
    rescale_resume: None,
    parallelism: None,
    rescale_parallelism: None,
    graph_update: None,
    authority: None,
  }
}

///|
fn driver_entry_line(entry : DriverJournalEntry) -> Bytes {
  @utf8.encode(@json.to_json(entry).stringify())
}

///|
fn driver_error(stage : String, error : Error) -> ApiError {
  ApiError::Cluster(stage + ": " + @debug.to_string(error))
}

///|
fn driver_submission_equal(left : JobSubmission, right : JobSubmission) -> Bool {
  left == right
}

///|
fn[T : ToJson + FromJson] driver_owned(value : T) -> T raise ApiError {
  @json.from_json(
    @json.parse(@json.to_json(value).stringify()) catch {
      error => raise driver_error("copy Driver value", error)
    },
  ) catch {
    error => raise driver_error("copy Driver value", error)
  }
}

///|
fn DriverStore::check_open(self : DriverStore) -> Unit raise ApiError {
  if self.journal.lifecycle() != @runtime.WalLifecycle::Open {
    raise ApiError::DriverClosed
  }
}

///|
fn DriverStore::install_submission(
  self : DriverStore,
  submission : JobSubmission,
) -> Unit raise ApiError {
  let compiled = submission.job().compile(registry=self.registry)
  match self.submission {
    None => {
      self.submission = Some(submission)
      self.compiled = Some(compiled)
      self.parallelism = compiled.initial_parallelism()
    }
    Some(previous) if driver_submission_equal(previous, submission) => ()
    Some(_) => raise ApiError::RequestConflict(submission.submission_id())
  }
}

///|
fn DriverStore::compile_submission(
  self : DriverStore,
  submission : JobSubmission,
) -> CompiledJob raise ApiError {
  if submission.submission_id.is_empty() {
    raise ApiError::EmptySubmissionId
  }
  let compiled = submission.job().compile(registry=self.registry)
  match self.submission {
    None => ()
    Some(previous) if driver_submission_equal(previous, submission) => ()
    Some(_) => raise ApiError::RequestConflict(submission.submission_id())
  }
  compiled
}

///|
fn DriverStore::validate_input(
  self : DriverStore,
  events : ArrayView[Event],
) -> Array[Event] raise ApiError {
  guard self.compiled is Some(compiled) else {
    raise ApiError::UnknownSubmission("input before submit")
  }
  if compiled.definition.source is SourceSpec::Kafka(_) {
    raise ApiError::InvalidDriverCommand(
      "Kafka source Jobs do not accept Driver input",
    )
  }
  if self.phase != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  if self.paused {
    raise ApiError::InvalidDriverCommand("Job is paused")
  }
  let accepted : Array[Event] = []
  let pending_events : Map[@causal.EventId, Event] = Map([])
  for event in events {
    if !compiled.definition.source.accepts_driver_input(event.input_id()) {
      raise ApiError::InvalidDriverCommand(
        "Driver input must target a declared Events source",
      )
    }
    compiled.validate_event(event)
    if self.retired.any(range => range.contains(event.id())) {
      continue
    }
    match self.event_index.get(event.id()) {
      Some(previous) if previous != event =>
        raise ApiError::RequestConflict(event.id().origin())
      Some(_) => ()
      None =>
        match pending_events.get(event.id()) {
          Some(previous) if previous != event =>
            raise ApiError::RequestConflict(event.id().origin())
          Some(_) => ()
          None => {
            pending_events[event.id()] = event
            accepted.push(event)
          }
        }
    }
  }
  if self.maximum_events > 0 &&
    self.event_index.length() + pending_events.length() > self.maximum_events {
    raise ApiError::StateLimitExceeded(
      limit=self.maximum_events,
      actual=self.event_index.length() + pending_events.length(),
    )
  }
  accepted
}

///|
fn DriverStore::install_input(
  self : DriverStore,
  events : ArrayView[Event],
) -> Unit raise ApiError {
  let accepted = self.validate_input(events)
  for event in accepted {
    self.event_index[event.id()] = event
    self.inputs.push(event)
  }
  self.accepted_events += accepted.length().to_int64()
}

///|
fn DriverStore::install_entry(
  self : DriverStore,
  entry : DriverJournalEntry,
) -> Unit raise ApiError {
  if entry.version != 1 {
    raise ApiError::Cluster("unsupported Driver journal version")
  }
  match entry.kind {
    "submit" => {
      if self.phase != JobRunPhase::Created {
        raise ApiError::Cluster("duplicate Driver journal submission")
      }
      guard entry.submission is Some(submission) else {
        raise ApiError::Cluster("malformed Driver submission entry")
      }
      self.install_submission(submission)
      self.phase = JobRunPhase::Running
    }
    "update" => {
      guard entry.update_submission is Some(submission) else {
        raise ApiError::Cluster("malformed Driver update entry")
      }
      if self.phase != JobRunPhase::Running || !self.paused {
        raise ApiError::InvalidDriverCommand(
          "Job update requires a paused Running job",
        )
      }
      let compiled = submission.job().compile(registry=self.registry)
      guard self.submission is Some(previous) &&
        previous.submission_id() == submission.submission_id() else {
        raise ApiError::RequestConflict(submission.submission_id())
      }
      self.submission = Some(submission)
      self.compiled = Some(compiled)
    }
    "graph_update" => {
      guard entry.graph_update is Some(candidate) &&
        self.phase == Running &&
        self.paused else {
        raise ApiError::Cluster("invalid graph update intent")
      }
      ignore(self.validate_update(candidate))
      self.graph_update = Some(candidate)
    }
    "graph_updated" => self.graph_update = None
    "input" => self.install_input(entry.events[:])
    "pause" => {
      if self.phase != JobRunPhase::Running {
        raise ApiError::InvalidJobPhase(
          expected=JobRunPhase::Running,
          actual=self.phase,
        )
      }
      self.paused = true
    }
    "resume" => {
      if self.phase != JobRunPhase::Running {
        raise ApiError::InvalidJobPhase(
          expected=JobRunPhase::Running,
          actual=self.phase,
        )
      }
      self.paused = false
    }
    "rescale" => {
      guard entry.rescale_workers is Some(workers) &&
        !workers.is_empty() &&
        self.phase == JobRunPhase::Running else {
        raise ApiError::Cluster("invalid rescale intent")
      }
      self.rescale_workers = Some(workers)
      self.rescale_resume = entry.rescale_resume.unwrap_or(true)
      self.rescale_parallelism = entry.rescale_parallelism
      self.paused = true
    }
    "rescaled" => {
      if self.rescale_workers is None {
        raise ApiError::Cluster("rescale completion without intent")
      }
      self.rescale_workers = None
      self.parallelism = self.rescale_parallelism.unwrap_or(self.parallelism)
      self.rescale_parallelism = None
      self.paused = !self.rescale_resume
    }
    "parallelism" => {
      guard entry.parallelism is Some(value) &&
        value > 0 &&
        self.compiled is Some(job) &&
        value <= job.maximum_parallelism() else {
        raise ApiError::Cluster("invalid execution parallelism")
      }
      self.parallelism = value
    }
    "checkpoint" => {
      if self.phase != JobRunPhase::Running ||
        !self.inputs.is_empty() ||
        entry.accepted_events < 0L {
        raise ApiError::Cluster("invalid Driver checkpoint")
      }
      self.retired = @causal.CausalRetirement::new(
          "driver",
          entry.retired.unwrap_or([]),
        )
        .ranges()
        .to_owned() catch {
          error => raise driver_error("restore Driver retirement", error)
        }
      self.accepted_events = entry.accepted_events
    }
    "drain" => {
      if self.phase != JobRunPhase::Running {
        raise ApiError::InvalidJobPhase(
          expected=JobRunPhase::Running,
          actual=self.phase,
        )
      }
      self.phase = JobRunPhase::Draining
    }
    "cancel" => {
      if self.phase == JobRunPhase::Succeeded ||
        self.phase == JobRunPhase::Failed {
        raise ApiError::InvalidJobPhase(
          expected=JobRunPhase::Running,
          actual=self.phase,
        )
      }
      if entry.accepted_events >= 0L {
        self.accepted_events = entry.accepted_events
      }
      self.phase = JobRunPhase::Cancelled
    }
    "fail" => {
      if self.phase == JobRunPhase::Succeeded ||
        self.phase == JobRunPhase::Cancelled {
        raise ApiError::Cluster("terminal Driver journal transition")
      }
      if entry.reason.is_empty() {
        raise ApiError::Cluster("empty Driver failure")
      }
      if entry.accepted_events >= 0L {
        self.accepted_events = entry.accepted_events
      }
      self.failure = Some(entry.reason)
      self.phase = JobRunPhase::Failed
    }
    "succeeded" => {
      if self.phase != JobRunPhase::Draining {
        raise ApiError::Cluster("completion without Driver drain")
      }
      if entry.accepted_events < 0L {
        raise ApiError::Cluster("completion without accepted event count")
      }
      if entry.accepted_events < self.accepted_events {
        raise ApiError::Cluster("completion regresses accepted event count")
      }
      self.accepted_events = entry.accepted_events
      self.phase = JobRunPhase::Succeeded
    }
    "authority_snapshot" => {
      guard entry.authority is Some(snapshot) else {
        raise ApiError::Cluster("missing Driver authority")
      }
      self.install_authority_snapshot(snapshot)
    }
    _ => raise ApiError::Cluster("unknown Driver journal command")
  }
}

///|
pub async fn DriverStore::open(
  path : String,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
  maximum_bytes? : Int64,
  maximum_events? : Int = 100000,
) -> DriverStore raise ApiError {
  if path.is_empty() {
    raise ApiError::Cluster("Driver store path is empty")
  }
  if maximum_events < 0 {
    raise ApiError::Cluster("Driver event limit cannot be negative")
  }
  let (journal, replay) = @runtime.Wal::open(path, maximum_bytes?) catch {
    error => raise driver_error("open Driver store", error)
  }
  errdefer journal.close()
  let store = DriverStore::{
    path,
    journal,
    registry,
    submission: None,
    compiled: None,
    phase: JobRunPhase::Created,
    accepted_events: 0L,
    failure: None,
    paused: false,
    rescale_workers: None,
    rescale_resume: true,
    parallelism: 1,
    rescale_parallelism: None,
    graph_update: None,
    inputs: [],
    event_index: Map([]),
    retired: [],
    maximum_events,
    write_lock: @async_runtime.Mutex::Mutex(),
  }
  for payload in replay.records() {
    let text = @utf8.decode(payload) catch {
      error => {
        store.close()
        raise driver_error("decode Driver journal", error)
      }
    }
    let json = @json.parse(text) catch {
      error => {
        store.close()
        raise driver_error("parse Driver journal", error)
      }
    }
    let entry : DriverJournalEntry = @json.from_json(json) catch {
      error => {
        store.close()
        raise driver_error("decode Driver journal entry", error)
      }
    }
    store.install_entry(entry)
  }
  store
}

///|
pub fn DriverStore::close(self : DriverStore) -> Unit {
  self.journal.close()
}

///|
pub fn DriverStore::path(self : DriverStore) -> String {
  self.path
}

///|
pub fn DriverStore::stats(self : DriverStore) -> @runtime.WalStats {
  self.journal.stats()
}

///|
pub fn DriverStore::submission(
  self : DriverStore,
) -> JobSubmission? raise ApiError {
  driver_owned(self.submission)
}

///|
pub fn DriverStore::phase(self : DriverStore) -> JobRunPhase {
  self.phase
}

///|
pub fn DriverStore::accepted_events(self : DriverStore) -> Int64 {
  self.accepted_events
}

///|
pub fn DriverStore::is_paused(self : DriverStore) -> Bool {
  self.paused
}

///|
pub fn DriverStore::inputs(self : DriverStore) -> Array[Event] raise ApiError {
  driver_owned(self.inputs)
}

///|
pub fn DriverStore::status(self : DriverStore) -> JobRunStatus raise ApiError {
  self.check_open()
  guard self.submission is Some(submission) && self.compiled is Some(compiled) else {
    raise ApiError::UnknownSubmission("no Driver submission")
  }
  JobRunStatus::new(
    submission.submission_id(),
    compiled.job_id(),
    compiled.plan_digest(),
    self.phase,
    self.accepted_events,
    self.failure,
    idempotent_sink_required=compiled.sink().requires_idempotent_consumer(),
    paused=self.paused,
    parallelism=self.parallelism,
  )
}

///|
pub async fn DriverStore::submit(
  self : DriverStore,
  submission : JobSubmission,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver store", error)
  }
  defer self.write_lock.release()
  self.check_open()
  let submission = driver_owned(submission)
  let compiled = self.compile_submission(submission)
  if self.submission is Some(_) {
    return self.status()
  }
  match self.phase {
    JobRunPhase::Created => ()
    JobRunPhase::Running => return self.status()
    _ =>
      raise ApiError::InvalidJobPhase(
        expected=JobRunPhase::Created,
        actual=self.phase,
      )
  }
  let entry = DriverJournalEntry::{
    ..driver_entry("submit"),
    submission: Some(submission),
  }
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist Driver submission", error)
    },
  )
  self.submission = Some(submission)
  self.compiled = Some(compiled)
  self.parallelism = compiled.initial_parallelism()
  self.phase = JobRunPhase::Running
  self.status()
}

///|
/// Validate an update without changing durable state. The Coordinator calls
/// this before stopping the current live execution so a rejected topology or
/// state-schema change leaves the paused execution recoverable.
fn DriverStore::validate_update(
  self : DriverStore,
  submission : JobSubmission,
) -> CompiledJob raise ApiError {
  self.check_open()
  guard self.submission is Some(previous) && self.compiled is Some(old_job) else {
    raise ApiError::UnknownSubmission("update before submit")
  }
  if self.phase != JobRunPhase::Running || !self.paused {
    raise ApiError::InvalidDriverCommand(
      "Job update requires a paused Running job",
    )
  }
  if submission.submission_id() != previous.submission_id() {
    raise ApiError::RequestConflict(submission.submission_id())
  }
  let new_job = submission.job().compile(registry=self.registry)
  old_job.validate_compatible_update(new_job)
  new_job
}

///|
/// Persist a compatible update. The Coordinator pauses and recreates the live
/// execution around this operation; this store owns the durable intent.
pub async fn DriverStore::update(
  self : DriverStore,
  submission : JobSubmission,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver update", error)
  }
  defer self.write_lock.release()
  let new_job = self.validate_update(submission)
  let entry = DriverJournalEntry::{
    ..driver_entry("update"),
    update_submission: Some(driver_owned(submission)),
  }
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist Driver update", error)
    },
  )
  guard entry.update_submission is Some(updated) else {
    raise ApiError::Cluster("missing Driver update")
  }
  self.submission = Some(updated)
  self.compiled = Some(new_job)
  self.status()
}

///|
pub async fn DriverStore::input(
  self : DriverStore,
  events : ArrayView[Event],
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver store", error)
  }
  defer self.write_lock.release()
  self.check_open()
  let owned = driver_owned(events.to_owned())
  let accepted = self.validate_input(owned)
  if accepted.is_empty() {
    return self.status()
  }
  let entry = DriverJournalEntry::{ ..driver_entry("input"), events: accepted, }
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist Driver input", error)
    },
  )
  self.install_input(accepted[:])
  self.status()
}

///|
pub async fn DriverStore::drain(
  self : DriverStore,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver store", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.phase == JobRunPhase::Succeeded || self.phase == JobRunPhase::Draining {
    return self.status()
  }
  if self.phase != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  ignore(
    self.journal.append(driver_entry_line(driver_entry("drain"))) catch {
      error => raise driver_error("persist Driver drain", error)
    },
  )
  self.phase = JobRunPhase::Draining
  self.status()
}

///|
/// Persist a pause intent. It is idempotent and replayable.
pub async fn DriverStore::pause(
  self : DriverStore,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver pause", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.phase != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  if !self.paused {
    let entry = DriverJournalEntry::{
      ..driver_entry("pause"),
      paused: Some(true),
    }
    ignore(
      self.journal.append(driver_entry_line(entry)) catch {
        error => raise driver_error("persist Driver pause", error)
      },
    )
    self.paused = true
  }
  self.status()
}

///|
/// Persist a resume intent. It is idempotent and replayable.
pub async fn DriverStore::resume(
  self : DriverStore,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver resume", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.phase != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  if self.paused {
    let entry = DriverJournalEntry::{
      ..driver_entry("resume"),
      paused: Some(false),
    }
    ignore(
      self.journal.append(driver_entry_line(entry)) catch {
        error => raise driver_error("persist Driver resume", error)
      },
    )
    self.paused = false
  }
  self.status()
}

///|
pub async fn DriverStore::cancel(
  self : DriverStore,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver store", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.phase == JobRunPhase::Cancelled {
    return self.status()
  }
  if self.submission is None ||
    self.phase == JobRunPhase::Succeeded ||
    self.phase == JobRunPhase::Failed {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  ignore(
    self.journal.append(driver_entry_line(driver_entry("cancel"))) catch {
      error => raise driver_error("persist Driver cancel", error)
    },
  )
  self.phase = JobRunPhase::Cancelled
  self.status()
}

///|
/// Mark execution complete after the Coordinator has returned a successful
/// summary. The Coordinator listener owns JobStore and has already enforced
/// every partition drain barrier before this method is called.
async fn DriverStore::succeed(
  self : DriverStore,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.phase == JobRunPhase::Succeeded {
    return self.status()
  }
  if self.phase != JobRunPhase::Draining {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Draining,
      actual=self.phase,
    )
  }
  // Keep the terminal transition self-describing so a compacted Driver WAL
  // can drop the full input history while preserving the public count.
  let entry = DriverJournalEntry::{
    ..driver_entry("succeeded"),
    accepted_events: self.accepted_events,
  }
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist completion", error)
    },
  )
  self.phase = JobRunPhase::Succeeded
  self.status()
}

///|
/// Record an execution failure without changing a cancelled or already
/// terminal submission.
async fn DriverStore::fail(
  self : DriverStore,
  reason : String,
) -> JobRunStatus raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if reason.is_empty() {
    raise ApiError::Cluster("empty execution failure")
  }
  if self.phase == JobRunPhase::Cancelled ||
    self.phase == JobRunPhase::Succeeded ||
    self.phase == JobRunPhase::Failed {
    return self.status()
  }
  let entry = DriverJournalEntry::{ ..driver_entry("fail"), reason, }
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist execution failure", error)
    },
  )
  self.install_entry(entry)
  self.status()
}

///|
/// Compact the Driver intent WAL without changing its logical recovery state.
/// The retained command sequence is sufficient to reconstruct submission,
/// input identity, and the current terminal phase after a process restart.
pub async fn DriverStore::compact(
  self : DriverStore,
  retired? : Array[@causal.EventRange],
) -> Unit raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver store", error)
  }
  defer self.write_lock.release()
  self.check_open()
  guard self.submission is Some(submission) else { return }
  let entries : Array[Bytes] = []
  entries.push(
    driver_entry_line(DriverJournalEntry::{
      ..driver_entry("submit"),
      submission: Some(submission),
    }),
  )
  if self.compiled
    .map(job => self.parallelism != job.initial_parallelism())
    .unwrap_or(false) {
    entries.push(
      driver_entry_line({
        ..driver_entry("parallelism"),
        parallelism: Some(self.parallelism),
      }),
    )
  }
  // A terminal Driver no longer needs to replay every input: the live
  // execution and JobStore have already reached a durable terminal state.
  // Keeping the input records for Running/Draining is required for restart.
  let retain_inputs = self.phase == JobRunPhase::Running ||
    self.phase == JobRunPhase::Draining
  let ranges = self.retired.copy()
  match retired {
    Some(values) => ranges.push_iter(values.iter())
    None => ()
  }
  let ranges = @causal.CausalRetirement::new("driver", ranges)
    .ranges()
    .to_owned() catch {
      error => raise driver_error("compact Driver identities", error)
    }
  let retained = if retain_inputs {
    self.inputs.filter(event => !ranges.any(range => range.contains(event.id())))
  } else {
    []
  }
  if retain_inputs &&
    (!ranges.is_empty() || self.accepted_events != retained.length().to_int64()) {
    entries.push(
      driver_entry_line(DriverJournalEntry::{
        ..driver_entry("checkpoint"),
        accepted_events: self.accepted_events - retained.length().to_int64(),
        retired: Some(ranges),
      }),
    )
  }
  if !retained.is_empty() {
    entries.push(
      driver_entry_line(DriverJournalEntry::{
        ..driver_entry("input"),
        events: retained,
      }),
    )
  }
  if self.rescale_workers is Some(workers) {
    entries.push(
      driver_entry_line({
        ..driver_entry("rescale"),
        rescale_workers: Some(workers),
        rescale_resume: Some(self.rescale_resume),
        rescale_parallelism: self.rescale_parallelism,
      }),
    )
  }
  if self.graph_update is Some(candidate) {
    entries.push(driver_entry_line(driver_entry("pause")))
    entries.push(
      driver_entry_line({
        ..driver_entry("graph_update"),
        graph_update: Some(candidate),
      }),
    )
  }
  match self.phase {
    JobRunPhase::Created => ()
    JobRunPhase::Running =>
      if self.paused {
        entries.push(
          driver_entry_line(DriverJournalEntry::{
            ..driver_entry("pause"),
            paused: Some(true),
          }),
        )
        guard self.submission is Some(submission) else {
          raise ApiError::UnknownSubmission("paused Driver without submission")
        }
        entries.push(
          driver_entry_line(DriverJournalEntry::{
            ..driver_entry("update"),
            update_submission: Some(submission),
          }),
        )
      }
    JobRunPhase::Draining =>
      entries.push(driver_entry_line(driver_entry("drain")))
    JobRunPhase::Succeeded => {
      entries.push(driver_entry_line(driver_entry("drain")))
      entries.push(
        driver_entry_line(DriverJournalEntry::{
          ..driver_entry("succeeded"),
          accepted_events: self.accepted_events,
        }),
      )
    }
    JobRunPhase::Cancelled =>
      entries.push(
        driver_entry_line(DriverJournalEntry::{
          ..driver_entry("cancel"),
          accepted_events: self.accepted_events,
        }),
      )
    JobRunPhase::Failed =>
      entries.push(
        driver_entry_line(DriverJournalEntry::{
          ..driver_entry("fail"),
          accepted_events: self.accepted_events,
          reason: self.failure.unwrap_or("execution failed"),
        }),
      )
  }
  self.journal.rewrite(entries[:]) catch {
    error => raise driver_error("compact Driver store", error)
  }
  @crash_boundary.hit("driver.history.reclaimed", self.path) catch {
    error => raise driver_error("Driver reclamation boundary", error)
  }
  if retain_inputs {
    self.inputs.clear()
    self.inputs.push_iter(retained.iter())
    self.event_index = Map::from_array(
      retained.map(event => (event.id(), event)),
    )
    self.retired = ranges
  }
  if self.phase != JobRunPhase::Created &&
    self.phase != JobRunPhase::Running &&
    self.phase != JobRunPhase::Draining {
    // Terminal jobs never re-enter execution. Release their in-memory input
    // index after the compacted WAL has become authoritative as well.
    self.inputs.clear()
    self.event_index = Map([])
  }
}

///|
/// Dispatch one wire command to the durable Driver state. The Result boundary
/// lets a control listener return a command rejection without terminating the
/// process or changing the Job lifecycle.
pub async fn DriverStore::handle(
  self : DriverStore,
  command : JobCommand,
) -> Result[JobRunStatus, String] {
  try {
    match command {
      JobCommand::Submit(value) => Ok(self.submit(value))
      JobCommand::Input(events) => Ok(self.input(events[:]))
      JobCommand::Status => Ok(self.status())
      JobCommand::Pause => Ok(self.pause())
      JobCommand::Resume => Ok(self.resume())
      JobCommand::Rescale(_) =>
        Err("rescale must be handled by the Coordinator")
      JobCommand::Update(_) => Err("update must be handled by the Coordinator")
      JobCommand::Drain => Ok(self.drain())
      JobCommand::Cancel => Ok(self.cancel())
    }
  } catch {
    error => Err(@debug.to_string(error))
  }
}
