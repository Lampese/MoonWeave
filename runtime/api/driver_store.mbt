///|
/// One durable Driver command. Input events are retained here until the
/// Coordinator has lowered them into its SourceLog; this makes a Driver retry
/// independent from the network connection that carried the first attempt.
struct DriverJournalEntry {
  version : Int
  kind : String
  submission : JobSubmission?
  events : Array[Event]
  reason : String
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
  inputs : Array[Event]
  event_index : Map[@causal.EventId, Event]
  write_lock : @async_runtime.Mutex
}

///|
fn driver_entry(kind : String) -> DriverJournalEntry {
  { version: 1, kind, submission: None, events: [], reason: "", }
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
  if self.phase != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  let accepted : Array[Event] = []
  let pending_events : Map[@causal.EventId, Event] = Map([])
  for event in events {
    compiled.operator_descriptor().validate_event(event)
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
    "input" => self.install_input(entry.events[:])
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
      self.failure = Some(entry.reason)
      self.phase = JobRunPhase::Failed
    }
    "succeeded" => {
      if self.phase != JobRunPhase::Draining {
        raise ApiError::Cluster("completion without Driver drain")
      }
      self.phase = JobRunPhase::Succeeded
    }
    _ => raise ApiError::Cluster("unknown Driver journal command")
  }
}

///|
pub async fn DriverStore::open(
  path : String,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> DriverStore raise ApiError {
  if path.is_empty() {
    raise ApiError::Cluster("Driver store path is empty")
  }
  let (journal, replay) = @runtime.Wal::open(path) catch {
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
    inputs: [],
    event_index: Map([]),
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
  self.phase = JobRunPhase::Running
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
  ignore(
    self.journal.append(driver_entry_line(driver_entry("succeeded"))) catch {
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
pub async fn DriverStore::compact(self : DriverStore) -> Unit raise ApiError {
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
  if !self.inputs.is_empty() {
    entries.push(
      driver_entry_line(DriverJournalEntry::{
        ..driver_entry("input"),
        events: self.inputs.copy(),
      }),
    )
  }
  match self.phase {
    JobRunPhase::Created | JobRunPhase::Running => ()
    JobRunPhase::Draining =>
      entries.push(driver_entry_line(driver_entry("drain")))
    JobRunPhase::Succeeded => {
      entries.push(driver_entry_line(driver_entry("drain")))
      entries.push(driver_entry_line(driver_entry("succeeded")))
    }
    JobRunPhase::Cancelled =>
      entries.push(driver_entry_line(driver_entry("cancel")))
    JobRunPhase::Failed =>
      entries.push(
        driver_entry_line(DriverJournalEntry::{
          ..driver_entry("fail"),
          reason: self.failure.unwrap_or("execution failed"),
        }),
      )
  }
  self.journal.rewrite(entries[:]) catch {
    error => raise driver_error("compact Driver store", error)
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
      JobCommand::Drain => Ok(self.drain())
      JobCommand::Cancel => Ok(self.cancel())
    }
  } catch {
    error => Err(@debug.to_string(error))
  }
}
