///|
struct DriverAuthoritySnapshot {
  submission : JobSubmission
  phase : JobRunPhase
  accepted_events : Int64
  inputs : Array[Event]
  retired : Array[@causal.EventRange]
  paused : Bool
  failure : String?
  rescale_workers : Array[String]?
  rescale_resume : Bool
  parallelism : Int
  rescale_parallelism : Int?
  graph_update : JobSubmission?
} derive(Debug, Eq, ToJson, FromJson)

///|
fn DriverStore::authority_snapshot(
  self : DriverStore,
) -> DriverAuthoritySnapshot raise ApiError {
  guard self.submission is Some(submission) else {
    raise ApiError::UnknownSubmission("authority without submission")
  }
  driver_owned({
    submission,
    phase: self.phase,
    accepted_events: self.accepted_events,
    inputs: self.inputs.copy(),
    retired: self.retired.copy(),
    paused: self.paused,
    failure: self.failure,
    rescale_workers: self.rescale_workers,
    rescale_resume: self.rescale_resume,
    parallelism: self.parallelism,
    rescale_parallelism: self.rescale_parallelism,
    graph_update: self.graph_update,
  })
}

///|
fn DriverStore::install_authority_snapshot(
  self : DriverStore,
  snapshot : DriverAuthoritySnapshot,
) -> Unit raise ApiError {
  let compiled = snapshot.submission.job().compile(registry=self.registry)
  let retired = @causal.CausalRetirement::new("driver", snapshot.retired)
    .ranges()
    .to_owned() catch {
      error => raise driver_error("validate authority retirement", error)
    }
  if snapshot.submission.submission_id().is_empty() ||
    snapshot.phase == Created ||
    snapshot.accepted_events < snapshot.inputs.length().to_int64() ||
    (self.maximum_events > 0 && snapshot.inputs.length() > self.maximum_events) ||
    (snapshot.phase == Failed && snapshot.failure.unwrap_or("").is_empty()) {
    raise ApiError::Cluster("invalid Driver authority")
  }
  if snapshot.parallelism <= 0 ||
    snapshot.parallelism > compiled.maximum_parallelism() ||
    (
      snapshot.rescale_parallelism is Some(value) &&
      (
        snapshot.rescale_workers is None ||
        value <= 0 ||
        value > compiled.maximum_parallelism()
      )
    ) {
    raise ApiError::Cluster("invalid authority parallelism")
  }
  let index : Map[@causal.EventId, Event] = Map([])
  for event in snapshot.inputs {
    compiled.validate_event(event)
    if index.contains(event.id()) ||
      retired.any(range => range.contains(event.id())) {
      raise ApiError::Cluster("duplicate or retired authority input")
    }
    index[event.id()] = event
  }
  if snapshot.rescale_workers is Some(workers) {
    let seen : Set[String] = Set([])
    if workers.is_empty() ||
      snapshot.phase != Running ||
      !snapshot.paused ||
      workers.any(id => id.is_empty() || !seen.add_and_check(id)) {
      raise ApiError::Cluster("invalid authority rescale intent")
    }
  }
  if snapshot.graph_update is Some(candidate) {
    if snapshot.phase != Running ||
      !snapshot.paused ||
      !compiled.is_graph() ||
      candidate.submission_id() != snapshot.submission.submission_id() {
      raise ApiError::Cluster("invalid authority graph update")
    }
    compiled.validate_compatible_update(
      candidate.job().compile(registry=self.registry),
    )
  }
  self.submission = Some(snapshot.submission)
  self.compiled = Some(compiled)
  self.phase = snapshot.phase
  self.accepted_events = snapshot.accepted_events
  self.inputs.clear()
  self.inputs.push_iter(snapshot.inputs.iter())
  self.event_index = index
  self.retired = retired
  self.paused = snapshot.paused
  self.failure = snapshot.failure
  self.rescale_workers = snapshot.rescale_workers
  self.rescale_resume = snapshot.rescale_resume
  self.parallelism = snapshot.parallelism
  self.rescale_parallelism = snapshot.rescale_parallelism
  self.graph_update = snapshot.graph_update
}

///|
async fn DriverStore::replace_authority_snapshot(
  self : DriverStore,
  snapshot : DriverAuthoritySnapshot,
) -> Unit raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock Driver authority", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.submission is Some(previous) &&
    previous.submission_id() != snapshot.submission.submission_id() {
    raise ApiError::RequestConflict(snapshot.submission.submission_id())
  }
  let preview : DriverStore = { ..self, inputs: [], event_index: Map([]), }
  preview.install_authority_snapshot(snapshot)
  self.journal.rewrite([
    driver_entry_line({
      ..driver_entry("authority_snapshot"),
      authority: Some(snapshot),
    }),
  ]) catch {
    error => raise driver_error("replace Driver authority", error)
  }
  self.install_authority_snapshot(snapshot)
}
