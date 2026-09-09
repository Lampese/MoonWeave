///|
/// A local lifecycle handle backed by the same compiled Job semantics used by
/// the cluster runner. It is the reference behavior for future remote handles.
pub struct LocalJobHandle {
  submission : JobSubmission
  compiled : CompiledJob
  run : LocalRun
  mut phase : JobRunPhase
  mut accepted_events : Int64
  failure : String?
}

///|
pub fn JobSubmission::compile(
  self : JobSubmission,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> CompiledJob raise ApiError {
  self.job.compile(registry~)
}

///|
pub fn JobSubmission::run_local(
  self : JobSubmission,
  registry? : OperatorRegistry = OperatorRegistry::builtins(),
) -> LocalJobHandle raise ApiError {
  let compiled = self.compile(registry~)
  let run = compiled.run_local()
  {
    submission: self,
    compiled,
    run,
    phase: JobRunPhase::Running,
    accepted_events: 0L,
    failure: None,
  }
}

///|
pub fn LocalJobHandle::status(self : LocalJobHandle) -> JobRunStatus {
  {
    submission_id: self.submission.submission_id,
    job_id: self.compiled.job_id(),
    plan_digest: self.compiled.plan_digest(),
    phase: self.phase,
    accepted_events: self.accepted_events,
    failure: self.failure,
    idempotent_sink_required: self.compiled
    .sink()
    .requires_idempotent_consumer(),
  }
}

///|
pub fn LocalJobHandle::submit(
  self : LocalJobHandle,
  events : ArrayView[Event],
) -> Array[Output] raise ApiError {
  if self.phase != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  let outputs = self.run.submit(events)
  self.accepted_events += events.length().to_int64()
  outputs
}

///|
pub fn LocalJobHandle::drain(self : LocalJobHandle) -> Unit raise ApiError {
  if self.phase != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.phase,
    )
  }
  self.run.close()
  self.phase = JobRunPhase::Succeeded
}

///|
pub fn LocalJobHandle::cancel(self : LocalJobHandle) -> Unit {
  if self.phase == JobRunPhase::Running || self.phase == JobRunPhase::Draining {
    self.run.cancel()
    self.phase = JobRunPhase::Cancelled
  }
}

///|
pub fn LocalJobHandle::await_completion(self : LocalJobHandle) -> JobRunStatus {
  self.status()
}

///|
pub fn LocalJobHandle::close(self : LocalJobHandle) -> Unit raise ApiError {
  match self.phase {
    JobRunPhase::Running => self.drain()
    JobRunPhase::Draining => {
      self.run.close()
      self.phase = JobRunPhase::Succeeded
    }
    JobRunPhase::Succeeded | JobRunPhase::Failed | JobRunPhase::Cancelled => ()
    JobRunPhase::Created =>
      raise ApiError::InvalidJobPhase(
        expected=JobRunPhase::Running,
        actual=JobRunPhase::Created,
      )
  }
}
