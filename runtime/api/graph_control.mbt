///|
/// Parent pause is the durable barrier. Allocation queries withdraw every
/// stage, including branches whose individual pause has not reached disk yet.
async fn GraphExecution::pause_stages(
  self : GraphExecution,
) -> Unit raise ApiError {
  for (_, stage) in self.stages {
    if stage.store.phase() == Running {
      ignore(stage.apply_command(Pause))
      if stage.live_task is Some(task) {
        task.cancel()
        ignore(task.wait() catch { _ => Err("graph barrier") })
        stage.live_task = None
      }
    }
  }
}

///|
async fn GraphExecution::resume_stages(
  self : GraphExecution,
) -> Unit raise ApiError {
  let parent = self.parent
  if parent.store.rescale_workers is Some(_) ||
    parent.store.graph_update is Some(_) {
    raise ApiError::InvalidDriverCommand(
      "finish the interrupted graph operation before resuming",
    )
  }
  for (_, stage) in self.stages {
    if stage.store.phase() == Running {
      ignore(stage.apply_command(Resume))
    }
  }
  // A crash can interrupt fan-out after parent input is durable. Keep the
  // entire graph withdrawn until every retained input is present at its root.
  for input in parent.store.inputs() {
    let root = self.job.graph_root(input.input_id())
    ignore(
      self.stages[root.id].apply_command(Input([input.bind_input("main")])),
    )
  }
  ignore(parent.store.resume())
}

///|
async fn GraphExecution::rescale(
  self : GraphExecution,
  request : RescaleRequest,
) -> Unit raise ApiError {
  if self.parent.store.graph_update is Some(_) {
    raise ApiError::InvalidDriverCommand(
      "finish the interrupted graph update before rescaling",
    )
  }
  // Validate every stage before writing the parent intent. Individual stage
  // migration intents then form restartable steps inside this barrier.
  for stage in self.stages.values() {
    stage.validate_rescale_workers(request.workers)
    if request.parallelism is Some(value) &&
      (value <= 0 || value > stage.store.compiled.unwrap().partition_count()) {
      raise ApiError::InvalidPartitionCount(value)
    }
  }
  self.parent.store.begin_rescale(
    request.workers,
    parallelism?=request.parallelism,
  )
  if self.parent.confirm_authority is Some(confirm) {
    confirm()
  }
  self.pause_stages()
  for (id, _) in self.job.graph_stages {
    ignore(self.stages[id].apply_command(Rescale(request)))
  }
  if self.parent.store.rescale_resume {
    for stage in self.stages.values() {
      ignore(stage.apply_command(Resume))
    }
  }
  self.parent.store.complete_rescale()
  if self.parent.confirm_authority is Some(confirm) {
    confirm()
  }
}

///|
async fn GraphExecution::update(
  self : GraphExecution,
  candidate : JobSubmission,
) -> Unit raise ApiError {
  if self.parent.store.rescale_workers is Some(_) {
    raise ApiError::InvalidDriverCommand(
      "finish the interrupted graph rescale before updating",
    )
  }
  let next = self.parent.store.validate_update(candidate)
  self.parent.store.begin_graph_update(candidate)
  if self.parent.confirm_authority is Some(confirm) {
    confirm()
  }
  self.pause_stages()
  for (id, job) in next.graph_stages {
    let stage = self.stages[id]
    let submission = job.submission_with_id(job.job_id())
    if stage.store.submission() != Some(submission) {
      ignore(stage.apply_command(Update(submission)))
    }
    self.submissions[id] = submission
  }
  ignore(self.parent.store.update(candidate))
  self.job = next
  self.parent.store.complete_graph_update()
  if self.parent.confirm_authority is Some(confirm) {
    confirm()
  }
}
