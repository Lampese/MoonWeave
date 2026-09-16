///|
struct GraphExecution {
  mut job : CompiledJob
  parent : DriverCoordinator
  stages : Map[String, DriverCoordinator]
  submissions : Map[String, JobSubmission]
  mut draining : @async_runtime.Task[Result[Unit, String]]?
}

///|
async fn GraphExecution::open(
  job : CompiledJob,
  parent : DriverCoordinator,
  registry : OperatorRegistry,
  maximum_events : Int,
  authority? : Array[NetworkStageSnapshot],
) -> GraphExecution raise ApiError {
  let stages : Map[String, DriverCoordinator] = Map([])
  errdefer (for stage in stages.values() {
    stage.close()
  })
  let submissions : Map[String, JobSubmission] = Map([])
  for (id, stage_job) in job.graph_stages {
    let data = parent.data_listener.config
    let config = {
      ..data,
      bind: NodeEndpoint::new("127.0.0.1", 0),
      advertised: None,
      storage_dir: data.storage_dir + "/stages/" + id,
      // Preserve registered spare slots for stage workers as well. A graph
      // stage may be expanded after submission; its initial worker list still
      // validates the fixed plan, while the Coordinator owns the live lease.
      workers: data.workers.map(slot => {
        ..slot,
        storage_dir: slot.storage_dir.map(path => path + "/stages/" + id),
      }),
    }
    ensure_live_storage(config.storage_dir)
    let store = DriverStore::open(
      config.storage_dir + "/driver.wal",
      registry~,
      maximum_events~,
      maximum_bytes=config.storage_budget_bytes,
    )
    let submission = stage_job.submission_with_id(stage_job.job_id())
    let saved = authority.bind(items => {
      items.iter().find_first(item => item.id == id)
    })
    match saved {
      Some(item) => store.replace_authority_snapshot(item.driver)
      None =>
        if store.submission is None {
          ignore(store.submit(submission))
        } else {
          stage_job.validate_compatible_update(store.compiled.unwrap())
        }
    }
    let child = DriverCoordinator::bind(
      config,
      {
        ..parent.control_listener.config,
        bind: NodeEndpoint::new("127.0.0.1", 0),
        advertised: None,
      },
      store,
    )
    child.pending_authority = saved.bind(item => item.execution)
    child.confirm_authority = parent.confirm_authority
    stages[id] = child
    submissions[id] = store.submission().unwrap()
    if authority is None &&
      !parent.store.is_paused() &&
      (store.phase() == Running || store.phase() == Draining) {
      ignore(child.ensure_live_execution())
    }
  }
  let execution = { job, parent, stages, submissions, draining: None, }
  // Replay any input persisted by the parent before a crash in fan-out.
  if authority is None &&
    !parent.store.is_paused() &&
    (parent.store.phase() == Running || parent.store.phase() == Draining) {
    for input in parent.store.inputs() {
      let root = job.graph_root(input.input_id())
      let stage = stages[root.id]
      if stage.store.phase() == Running {
        ignore(stage.apply_command(Input([input.bind_input("main")])))
      }
    }
  }
  execution
}

///|
fn GraphExecution::close(self : GraphExecution) -> Unit {
  for stage in self.stages.values() {
    stage.close()
  }
}

///|
fn GraphExecution::worker_child(
  self : GraphExecution,
  job_id : String,
) -> DriverCoordinator? {
  for (id, job) in self.job.graph_stages {
    if job.job_id() == job_id {
      return self.stages.get(id)
    }
  }
  None
}

///|
fn GraphExecution::submitted_child(
  self : GraphExecution,
  submission : JobSubmission,
) -> DriverCoordinator? raise ApiError {
  for (id, expected) in self.submissions {
    if submission.submission_id() == expected.submission_id() {
      if submission != expected {
        raise ApiError::RequestConflict(submission.submission_id())
      }
      return self.stages.get(id)
    }
  }
  None
}

///|
async fn GraphExecution::apply_command(
  self : GraphExecution,
  command : JobCommand,
) -> JobRunStatus raise ApiError {
  let parent = self.parent
  parent.command_lock.acquire() catch {
    error => raise driver_error("lock graph command", error)
  }
  defer parent.command_lock.release()
  match command {
    Submit(submission) => ignore(parent.store.submit(submission))
    Input(events) => {
      ignore(parent.store.input(events))
      for event in events {
        let root = self.job.graph_root(event.input_id())
        ignore(
          self.stages[root.id].apply_command(Input([event.bind_input("main")])),
        )
      }
    }
    Status => ()
    Pause => {
      ignore(parent.store.pause())
      self.pause_stages()
    }
    Resume => self.resume_stages()
    Rescale(request) => self.rescale(request)
    Update(submission) => self.update(submission)
    Drain => {
      if parent.store.is_paused() {
        self.resume_stages()
      }
      ignore(parent.store.drain())
    }
    Cancel => {
      ignore(parent.store.cancel())
      for stage in self.stages.values() {
        if stage.store.phase() == Running || stage.store.phase() == Draining {
          ignore(stage.apply_command(Cancel))
        }
      }
    }
  }
  parent.store.status()
}

///|
async fn GraphExecution::handle_command(
  self : GraphExecution,
  command : JobCommand,
) -> Result[JobRunStatus, String] {
  try {
    let status = self.apply_command(command)
    if command is Drain {
      while self.parent.store.phase() == Draining {
        if self.parent.closed {
          raise ApiError::DriverClosed
        }
        @async_runtime.sleep(5)
      }
      Ok(self.parent.store.status())
    } else {
      Ok(status)
    }
  } catch {
    error => Err(@debug.to_string(error))
  }
}

///|
async fn[T] GraphExecution::tick(
  self : GraphExecution,
  group : @async_runtime.TaskGroup[T],
) -> Unit raise ApiError {
  if !self.parent.command_lock.try_acquire() {
    return
  }
  defer self.parent.command_lock.release()
  if self.parent.store.rescale_workers is Some(workers) {
    self.rescale({
      workers,
      parallelism: self.parent.store.rescale_parallelism,
    }) catch {
      ApiError::WorkerMigrationUnavailable(_) => return
      error => raise error
    }
  }
  if self.parent.store.graph_update is Some(candidate) {
    self.update(candidate)
  }
  if self.parent.store.is_paused() {
    return
  }
  for (_, child) in self.stages {
    child.tick(group)
  }
  for edge in self.job.definition.graph.unwrap().edges {
    let upstream = self.stages[edge.from]
    let downstream = self.stages[edge.to]
    if upstream.live_execution is Some(source) &&
      downstream.live_execution is Some(target) {
      target.source.advance_graph_watermark(
        edge.identity(),
        source.source.confirmed_watermark(),
      ) catch {
        error => raise driver_error("advance graph watermark", error)
      }
    }
  }
  if self.parent.store.phase() == Running {
    for stage in self.stages.values() {
      if stage.store.phase() == Failed {
        ignore(
          self.parent.store.fail(
            stage.store.status().failure().unwrap_or("graph stage failed"),
          ),
        )
      }
    }
    // Parent input payloads can retire once the selected root's own globally
    // checkpointed source proves them safe. Counts and identities survive.
    let ranges = self.parent.store.retired.copy()
    for stage in self.stages.values() {
      ranges.push_iter(stage.store.retired.iter())
    }
    let merged = @causal.CausalRetirement::new("graph-parent", ranges) catch {
      error => raise driver_error("graph retirement", error)
    }
    if merged.ranges() != self.parent.store.retired {
      self.parent.store.compact(retired=merged.ranges().to_owned())
    }
  }
  if self.parent.store.phase() == Draining && self.draining is None {
    self.draining = Some(
      group.spawn(no_wait=true, allow_failure=true) <| () => {
        try {
          // Closing an upstream stage first flushes its event-time outputs and
          // waits for every downstream durable ACK before sealing the next one.
          for (id, _) in self.job.graph_stages {
            let result = self.stages[id].handle_command(Drain)
            match result {
              Ok(status) if status.phase() == Succeeded => ()
              Ok(status) =>
                raise ApiError::Cluster(
                  id + ": " + status.failure().unwrap_or("stage drain failed"),
                )
              Err(reason) => raise ApiError::Cluster(id + ": " + reason)
            }
          }
          Ok(())
        } catch {
          error => Err(@debug.to_string(error))
        }
      },
    )
  }
  if self.draining is Some(task) && self.parent.store.phase() == Draining {
    let completed = task.try_wait() catch {
      error => raise driver_error("wait for graph drain", error)
    }
    match completed {
      Some(Ok(_)) => {
        ignore(self.parent.store.succeed())
        self.parent.store.compact()
      }
      Some(Err(reason)) => ignore(self.parent.store.fail(reason))
      None => ()
    }
  }
  if self.parent.store.phase() == Failed ||
    self.parent.store.phase() == Cancelled {
    for child in self.stages.values() {
      if child.store.phase() == Running || child.store.phase() == Draining {
        ignore(child.apply_command(Cancel))
      }
    }
  }
}
