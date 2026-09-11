///|
/// One submitted Job with separate Driver control and Worker data listeners.
pub struct DriverCoordinator {
  data_listener : CoordinatorListener
  control_listener : DriverControlListener
  store : DriverStore
  command_lock : @async_runtime.Mutex
  mut execution : CoordinatorNodeSummary?
  mut live_execution : SourceBackedExecution?
  mut live_task : @async_runtime.Task[Result[CoordinatorNodeSummary, String]]?
  mut running : Bool
  mut closed : Bool
}

///|
pub(all) struct DriverCoordinatorSummary {
  status : JobRunStatus
  execution : CoordinatorNodeSummary?
} derive(Debug, Eq)

///|
pub fn DriverCoordinatorSummary::status(
  self : DriverCoordinatorSummary,
) -> JobRunStatus {
  self.status
}

///|
pub fn DriverCoordinatorSummary::execution(
  self : DriverCoordinatorSummary,
) -> CoordinatorNodeSummary? {
  self.execution
}

///|
pub async fn DriverCoordinator::bind(
  data_config : CoordinatorNodeConfig,
  control_config : DriverControlConfig,
  store : DriverStore,
) -> DriverCoordinator raise ApiError {
  if data_config.cluster_id != control_config.cluster_id ||
    data_config.coordinator_id != control_config.coordinator_id {
    raise ApiError::Cluster(
      "Driver and Worker listeners must share a cluster identity",
    )
  }
  let data_listener = CoordinatorNode::bind(data_config)
  errdefer data_listener.close()
  errdefer store.close()
  let control_listener = DriverControlListener::bind(control_config)
  {
    data_listener,
    control_listener,
    store,
    command_lock: @async_runtime.Mutex::Mutex(),
    execution: None,
    live_execution: None,
    live_task: None,
    running: false,
    closed: false,
  }
}

///|
pub fn DriverCoordinator::data_endpoint(
  self : DriverCoordinator,
) -> NodeEndpoint raise ApiError {
  self.data_listener.endpoint()
}

///|
pub fn DriverCoordinator::control_endpoint(
  self : DriverCoordinator,
) -> NodeEndpoint raise ApiError {
  self.control_listener.endpoint()
}

///|
fn DriverCoordinator::release_stores(self : DriverCoordinator) -> Unit {
  match self.live_execution {
    Some(execution) => execution.close()
    None => ()
  }
  self.store.close()
}

///|
/// Stop transport without recording a terminal Job intent. Stores remain open
/// until the running task group has released all their users.
pub fn DriverCoordinator::close(self : DriverCoordinator) -> Unit {
  self.closed = true
  self.control_listener.close()
  self.data_listener.close()
  match self.live_task {
    Some(task) => task.cancel()
    None => ()
  }
  if !self.running {
    self.release_stores()
  }
}

///|
/// Called with command_lock held, including during restart initialization.
async fn DriverCoordinator::ensure_live_execution(
  self : DriverCoordinator,
) -> SourceBackedExecution raise ApiError {
  match self.live_execution {
    Some(execution) => return execution
    None => ()
  }
  guard self.store.submission() is Some(submission) else {
    raise ApiError::UnknownSubmission("live execution without submission")
  }
  let execution = self.data_listener.open_source_backed_execution(
    self.store.compile_submission(submission),
    self.store.inputs(),
  )
  errdefer execution.close()
  if self.store.phase() == JobRunPhase::Draining {
    execution.drain()
  } else if execution.phase() != JobRunPhase::Running {
    raise ApiError::Cluster("Driver intent conflicts with execution lifecycle")
  }
  self.live_execution = Some(execution)
  execution
}

///|
async fn DriverCoordinator::apply_live_result(
  self : DriverCoordinator,
  result : Result[CoordinatorNodeSummary, String],
) -> Unit raise ApiError {
  match result {
    Ok(summary) => {
      self.execution = Some(summary)
      if summary.failed_partitions() > 0 {
        ignore(
          self.store.fail(
            "Worker execution failed: " + summary.failures().join(", "),
          ),
        )
      } else if self.store.phase() == JobRunPhase::Draining {
        ignore(self.store.succeed())
        // Once every partition has crossed its durable drain barrier, the
        // Driver intent log no longer needs the full input prefix. Compacting
        // here bounds terminal-job history while preserving the submission,
        // phase, and accepted-event count needed by status/restart.
        ignore(self.store.compact())
        match self.live_execution {
          Some(execution) =>
            execution.compact_completed_source() catch {
              error =>
                raise live_execution_error("compact completed source", error)
            }
          None => ()
        }
      } else if self.store.phase() == JobRunPhase::Running {
        ignore(self.store.fail("live execution completed before Driver drain"))
      }
    }
    Err(reason) => ignore(self.store.fail(reason))
  }
}

///|
async fn DriverCoordinator::apply_command(
  self : DriverCoordinator,
  command : JobCommand,
) -> JobRunStatus raise ApiError {
  self.command_lock.acquire() catch {
    error => raise driver_error("lock Driver command", error)
  }
  defer self.command_lock.release()
  if self.closed {
    raise ApiError::DriverClosed
  }
  match command {
    JobCommand::Submit(submission) => {
      let compiled = self.store.compile_submission(submission)
      validate_coordinator_workers(compiled, self.data_listener.config)
      ignore(self.store.submit(submission))
      if self.store.phase() == JobRunPhase::Running ||
        self.store.phase() == JobRunPhase::Draining {
        ignore(self.ensure_live_execution())
      }
    }
    JobCommand::Input(events) => {
      let before = self.store.accepted_events()
      ignore(self.store.input(events))
      let execution = self.ensure_live_execution()
      if execution.event_count().to_int64() != self.store.accepted_events() ||
        before != self.store.accepted_events() {
        errdefer execution.close()
        execution.sync_inputs(events) catch {
          error => {
            let reason = @debug.to_string(error)
            ignore(self.store.fail(reason))
            raise ApiError::Cluster("persist live input: " + reason)
          }
        }
      }
    }
    JobCommand::Drain => {
      ignore(self.store.drain())
      if self.store.phase() == JobRunPhase::Draining {
        self.ensure_live_execution().drain()
      }
    }
    JobCommand::Cancel => {
      ignore(self.store.cancel())
      match self.live_execution {
        Some(execution) => execution.cancel()
        None => ()
      }
    }
    JobCommand::Status => ()
  }
  self.store.status()
}

///|
async fn DriverCoordinator::handle_command(
  self : DriverCoordinator,
  command : JobCommand,
) -> Result[JobRunStatus, String] {
  try {
    let status = self.apply_command(command)
    if command is JobCommand::Drain {
      // Do not hold the command lock while waiting. Another Driver can cancel.
      while self.store.phase() == JobRunPhase::Draining {
        if self.closed {
          raise ApiError::DriverClosed
        }
        @async_runtime.sleep(5)
      }
      Ok(self.store.status())
    } else {
      Ok(status)
    }
  } catch {
    error => Err(@debug.to_string(error))
  }
}

///|
async fn DriverCoordinator::tick(
  self : DriverCoordinator,
  group : @async_runtime.TaskGroup[DriverCoordinatorSummary],
) -> Unit raise ApiError {
  self.command_lock.acquire() catch {
    error => raise driver_error("lock Driver execution", error)
  }
  defer self.command_lock.release()
  if self.closed {
    raise ApiError::DriverClosed
  }
  if self.store.phase() != JobRunPhase::Running &&
    self.store.phase() != JobRunPhase::Draining {
    return
  }
  match self.live_task {
    None => {
      let execution = self.ensure_live_execution()
      self.live_task = Some(
        group.spawn(no_wait=true, allow_failure=true) <| () => {
          Ok(execution.run()) catch {
            error => Err(@debug.to_string(error))
          }
        },
      )
    }
    Some(task) => {
      let result = task.try_wait() catch {
        error => raise driver_error("wait for live execution", error)
      }
      match result {
        None => ()
        Some(value) => self.apply_live_result(value)
      }
    }
  }
}

///|
/// Start delivery after submission, not after drain. A durable drain closes
/// ingress; success is recorded only after every Worker acknowledges its cut.
pub async fn DriverCoordinator::run(
  self : DriverCoordinator,
) -> DriverCoordinatorSummary raise ApiError {
  if self.closed {
    raise ApiError::DriverClosed
  }
  if self.running {
    raise ApiError::Cluster("Driver coordinator is already running")
  }
  self.running = true
  defer {
    self.running = false
    self.closed = true
    self.control_listener.close()
    self.data_listener.close()
    self.release_stores()
  }
  (@async_runtime.with_task_group <| group => {
    group.spawn_bg(no_wait=true) <| () => {
      self.control_listener.run(command => self.handle_command(command))
    }
    for ;; {
      if self.closed {
        raise ApiError::DriverClosed
      }
      self.tick(group) catch {
        error => {
          if self.closed || @async_runtime.is_being_cancelled() {
            raise error
          }
          ignore(self.store.fail(@debug.to_string(error)))
        }
      }
      match self.store.phase() {
        JobRunPhase::Cancelled | JobRunPhase::Failed | JobRunPhase::Succeeded =>
          if self.control_listener.active_requests == 0 {
            break
          }
        _ => ()
      }
      @async_runtime.sleep(5)
    }
    { status: self.store.status(), execution: self.execution, }
  }) catch {
    error => raise driver_error("Driver coordinator", error)
  }
}
