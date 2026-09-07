///|
/// A Coordinator process with a separately bound Driver control listener. The
/// data listener is opened before submission so Workers can be started by a
/// supervisor without racing the first Driver command.
pub struct DriverCoordinator {
  data_listener : CoordinatorListener
  control_listener : DriverControlListener
  store : DriverStore
  execution_lock : @async_runtime.Mutex
  mut execution : CoordinatorNodeSummary?
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
  let data_listener = CoordinatorNode::bind(data_config)
  errdefer data_listener.close()
  errdefer store.close()
  let control_listener = DriverControlListener::bind(control_config)
  {
    data_listener,
    control_listener,
    store,
    execution_lock: @async_runtime.Mutex::Mutex(),
    execution: None,
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
pub fn DriverCoordinator::close(self : DriverCoordinator) -> Unit {
  self.control_listener.close()
  self.data_listener.close()
  self.store.close()
}

///|
async fn DriverCoordinator::execute_draining(
  self : DriverCoordinator,
) -> JobRunStatus raise ApiError {
  self.execution_lock.acquire() catch {
    error =>
      raise ApiError::Cluster(
        "lock Driver execution: " + @debug.to_string(error),
      )
  }
  defer self.execution_lock.release()
  if self.store.phase() != JobRunPhase::Draining {
    return self.store.status()
  }
  match self.execution {
    Some(_) => return self.store.status()
    None => ()
  }
  let submission = match self.store.submission() {
    Some(value) => value
    None => raise ApiError::UnknownSubmission("drain without submission")
  }
  let execution : Result[CoordinatorNodeSummary, String] = Ok(
    self.data_listener.run(submission.compile(), self.store.inputs()),
  ) catch {
    error => Err(@debug.to_string(error))
  }
  match execution {
    Ok(summary) => {
      self.execution = Some(summary)
      if summary.failed_partitions() == 0 {
        self.store.succeed()
      } else {
        self.store.fail(
          "Worker execution failed: " + summary.failures().join(", "),
        )
      }
    }
    Err(reason) => self.store.fail(reason)
  }
}

///|
async fn DriverCoordinator::handle_command(
  self : DriverCoordinator,
  command : JobCommand,
) -> Result[JobRunStatus, String] {
  try {
    match command {
      JobCommand::Drain => {
        let status = self.store.drain()
        if status.phase() == JobRunPhase::Draining {
          Ok(self.execute_draining())
        } else {
          Ok(status)
        }
      }
      _ => self.store.handle(command)
    }
  } catch {
    error => Err(@debug.to_string(error))
  }
}

///|
/// Run one submitted Driver job. Inputs are read only after the Driver has
/// durably entered `Draining`; the execution plan therefore cannot miss an
/// event acknowledged to the client.
pub async fn DriverCoordinator::run(
  self : DriverCoordinator,
) -> DriverCoordinatorSummary raise ApiError {
  let control = self.control_listener
  let data = self.data_listener
  let store = self.store
  defer control.close()
  defer data.close()
  defer store.close()
  try
    @async_runtime.with_task_group <| group => {
      group.spawn_bg(no_wait=true) <| () => {
        control.run(command => self.handle_command(command)) catch {
          _ => ()
        }
      }
      for ;; {
        match store.phase() {
          JobRunPhase::Created | JobRunPhase::Running =>
            @async_runtime.sleep(20)
          JobRunPhase::Draining => ignore(self.execute_draining())
          JobRunPhase::Cancelled
          | JobRunPhase::Failed
          | JobRunPhase::Succeeded => {
            control.close()
            data.close()
            break
          }
        }
      }
      { status: store.status(), execution: self.execution, }
    }
  catch {
    error =>
      raise ApiError::Cluster("Driver coordinator: " + @debug.to_string(error))
  }
}
