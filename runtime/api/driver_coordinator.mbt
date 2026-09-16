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
  mut pending_authority : ExecutionAuthoritySnapshot?
  mut confirm_authority : (async () -> Unit raise ApiError)?
  mut running : Bool
  mut closed : Bool
  mut rebalancing : Bool
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
    pending_authority: None,
    confirm_authority: None,
    running: false,
    closed: false,
    rebalancing: store.rescale_workers is Some(_),
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
/// Gracefully stop a running Driver coordinator.
///
/// The drain intent is written before this function waits for the live
/// execution.  This method is deliberately cancellation-protected: it is
/// called from the SIGTERM/SIGINT cancellation handler, and an interrupted
/// drain would otherwise leave a running job with an ambiguous shutdown
/// outcome.  Listener/store cleanup remains owned by `run`'s structured
/// lifetime after the durable terminal state is reached.
pub async fn DriverCoordinator::shutdown_gracefully(
  self : DriverCoordinator,
) -> Unit raise ApiError {
  try
    @async_runtime.protect_from_cancel <| () => {
      if self.closed {
        return
      }
      if self.store.phase() == JobRunPhase::Running ||
        self.store.phase() == JobRunPhase::Draining {
        self.command_lock.acquire() catch {
          error => raise driver_error("lock graceful Driver shutdown", error)
        }
        defer self.command_lock.release()
        if !self.closed && self.store.phase() == JobRunPhase::Running {
          ignore(self.store.drain())
        }
        if !self.closed && self.store.phase() == JobRunPhase::Draining {
          self.ensure_live_execution().drain()
        }
      }
      while self.store.phase() == JobRunPhase::Draining {
        match self.live_task {
          Some(task) => {
            let result = task.try_wait() catch {
              error =>
                raise driver_error("wait for graceful Driver shutdown", error)
            }
            match result {
              Some(value) => self.apply_live_result(value)
              None => @async_runtime.sleep(5)
            }
          }
          None => {
            // The normal tick can start the execution while shutdown yields.
            // Own the same lock and recheck before running inline, otherwise
            // two supervisors may claim the same durable JobStore.
            self.command_lock.acquire() catch {
              error => raise driver_error("lock graceful execution", error)
            }
            defer self.command_lock.release()
            if self.live_task is None &&
              self.store.phase() == JobRunPhase::Draining {
              let execution = self.ensure_live_execution()
              let result = Ok(execution.run()) catch {
                error => Err(@debug.to_string(error))
              }
              self.apply_live_result(result)
            }
          }
        }
      }
    }
  catch {
    error => raise driver_error("graceful Driver shutdown", error)
  }
}

///|
/// Rebalance a running execution using the durable Worker migration protocol.
/// The caller holds command_lock. Every moved partition is captured before its
/// lease epoch changes, then installed and committed before ingress resumes.
async fn DriverCoordinator::rescale_workers(
  self : DriverCoordinator,
  workers : ArrayView[String],
  parallelism? : Int,
) -> Unit raise ApiError {
  self.validate_rescale_workers(workers)
  guard self.store.compiled is Some(job) else {
    raise ApiError::UnknownSubmission("rescale before submit")
  }
  let requested = workers.to_owned()
  if self.store.phase() != JobRunPhase::Running {
    raise ApiError::InvalidJobPhase(
      expected=JobRunPhase::Running,
      actual=self.store.phase(),
    )
  }
  let execution = self.ensure_live_execution()
  self.store.begin_rescale(requested, parallelism?)
  if self.confirm_authority is Some(confirm) {
    confirm()
  }
  for id in requested {
    execution.job_store.allow_worker(id) catch {
      error => raise live_execution_error("authorize rescale target", error)
    }
  }
  // A rescale is a stop-the-world control barrier for the source supervisor;
  // it never changes an assignment while an old session can still send data.
  execution.source.pause() catch {
    error => raise live_execution_error("pause before rescale", error)
  }
  self.rebalancing = true
  match self.live_task {
    Some(task) => {
      task.cancel()
      ignore(task.wait() catch { _ => Err("cancelled") })
      self.live_task = None
    }
    None => ()
  }
  let assignments : Map[Int, @control.PartitionAssignment] = Map([])
  for raw in execution.coordinator.snapshot().assignments() {
    let assignment = @control.PartitionAssignment::from_snapshot(raw) catch {
      error => raise live_execution_error("decode rescale assignment", error)
    }
    assignments[assignment.partition()] = assignment
  }
  resume_pending_node_migrations(
    execution.coordinator,
    execution.job_store,
    job,
    self.data_listener.config,
    assignments,
  )
  let ordered = requested.copy()
  ordered.sort()
  for partition in 0..<job.partition_count() {
    guard assignments.get(partition) is Some(current) else {
      raise ApiError::InvalidWorkerAssignment(partition.to_string())
    }
    let count = parallelism.unwrap_or(self.store.parallelism)
    let target_id = ordered[partition % count % ordered.length()]
    if current.worker_id() == target_id {
      continue
    }
    guard self.data_listener.config.workers.search_by(slot => {
        slot.worker_id() == current.worker_id()
      })
      is Some(source_index) &&
      self.data_listener.config.workers.search_by(slot => {
        slot.worker_id() == target_id
      })
      is Some(target_index) else {
      raise ApiError::InvalidWorkerAssignment(target_id)
    }
    let source_slot = self.data_listener.config.workers[source_index]
    let target_slot = self.data_listener.config.workers[target_index]
    let target_epoch = current.epoch() + 1
    if target_epoch < 0 {
      raise ApiError::Cluster("assignment epoch exhausted")
    }
    let config = self.data_listener.config
    let intent : NodeMigrationIntent = {
      ..node_migration_intent(
        config, job, source_slot, target_slot, current, partition,
      ),
      bundle_path: config.storage_dir +
      "/rescale-p" +
      partition.to_string() +
      "-e" +
      target_epoch.to_string() +
      ".json",
      online: Some(true),
    }
    let log = NodeMigrationLog::open(
      config.storage_dir + "/node-migrations.wal",
      maximum_bytes=config.storage_budget_bytes,
    )
    defer log.wal.close()
    execution.active_migrations = Some(log)
    defer {
      execution.active_migrations = None
    }
    log.phase(intent)
    if self.confirm_authority is Some(confirm) {
      confirm()
    }
    resume_online_migration(
      execution.coordinator,
      execution.job_store,
      job,
      config,
      assignments,
      log,
      intent,
      confirm_authority?=self.confirm_authority,
    )
    log.compact()
  }
  execution.source.refresh_work_plans() catch {
    error => raise live_execution_error("refresh rescaled work", error)
  }
  self.store.complete_rescale()
  if !self.store.is_paused() {
    execution.source.resume() catch {
      error => raise live_execution_error("resume after rescale", error)
    }
  }
  self.rebalancing = false
  if self.confirm_authority is Some(confirm) {
    confirm()
  }
}

///|
fn DriverCoordinator::validate_rescale_workers(
  self : DriverCoordinator,
  workers : ArrayView[String],
) -> Unit raise ApiError {
  if workers.is_empty() || workers.any(id => id.is_empty()) {
    raise ApiError::InvalidWorkerAssignment("rescale requires workers")
  }
  let seen : Set[String] = Set([])
  for id in workers {
    if !seen.add_and_check(id) ||
      !self.data_listener.config.workers.any(slot => slot.worker_id() == id) {
      raise ApiError::InvalidWorkerAssignment(id)
    }
  }
  if self.store.phase() != Running {
    raise ApiError::InvalidJobPhase(expected=Running, actual=self.store.phase())
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
    authority?=self.pending_authority,
  )
  errdefer execution.close()
  execution.confirm_authority = self.confirm_authority
  if self.confirm_authority is Some(confirm) {
    execution.source.set_authority_barrier(() => {
      confirm() catch {
        error => raise @runtime.FrameIoError::Handler(@debug.to_string(error))
      }
    })
  }
  if self.store.phase() == JobRunPhase::Draining {
    execution.drain()
  } else if execution.phase() != JobRunPhase::Running {
    raise ApiError::Cluster("Driver intent conflicts with execution lifecycle")
  }
  if self.store.is_paused() && self.store.phase() == JobRunPhase::Running {
    execution.source.pause() catch {
      error => raise live_execution_error("restore paused source", error)
    }
  }
  self.live_execution = Some(execution)
  self.pending_authority = None
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
      @crash_boundary.hit("driver.input.persisted", self.store.path) catch {
        error => raise driver_error("Driver input boundary", error)
      }
      let execution = self.ensure_live_execution()
      // Only newly accepted Driver events need to cross into SourceLog.
      // A retry after a quorum commit can arrive after the execution has
      // compacted its sealed source; comparing source length with the Driver
      // high-water mark would incorrectly re-ingest an already durable event
      // and turn an idempotent retry into "live source is sealed".
      if before != self.store.accepted_events() {
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
    JobCommand::Pause => {
      ignore(self.store.pause())
      if self.live_task is Some(task) {
        task.cancel()
        ignore(task.wait() catch { _ => Err("paused") })
        self.live_task = None
      }
      match self.live_execution {
        Some(execution) =>
          execution.source.pause() catch {
            error => raise live_execution_error("pause live source", error)
          }
        None => ()
      }
    }
    JobCommand::Resume => {
      if self.rebalancing || self.store.rescale_workers is Some(_) {
        raise ApiError::InvalidDriverCommand(
          "retry the interrupted rescale before resuming",
        )
      }
      ignore(self.store.resume())
      match self.live_execution {
        Some(execution) =>
          execution.source.resume() catch {
            error => raise live_execution_error("resume live source", error)
          }
        None => ()
      }
    }
    JobCommand::Rescale(request) =>
      self.rescale_workers(request.workers[:], parallelism?=request.parallelism)
    JobCommand::Update(submission) => {
      if self.store.compiled is Some(job) && job.is_graph() {
        raise ApiError::InvalidDriverCommand(
          "graph stage updates require a graph-wide barrier",
        )
      }
      // Validate before touching the live source. A rejected update must
      // leave the paused execution available for resume.
      ignore(self.store.validate_update(submission))
      match self.live_task {
        Some(task) => {
          task.cancel()
          ignore(task.wait() catch { _ => Err("cancelled") })
          self.live_task = None
        }
        None => ()
      }
      match self.live_execution {
        Some(execution) => {
          execution.close()
          self.live_execution = None
        }
        None => ()
      }
      ignore(self.store.update(submission))
      ignore(self.ensure_live_execution())
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
async fn[T] DriverCoordinator::tick(
  self : DriverCoordinator,
  group : @async_runtime.TaskGroup[T],
) -> Unit raise ApiError {
  if self.store.compiled is Some(job) && job.is_graph() {
    return
  }
  // A backpressured input may hold this Job's command lock. Other Jobs in
  // the service must still be able to start and observe their own progress.
  if !self.command_lock.try_acquire() {
    return
  }
  defer self.command_lock.release()
  if self.closed {
    raise ApiError::DriverClosed
  }
  if self.store.rescale_workers is Some(workers) &&
    self.store.phase() == Running {
    self.rescale_workers(
      workers[:],
      parallelism?=self.store.rescale_parallelism,
    ) catch {
      ApiError::WorkerMigrationUnavailable(_) => return
      error => raise error
    }
  }
  if self.rebalancing {
    return
  }
  if self.store.is_paused() {
    return
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
        None =>
          match self.live_execution {
            Some(execution) => {
              let retired = execution.source.retired_inputs()
              if retired != self.store.retired {
                self.store.compact(retired~)
              }
            }
            None => ()
          }
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
  keep_terminal? : Bool = false,
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
        if keep_terminal {
          break
        } else {
          raise ApiError::DriverClosed
        }
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
          if !keep_terminal && self.control_listener.active_requests == 0 {
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
