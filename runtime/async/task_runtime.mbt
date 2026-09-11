///|
fn output_consumer_checkpoint_path(prefix : String, epoch : Int) -> String {
  prefix + "-e" + epoch.to_string() + ".wal"
}

///|
/// Run a built-in Task through the durable Worker transport. The transport
/// remains responsible for WAL/ACK/frontier metadata; the Task owns operator
/// state and receives both primary and exchange inputs in one causal domain.
pub async fn run_worker_task(
  connection : @socket.Tcp,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  incarnation : String,
  capabilities : Array[String],
  wal_path : String,
  task : @task.Task,
  output_handler : (@task.TaskOutput) -> Unit raise,
  checkpoint_path? : String,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
  initial_credit_events? : Int64 = 128L,
  initial_credit_bytes? : Int64 = 4L * 1024L * 1024L,
  heartbeat_interval_ms? : Int = 0,
  delta_wal_path? : String,
  output_log_path? : String,
  dynamic_assignment? : Bool = false,
  auth_token? : String,
  output_delivery? : async (TaskOutputRecord) -> Unit,
  output_consumer_checkpoint_prefix? : String,
  output_consumer_id? : String,
) -> WorkerRunSummary raise FrameIoError {
  let canonical_wal_path = wal_path
  if !dynamic_assignment {
    task.start() catch {
      error => raise FrameIoError::Task(error)
    }
  }
  let mut storage_owner : @fs.File? = None
  defer (match storage_owner {
    None => ()
    Some(owner) => owner.close()
  })
  let mut output_log : TaskOutputLog? = None
  let mut output_consumer : TaskOutputConsumer? = None
  defer {
    match output_consumer {
      None => ()
      Some(consumer) => consumer.close()
    }
    match output_log {
      None => ()
      Some(log) => log.close()
    }
  }
  let pending_outputs : Array[(Int64, @task.TaskOutput)] = []
  let mut next_output_sequence = task.progress().emitted_outputs().to_int64()
  let emit = (outputs : ArrayView[@task.TaskOutput]) => {
    for output in outputs {
      let sequence = next_output_sequence
      next_output_sequence += 1L
      match output_log {
        None =>
          output_handler(output) catch {
            error => raise FrameIoError::Handler(@debug.to_string(error))
          }
        Some(_) => pending_outputs.push((sequence, output))
      }
    }
  }
  let output_barrier : async () -> Unit raise FrameIoError = () => {
    match output_log {
      None => ()
      Some(log) => {
        if !pending_outputs.is_empty() {
          let first_sequence = pending_outputs[0].0
          let outputs = pending_outputs.map(pair => pair.1)
          log.append(
            task.task_id(),
            task.partition(),
            task.epoch(),
            first_sequence,
            outputs[:],
          ) catch {
            error => raise FrameIoError::OutputLog(error)
          }
          for pair in pending_outputs {
            match output_delivery {
              None =>
                output_handler(pair.1) catch {
                  error => raise FrameIoError::Handler(@debug.to_string(error))
                }
              Some(_) => ()
            }
          }
          pending_outputs.clear()
        }
        match output_delivery {
          None => ()
          Some(handler) => {
            guard output_consumer is Some(consumer) else {
              raise FrameIoError::Handler("output delivery consumer is missing")
            }
            let mut delivered = 1
            while delivered > 0 {
              delivered = consumer.consume(handler, maximum_records=128) catch {
                error => raise FrameIoError::Handler(@debug.to_string(error))
              }
            }
          }
        }
      }
    }
  }
  let output_summary : () -> (String, Int64, String, String) raise FrameIoError = () => {
    match output_log {
      None =>
        raise FrameIoError::Handler("output summary requires an output log")
      Some(log) => {
        let snapshot = task.snapshot() catch {
          error => raise FrameIoError::Task(error)
        }
        (
          task.task_id(),
          log.next_sequence(task.task_id()),
          log.output_digest(task.task_id()),
          task_snapshot_digest(snapshot),
        )
      }
    }
  }
  let primary_handler : (@protocol.EventBatch) -> Unit raise = batch => {
    let outputs = task.process_batch(batch) catch {
      error => raise FrameIoError::Task(error)
    }
    emit(outputs[:])
  }
  let delta_handler : (@protocol.DeltaBatch) -> Unit raise = delta => {
    let outputs = task.process_delta(delta) catch {
      error => raise FrameIoError::Task(error)
    }
    emit(outputs[:])
  }
  let state_restore : (Json, Int64, String) -> Unit raise FrameIoError = (
    state,
    offset,
    frontier_digest,
  ) => {
    let snapshot : @task.TaskSnapshot = @json.from_json(state) catch {
      _ => raise FrameIoError::Task(@task.TaskError::InvalidTransition)
    }
    if snapshot.offset() != offset {
      raise FrameIoError::Handler(
        "operator snapshot offset does not match the checkpoint",
      )
    }
    if snapshot.frontier_digest() != frontier_digest {
      raise FrameIoError::Handler(
        "operator snapshot frontier does not match the checkpoint",
      )
    }
    next_output_sequence = snapshot.emitted_outputs().to_int64()
    task.restore(snapshot) catch {
      error => raise FrameIoError::Task(error)
    }
  }
  let state_snapshot : () -> Json raise FrameIoError = () => {
    let snapshot = task.snapshot() catch {
      error => raise FrameIoError::Task(error)
    }
    @json.to_json(snapshot)
  }
  let result : Result[WorkerRunSummary, FrameIoError] = Ok(
    run_worker_impl(
      connection,
      cluster_id,
      coordinator_id,
      worker_id,
      incarnation,
      capabilities,
      wal_path,
      primary_handler,
      checkpoint_path?,
      maximum_frame_bytes~,
      initial_credit_events~,
      initial_credit_bytes~,
      heartbeat_interval_ms~,
      delta_handler~,
      delta_wal_path?,
      assignment_validator=assignment => {
        if dynamic_assignment {
          task.adopt_assignment(assignment) catch {
            error => raise FrameIoError::Task(error)
          }
        } else {
          task.validate_assignment(assignment) catch {
            error => raise FrameIoError::Task(error)
          }
        }
      },
      assignment_setup?=if dynamic_assignment {
        Some(() => {
          task.start() catch {
            error => raise FrameIoError::Task(error)
          }
        })
      } else {
        None
      },
      state_restore~,
      state_snapshot~,
      output_barrier~,
      requested_partition=task.partition(),
      auth_token?,
      storage_prepare=assignment => {
        storage_owner = Some(
          migration_lock(canonical_wal_path + ".migration") catch {
            error => raise FrameIoError::Migration(error)
          },
        )
        let installed = load_worker_installation(canonical_wal_path) catch {
          error => raise FrameIoError::Migration(error)
        }
        match installed {
          None => ()
          Some(installed) => {
            let identity = installed.identity
            if identity.cluster_id != cluster_id ||
              identity.coordinator_id != coordinator_id ||
              identity.target_worker_id != worker_id ||
              identity.target_incarnation != incarnation ||
              identity.partition != assignment.partition() ||
              identity.target_epoch != assignment.epoch() ||
              identity.start_offset != assignment.next_offset() ||
              installed.task_id != task.task_id() {
              raise FrameIoError::Migration(
                WorkerMigrationError::InvalidIdentity,
              )
            }
          }
        }
        let resolved = resolve_worker_storage_paths(
          canonical_wal_path, checkpoint_path, delta_wal_path, output_log_path,
        ) catch {
          error => {
            task.fail()
            raise FrameIoError::Migration(error)
          }
        }
        match resolved.output_log_path {
          None => ()
          Some(path) =>
            output_log = Some(
              TaskOutputLog::open(path) catch {
                error => {
                  task.fail()
                  raise FrameIoError::OutputLog(error)
                }
              },
            )
        }
        match output_delivery {
          None => ()
          Some(_) => {
            guard output_log is Some(log) else {
              raise FrameIoError::Handler(
                "output delivery requires an output log",
              )
            }
            guard output_consumer_checkpoint_prefix is Some(checkpoint_prefix) else {
              raise FrameIoError::Handler(
                "output delivery requires a consumer checkpoint",
              )
            }
            let checkpoint_path = output_consumer_checkpoint_path(
              checkpoint_prefix,
              assignment.epoch(),
            )
            let consumer_id = output_consumer_id.unwrap_or(
              task.task_id() + "-output",
            )
            output_consumer = Some(
              TaskOutputConsumer::open(
                log,
                checkpoint_path,
                consumer_id,
                task.task_id(),
                assignment.partition(),
                assignment.epoch(),
              ) catch {
                error => raise FrameIoError::Handler(@debug.to_string(error))
              },
            )
          }
        }
        (
          resolved.wal_path,
          if checkpoint_path is Some(_) {
            Some(resolved.checkpoint_path)
          } else {
            None
          },
          resolved.delta_wal_path,
        )
      },
      state_schema=task.operator().state_schema(),
      drain_handler=() => {
        task.drain() catch {
          error => raise FrameIoError::Task(error)
        }
        task.stop() catch {
          error => raise FrameIoError::Task(error)
        }
      },
      output_summary?=match output_log_path {
        None => None
        Some(_) => Some(output_summary)
      },
    ),
  ) catch {
    error => Err(error)
  }
  match result {
    Ok(summary) => {
      if task.phase() == @task.TaskPhase::Stopped {
        return summary
      }
      task.drain() catch {
        error => {
          task.fail()
          raise FrameIoError::Task(error)
        }
      }
      task.stop() catch {
        error => {
          task.fail()
          raise FrameIoError::Task(error)
        }
      }
      summary
    }
    Err(error) => {
      task.fail()
      raise error
    }
  }
}
