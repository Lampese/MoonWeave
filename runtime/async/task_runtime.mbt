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
) -> WorkerRunSummary raise FrameIoError {
  task.start() catch {
    error => raise FrameIoError::Task(error)
  }
  let output_log = match output_log_path {
    None => None
    Some(path) =>
      Some(
        TaskOutputLog::open(path) catch {
          error => {
            task.fail()
            raise FrameIoError::OutputLog(error)
          }
        },
      )
  }
  defer (match output_log {
    None => ()
    Some(log) => log.close()
  })
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
      Some(log) =>
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
            output_handler(pair.1) catch {
              error => raise FrameIoError::Handler(@debug.to_string(error))
            }
          }
          pending_outputs.clear()
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
  let state_restore : (Json, Int64) -> Unit raise FrameIoError = (state, offset) => {
    let snapshot : @task.TaskSnapshot = @json.from_json(state) catch {
      _ => raise FrameIoError::Task(@task.TaskError::InvalidTransition)
    }
    if snapshot.offset() != offset {
      raise FrameIoError::Handler(
        "operator snapshot offset does not match the checkpoint",
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
        task.validate_assignment(assignment) catch {
          error => raise FrameIoError::Task(error)
        }
      },
      state_restore~,
      state_snapshot~,
      output_barrier~,
    ),
  ) catch {
    error => Err(error)
  }
  match result {
    Ok(summary) => {
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
