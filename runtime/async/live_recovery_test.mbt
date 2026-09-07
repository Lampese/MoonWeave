///|
async fn with_live_recovery(
  f : async (String, SourceBackedJob, JobStore) -> Unit,
) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-live-recovery")
  defer @fs.rmdir(directory, recursive=true)
  let spec = @plan.JobSpec::new(
    "live-recovery",
    "reachability",
    1,
    ["worker"],
    [],
  )
  let coordinator = CoordinatorStore::open(
    "live-recovery",
    "coordinator",
    directory + "/control.wal",
  )
  defer coordinator.close()
  let job = JobStore::open(spec, directory + "/job.wal")
  defer job.close()
  ignore(
    coordinator.register_worker("worker", @protocol.Incarnation::new("boot"), [
      @task.OperatorKind::Reachability.capability(),
    ]),
  )
  ignore(coordinator.assign(0, "worker"))
  ignore(job.assign(0, "worker", 0, 0L))
  let live = SourceBackedJob::open(
    directory + "/source.wal",
    spec,
    coordinator,
    job,
    required_capability=@task.OperatorKind::Reachability.capability(),
  )
  defer live.close()
  job.start()
  @async_runtime.with_timeout(5000, () => f(directory, live, job))
}

///|
async test "live reconnect resumes the same lease from its Worker checkpoint" {
  with_live_recovery <| ((directory, live, job) => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let first = live_edge_event(0, [], "A", "B")
    let second = live_edge_event(1, [first.id()], "B", "C")
    ignore(live.ingest(first))
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn(allow_failure=true) <| () => {
        Ok(
          run_live_source_supervisor(
            server,
            live,
            poll_interval_ms=1,
            reconnect_attempts=1,
          ),
        ) catch {
          error => Err(error)
        }
      }
      let first_connection = @socket.Tcp::connect(server.addr)
      let first_worker = group.spawn(allow_failure=true) <| () => {
        let task = @task.Task::new(
          "task",
          "reachability",
          @task.OperatorKind::Reachability,
          0,
          0,
        )
        Ok(
          run_worker_task(
            first_connection,
            "live-recovery",
            "coordinator",
            "worker",
            "boot",
            [@task.OperatorKind::Reachability.capability()],
            directory + "/worker.wal",
            task,
            _ => (),
            checkpoint_path=directory + "/worker.json",
          ),
        ) catch {
          error => Err(error)
        }
      }
      for ;; {
        if Checkpoint::load(directory + "/worker.json") is Some(saved) &&
          saved.applied_offset() == 0L {
          break
        }
        @async_runtime.sleep(1)
      }
      first_worker.cancel()
      first_connection.close()
      let stopped : Result[WorkerRunSummary, FrameIoError] = first_worker.wait() catch {
        error => Err(FrameIoError::Io(@debug.to_string(error)))
      }
      assert_true(stopped is Err(_))
      assert_eq(job.phase(), @control.JobPhase::Running)
      ignore(live.ingest(second))
      live.seal()
      let outputs : Array[@task.TaskOutput] = []
      let task = @task.Task::new(
        "task",
        "reachability",
        @task.OperatorKind::Reachability,
        0,
        0,
      )
      let recovered : Result[WorkerRunSummary, Error] = Ok(
        run_worker_task(
          @socket.Tcp::connect(server.addr),
          "live-recovery",
          "coordinator",
          "worker",
          "boot",
          [@task.OperatorKind::Reachability.capability()],
          directory + "/worker.wal",
          task,
          output => outputs.push(output),
          checkpoint_path=directory + "/worker.json",
        ),
      ) catch {
        error => Err(error)
      }
      let supervisor_result = supervisor.wait()
      guard supervisor_result is Ok(result) else {
        fail(
          "supervisor: " +
          @debug.to_string(supervisor_result) +
          "; worker: " +
          @debug.to_string(recovered),
        )
      }
      guard recovered is Ok(summary) else {
        fail(
          "Worker: " +
          @debug.to_string(recovered) +
          "; supervisor: " +
          @debug.to_string(result),
        )
      }
      assert_eq(summary.replayed_batches(), 0)
      assert_eq(summary.live_batches(), 1)
      assert_eq(outputs.length(), 2)
      assert_eq(result.failed_partitions(), 0)
      assert_eq(job.phase(), @control.JobPhase::Succeeded)
    }
  })
}

///|
async test "live protocol failures never wait for a reconnect" {
  with_live_recovery <| ((_, live, _) => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_live_source_supervisor(
          server,
          live,
          poll_interval_ms=1,
          reconnect_timeout_ms=4000,
        )
      }
      let connection = @socket.Tcp::connect(server.addr)
      defer connection.close()
      let (_, outbound) = worker_handshake(
        connection,
        "live-recovery",
        "coordinator",
        "worker",
        "boot",
        [@task.OperatorKind::Reachability.capability()],
      )
      let frame = outbound.build(
        @protocol.FrameKind::Credit,
        @protocol.Credit::new(0, 0, 1L, 1024L).to_payload(),
      )
      write_frame(connection, frame)
      let result = @async_runtime.with_timeout(1000, () => supervisor.wait())
      assert_eq(result.failed_partitions(), 1)
      assert_eq(result.failures()[0].worker_id(), "worker")
      assert_eq(result.failures()[0].partition(), Some(0))
      assert_eq(result.failures()[0].attempts(), 1)
    }
  })
}

///|
async test "live reconnect expires without another Worker" {
  with_live_recovery <| ((_, live, _) => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    @async_runtime.with_task_group <| group => {
      let supervisor = group.spawn() <| () => {
        run_live_source_supervisor(
          server,
          live,
          poll_interval_ms=1,
          reconnect_attempts=1,
          reconnect_timeout_ms=50,
        )
      }
      let connection = @socket.Tcp::connect(server.addr)
      ignore(
        worker_handshake(
          connection,
          "live-recovery",
          "coordinator",
          "worker",
          "boot",
          [@task.OperatorKind::Reachability.capability()],
        ),
      )
      connection.close()
      let result = supervisor.wait()
      assert_eq(result.failed_partitions(), 1)
      assert_true(result.recoverable_failures())
      assert_eq(result.failures()[0].attempts(), 2)
      assert_true(result.failures()[0].reason().contains("reconnect deadline"))
    }
  })
}
