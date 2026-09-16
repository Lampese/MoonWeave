///|
fn recovery_sum_event(
  sequence : Int64,
  value : Int64,
  timestamp : Int64,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new("clocked", sequence),
    [],
    "a",
    Json::object({ "value": Json::string(value.to_string()) }),
    event_time=timestamp,
  )
}

///|
async fn recovery_wait_watermark(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  timestamp : Int64,
) -> Unit {
  for ;; {
    let frame = read_frame(connection)
    session.accept_frame(frame)
    if frame.kind() == @protocol.FrameKind::Progress {
      let progress = @protocol.Progress::from_payload(frame.payload())
      if progress.watermark == Some(timestamp) {
        return
      }
    }
  }
}

///|
async test "watermarks replay in order with exchange data and migrate with checkpoints" {
  let root = @fs.tmpdir(prefix="moonweave-event-time-replay")
  defer @fs.rmdir(root, recursive=true)
  let wal_path = root + "/worker.wal"
  let delta_path = root + "/exchange.wal"
  let checkpoint_path = root + "/worker.json"
  let expected : Array[@task.TaskOutput] = [
    @task.TaskOutput::WindowedSum({
      key: "a",
      start_ms: 0L,
      end_ms: 10L,
      total: 5L,
    }),
    @task.TaskOutput::WindowedSum({
      key: "a",
      start_ms: 10L,
      end_ms: 20L,
      total: 7L,
    }),
  ]
  @async_runtime.with_timeout(10000, () => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    let mut first_checkpoint : Checkpoint? = None
    for attempt in 0..<3 {
      if attempt == 1 {
        first_checkpoint.unwrap().save(checkpoint_path)
      }
      if attempt == 2 {
        @fs.remove(checkpoint_path)
      }
      @async_runtime.with_task_group <| group => {
        let coordinator_task = group.spawn(allow_failure=true) <| () => {
          let (connection, _) = server.accept()
          defer connection.close()
          let (session, outbound) = coordinator_handshake(
            connection,
            "timed",
            "coordinator",
            "worker",
            "s" + attempt.to_string(),
            assignment=@protocol.AssignmentSpec::new(0, 4, "boot"),
          )
          if attempt > 0 {
            session.restore_send_cursor(0, 4, 2L)
          }
          for _ in 0..<3 {
            session.accept_frame(read_frame(connection))
          }
          if attempt > 0 {
            recovery_wait_watermark(connection, session, 20L)
          }
          if attempt == 0 {
            let batch = @protocol.EventBatch::new(0, 4, 0L, [
              recovery_sum_event(0L, 2L, 2L),
            ])
            coordinator_send_batch(
              connection,
              session,
              outbound,
              batch,
              @utf8.encode(batch.to_payload()).length().to_int64(),
            )
            for _ in 0..<3 {
              session.accept_frame(read_frame(connection))
            }
            first_checkpoint = Checkpoint::load(checkpoint_path)
            let delta = @protocol.DeltaBatch::new("d1", 1, 0, 0, 4, [
              recovery_sum_event(1L, 3L, 3L),
            ])
            ignore(
              coordinator_send_delta(
                connection,
                session,
                outbound,
                delta,
                @utf8.encode(delta.to_payload()).length().to_int64(),
              ),
            )
            for _ in 0..<2 {
              session.accept_frame(read_frame(connection))
            }
            coordinator_send_watermark(connection, session, outbound, 0, 4, 10L)
            recovery_wait_watermark(connection, session, 10L)
            let late = @protocol.EventBatch::new(0, 4, 1L, [
              recovery_sum_event(2L, 99L, 5L),
            ])
            coordinator_send_batch(
              connection,
              session,
              outbound,
              late,
              @utf8.encode(late.to_payload()).length().to_int64(),
            )
            for _ in 0..<3 {
              session.accept_frame(read_frame(connection))
            }
            let delta = @protocol.DeltaBatch::new("d2", 1, 0, 0, 4, [
              recovery_sum_event(3L, 7L, 15L),
            ])
            ignore(
              coordinator_send_delta(
                connection,
                session,
                outbound,
                delta,
                @utf8.encode(delta.to_payload()).length().to_int64(),
              ),
            )
            for _ in 0..<2 {
              session.accept_frame(read_frame(connection))
            }
            coordinator_send_watermark(connection, session, outbound, 0, 4, 20L)
            recovery_wait_watermark(connection, session, 20L)
          }
          write_frame(
            connection,
            outbound.build(@protocol.FrameKind::Drain, "{}"),
          )
        }
        let task = @task.Task::new(
          "timed-task",
          "keyed-sum",
          @task.OperatorKind::KeyedSum,
          0,
          4,
        ).with_event_time(@task.TaskEventTimeConfig::new(window_size_ms=10L))
        let outputs : Array[@task.TaskOutput] = []
        let result = run_worker_task(
          @socket.Tcp::connect(server.addr),
          "timed",
          "coordinator",
          "worker",
          "boot",
          [],
          wal_path,
          task,
          output => outputs.push(output),
          checkpoint_path~,
          delta_wal_path=delta_path,
        ) catch {
          error => {
            coordinator_task.wait() catch {
              cause =>
                fail(
                  "coordinator attempt \{attempt}: " + @debug.to_string(cause),
                )
            }
            fail("worker attempt \{attempt}: " + @debug.to_string(error))
          }
        }
        coordinator_task.wait()
        assert_eq(outputs, expected)
        assert_eq(task.snapshot().late_events(), 1)
        assert_eq(task.snapshot().watermark_requested(), 20L)
        assert_eq(task.snapshot().emitted_outputs(), 2)
        if attempt > 0 {
          assert_eq(result.replayed_deltas(), 2)
        }
      }
    }
    let identity = WorkerMigrationIdentity::new(
      "timed", "coordinator", "timed-job", 0, 0L, "worker", "boot", 4, "replacement",
      "new-boot", 5,
    )
    let source_paths = WorkerStoragePaths::new(
      wal_path,
      checkpoint_path,
      Some(delta_path),
      None,
    )
    let bundle = WorkerMigration::capture(identity, source_paths)
    let target_paths = WorkerStoragePaths::new(
      root + "/target.wal",
      root + "/target.json",
      Some(root + "/target.delta.wal"),
      None,
    )
    bundle.install(identity, target_paths)
    @async_runtime.with_task_group <| group => {
      let coordinator_task = group.spawn(allow_failure=true) <| () => {
        let (connection, _) = server.accept()
        defer connection.close()
        let (session, outbound) = coordinator_handshake(
          connection,
          "timed",
          "coordinator",
          "replacement",
          "migrated",
          assignment=@protocol.AssignmentSpec::new(0, 5, "new-boot"),
        )
        session.restore_send_cursor(0, 5, 2L)
        for _ in 0..<3 {
          session.accept_frame(read_frame(connection))
        }
        recovery_wait_watermark(connection, session, 20L)
        write_frame(
          connection,
          outbound.build(@protocol.FrameKind::Drain, "{}"),
        )
      }
      let task = @task.Task::new(
        "timed-task",
        "keyed-sum",
        @task.OperatorKind::KeyedSum,
        0,
        5,
      ).with_event_time(@task.TaskEventTimeConfig::new(window_size_ms=10L))
      let outputs : Array[@task.TaskOutput] = []
      let result = run_worker_task(
        @socket.Tcp::connect(server.addr),
        "timed",
        "coordinator",
        "replacement",
        "new-boot",
        [],
        target_paths.wal_path(),
        task,
        output => outputs.push(output),
        checkpoint_path=target_paths.checkpoint_path(),
        delta_wal_path=target_paths.delta_wal_path().unwrap(),
      ) catch {
        error => {
          coordinator_task.wait() catch {
            cause => fail("migration coordinator: " + @debug.to_string(cause))
          }
          fail("migration worker: " + @debug.to_string(error))
        }
      }
      coordinator_task.wait()
      assert_eq(result.replayed_batches(), 0)
      assert_eq(result.replayed_deltas(), 0)
      assert_eq(outputs, [])
      assert_eq(task.snapshot().late_events(), 1)
      assert_eq(task.snapshot().emitted_outputs(), 2)
      assert_eq(task.snapshot().watermark_requested(), 20L)
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
}
