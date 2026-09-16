///|
async test "running Worker reclaims input and sink history repeatedly and recovers from its journal checkpoint" {
  let root = @fs.tmpdir(prefix="moonweave-worker-reclaim")
  defer @fs.rmdir(root, recursive=true)
  let wal_path = root + "/worker.wal"
  let delta_path = root + "/delta.wal"
  let checkpoint_path = root + "/worker.json"
  let output_path = root + "/output.wal"
  let delivered : Map[Int64, @task.TaskOutput] = Map([])
  let counts = [1600, 5]
  @async_runtime.with_timeout(60000, () => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer server.close()
    for attempt in 0..<2 {
      if attempt == 1 {
        // A crash after journal replacement can leave both an obsolete
        // sidecar and the old Delta file. Neither may be replayed as new work.
        @fs.write_file(checkpoint_path, b"obsolete sidecar")
        let (old_delta, _) = Wal::open(delta_path)
        ignore(old_delta.append(b"obsolete delta payload"))
        old_delta.close()
      }
      let offset_base = if attempt == 0 { 0 } else { counts[0] }
      @async_runtime.with_task_group <| group => {
        let coordinator = group.spawn(allow_failure=true) <| () => {
          let (connection, _) = server.accept()
          defer connection.close()
          let (session, outbound) = coordinator_handshake(
            connection,
            "reclaim",
            "coordinator",
            "worker",
            "s" + attempt.to_string(),
            assignment=@protocol.AssignmentSpec::new(0, 0, "boot"),
          )
          session.restore_send_cursor(0, 0, offset_base.to_int64())
          for _ in 0..<3 {
            session.accept_frame(read_frame(connection))
          }
          for index in 0..<counts[attempt] {
            let offset = (offset_base + index).to_int64()
            let batch = @protocol.EventBatch::new(0, 0, offset, [
              recovery_sum_event(offset * 2L, 1L, offset),
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
            let delta = @protocol.DeltaBatch::new(
              "d" + offset.to_string(),
              1,
              0,
              0,
              0,
              [recovery_sum_event(offset * 2L + 1L, 1L, offset)],
            )
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
          }
          write_frame(
            connection,
            outbound.build(@protocol.FrameKind::Drain, "{}"),
          )
        }
        let task = @task.Task::new(
          "sum",
          "keyed-sum",
          @task.OperatorKind::KeyedSum,
          0,
          0,
          max_state_events=128,
        )
        let worker = run_worker_task(
          @socket.Tcp::connect(server.addr),
          "reclaim",
          "coordinator",
          "worker",
          "boot",
          [],
          wal_path,
          task,
          _ => (),
          checkpoint_path~,
          delta_wal_path=delta_path,
          output_log_path=output_path,
          output_consumer_checkpoint_prefix=root + "/consumer",
          storage_budget_bytes=65536L,
          output_delivery=record => {
            let sequence = record.sequence()
            if delivered.get(sequence) is Some(previous) {
              assert_eq(previous, record.output())
            }
            delivered[sequence] = record.output()
            guard record.output() is @task.TaskOutput::KeyedSum(sum) else {
              fail("expected sum")
            }
            assert_eq(sum.total(), sequence + 1L)
          },
        ) catch {
          error => {
            coordinator.wait() catch {
              cause => fail("coordinator: " + @debug.to_string(cause))
            }
            fail("worker: " + @debug.to_string(error))
          }
        }
        coordinator.wait()
        assert_eq(
          worker.applied_offset(),
          (offset_base + counts[attempt] - 1).to_int64(),
        )
        assert_eq(
          task.snapshot().emitted_outputs(),
          (offset_base + counts[attempt]) * 2,
        )
        assert_true(task.state_memory_count() < 64)
      }
      let (journal, replay) = Wal::open(wal_path)
      assert_true(replay.records().length() < 18)
      assert_true(
        @json.parse(@utf8.decode(replay.records()[0]))
        is { "kind": "checkpoint", .. },
      )
      assert_true(journal.stats().physical_bytes() < 16384L)
      journal.close()
      let outputs = TaskOutputLog::open(output_path)
      assert_true(outputs.records().length() < 18)
      assert_eq(
        outputs.next_sequence("sum"),
        (offset_base + counts[attempt]).to_int64() * 2L,
      )
      outputs.close()
    }
    let identity = WorkerMigrationIdentity::new(
      "reclaim", "coordinator", "job", 0, 0L, "worker", "boot", 0, "replacement",
      "new-boot", 1,
    )
    let bundle = WorkerMigration::capture(
      identity,
      WorkerStoragePaths::new(
        wal_path,
        checkpoint_path,
        Some(delta_path),
        Some(output_path),
      ),
    )
    let target = WorkerStoragePaths::new(
      root + "/target.wal",
      root + "/target.json",
      Some(root + "/target.delta"),
      Some(root + "/target.output"),
    )
    bundle.install(identity, target)
    @async_runtime.with_task_group <| group => {
      let coordinator = group.spawn(allow_failure=true) <| () => {
        let (connection, _) = server.accept()
        defer connection.close()
        let (session, outbound) = coordinator_handshake(
          connection,
          "reclaim",
          "coordinator",
          "replacement",
          "migrated",
          assignment=@protocol.AssignmentSpec::new(0, 1, "new-boot"),
        )
        session.restore_send_cursor(0, 1, 1605L)
        for _ in 0..<3 {
          session.accept_frame(read_frame(connection))
        }
        let batch = @protocol.EventBatch::new(0, 1, 1605L, [
          recovery_sum_event(3210L, 1L, 1605L),
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
        write_frame(
          connection,
          outbound.build(@protocol.FrameKind::Drain, "{}"),
        )
      }
      let task = @task.Task::new(
        "sum",
        "keyed-sum",
        @task.OperatorKind::KeyedSum,
        0,
        1,
        max_state_events=128,
      )
      ignore(
        run_worker_task(
          @socket.Tcp::connect(server.addr),
          "reclaim",
          "coordinator",
          "replacement",
          "new-boot",
          [],
          target.wal_path(),
          task,
          _ => (),
          checkpoint_path=target.checkpoint_path(),
          delta_wal_path=target.delta_wal_path().unwrap(),
          output_log_path=target.output_log_path().unwrap(),
          output_consumer_checkpoint_prefix=root + "/new-consumer",
          output_delivery=record => {
            if delivered.get(record.sequence()) is Some(previous) {
              assert_eq(previous, record.output())
            }
            delivered[record.sequence()] = record.output()
            guard record.output() is @task.TaskOutput::KeyedSum(value) else {
              fail("expected migrated sum")
            }
            assert_eq(value.total(), record.sequence() + 1L)
          },
        ),
      )
      coordinator.wait()
      assert_eq(task.snapshot().emitted_outputs(), 3211)
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
  assert_eq(delivered.length(), (counts[0] + counts[1]) * 2 + 1)
}
