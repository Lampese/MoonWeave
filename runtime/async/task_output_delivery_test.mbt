///|
async fn with_task_output_delivery_directory(
  f : async (String) -> Unit,
) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-task-output-delivery")
  defer {
    for name in @fs.readdir(directory) {
      @fs.remove(directory + "/" + name) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  @async_runtime.with_timeout(10000, () => f(directory))
}

///|
fn task_output_delivery_event(
  origin : String,
  sequence : Int64,
  source : String,
  target : String,
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new(origin, sequence),
    [],
    "graph",
    Json::object({
      "source": Json::string(source),
      "target": Json::string(target),
    }),
  )
}

///|
async test "worker output delivery replays failed backlog before live input" {
  with_task_output_delivery_directory <| directory => {
    let wal_path = directory + "/worker.wal"
    let output_log_path = directory + "/outputs.wal"
    let checkpoint_prefix = directory + "/sink-kafka-p0"
    let checkpoint_path = checkpoint_prefix + "-e5.wal"
    let assignment = @protocol.AssignmentSpec::new(0, 5, "boot-output-delivery")
    let backlog_event = task_output_delivery_event(
      "output-delivery", 0, "A", "B",
    )
    let backlog_batch = @protocol.EventBatch::new(0, 5, 0L, [backlog_event])
    let first_server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer first_server.close()
    @async_runtime.with_task_group <| group => {
      let coordinator_task = group.spawn() <| () => {
        let (connection, _) = first_server.accept()
        defer connection.close()
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-output-delivery",
          "coordinator",
          "worker-output-delivery",
          "output-delivery-first",
          assignment~,
        )
        coordinator.accept_frame(read_frame(connection))
        coordinator.accept_frame(read_frame(connection))
        coordinator.accept_frame(read_frame(connection))
        coordinator_send_batch(
          connection,
          coordinator,
          outbound,
          backlog_batch,
          @utf8.encode(backlog_batch.to_payload()).length().to_int64(),
        )
        coordinator.accept_frame(read_frame(connection))
      }
      let task = @task.Task::from_assignment(
        "output-delivery-task",
        "reachability",
        @task.OperatorKind::Reachability,
        assignment,
      )
      let fallback_outputs : Array[@task.TaskOutput] = []
      try
        run_worker_task(
          @socket.Tcp::connect(first_server.addr),
          "cluster-output-delivery",
          "coordinator",
          "worker-output-delivery",
          "boot-output-delivery",
          [],
          wal_path,
          task,
          output => fallback_outputs.push(output),
          initial_credit_events=2L,
          initial_credit_bytes=4096L,
          output_log_path~,
          output_delivery=record => {
            assert_eq(record.sequence(), 0L)
            fail("simulated Kafka acknowledgement loss")
          },
          output_consumer_checkpoint_prefix=checkpoint_prefix,
          output_consumer_id="job-kafka-output",
        )
      catch {
        FrameIoError::Handler(message) =>
          assert_true(message.contains("simulated Kafka acknowledgement loss"))
        error => fail("unexpected delivery error: \{@debug.to_string(error)}")
      } noraise {
        _ => fail("failed sink delivery was accepted")
      }
      assert_eq(task.phase(), @task.TaskPhase::Failed)
      assert_eq(fallback_outputs, [])
      coordinator_task.wait()
    }
    assert_true(@fs.exists(checkpoint_path))
    let failed_log = TaskOutputLog::open(output_log_path)
    let failed_consumer = TaskOutputConsumer::open(
      failed_log, checkpoint_path, "job-kafka-output", "output-delivery-task", 0,
      5,
    )
    assert_eq(failed_consumer.next_sequence(), 0L)
    assert_eq(failed_consumer.read().map(record => record.sequence()), [0L])
    failed_consumer.close()
    failed_log.close()

    let live_event = task_output_delivery_event("output-delivery", 1, "C", "D")
    let live_batch = @protocol.EventBatch::new(0, 5, 1L, [live_event])
    let delivered : Array[Int64] = []
    let live_sent = Ref(false)
    let second_server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    defer second_server.close()
    @async_runtime.with_task_group <| group => {
      let coordinator_task = group.spawn() <| () => {
        let (connection, _) = second_server.accept()
        defer connection.close()
        let (coordinator, outbound) = coordinator_handshake(
          connection,
          "cluster-output-delivery",
          "coordinator",
          "worker-output-delivery",
          "output-delivery-retry",
          assignment~,
        )
        coordinator.restore_send_cursor(0, 5, 1L)
        coordinator.accept_frame(read_frame(connection))
        assert_eq(delivered, [0L])
        coordinator.accept_frame(read_frame(connection))
        coordinator.accept_frame(read_frame(connection))
        live_sent.val = true
        coordinator_send_batch(
          connection,
          coordinator,
          outbound,
          live_batch,
          @utf8.encode(live_batch.to_payload()).length().to_int64(),
        )
        coordinator.accept_frame(read_frame(connection))
        coordinator.accept_frame(read_frame(connection))
        coordinator.accept_frame(read_frame(connection))
        write_frame(
          connection,
          outbound.build(@protocol.FrameKind::Drain, "{}"),
        )
      }
      let task = @task.Task::from_assignment(
        "output-delivery-task",
        "reachability",
        @task.OperatorKind::Reachability,
        assignment,
      )
      let fallback_outputs : Array[@task.TaskOutput] = []
      let summary = run_worker_task(
        @socket.Tcp::connect(second_server.addr),
        "cluster-output-delivery",
        "coordinator",
        "worker-output-delivery",
        "boot-output-delivery",
        [],
        wal_path,
        task,
        output => fallback_outputs.push(output),
        initial_credit_events=2L,
        initial_credit_bytes=4096L,
        output_log_path~,
        output_delivery=record => {
          if record.sequence() == 0L {
            assert_false(live_sent.val)
          } else {
            assert_true(live_sent.val)
          }
          delivered.push(record.sequence())
        },
        output_consumer_checkpoint_prefix=checkpoint_prefix,
        output_consumer_id="job-kafka-output",
      )
      assert_eq(summary.replayed_batches(), 1)
      assert_eq(summary.live_batches(), 1)
      assert_eq(delivered, [0L, 1L])
      assert_eq(fallback_outputs, [])
      assert_eq(task.phase(), @task.TaskPhase::Stopped)
      coordinator_task.wait()
    }
    assert_true(@fs.exists(checkpoint_path))
    assert_false(@fs.exists(checkpoint_prefix + ".wal"))
    let completed_log = TaskOutputLog::open(output_log_path)
    defer completed_log.close()
    assert_eq(
      completed_log
      .records_for("output-delivery-task")
      .map(record => record.sequence()),
      [0L, 1L],
    )
    let completed_consumer = TaskOutputConsumer::open(
      completed_log, checkpoint_path, "job-kafka-output", "output-delivery-task",
      0, 5,
    )
    defer completed_consumer.close()
    assert_eq(completed_consumer.checkpoint_path(), checkpoint_path)
    assert_eq(completed_consumer.next_sequence(), 2L)
    assert_eq(completed_consumer.read(), [])
  }
}
