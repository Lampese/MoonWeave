///|
async fn with_output_consumer_directory(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-output-consumer")
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
fn consumer_output(target : String) -> @task.TaskOutput {
  @task.TaskOutput::Reachable(@reachability.Reachable::new("A", target))
}

///|
async test "output consumer replays only unacknowledged results after restart" {
  with_output_consumer_directory <| directory => {
    let output_path = directory + "/outputs.wal"
    let checkpoint_path = directory + "/consumer.wal"
    let log = TaskOutputLog::open(output_path)
    log.append("job/p0/e2", 0, 2, 0L, [
      consumer_output("B"),
      consumer_output("C"),
    ])
    let consumer = TaskOutputConsumer::open(
      log, checkpoint_path, "sink", "job/p0/e2", 0, 2,
    )
    let first = consumer.read(maximum_records=1)[0]
    assert_eq(first.idempotency_key(), "9:job/p0/e2:0")
    consumer.ack(first)
    consumer.ack(first)
    assert_eq(consumer.next_sequence(), 1L)
    consumer.close()
    log.close()
    let reopened_log = TaskOutputLog::open(output_path)
    defer reopened_log.close()
    let restored = TaskOutputConsumer::open(
      reopened_log, checkpoint_path, "sink", "job/p0/e2", 0, 2,
    )
    defer restored.close()
    assert_eq(restored.next_sequence(), 1L)
    let delivered = []
    assert_eq(restored.consume(record => delivered.push(record.output())), 1)
    assert_eq(delivered, [consumer_output("C")])
    assert_eq(restored.read(), [])
    reopened_log.append("job/p0/e2", 0, 2, 2L, [consumer_output("D")])
    assert_eq(restored.read()[0].sequence(), 2L)
  }
}

///|
async test "output consumer rejects skips, foreign records, and altered records" {
  with_output_consumer_directory <| directory => {
    let log = TaskOutputLog::open(directory + "/outputs.wal")
    defer log.close()
    log.append("task", 1, 4, 0L, [consumer_output("B"), consumer_output("C")])
    log.append("other", 2, 4, 0L, [consumer_output("B")])
    let consumer = TaskOutputConsumer::open(
      log,
      directory + "/consumer.wal",
      "sink",
      "task",
      1,
      4,
    )
    defer consumer.close()
    let records = consumer.read()
    let altered = TaskOutputRecord::{
      ..records[0],
      output: consumer_output("changed"),
    }
    try consumer.ack(records[1]) catch {
      error =>
        assert_eq(
          error,
          OutputConsumerError::SequenceGap(expected=0L, actual=1L),
        )
    } noraise {
      _ => fail("skipped output acknowledgement accepted")
    }
    try consumer.ack(altered) catch {
      error => assert_eq(error, OutputConsumerError::OutputConflict(0L))
    } noraise {
      _ => fail("altered output acknowledgement accepted")
    }
    try consumer.ack(log.records_for("other")[0]) catch {
      error => assert_eq(error, OutputConsumerError::IdentityMismatch)
    } noraise {
      _ => fail("foreign output acknowledgement accepted")
    }
    try consumer.ack(TaskOutputRecord::{ ..records[0], epoch: 3, }) catch {
      error => assert_eq(error, OutputConsumerError::IdentityMismatch)
    } noraise {
      _ => fail("wrong epoch acknowledgement accepted")
    }
    try consumer.ack(TaskOutputRecord::{ ..records[0], sequence: 2L, }) catch {
      error => assert_eq(error, OutputConsumerError::MissingOutput(2L))
    } noraise {
      _ => fail("missing output acknowledgement accepted")
    }
    assert_eq(consumer.next_sequence(), 0L)
    consumer.ack(records[0])
    consumer.ack(records[1])
    assert_eq(consumer.next_sequence(), 2L)
  }
}

///|
async test "sink failure leaves the failed output replayable with its stable identity" {
  with_output_consumer_directory <| directory => {
    let log = TaskOutputLog::open(directory + "/outputs.wal")
    defer log.close()
    log.append("task", 0, 0, 0L, [consumer_output("B"), consumer_output("C")])
    let checkpoint = directory + "/consumer.wal"
    let consumer = TaskOutputConsumer::open(
      log, checkpoint, "sink", "task", 0, 0,
    )
    let sink : Set[String] = Set([])
    try
      consumer.consume(record => {
        sink.add(record.idempotency_key())
        if record.sequence() == 1L {
          fail("sink response was lost")
        }
      })
    catch {
      error => assert_true(error is Failure(_))
    } noraise {
      _ => fail("sink failure was swallowed")
    }
    assert_eq(consumer.next_sequence(), 1L)
    consumer.close()
    let restored = TaskOutputConsumer::open(
      log, checkpoint, "sink", "task", 0, 0,
    )
    defer restored.close()
    assert_eq(restored.consume(record => sink.add(record.idempotency_key())), 1)
    assert_eq(sink.length(), 2)
    assert_eq(restored.next_sequence(), 2L)
  }
}

///|
async test "output consumer serializes operations and cancellation does not acknowledge" {
  with_output_consumer_directory <| directory => {
    let log = TaskOutputLog::open(directory + "/outputs.wal")
    defer log.close()
    log.append("task", 0, 0, 0L, [consumer_output("B")])
    let checkpoint = directory + "/consumer.wal"
    let consumer = TaskOutputConsumer::open(
      log, checkpoint, "sink", "task", 0, 0,
    )
    let record = consumer.read()[0]
    let entered = Ref(false)
    @async_runtime.with_task_group <| group => {
      let delivery = group.spawn(allow_failure=true) <| () => {
        consumer.consume(_ => {
          entered.val = true
          @async_runtime.sleep(10000)
        })
      }
      while !entered.val {
        @async_runtime.pause()
      }
      try consumer.ack(record) catch {
        error => assert_eq(error, OutputConsumerError::Busy)
      } noraise {
        _ => fail("concurrent acknowledgement accepted")
      }
      delivery.cancel()
      try delivery.wait() catch {
        _ => ()
      } noraise {
        _ => fail("cancelled delivery completed")
      }
    }
    assert_eq(consumer.next_sequence(), 0L)
    consumer.ack(record)
    consumer.close()
  }
}

///|
async test "output consumer binds its checkpoint and detects source replacement" {
  with_output_consumer_directory <| directory => {
    let path = directory + "/outputs.wal"
    let checkpoint = directory + "/consumer.wal"
    let log = TaskOutputLog::open(path)
    defer log.close()
    let consumer = TaskOutputConsumer::open(
      log, checkpoint, "sink", "task", 0, 1,
    )
    assert_eq(consumer.read(), [])
    consumer.close()
    try
      TaskOutputConsumer::open(log, checkpoint, "other-sink", "task", 0, 1)
    catch {
      error => assert_eq(error, OutputConsumerError::IdentityMismatch)
    } noraise {
      value => {
        value.close()
        fail("changed consumer identity accepted")
      }
    }
    log.append("task", 0, 1, 0L, [consumer_output("B")])
    let resumed = TaskOutputConsumer::open(
      log, checkpoint, "sink", "task", 0, 1,
    )
    resumed.ack(resumed.read()[0])
    resumed.close()
    let replacement = TaskOutputLog::open(directory + "/replacement.wal")
    defer replacement.close()
    try
      TaskOutputConsumer::open(replacement, checkpoint, "sink", "task", 0, 1)
    catch {
      error => assert_eq(error, OutputConsumerError::MissingOutput(0L))
    } noraise {
      value => {
        value.close()
        fail("missing acknowledged output accepted")
      }
    }
    replacement.append("task", 0, 1, 0L, [consumer_output("different")])
    try
      TaskOutputConsumer::open(replacement, checkpoint, "sink", "task", 0, 1)
    catch {
      error => assert_eq(error, OutputConsumerError::OutputConflict(0L))
    } noraise {
      value => {
        value.close()
        fail("replaced output accepted")
      }
    }
  }
}
