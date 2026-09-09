///|
async test "JSONL sink restores idempotency keys and consumes output records" {
  let directory = @fs.tmpdir(prefix="moonweave-jsonl-sink")
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
  let sink_path = directory + "/outputs.jsonl"
  let record = TaskOutputRecord::{
    version: 1,
    task_id: "task/p0/e0",
    partition: 0,
    epoch: 0,
    sequence: 0L,
    output: @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
  }
  let sink = JsonLinesSink::open(sink_path)
  sink.write(record)
  sink.write(record)
  assert_eq(sink.written_count(), 1)
  sink.close()
  let reopened = JsonLinesSink::open(sink_path)
  assert_true(reopened.contains(record.idempotency_key()))
  assert_eq(reopened.written_count(), 1)
  let log = TaskOutputLog::open(directory + "/outputs.wal")
  log.append(record.task_id(), 0, 0, 0L, [record.output()])
  let consumer = TaskOutputConsumer::open(
    log,
    directory + "/consumer.wal",
    "json-lines-test",
    record.task_id(),
    0,
    0,
  )
  assert_eq(reopened.consume(consumer), 1)
  consumer.close()
  log.close()
  assert_eq(reopened.written_count(), 1)
  reopened.close()
}
