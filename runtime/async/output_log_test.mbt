///|
async fn with_output_log_path(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-output-log")
  let path = directory + "/outputs.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.remove(path + ".repair") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  f(path)
}

///|
async test "output log is durable and idempotent by task sequence" {
  with_output_log_path <| path => {
    let first = @task.TaskOutput::Reachable(
      @reachability.Reachable::new("A", "B"),
    )
    let second = @task.TaskOutput::Reachable(
      @reachability.Reachable::new("A", "C"),
    )
    let log = TaskOutputLog::open(path)
    log.append("task-output", 2, 4, 0L, [first, second])
    log.append("task-output", 2, 4, 0L, [first, second])
    assert_eq(log.records_for("task-output").length(), 2)
    log.close()

    let reopened = TaskOutputLog::open(path)
    let records = reopened.records_for("task-output")
    assert_eq(records.length(), 2)
    assert_eq(records[0].sequence(), 0L)
    assert_eq(records[1].sequence(), 1L)
    assert_eq(records[0].output(), first)
    assert_eq(reopened.next_sequence("task-output"), 2L)
    assert_eq(reopened.records_after("task-output", 0L).length(), 1)
    try reopened.append("task-output", 2, 4, 3L, [first]) catch {
      OutputLogError::SequenceGap(expected~, actual~) => {
        assert_eq(expected, 2L)
        assert_eq(actual, 3L)
      }
      _ => fail("expected output sequence gap")
    } noraise {
      _ => fail("expected output sequence gap")
    }
    try
      reopened.append("task-output", 2, 4, 1L, [
        @task.TaskOutput::Reachable(@reachability.Reachable::new("X", "Y")),
      ])
    catch {
      OutputLogError::IdentityConflict(_) => ()
      _ => fail("expected output identity conflict")
    } noraise {
      _ => fail("expected output identity conflict")
    }
    reopened.close()
  }
}
