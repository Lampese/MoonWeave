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
