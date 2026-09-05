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

///|
async test "output log rejects task identity changes and sequence overflow before appending" {
  with_output_log_path <| path => {
    let log = TaskOutputLog::open(path)
    let output = @task.TaskOutput::Reachable(
      @reachability.Reachable::new("A", "B"),
    )
    log.append("task", 0, 0, 0L, [output])
    try log.append("task", 1, 0, 1L, [output]) catch {
      error => assert_eq(error, OutputLogError::IdentityConflict("task"))
    } noraise {
      _ => fail("changed output partition accepted")
    }
    try log.append("task", 0, 1, 1L, [output]) catch {
      error => assert_eq(error, OutputLogError::IdentityConflict("task"))
    } noraise {
      _ => fail("changed output epoch accepted")
    }
    try log.append("task", 0, 0, 9223372036854775807L, [output]) catch {
      error => assert_true(error is OutputLogError::InvalidSequence(_))
    } noraise {
      _ => fail("overflowing output sequence accepted")
    }
    assert_eq(log.next_sequence("task"), 1L)
    log.close()
    try log.append("task", 0, 0, 0L, [output]) catch {
      error => assert_eq(error, OutputLogError::Wal(WalError::Closed))
    } noraise {
      _ => fail("closed output log accepted duplicate append")
    }
    let restored = TaskOutputLog::open(path)
    defer restored.close()
    assert_eq(restored.records_for("task").length(), 1)
    assert_eq(restored.next_sequence("task"), 1L)
  }
}

///|
async test "output log replay rejects gaps and lease changes instead of treating a missing prefix as consumed" {
  for invalid in ["missing-prefix", "gap", "epoch", "partition"] {
    with_output_log_path <| path => {
      let first = TaskOutputRecord::{
        version: 1,
        task_id: "task",
        partition: 0,
        epoch: 0,
        sequence: 0L,
        output: @task.TaskOutput::Reachable(
          @reachability.Reachable::new("A", "B"),
        ),
      }
      let (wal, _) = Wal::open(path)
      if invalid != "missing-prefix" {
        ignore(wal.append(@utf8.encode(@json.to_json(first).stringify())))
      }
      let next = TaskOutputRecord::{
        ..first,
        sequence: if invalid == "gap" {
          2L
        } else {
          1L
        },
        partition: if invalid == "partition" {
          1
        } else {
          0
        },
        epoch: if invalid == "epoch" {
          1
        } else {
          0
        },
      }
      ignore(wal.append(@utf8.encode(@json.to_json(next).stringify())))
      wal.close()
      try TaskOutputLog::open(path) catch {
        error =>
          if invalid == "missing-prefix" || invalid == "gap" {
            assert_true(error is OutputLogError::SequenceGap(..))
          } else {
            assert_eq(error, OutputLogError::IdentityConflict("task"))
          }
      } noraise {
        unexpected => {
          unexpected.close()
          fail("invalid output log replay accepted")
        }
      }
    }
  }
}
