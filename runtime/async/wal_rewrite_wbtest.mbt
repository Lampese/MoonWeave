///|
async fn with_wal_failure_path(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-wal-failure")
  let path = directory + "/events.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.remove(path + ".compact") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  f(path)
}

///|
async test "wal refuses further mutation after an uncertain append failure" {
  with_wal_failure_path <| path => {
    let (wal, _) = Wal::open(path)
    wal.file.close()
    try wal.append(b"failed") catch {
      error => assert_true(error is WalError::Io(_))
    } noraise {
      _ => fail("expected failed write")
    }
    assert_eq(wal.write_offset(), 0L)
    try wal.append(b"later") catch {
      error => assert_eq(error, WalError::Failed)
    } noraise {
      _ => fail("expected poisoned WAL")
    }
    assert_eq(wal.lifecycle(), WalLifecycle::Failed)
    try wal.rewrite([b"later"]) catch {
      error => assert_eq(error, WalError::Failed)
    } noraise {
      _ => fail("expected poisoned WAL")
    }
    wal.close()
    assert_eq(wal.lifecycle(), WalLifecycle::Closed)
    let (restored, replay) = Wal::open(path)
    defer restored.close()
    assert_eq(replay.records(), [])
    assert_eq(restored.append(b"restored"), 0L)
  }
}

///|
async test "cancelling a queued rewrite leaves the WAL available" {
  with_wal_failure_path <| path => {
    let (wal, _) = Wal::open(path)
    ignore(wal.append(b"original"))
    @async.with_timeout(5000, () => {
      @async.with_task_group <| group => {
        wal.write_lock.acquire()
        defer wal.write_lock.release()
        let rewriting = group.spawn(allow_failure=true) <| () => {
          wal.rewrite([b"replacement"])
        }
        @async.pause()
        rewriting.cancel()
        try rewriting.wait() catch {
          _ => ()
        } noraise {
          _ => fail("expected cancellation")
        }
      }
    })
    ignore(wal.append(b"tail"))
    wal.close()
    let (restored, replay) = Wal::open(path)
    defer restored.close()
    assert_eq(replay.records(), [b"original", b"tail"])
  }
}
