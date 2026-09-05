///|
async test "outbox retains lock discipline on a WAL failure and during compaction" {
  let directory = @fs.tmpdir(prefix="moonweave-outbox-lock")
  let path = directory + "/outbox.wal"
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
  let delta = @protocol.DeltaBatch::new("io-failure", 0, 0, 1, 0, [
    @protocol.EventRecord::new(
      @causal.EventId::new("source", 0),
      [],
      "key",
      Json::null(),
    ),
  ])
  let outbox = DurableExchangeOutbox::open(path, 1)
  defer outbox.close()
  outbox.wal.close()
  try outbox.push(delta) catch {
    error => assert_eq(error, DurableOutboxError::Wal(WalError::Closed))
  } noraise {
    _ => fail("expected closed WAL")
  }
  assert_true(outbox.write_lock.try_acquire())
  outbox.write_lock.release()
  assert_eq(outbox.pending_count(), 0)
  outbox.seal()
  outbox.write_lock.acquire()
  assert_false(outbox.is_drained())
  outbox.write_lock.release()
  assert_true(outbox.is_drained())
}
