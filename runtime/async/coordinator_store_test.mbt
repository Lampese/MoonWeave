///|
async test "coordinator store replays checkpoint suffix" {
  let directory = @fs.tmpdir(prefix="moonweave-coordinator-store")
  let journal_path = directory + "/coordinator.journal"
  let checkpoint_path = directory + "/coordinator.json"
  defer {
    @fs.remove(journal_path) catch {
      _ => ()
    }
    @fs.remove(journal_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(checkpoint_path) catch {
      _ => ()
    }
    @fs.remove(checkpoint_path + ".tmp") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let store = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  let boot = @protocol.Incarnation::new("boot-1")
  ignore(store.register_worker("worker-a", boot, ["graph"]))
  let assignment = store.assign(0, "worker-a")
  ignore(
    store.record_batch_sent(0, "worker-a", boot, assignment.epoch(), 0L, 2L),
  )
  store.checkpoint()
  let checkpoint_offset = store.journal_offset()
  store.record_progress(0, "worker-a", boot, assignment.epoch(), 1L, 0L)
  store.close()
  let restored = CoordinatorStore::open(
    "cluster-a",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  assert_eq(restored.journal_offset() > checkpoint_offset, true)
  let snapshot = restored.snapshot()
  guard snapshot.assignments() is [current] else {
    fail("assignment missing after journal replay")
  }
  assert_eq(current.next_offset(), 2L)
  assert_eq(current.persisted_offset(), 1L)
  assert_eq(current.applied_offset(), 0L)
  restored.close()
}

///|
async test "coordinator store discards an incomplete journal tail" {
  let directory = @fs.tmpdir(prefix="moonweave-coordinator-tail")
  let journal_path = directory + "/coordinator.journal"
  defer {
    @fs.remove(journal_path) catch {
      _ => ()
    }
    @fs.remove(journal_path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let store = CoordinatorStore::open("cluster-a", "coordinator", journal_path)
  let boot = @protocol.Incarnation::new("boot-1")
  ignore(store.register_worker("worker-a", boot, []))
  store.close()
  let corrupt_tail = @fs.open(
    journal_path,
    mode=@fs.WriteOnly,
    append=true,
    create_mode=@fs.OpenExisting,
    sync=@fs.Full,
  )
  corrupt_tail.write(b"MWAL\x01\x00")
  corrupt_tail.close()
  let restored = CoordinatorStore::open(
    "cluster-a", "coordinator", journal_path,
  )
  assert_eq(restored.journal_offset() > 0L, true)
  assert_eq(restored.snapshot().workers().length(), 1)
  restored.close()
}
