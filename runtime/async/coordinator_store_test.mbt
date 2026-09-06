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
    @fs.remove(journal_path + ".next") catch {
      _ => ()
    }
    @fs.remove(journal_path + ".next.lock") catch {
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

///|
async test "coordinator store rotates its journal and reopens the new generation" {
  let directory = @fs.tmpdir(prefix="moonweave-coordinator-rotate")
  let journal_path = directory + "/coordinator.journal"
  let checkpoint_path = directory + "/coordinator.json"
  defer {
    for
      path in [
        journal_path,
        journal_path + ".lock",
        journal_path + ".next",
        journal_path + ".next.lock",
        journal_path + ".repair",
        checkpoint_path,
        checkpoint_path + ".tmp",
      ] {
      @fs.remove(path) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let store = CoordinatorStore::open(
    "cluster-rotate",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  let boot = @protocol.Incarnation::new("boot-rotate")
  ignore(store.register_worker("worker", boot, ["graph"]))
  ignore(store.assign(0, "worker"))
  let before = @fs.read_file(journal_path).binary().length()
  store.checkpoint_and_compact()
  let compacted = @fs.read_file(journal_path).binary().length()
  assert_true(compacted < before)
  assert_eq(store.journal_generation(), 1L)
  assert_eq(@fs.exists(journal_path + ".next"), false)
  store.close()

  let restored = CoordinatorStore::open(
    "cluster-rotate",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  defer restored.close()
  assert_eq(restored.journal_generation(), 1L)
  assert_eq(restored.snapshot().workers().length(), 1)
  assert_eq(restored.snapshot().assignments().length(), 1)
}

///|
async test "coordinator store adopts a staged generation after checkpoint commit" {
  let directory = @fs.tmpdir(prefix="moonweave-coordinator-staged")
  let journal_path = directory + "/coordinator.journal"
  let checkpoint_path = directory + "/coordinator.json"
  defer {
    for
      path in [
        journal_path,
        journal_path + ".lock",
        journal_path + ".next",
        journal_path + ".next.lock",
        journal_path + ".repair",
        checkpoint_path,
        checkpoint_path + ".tmp",
      ] {
      @fs.remove(path) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let store = CoordinatorStore::open(
    "cluster-staged",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  let boot = @protocol.Incarnation::new("boot-staged")
  ignore(store.register_worker("worker", boot, []))
  ignore(store.assign(0, "worker"))
  let old_journal = @fs.read_file(journal_path).binary()
  store.checkpoint_and_compact()
  let staged_journal = @fs.read_file(journal_path).binary()
  store.close()
  @fs.write_file(
    journal_path + ".next",
    staged_journal,
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  )
  @fs.write_file(
    journal_path,
    old_journal,
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  )
  let restored = CoordinatorStore::open(
    "cluster-staged",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  defer restored.close()
  assert_eq(restored.journal_generation(), 1L)
  assert_eq(restored.snapshot().assignments().length(), 1)
  assert_eq(@fs.exists(journal_path + ".next"), false)
}

///|
async test "coordinator store accepts a legacy checkpoint without a generation" {
  let directory = @fs.tmpdir(prefix="moonweave-coordinator-legacy")
  let journal_path = directory + "/coordinator.journal"
  let checkpoint_path = directory + "/coordinator.json"
  defer {
    for
      path in [
        journal_path,
        journal_path + ".lock",
        journal_path + ".next",
        journal_path + ".next.lock",
        checkpoint_path,
        checkpoint_path + ".tmp",
      ] {
      @fs.remove(path) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let store = CoordinatorStore::open(
    "cluster-legacy",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  let boot = @protocol.Incarnation::new("boot-legacy")
  ignore(store.register_worker("worker", boot, []))
  store.checkpoint()
  store.close()
  let json = @json.parse(@fs.read_file(checkpoint_path).text())
  guard json is Object(fields) else { fail("checkpoint was not an object") }
  fields["version"] = Json::number(1.0)
  fields.remove("journal_generation")
  @fs.write_file(
    checkpoint_path,
    @utf8.encode(json.stringify()),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  )
  let restored = CoordinatorStore::open(
    "cluster-legacy",
    "coordinator",
    journal_path,
    checkpoint_path~,
  )
  defer restored.close()
  assert_eq(restored.journal_generation(), 0L)
  assert_eq(restored.snapshot().workers().length(), 1)
}
