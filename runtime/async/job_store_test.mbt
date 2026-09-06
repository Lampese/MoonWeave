///|
fn durable_job_spec() -> @plan.JobSpec raise @plan.PlanError {
  @plan.JobSpec::new(
    "durable-job",
    "reachability",
    2,
    ["worker-a", "worker-b"],
    [@plan.ExchangeRoute::new(0, 1)],
  )
}

///|
async fn with_job_store_paths(f : async (String, String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-job-store")
  let journal_path = directory + "/job.journal"
  let checkpoint_path = directory + "/job.json"
  defer {
    @fs.remove(journal_path) catch {
      _ => ()
    }
    @fs.remove(journal_path + ".lock") catch {
      _ => ()
    }
    @fs.remove(journal_path + ".repair") catch {
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
  f(journal_path, checkpoint_path)
}

///|
async test "job store replays lifecycle commands after a checkpoint" {
  with_job_store_paths <| ((journal_path, checkpoint_path) => {
    let spec = durable_job_spec()
    let store = JobStore::open(spec, journal_path, checkpoint_path~) catch {
      error => fail("open: " + @debug.to_string(error))
    }
    ignore(
      store.assign(0, "worker-a", 0, 0L) catch {
        error => fail("assign0: " + @debug.to_string(error))
      },
    )
    ignore(
      store.assign(1, "worker-b", 0, 0L) catch {
        error => fail("assign1: " + @debug.to_string(error))
      },
    )
    store.start() catch {
      error => fail("start: " + @debug.to_string(error))
    }
    assert_eq(store.snapshot().partitions()[0].next_offset(), 0L)
    assert_eq(
      store.snapshot().partitions()[0].phase(),
      @control.JobPartitionPhase::Running,
    )
    ignore(
      store.record_batch_sent(0, "worker-a", 0, 0L, 1L) catch {
        error => fail("sent: " + @debug.to_string(error))
      },
    )
    store.checkpoint() catch {
      error => fail("checkpoint: " + @debug.to_string(error))
    }
    let checkpoint_offset = store.journal_offset()
    store.record_progress(0, "worker-a", 0, 0L, 0L) catch {
      error => fail("progress1: " + @debug.to_string(error))
    }
    store.fail_partition(0, "worker-a", 0, "connection lost") catch {
      error => fail("fail partition: " + @debug.to_string(error))
    }
    ignore(
      store.reassign(0, "worker-b", 1, 0L) catch {
        error => fail("reassign: " + @debug.to_string(error))
      },
    )
    ignore(
      store.record_batch_sent(0, "worker-b", 1, 0L, 1L) catch {
        error => fail("sent2: " + @debug.to_string(error))
      },
    )
    store.record_progress(0, "worker-b", 1, 0L, 0L) catch {
      error => fail("progress2: " + @debug.to_string(error))
    }
    store.close()

    let restored = JobStore::open(spec, journal_path, checkpoint_path~) catch {
      error => fail("restore: " + @debug.to_string(error))
    }
    assert_true(restored.journal_offset() > checkpoint_offset)
    assert_eq(restored.phase(), @control.JobPhase::Running)
    let restored_snapshot = restored.snapshot()
    let partition = restored_snapshot.partitions()[0]
    assert_eq(partition.worker_id(), "worker-b")
    assert_eq(partition.epoch(), 1)
    assert_eq(partition.next_offset(), 1L)
    assert_eq(partition.applied_offset(), 0L)
    restored.record_progress(1, "worker-b", 0, -1L, -1L)
    restored.drain()
    restored.stop_partition(0, "worker-b", 1, 0)
    restored.stop_partition(1, "worker-b", 0, 0)
    assert_eq(restored.phase(), @control.JobPhase::Succeeded)
    restored.checkpoint()
    restored.close()

    let completed = JobStore::open(spec, journal_path, checkpoint_path~)
    assert_eq(completed.phase(), @control.JobPhase::Succeeded)
    completed.close()
  })
}

///|
async test "job store rotates its journal and reopens the new generation" {
  with_job_store_paths <| ((journal_path, checkpoint_path) => {
    let store = JobStore::open(
      durable_job_spec(),
      journal_path,
      checkpoint_path~,
    )
    ignore(store.assign(0, "worker-a", 0, 0L))
    ignore(store.assign(1, "worker-b", 0, 0L))
    store.start()
    let before = @fs.read_file(journal_path).binary().length()
    let old_generation = store.journal_generation()
    store.checkpoint_and_compact()
    assert_eq(store.journal_generation(), old_generation + 1L)
    let compacted = @fs.read_file(journal_path).binary().length()
    assert_true(compacted < before)
    assert_eq(@fs.exists(journal_path + ".next"), false)
    store.close()

    let restored = JobStore::open(
      durable_job_spec(),
      journal_path,
      checkpoint_path~,
    )
    defer restored.close()
    assert_eq(restored.journal_generation(), old_generation + 1L)
    assert_eq(restored.snapshot().partitions()[0].worker_id(), "worker-a")
    assert_eq(restored.phase(), @control.JobPhase::Running)
  })
}

///|
async test "job store adopts a staged generation after checkpoint commit" {
  with_job_store_paths <| ((journal_path, checkpoint_path) => {
    let store = JobStore::open(
      durable_job_spec(),
      journal_path,
      checkpoint_path~,
    )
    ignore(store.assign(0, "worker-a", 0, 0L))
    ignore(store.assign(1, "worker-b", 0, 0L))
    store.start()
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
    let restored = JobStore::open(
      durable_job_spec(),
      journal_path,
      checkpoint_path~,
    )
    defer restored.close()
    assert_eq(restored.journal_generation(), 1L)
    assert_eq(restored.phase(), @control.JobPhase::Running)
    assert_eq(@fs.exists(journal_path + ".next"), false)
  })
}

///|
async test "job store accepts a legacy checkpoint without a generation" {
  with_job_store_paths <| ((journal_path, checkpoint_path) => {
    let store = JobStore::open(
      durable_job_spec(),
      journal_path,
      checkpoint_path~,
    )
    ignore(store.assign(0, "worker-a", 0, 0L))
    ignore(store.assign(1, "worker-b", 0, 0L))
    store.start()
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
    let restored = JobStore::open(
      durable_job_spec(),
      journal_path,
      checkpoint_path~,
    )
    defer restored.close()
    assert_eq(restored.journal_generation(), 0L)
    assert_eq(restored.phase(), @control.JobPhase::Running)
  })
}

///|
async test "job store rejects a checkpoint for a different topology" {
  with_job_store_paths <| ((journal_path, checkpoint_path) => {
    let spec = durable_job_spec()
    let store = JobStore::open(spec, journal_path, checkpoint_path~)
    store.checkpoint()
    store.close()
    let different = @plan.JobSpec::new(
      "durable-job",
      "reachability",
      2,
      ["worker-a", "worker-b"],
      [],
    )
    try JobStore::open(different, journal_path, checkpoint_path~) catch {
      JobStoreError::Malformed => ()
      _ => fail("expected topology mismatch")
    } noraise {
      _ => fail("expected topology mismatch")
    }
  })
}
