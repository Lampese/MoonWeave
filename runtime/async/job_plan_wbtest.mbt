///|
async test "job work identity replays from the journal without a checkpoint" {
  let directory = @fs.tmpdir(prefix="moonweave-job-plan")
  let journal = directory + "/job.wal"
  defer {
    @fs.remove(journal) catch {
      _ => ()
    }
    @fs.remove(journal + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let spec = @plan.JobSpec::new("bound-job", "reachability", 1, ["worker"], [])
  let store = JobStore::open(spec, journal)
  ignore(store.assign(0, "worker", 0, 0L))
  let batch = @protocol.EventBatch::new(0, 0, 0L, [
    @protocol.EventRecord::new(
      @causal.EventId::new("plan", 0),
      [],
      "key",
      Json::string("original"),
    ),
  ])
  let work = SupervisorWork::new("worker", "first-session", 0, [batch])
  store.bind_work(work)
  store.start()
  store.close()
  let restored = JobStore::open(spec, journal)
  defer restored.close()
  assert_eq(restored.phase(), @control.JobPhase::Running)
  let offset = restored.journal_offset()
  restored.bind_work(SupervisorWork::new("worker", "new-session", 0, [batch]))
  assert_eq(restored.journal_offset(), offset)
  try
    restored.bind_work(SupervisorWork::new("worker", "changed", 0, []))
  catch {
    error => assert_eq(error, JobStoreError::PlanMismatch(0))
  } noraise {
    _ => fail("changed journal-bound plan accepted")
  }
  assert_eq(restored.journal_offset(), offset)
}

///|
async test "job work plan binds the registered operator capability" {
  let directory = @fs.tmpdir(prefix="moonweave-job-capability")
  let journal = directory + "/job.wal"
  defer {
    @fs.remove(journal) catch {
      _ => ()
    }
    @fs.remove(journal + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let spec = @plan.JobSpec::new(
    "capability-job",
    "reachability",
    1,
    ["worker"],
    [],
  )
  let store = JobStore::open(spec, journal)
  ignore(store.assign(0, "worker", 0, 0L))
  let wrong = SupervisorWork::new(
    "worker",
    "capability-session",
    0,
    [],
    required_capability="operator:wrong",
  )
  try store.bind_work(wrong) catch {
    JobStoreError::PlanMismatch(partition) => assert_eq(partition, 0)
    _ => fail("expected capability plan mismatch")
  } noraise {
    _ => fail("expected capability plan mismatch")
  }
  store.close()
}

///|
async test "job work plan accepts append-only source growth" {
  let directory = @fs.tmpdir(prefix="moonweave-job-plan-growth")
  let journal = directory + "/job.wal"
  defer {
    @fs.remove(journal) catch {
      _ => ()
    }
    @fs.remove(journal + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let spec = @plan.JobSpec::new("growing-job", "reachability", 1, ["worker"], [])
  let store = JobStore::open(spec, journal)
  ignore(store.assign(0, "worker", 0, 0L))
  let first = @protocol.EventBatch::new(0, 0, 0L, [
    @protocol.EventRecord::new(
      @causal.EventId::new("growth", 0),
      [],
      "key",
      Json::string("first"),
    ),
  ])
  let second = @protocol.EventBatch::new(0, 0, 1L, [
    @protocol.EventRecord::new(
      @causal.EventId::new("growth", 1),
      [],
      "key",
      Json::string("second"),
    ),
  ])
  let initial = SupervisorWork::new("worker", "initial", 0, [first])
  store.bind_work(initial, append_only=true)
  let initial_offset = store.journal_offset()
  let extended = SupervisorWork::new("worker", "extended", 0, [first, second])
  store.start()
  store.drain()
  store.bind_work(extended, append_only=true)
  assert_true(store.journal_offset() > initial_offset)
  store.close()

  let restored = JobStore::open(spec, journal)
  defer restored.close()
  assert_eq(restored.work_plans[0].batches, [first, second])
  try
    restored.bind_work(SupervisorWork::new("worker", "rewritten", 0, [second]))
  catch {
    JobStoreError::PlanMismatch(partition) => assert_eq(partition, 0)
    _ => fail("expected append-only plan rejection")
  } noraise {
    _ => fail("expected append-only plan rejection")
  }
}
