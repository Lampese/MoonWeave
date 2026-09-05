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
