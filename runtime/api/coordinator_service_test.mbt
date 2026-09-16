///|
async fn bind_test_service(root : String) -> CoordinatorService {
  CoordinatorService::bind(
    CoordinatorNodeConfig::new(
      "shared-cluster",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
      root + "/coordinator",
      [WorkerSlot::new("worker", "boot-worker")],
    ),
    DriverControlConfig::new(
      "shared-cluster",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
    ),
  )
}

///|
fn service_job(id : String) -> CompiledJob raise ApiError {
  JobDefinition::new(id, OperatorRef::keyed_sum())
  .with_workers(["worker"])
  .with_sink(SinkSpec::durable("totals"))
  .compile()
}

///|
fn service_driver(
  service : CoordinatorService,
  id : String,
) -> DriverConfig raise ApiError {
  DriverConfig::new(
    "shared-cluster",
    "coordinator",
    id,
    service.control_endpoint(),
  )
}

///|
async fn service_worker(
  service : CoordinatorService,
  job : CompiledJob,
  root : String,
) -> Result[WorkerNodeSummary, String] {
  Ok(
    job.run_worker_node(
      WorkerNodeConfig::new_auto(
        "shared-cluster",
        "coordinator",
        service.data_endpoint(),
        "worker",
        "boot-worker",
        root + "/" + job.job_id(),
      ),
    ),
  ) catch {
    error => Err(@debug.to_string(error))
  }
}

///|
async test "one service isolates concurrent jobs and accepts work after a job terminates" {
  let root = @fs.tmpdir(prefix="moonweave-multi-job")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(20000, () => {
    let service = bind_test_service(root)
    let a = service_job("job-a")
    let b = service_job("job-b")
    let c = service_job("job-c")
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { service.run() }
      let wa = group.spawn() <| () => { service_worker(service, a, root) }
      let wb = group.spawn() <| () => { service_worker(service, b, root) }
      let ha = a
        .submission_with_id("submit-a")
        .run_remote(service_driver(service, "driver-a"))
      defer ha.close()
      let hb = b
        .submission_with_id("submit-b")
        .run_remote(service_driver(service, "driver-b"))
      defer hb.close()
      // The same causal identity belongs to separate job domains.
      assert_eq(
        ha
        .input([
          Event::keyed_sum(@causal.EventId::new("orders", 0L), "store", 3L),
        ])
        .accepted_events(),
        1L,
      )
      assert_eq(
        hb
        .input([
          Event::keyed_sum(@causal.EventId::new("orders", 0L), "store", 70L),
        ])
        .accepted_events(),
        1L,
      )
      ignore(wait_live_checkpoint(root + "/job-a/worker-p0.json", 0L))
      ignore(wait_live_checkpoint(root + "/job-b/worker-p0.json", 0L))
      assert_eq(service.statuses().map(status => status.phase()), [
        JobRunPhase::Running,
        JobRunPhase::Running,
      ])
      assert_eq(ha.cancel().phase(), JobRunPhase::Cancelled)
      guard @async_runtime.with_timeout(2000, () => wa.wait()) is Err(_) else {
        fail("cancelled Worker must stop without a success result")
      }
      guard @async_runtime.with_timeout(2000, () => {
          service_worker(service, a, root)
        })
        is Err(reason) else {
        fail("terminal job assigned work to a new Worker")
      }
      assert_true(reason.contains("MissingAssignment"))
      assert_eq(hb.refresh().phase(), JobRunPhase::Running)
      assert_eq(
        hb
        .input([
          Event::keyed_sum(@causal.EventId::new("orders", 1L), "store", 7L),
        ])
        .accepted_events(),
        2L,
      )
      assert_eq(hb.drain().phase(), JobRunPhase::Succeeded)
      guard wb.wait() is Ok(_) else {
        fail("job-b failed after job-a cancellation")
      }
      let wc = group.spawn() <| () => { service_worker(service, c, root) }
      let hc = c
        .submission_with_id("submit-c")
        .run_remote(service_driver(service, "driver-c"))
      defer hc.close()
      ignore(
        hc.input([
          Event::keyed_sum(@causal.EventId::new("orders", 0L), "store", 9L),
        ]),
      )
      assert_eq(hc.drain().phase(), JobRunPhase::Succeeded)
      guard wc.wait() is Ok(_) else {
        fail("new job failed after prior jobs ended")
      }
      assert_eq(service.job_count(), 3)
      let log = @runtime.TaskOutputLog::open(root + "/job-b/output-p0.wal")
      defer log.close()
      assert_eq(log.records().map(record => record.output()), [
        @task.TaskOutput::KeyedSum({ key: "store", total: 70L, }),
        @task.TaskOutput::KeyedSum({ key: "store", total: 77L, }),
      ])
      service.close()
      runner.wait()
    }
    let restored = bind_test_service(root)
    defer restored.close()
    assert_eq(
      restored
      .statuses()
      .map(status => (status.job_id(), status.phase(), status.accepted_events())),
      [
        ("job-a", JobRunPhase::Cancelled, 1L),
        ("job-b", JobRunPhase::Succeeded, 2L),
        ("job-c", JobRunPhase::Succeeded, 1L),
      ],
    )
  }) catch {
    error => fail(@debug.to_string(error))
  }
}

///|
async test "service restart restores two running jobs and their independent deduplication" {
  let root = @fs.tmpdir(prefix="moonweave-multi-restart")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(20000, () => {
    let a = service_job("restart-a")
    let b = service_job("restart-b")
    let ea = Event::keyed_sum(@causal.EventId::new("same", 0L), "key", 11L)
    let eb = Event::keyed_sum(@causal.EventId::new("same", 0L), "key", 22L)
    for round in 0..<2 {
      let service = bind_test_service(root)
      @async_runtime.with_task_group <| group => {
        let runner = group.spawn() <| () => { service.run() }
        let wa = group.spawn() <| () => { service_worker(service, a, root) }
        let wb = group.spawn() <| () => { service_worker(service, b, root) }
        let ha = a
          .submission_with_id("restart-submit-a")
          .run_remote(service_driver(service, "driver-a"))
        defer ha.close()
        let hb = b
          .submission_with_id("restart-submit-b")
          .run_remote(service_driver(service, "driver-b"))
        defer hb.close()
        assert_eq(ha.input([ea]).accepted_events(), 1L)
        assert_eq(hb.input([eb]).accepted_events(), 1L)
        ignore(wait_live_checkpoint(root + "/restart-a/worker-p0.json", 0L))
        ignore(wait_live_checkpoint(root + "/restart-b/worker-p0.json", 0L))
        if round == 0 {
          service.close()
          runner.wait()
          ignore(wa.wait())
          ignore(wb.wait())
        } else {
          assert_eq(ha.refresh().phase(), JobRunPhase::Running)
          assert_eq(hb.refresh().phase(), JobRunPhase::Running)
          ignore(
            ha.input([
              Event::keyed_sum(@causal.EventId::new("same", 1L), "key", 1L),
            ]),
          )
          ignore(
            hb.input([
              Event::keyed_sum(@causal.EventId::new("same", 1L), "key", 2L),
            ]),
          )
          assert_eq(ha.drain().phase(), JobRunPhase::Succeeded)
          assert_eq(hb.drain().phase(), JobRunPhase::Succeeded)
          guard wa.wait() is Ok(sa) && wb.wait() is Ok(sb) else {
            fail("restarted jobs failed")
          }
          assert_eq(sa.replayed_batches(), 0)
          assert_eq(sb.replayed_batches(), 0)
          assert_eq(sa.live_batches(), 1)
          assert_eq(sb.live_batches(), 1)
          service.close()
          runner.wait()
        }
      }
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
}

///|
async test "one Worker task failure does not fail another job in the service" {
  let root = @fs.tmpdir(prefix="moonweave-multi-failure")
  defer @fs.rmdir(root, recursive=true)
  @async_runtime.with_timeout(15000, () => {
    let service = bind_test_service(root)
    let bad = JobDefinition::new("limited", OperatorRef::keyed_sum())
      .with_workers(["worker"])
      .with_state_limit(1)
      .compile()
    let good = service_job("healthy")
    @async_runtime.with_task_group <| group => {
      let runner = group.spawn() <| () => { service.run() }
      let failed_worker = group.spawn() <| () => {
        service_worker(service, bad, root)
      }
      let healthy_worker = group.spawn() <| () => {
        service_worker(service, good, root)
      }
      let failed = bad
        .submission_with_id("limited-submit")
        .run_remote(service_driver(service, "limited-driver"))
      defer failed.close()
      let healthy = good
        .submission_with_id("healthy-submit")
        .run_remote(service_driver(service, "healthy-driver"))
      defer healthy.close()
      let input = Event::keyed_sum(
        @causal.EventId::new("orders", 0L),
        "key",
        10L,
      )
      ignore(failed.input([input]))
      while failed.refresh().phase() == JobRunPhase::Running {
        @async_runtime.sleep(5)
      }
      assert_eq(failed.refresh().phase(), JobRunPhase::Failed)
      assert_true(failed_worker.wait() is Err(_))
      assert_eq(healthy.refresh().phase(), JobRunPhase::Running)
      assert_eq(healthy.input([input]).accepted_events(), 1L)
      assert_eq(healthy.drain().phase(), JobRunPhase::Succeeded)
      assert_true(healthy_worker.wait() is Ok(_))
      service.close()
      runner.wait()
    }
  }) catch {
    error => fail(@debug.to_string(error))
  }
}
