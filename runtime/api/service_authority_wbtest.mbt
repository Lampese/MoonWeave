///|
async test "authority waits for a durable catalog entry to acquire its child" {
  let root = @fs.tmpdir(prefix="moonweave-catalog-authority")
  defer @fs.rmdir(root, recursive=true)
  let service = CoordinatorService::bind(
    CoordinatorNodeConfig::new(
      "c",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
      root,
      [WorkerSlot::new("worker", "boot")],
    ),
    DriverControlConfig::new(
      "c",
      "coordinator",
      NodeEndpoint::new("127.0.0.1", 0),
    ),
  )
  defer service.close()
  let submission = JobDefinition::new("job", OperatorRef::keyed_sum())
    .with_workers(["worker"])
    .compile()
    .submission_with_id("submit")
  @async_runtime.with_task_group <| group => {
    service.catalog_lock.acquire()
    service.entries.push(submission)
    service.jobs["job"] = "submit"
    let capture = group.spawn() <| () => { service.authoritative_snapshot() }
    @async_runtime.sleep(10)
    assert_true(capture.try_wait() is None)
    ignore(service.open_child(submission, 0))
    service.catalog_lock.release()
    let snapshot : NetworkServiceSnapshot = @json.from_json(capture.wait())
    assert_eq(snapshot.entries.length(), 1)
    assert_eq(snapshot.jobs.length(), 1)
    assert_eq(snapshot.jobs[0].driver.submission, submission)
  }
}
