///|
test "local job handle supports submit drain cancel and serialized submission" {
  let compiled = JobDefinition::new("handle-job", OperatorRef::reachability()).compile()
  let submission = compiled.submission_with_id("submission-1")
  let encoded = @json.to_json(submission)
  let decoded : JobSubmission = @json.from_json(encoded)
  assert_eq(decoded.submission_id(), "submission-1")
  let input_command = JobCommand::input([])
  let decoded_command : JobCommand = @json.from_json(
    @json.to_json(input_command),
  )
  assert_true(decoded_command is JobCommand::Input(_))
  let handle = decoded.run_local()
  assert_eq(handle.status().phase(), JobRunPhase::Running)
  let event = Event::new(
    @causal.EventId::new("handle", 0),
    [],
    "key",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  assert_eq(handle.submit([event]).length(), 1)
  assert_eq(handle.status().accepted_events(), 1L)
  handle.drain()
  assert_eq(handle.await_completion().phase(), JobRunPhase::Succeeded)
  try handle.submit([event]) catch {
    ApiError::InvalidJobPhase(expected~, actual~) => {
      assert_eq(expected, JobRunPhase::Running)
      assert_eq(actual, JobRunPhase::Succeeded)
    }
    _ => fail("expected submit after drain to be rejected")
  } noraise {
    _ => fail("expected submit after drain to be rejected")
  }

  let cancelled = compiled.submission_with_id("submission-2").run_local()
  cancelled.cancel()
  assert_eq(cancelled.status().phase(), JobRunPhase::Cancelled)
}
