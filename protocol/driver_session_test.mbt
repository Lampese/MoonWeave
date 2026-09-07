///|
test "driver session validates submit command sequence and duplicate replay" {
  let driver = DriverSession::new("cluster", "coordinator", "driver-1")
  let builder = FrameBuilder::new("cluster", "driver-1", "driver")
  let submit = DriverSubmit::new(
    "submission-1",
    Json::object({ "job": Json::string("x") }),
  )
  let submit_frame = builder.build(FrameKind::DriverSubmit, submit.to_payload())
  assert_true(driver.accept_frame(submit_frame) is DriverInbound::Submit(_))
  assert_eq(driver.phase(), DriverSessionPhase::Running)
  assert_eq(driver.next_sequence(), 1L)
  assert_true(driver.accept_frame(submit_frame) is DriverInbound::Submit(_))
  builder.set_session_id(driver.session_id())
  let command = DriverCommand::new("drain")
  assert_true(
    driver.accept_frame(
      builder.build(FrameKind::DriverCommand, command.to_payload()),
    )
    is DriverInbound::Command(_),
  )
  assert_eq(driver.next_sequence(), 2L)
}

///|
test "driver session rejects gaps and non-driver frames" {
  let driver = DriverSession::new("cluster", "coordinator", "driver-1")
  let builder = FrameBuilder::new(
    "cluster",
    "driver-1",
    "driver",
    start_sequence=1L,
  )
  let submit = DriverSubmit::new("submission-1", Json::null())
  try
    driver.accept_frame(
      builder.build(FrameKind::DriverSubmit, submit.to_payload()),
    )
  catch {
    SessionError::MessageGap(expected~, actual~) => {
      assert_eq(expected, 0L)
      assert_eq(actual, 1L)
    }
    _ => fail("expected driver sequence gap")
  } noraise {
    _ => fail("expected driver sequence gap")
  }
}

///|
test "driver client validates Coordinator status frames" {
  let client = DriverClientSession::new("cluster", "coordinator", "driver-1")
  let builder = FrameBuilder::new(
    "cluster",
    "coordinator",
    "driver",
    session_id="driver-1/driver/0",
  )
  let status = DriverStatus::new(
    "submission-1",
    "job",
    "digest",
    "Running",
    0L,
    None,
  )
  let accepted = client.accept_status(
    builder.build(FrameKind::DriverStatus, status.to_payload()),
  )
  assert_eq(accepted.phase(), "Running")
  assert_eq(client.session_id(), "driver-1/driver/0")
}
