///|
fn drain_test_session() -> @protocol.CoordinatorSession raise {
  let session = @protocol.CoordinatorSession::new(
    "drain-cluster", "coordinator", "worker",
  )
  let hello = @protocol.Hello::new("worker", "boot", [])
  let builder = @protocol.FrameBuilder::new("drain-cluster", "worker", "boot")
  ignore(
    session.accept_hello(
      builder.build(@protocol.FrameKind::Hello, hello.to_payload()),
    ),
  )
  ignore(
    session.activate(
      "drain-session",
      assignment=@protocol.AssignmentSpec::new(2, 3, "boot"),
    ),
  )
  session
}

///|
fn drain_test_frame(
  ack : @protocol.DrainAck,
  epoch? : Int = 3,
  session_id? : String = "drain-session",
) -> @protocol.Frame raise {
  @protocol.Frame::new(
    @protocol.protocol_version(),
    "drain-cluster",
    session_id,
    "ack-1",
    "worker",
    "boot",
    1L,
    epoch,
    @protocol.FrameKind::DrainAck,
    ack.to_payload(),
  )
}

///|
test "drain acknowledgement is bound to the request and fenced session" {
  let request = @protocol.DrainRequest::new(2, 3, -1L)
  let ack = @protocol.DrainAck::new(
    "drain-1", 2, 3, -1L, -1L, 0, "empty-frontier",
  )
  assert_eq(@protocol.DrainRequest::from_payload(request.to_payload()), request)
  assert_eq(@protocol.DrainAck::from_payload(ack.to_payload()), ack)
  let session = drain_test_session()
  try session.accept_frame(drain_test_frame(ack)) catch {
    error => assert_true(error is @protocol.SessionError::WrongPhase(..))
  } noraise {
    _ => fail("unsolicited drain acknowledgement accepted")
  }
  session.begin_drain("drain-1", request)
  for
    invalid in [
      @protocol.DrainAck::{ ..ack, request_id: "wrong-request", },
      @protocol.DrainAck::{ ..ack, partition: 1, },
      @protocol.DrainAck::{ ..ack, epoch: 4, },
      @protocol.DrainAck::{ ..ack, persisted_offset: 0L, applied_offset: 0L, },
    ] {
    try session.accept_frame(drain_test_frame(invalid)) catch {
      _ => ()
    } noraise {
      _ => fail("mismatched completion accepted")
    }
    assert_eq(session.drain_ack(), None)
  }
  try
    session.accept_frame(drain_test_frame(ack, session_id="stale-session"))
  catch {
    error => assert_true(error is @protocol.SessionError::SessionMismatch)
  } noraise {
    _ => fail("stale session accepted")
  }
  try session.accept_frame(drain_test_frame(ack, epoch=2)) catch {
    _ => ()
  } noraise {
    _ => fail("stale frame epoch accepted")
  }
  let frame = drain_test_frame(ack)
  session.accept_frame(frame)
  session.accept_frame(frame)
  assert_eq(session.drain_ack(), Some(ack))
  try session.reserve_delta(2, 3, 1L, 1L) catch {
    error => assert_true(error is @protocol.SessionError::WrongPhase(..))
  } noraise {
    _ => fail("new data accepted during drain")
  }
}

///|
test "drain acknowledgement validates counters and preserves an incomplete report" {
  let outstanding = drain_test_session()
  outstanding.restore_send_cursor(2, 3, 1L)
  try
    outstanding.begin_drain("premature", @protocol.DrainRequest::new(2, 3, -1L))
  catch {
    _ => ()
  } noraise {
    _ => fail("drain accepted before all sent input was applied")
  }
  assert_eq(outstanding.phase(), @protocol.SessionPhase::Running)
  let ack = @protocol.DrainAck::new(
    "drain-2", 2, 3, -1L, -1L, 2, "pending-frontier",
  )
  let session = drain_test_session()
  session.begin_drain("drain-2", @protocol.DrainRequest::new(2, 3, -1L))
  session.accept_frame(drain_test_frame(ack))
  assert_eq(session.drain_ack().unwrap().pending_events, 2)
  let invalid : @protocol.DrainAck = { ..ack, pending_events: -1, }
  try @protocol.DrainAck::from_payload(invalid.to_payload()) catch {
    _ => ()
  } noraise {
    _ => fail("negative pending count accepted")
  }
}

///|
test "drain acknowledgement optionally carries a durable Task output summary" {
  let ack = @protocol.DrainAck::new(
    "drain-output",
    2,
    3,
    1L,
    1L,
    0,
    "frontier",
    task_id="task/p2/e3",
    output_count=4L,
    output_digest="manifest-output",
  )
  assert_eq(@protocol.DrainAck::from_payload(ack.to_payload()), ack)
  assert_eq(ack.task_id(), Some("task/p2/e3"))
  assert_eq(ack.output_count(), Some(4L))
  assert_eq(ack.output_digest(), Some("manifest-output"))
  try
    @protocol.DrainAck::new(
      "partial-output",
      2,
      3,
      1L,
      1L,
      0,
      "frontier",
      task_id="task/p2/e3",
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("partial output summary accepted")
  }
}
