///|
fn session_frame(
  sequence : Int64,
  kind : @protocol.FrameKind,
  payload : String,
  session_id? : String = "bootstrap",
  epoch? : Int = 0,
  incarnation? : String = "boot-1",
  message_id? : String,
) -> @protocol.Frame raise @protocol.ProtocolError {
  @protocol.Frame::new(
    1,
    "cluster-a",
    session_id,
    match message_id {
      Some(value) => value
      None => "message-\{sequence}"
    },
    "coordinator",
    incarnation,
    sequence,
    epoch,
    kind,
    payload,
  )
}

///|
fn session_worker() -> @protocol.WorkerSession raise @protocol.SessionError {
  @protocol.WorkerSession::new("cluster-a", "worker-a", "boot-1")
}

///|
test "welcome starts a session and installs assignment" {
  let state = session_worker()
  let assignment = @protocol.AssignmentSpec::new(
    0,
    7,
    "boot-1",
    start_offset=12L,
  )
  let welcome = @protocol.Welcome::new("session-a", assignment~)
  let frame = session_frame(
    0L,
    @protocol.FrameKind::Welcome,
    welcome.to_payload(),
    epoch=7,
  )
  state.accept_frame(frame)
  assert_eq(state.phase(), @protocol.SessionPhase::Running)
  assert_eq(state.session_id(), "session-a")
  assert_eq(state.next_sequence(), 1L)
  guard state.state().assignment() is Some(installed) else {
    fail("assignment should be installed")
  }
  assert_eq(installed.partition(), 0)
  assert_eq(installed.epoch(), 7)
  assert_eq(installed.next_offset(), 12L)
}

///|
test "session rejects wrong cluster, sender and incarnation" {
  let state = session_worker()
  let welcome = @protocol.Welcome::new("session-a")
  let valid = session_frame(
    0L,
    @protocol.FrameKind::Welcome,
    welcome.to_payload(),
  )
  let wrong_cluster = @protocol.Frame::new(
    1,
    "cluster-b",
    "bootstrap",
    "message-0",
    "worker-a",
    "boot-1",
    0L,
    0,
    @protocol.FrameKind::Welcome,
    welcome.to_payload(),
  )
  try state.accept_frame(wrong_cluster) catch {
    @protocol.SessionError::ClusterMismatch => ()
    _ => fail("expected cluster mismatch")
  } noraise {
    _ => fail("expected cluster mismatch")
  }
  let wrong_sender = @protocol.Frame::new(
    1,
    "cluster-a",
    "bootstrap",
    "message-0",
    "worker-b",
    "boot-1",
    0L,
    0,
    @protocol.FrameKind::Welcome,
    welcome.to_payload(),
  )
  try state.accept_frame(wrong_sender) catch {
    @protocol.SessionError::SenderMismatch => ()
    _ => fail("expected sender mismatch")
  } noraise {
    _ => fail("expected sender mismatch")
  }
  let wrong_incarnation = @protocol.Frame::new(
    1,
    "cluster-a",
    "bootstrap",
    "message-0",
    "coordinator",
    "boot-2",
    0L,
    0,
    @protocol.FrameKind::Welcome,
    welcome.to_payload(),
  )
  try state.accept_frame(wrong_incarnation) catch {
    @protocol.SessionError::IncarnationMismatch => ()
    _ => fail("expected incarnation mismatch")
  } noraise {
    _ => fail("expected incarnation mismatch")
  }
  state.accept_frame(valid)
}

///|
test "session enforces contiguous message sequence and session id" {
  let state = session_worker()
  let welcome = @protocol.Welcome::new("session-a")
  let first = session_frame(
    0L,
    @protocol.FrameKind::Welcome,
    welcome.to_payload(),
  )
  state.accept_frame(first)
  let heartbeat = @protocol.Heartbeat::new(0, 0, 0, 0L)
  let gap = session_frame(
    2L,
    @protocol.FrameKind::Heartbeat,
    heartbeat.to_payload(),
    session_id="session-a",
  )
  try state.accept_frame(gap) catch {
    @protocol.SessionError::MessageGap(expected~, actual~) => {
      assert_eq(expected, 1L)
      assert_eq(actual, 2L)
    }
    _ => fail("expected sequence gap")
  } noraise {
    _ => fail("expected sequence gap")
  }
  let duplicate = session_frame(
    0L,
    @protocol.FrameKind::Heartbeat,
    heartbeat.to_payload(),
    session_id="session-a",
    message_id="message-old-sequence",
  )
  try state.accept_frame(duplicate) catch {
    @protocol.SessionError::MessageRegression(expected_at_least~, actual~) => {
      assert_eq(expected_at_least, 1L)
      assert_eq(actual, 0L)
    }
    _ => fail("expected sequence regression")
  } noraise {
    _ => fail("expected sequence regression")
  }
  let wrong_session = session_frame(
    1L,
    @protocol.FrameKind::Heartbeat,
    heartbeat.to_payload(),
    session_id="session-b",
  )
  try state.accept_frame(wrong_session) catch {
    @protocol.SessionError::SessionMismatch => ()
    _ => fail("expected session mismatch")
  } noraise {
    _ => fail("expected session mismatch")
  }
}

///|
test "worker session routes assignment and drain" {
  let state = session_worker()
  let assignment = @protocol.AssignmentSpec::new(0, 3, "boot-1")
  let welcome = @protocol.Welcome::new("session-a", assignment~)
  state.accept_frame(
    session_frame(
      0L,
      @protocol.FrameKind::Welcome,
      welcome.to_payload(),
      epoch=3,
    ),
  )
  let replacement = @protocol.AssignmentSpec::new(
    0,
    4,
    "boot-1",
    start_offset=2L,
  )
  state.accept_frame(
    session_frame(
      1L,
      @protocol.FrameKind::Assign,
      replacement.to_payload(),
      session_id="session-a",
      epoch=4,
    ),
  )
  guard state.state().assignment() is Some(current) else {
    fail("assignment should be present")
  }
  assert_eq(current.epoch(), 4)
  assert_eq(current.next_offset(), 2L)
  let drain = session_frame(
    2L,
    @protocol.FrameKind::Drain,
    "{}",
    session_id="session-a",
    epoch=4,
  )
  state.accept_frame(drain)
  assert_eq(state.phase(), @protocol.SessionPhase::Draining)
}

///|
fn worker_frame(
  sequence : Int64,
  kind : @protocol.FrameKind,
  payload : String,
  session_id? : String = "bootstrap",
  epoch? : Int = 0,
  incarnation? : String = "boot-1",
  message_id? : String,
) -> @protocol.Frame raise @protocol.ProtocolError {
  @protocol.Frame::new(
    1,
    "cluster-a",
    session_id,
    match message_id {
      Some(value) => value
      None => "worker-message-\{sequence}"
    },
    "worker-a",
    incarnation,
    sequence,
    epoch,
    kind,
    payload,
  )
}

///|
fn coordinator_worker() -> @protocol.CoordinatorSession raise @protocol.SessionError {
  @protocol.CoordinatorSession::new("cluster-a", "coordinator", "worker-a")
}

///|
test "coordinator accepts hello, activates assignment, and tracks progress" {
  let coordinator = coordinator_worker()
  let hello = @protocol.Hello::new("worker-a", "boot-1", ["reachability"])
  let hello_frame = worker_frame(
    0L,
    @protocol.FrameKind::Hello,
    hello.to_payload(),
  )
  assert_eq(coordinator.accept_hello(hello_frame), hello)
  assert_eq(coordinator.phase(), @protocol.SessionPhase::Handshaking)
  assert_eq(coordinator.incarnation(), "boot-1")
  let spec = @protocol.AssignmentSpec::new(0, 7, "boot-1", start_offset=12L)
  let welcome = coordinator.activate("session-a", assignment=spec)
  assert_eq(welcome.session_id(), "session-a")
  assert_eq(coordinator.phase(), @protocol.SessionPhase::Running)
  let progress = @protocol.Progress::new(0, 7, 11L, 11L, "frontier-11")
  coordinator.accept_frame(
    worker_frame(
      1L,
      @protocol.FrameKind::Progress,
      progress.to_payload(),
      session_id="session-a",
      epoch=7,
    ),
  )
  assert_eq(coordinator.last_persisted_offset(), 11L)
  assert_eq(coordinator.last_applied_offset(), 11L)
}

///|
test "coordinator rejects stale progress and accepts duplicate retransmission" {
  let coordinator = coordinator_worker()
  let hello = @protocol.Hello::new("worker-a", "boot-1", [])
  let hello_frame = worker_frame(
    0L,
    @protocol.FrameKind::Hello,
    hello.to_payload(),
  )
  ignore(coordinator.accept_hello(hello_frame))
  ignore(
    coordinator.activate(
      "session-a",
      assignment=@protocol.AssignmentSpec::new(0, 1, "boot-1"),
    ),
  )
  let heartbeat = @protocol.Heartbeat::new(0, 1, 0, -1L)
  let first = worker_frame(
    1L,
    @protocol.FrameKind::Heartbeat,
    heartbeat.to_payload(),
    session_id="session-a",
    epoch=1,
    message_id="heartbeat-1",
  )
  coordinator.accept_frame(first)
  coordinator.accept_frame(first)
  assert_eq(coordinator.next_sequence(), 2L)
  let stale = @protocol.Progress::new(0, 1, -1L, -1L, "frontier--1")
  let stale_frame = worker_frame(
    2L,
    @protocol.FrameKind::Progress,
    stale.to_payload(),
    session_id="session-a",
    epoch=1,
  )
  coordinator.accept_frame(stale_frame)
  let regression = @protocol.Progress::new(0, 1, -1L, -1L, "frontier--1")
  let regression_frame = worker_frame(
    1L,
    @protocol.FrameKind::Progress,
    regression.to_payload(),
    session_id="session-a",
    epoch=1,
    message_id="progress-old-sequence",
  )
  try coordinator.accept_frame(regression_frame) catch {
    @protocol.SessionError::MessageRegression(expected_at_least~, actual~) => {
      assert_eq(expected_at_least, 3L)
      assert_eq(actual, 1L)
    }
    _ => fail("expected sequence regression")
  } noraise {
    _ => fail("expected sequence regression")
  }
}

///|
test "coordinator exposes a stable next send offset" {
  let coordinator = coordinator_worker()
  let hello = @protocol.Hello::new("worker-a", "boot-1", [])
  ignore(
    coordinator.accept_hello(
      worker_frame(0L, @protocol.FrameKind::Hello, hello.to_payload()),
    ),
  )
  ignore(
    coordinator.activate(
      "session-a",
      assignment=@protocol.AssignmentSpec::new(2, 4, "boot-1", start_offset=9L),
    ),
  )
  assert_eq(coordinator.next_offset(2, 4), 9L)
}

///|
test "coordinator rejects progress ahead of the source cursor" {
  let coordinator = coordinator_worker()
  let hello = @protocol.Hello::new("worker-a", "boot-1", [])
  ignore(
    coordinator.accept_hello(
      worker_frame(0L, @protocol.FrameKind::Hello, hello.to_payload()),
    ),
  )
  ignore(
    coordinator.activate(
      "session-a",
      assignment=@protocol.AssignmentSpec::new(0, 1, "boot-1"),
    ),
  )
  let progress = @protocol.Progress::new(0, 1, 0L, 0L, "offset:0")
  try
    coordinator.accept_frame(
      worker_frame(
        1L,
        @protocol.FrameKind::Progress,
        progress.to_payload(),
        session_id="session-a",
        epoch=1,
      ),
    )
  catch {
    @protocol.SessionError::Assignment(
      @protocol.ProtocolError::InvalidOffset(0L)
    ) => ()
    _ => fail("expected progress cursor rejection")
  } noraise {
    _ => fail("expected progress cursor rejection")
  }
}
