///|
test "frame builders own independent directional sequences" {
  let worker = @protocol.FrameBuilder::new("cluster-a", "worker-a", "boot-1")
  let coordinator = @protocol.FrameBuilder::new(
    "cluster-a", "coordinator", "boot-1",
  )
  let worker_hello = worker.build(@protocol.FrameKind::Hello, "{}")
  let coordinator_welcome = coordinator.build(
    @protocol.FrameKind::Welcome,
    "{}",
  )
  assert_eq(worker_hello.message_sequence(), 0L)
  assert_eq(coordinator_welcome.message_sequence(), 0L)
  assert_eq(worker_hello.sender_id(), "worker-a")
  assert_eq(coordinator_welcome.sender_id(), "coordinator")
  assert_eq(worker.next_sequence(), 1L)
  assert_eq(coordinator.next_sequence(), 1L)
}

///|
test "builder changes session and assignment metadata explicitly" {
  let builder = @protocol.FrameBuilder::new(
    "cluster-a", "coordinator", "boot-1",
  )
  builder.set_session_id("session-a")
  builder.set_assignment_epoch(9)
  let frame = builder.build(@protocol.FrameKind::Assign, "{}")
  assert_eq(frame.session_id(), "session-a")
  assert_eq(frame.assignment_epoch(), 9)
}
