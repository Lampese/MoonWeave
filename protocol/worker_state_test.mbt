///|
fn incarnation(
  value : String,
) -> @protocol.Incarnation raise @protocol.ProtocolError {
  @protocol.Incarnation::new(value)
}

///|
fn worker() -> @protocol.WorkerState raise @protocol.ProtocolError {
  @protocol.WorkerState::new("worker-a", incarnation("boot-1"))
}

///|
fn assignment(
  epoch : Int,
) -> @protocol.Assignment raise @protocol.ProtocolError {
  @protocol.Assignment::new(0, epoch, incarnation("boot-1"))
}

///|
test "assignment epoch fences stale output" {
  let state = worker()
  state.assign(assignment(7))
  state.assign(assignment(8))
  try state.ack_persisted(7, 0L) catch {
    @protocol.ProtocolError::StaleEpoch(expected~, actual~) => {
      assert_eq(expected, 8)
      assert_eq(actual, 7)
    }
    _ => fail("expected stale epoch")
  } noraise {
    _ => fail("expected stale epoch")
  }
}

///|
test "incarnation fences old process connections" {
  let state = worker()
  let old_assignment = @protocol.Assignment::new(0, 1, incarnation("boot-0"))
  try state.assign(old_assignment) catch {
    @protocol.ProtocolError::StaleIncarnation => ()
    _ => fail("expected stale incarnation")
  } noraise {
    _ => fail("expected stale incarnation")
  }
}

///|
test "credit limits outgoing batches" {
  let state = worker()
  state.assign(assignment(1))
  state.grant_credit(1, 3L, 300L)
  state.reserve_batch(1, 2L, 200L)
  guard state.assignment() is Some(current) else { fail("assignment missing") }
  assert_eq(current.credit_events(), 1L)
  assert_eq(current.credit_bytes(), 100L)
  assert_eq(current.next_offset(), 2L)
  try state.reserve_batch(1, 2L, 200L) catch {
    @protocol.ProtocolError::CreditExceeded(remaining~, requested~) => {
      assert_eq(remaining, 1L)
      assert_eq(requested, 2L)
    }
    _ => fail("expected credit exhaustion")
  } noraise {
    _ => fail("expected credit exhaustion")
  }
}

///|
test "persisted ACK cannot regress or acknowledge unsent data" {
  let state = worker()
  state.assign(assignment(1))
  state.grant_credit(1, 2L, 20L)
  state.reserve_batch(1, 2L, 20L)
  state.ack_persisted(1, 0L)
  try state.ack_persisted(1, -1L) catch {
    @protocol.ProtocolError::AckRegression(expected_at_least~, actual~) => {
      assert_eq(expected_at_least, 0L)
      assert_eq(actual, -1L)
    }
    _ => fail("expected ack regression")
  } noraise {
    _ => fail("expected ack regression")
  }
  try state.ack_persisted(1, 2L) catch {
    @protocol.ProtocolError::InvalidOffset(offset) => assert_eq(offset, 2L)
    _ => fail("expected invalid offset")
  } noraise {
    _ => fail("expected invalid offset")
  }
}

///|
test "resume accepts only a sent and non-regressing offset" {
  let state = worker()
  state.assign(assignment(1))
  state.grant_credit(1, 1L, 1L)
  state.reserve_batch(1, 1L, 1L)
  state.resume_from(1, 0L)
  guard state.assignment() is Some(current) else { fail("assignment missing") }
  assert_eq(current.acked_offset(), 0L)
  try state.resume_from(1, 2L) catch {
    @protocol.ProtocolError::InvalidOffset(offset) => assert_eq(offset, 2L)
    _ => fail("expected invalid resume offset")
  } noraise {
    _ => fail("expected invalid resume offset")
  }
}

///|
test "resume advances a restarted coordinator send cursor" {
  let state = worker()
  state.assign(assignment(1))
  state.resume_from(1, -1L)
  guard state.assignment() is Some(current) else { fail("assignment missing") }
  assert_eq(current.acked_offset(), -1L)
  assert_eq(current.next_offset(), 0L)
}

///|
test "resume rewinds an unconfirmed coordinator suffix" {
  let state = worker()
  state.assign(assignment(1))
  state.grant_credit(1, 3L, 3L)
  state.reserve_batch(1, 3L, 3L)
  state.resume_from(1, 0L)
  guard state.assignment() is Some(current) else { fail("assignment missing") }
  assert_eq(current.acked_offset(), 0L)
  assert_eq(current.next_offset(), 1L)
}
