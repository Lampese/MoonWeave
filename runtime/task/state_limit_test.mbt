///|
test "task state limit fails closed before committing a batch" {
  let task = Task::new(
    "limited-reachability",
    "reachability",
    OperatorKind::Reachability,
    0,
    0,
    max_state_events=1,
  )
  task.start()
  let first = edge_event("limited", 0, "A", "B")
  let second = edge_event("limited", 1, "B", "C")
  try
    task.process_batch(@protocol.EventBatch::new(0, 0, 0L, [first, second]))
  catch {
    TaskError::StateLimitExceeded(limit~, actual~) => {
      assert_eq(limit, 1)
      // Causal payload, materialized edge, and derived fact are retained.
      assert_eq(actual, 3)
      assert_eq(task.offset(), -1L)
      assert_eq(task.event_count(), 0)
    }
    _ => fail("expected state limit rejection")
  } noraise {
    _ => fail("expected state limit rejection")
  }
  assert_eq(task.state_event_count(), 0)
}
