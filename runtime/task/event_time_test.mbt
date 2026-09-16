///|
fn timed_record(
  sequence : Int64,
  timestamp : Int64,
  value : Int64,
  input_id? : String = "main",
  parents? : Array[@causal.EventId] = [],
) -> @protocol.EventRecord raise {
  @protocol.EventRecord::new(
    @causal.EventId::new("timed", sequence),
    parents,
    "account",
    Json::object({ "value": Json::string(value.to_string()) }),
    event_time=timestamp,
    input_id~,
  )
}

///|
fn window_task() -> Task raise {
  Task::new("timed-task", "keyed-sum", OperatorKind::KeyedSum, 0, 0).with_event_time(
    TaskEventTimeConfig::new(window_size_ms=10L),
  )
}

///|
test "join pair history retires with buffered inputs including saturated expiry" {
  let task = Task::new("retire-join", "keyed-sum", OperatorKind::KeyedSum, 0, 0).with_event_time(
    TaskEventTimeConfig::new(join_before_ms=10L, join_after_ms=10L),
  )
  task.start()
  let left = timed_record(0L, 9223372036854775806L, 1L, input_id="left")
  let right = timed_record(1L, 9223372036854775806L, 2L, input_id="right")
  ignore(task.process(0L, TaskInput::new(left)))
  ignore(task.process(1L, TaskInput::new(right)))
  assert_eq(task.process_watermark(9223372036854775806L).length(), 1)
  assert_eq(task.snapshot().join_emitted().length(), 1)
  let restored = Task::new(
    "retire-join",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
  )
  restored.start()
  restored.restore(task.snapshot())
  assert_eq(restored.process_watermark(9223372036854775807L), [])
  assert_eq(restored.snapshot().join_emitted().length(), 0)
  assert_eq(restored.snapshot().join_left().length(), 0)
  assert_eq(restored.snapshot().join_right().length(), 0)
  assert_eq(restored.process(2L, TaskInput::new(left)), [])
}

///|
test "restoring malformed window or join buffers leaves the current Task intact" {
  let task = window_task()
  task.start()
  ignore(task.process(0L, TaskInput::new(timed_record(0L, 1L, 2L))))
  let snapshot = task.snapshot()
  for
    buckets in [
      [TaskWindowBucket::{ key: "account", start_ms: 1L, total: 2L, }],
      [
        TaskWindowBucket::{ key: "account", start_ms: 0L, total: 2L, },
        TaskWindowBucket::{ key: "account", start_ms: 0L, total: 2L, },
      ],
    ] {
    try task.restore({ ..snapshot, window_buckets: Some(buckets), }) catch {
      TaskError::InvalidTransition => ()
      _ => fail("unexpected snapshot error")
    } noraise {
      _ => fail("malformed window accepted")
    }
    assert_eq(task.snapshot(), snapshot)
  }
  let join = Task::new("bad-join", "keyed-sum", OperatorKind::KeyedSum, 0, 0).with_event_time(
    TaskEventTimeConfig::new(join_before_ms=2L, join_after_ms=2L),
  )
  join.start()
  ignore(
    join.process(0L, TaskInput::new(timed_record(0L, 10L, 1L, input_id="left"))),
  )
  ignore(
    join.process(
      1L,
      TaskInput::new(timed_record(1L, 10L, 2L, input_id="right")),
    ),
  )
  ignore(join.process_watermark(10L))
  let snapshot = join.snapshot()
  try join.restore({ ..snapshot, join_emitted: Some([]), }) catch {
    TaskError::InvalidTransition => ()
    _ => fail("unexpected snapshot error")
  } noraise {
    _ => fail("missing pair history would duplicate an output")
  }
  assert_eq(join.snapshot(), snapshot)
}

///|
test "event time restore preserves active windows watermark and late count" {
  let task = window_task()
  task.start()
  ignore(task.process(0L, TaskInput::new(timed_record(0L, 1L, 2L))))
  ignore(task.process(1L, TaskInput::new(timed_record(1L, 12L, 3L))))
  assert_eq(task.process_watermark(10L).length(), 1)
  ignore(task.process(2L, TaskInput::new(timed_record(2L, 2L, 100L))))
  let snapshot = task.snapshot()
  assert_eq(snapshot.late_events(), 1)
  let restored = window_task()
  restored.start()
  restored.restore(snapshot)
  assert_eq(restored.phase(), TaskPhase::Running)
  assert_eq(restored.snapshot(), snapshot)
  assert_eq(restored.process_watermark(20L), task.process_watermark(20L))
  assert_eq(restored.process_watermark(20L), [])
}

///|
test "join emits pairs before a watermark retires both inputs" {
  let task = Task::new("join-task", "keyed-sum", OperatorKind::KeyedSum, 0, 0).with_event_time(
    TaskEventTimeConfig::new(join_before_ms=5L, join_after_ms=10L),
  )
  task.start()
  ignore(
    task.process(
      0L,
      TaskInput::new(timed_record(0L, 100L, 1L, input_id="left")),
    ),
  )
  ignore(
    task.process(
      1L,
      TaskInput::new(timed_record(1L, 105L, 2L, input_id="right")),
    ),
  )
  let outputs = task.process_watermark(9223372036854775807L)
  assert_eq(outputs, [
    TaskOutput::Custom(
      Json::object({
        "left": Json::object({ "value": Json::string("1") }),
        "right": Json::object({ "value": Json::string("2") }),
      }),
    ),
  ])
  assert_eq(task.snapshot().join_left(), [])
  assert_eq(task.snapshot().join_right(), [])
  let restored = Task::new(
    "join-task",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
  )
  restored.start()
  restored.restore(task.snapshot())
  assert_eq(restored.process_watermark(9223372036854775807L), [])
}

///|
test "event time computation waits for causal admission" {
  let task = window_task()
  task.start()
  let parent = timed_record(0L, 1L, 2L)
  let child = timed_record(1L, 2L, 3L, parents=[parent.id()])
  ignore(task.process(0L, TaskInput::new(child)))
  assert_eq(task.pending_count(), 1)
  assert_eq(task.snapshot().window_buckets(), [])
  ignore(task.process(1L, TaskInput::new(parent)))
  assert_eq(task.pending_count(), 0)
  assert_eq(task.snapshot().window_buckets(), [
    TaskWindowBucket::{ key: "account", start_ms: 0L, total: 5L, },
  ])
}

///|
test "event time duplicate identity conflict is transactional" {
  let task = window_task()
  task.start()
  let event = timed_record(0L, 1L, 2L)
  ignore(task.process(0L, TaskInput::new(event)))
  ignore(task.process(1L, TaskInput::new(event)))
  let before = task.snapshot()
  try task.process(2L, TaskInput::new(timed_record(0L, 1L, 99L))) catch {
    TaskError::Causal(@causal.CausalError::IdentityConflict(_)) => ()
    _ => fail("expected conflicting retransmission to fail")
  } noraise {
    _ => fail("conflicting retransmission accepted")
  }
  assert_eq(task.snapshot(), before)
}

///|
test "watermark waits for causal parents and resumes after restore" {
  let task = window_task()
  task.start()
  let parent = timed_record(0L, 1L, 2L)
  let child = timed_record(1L, 2L, 3L, parents=[parent.id()])
  ignore(task.process(0L, TaskInput::new(child)))
  assert_eq(task.process_watermark(20L), [])
  assert_eq(task.watermark(), 2L)
  let restored = window_task()
  restored.start()
  restored.restore(task.snapshot())
  assert_eq(restored.process(1L, TaskInput::new(parent)), [
    TaskOutput::WindowedSum({
      key: "account",
      start_ms: 0L,
      end_ms: 10L,
      total: 5L,
    }),
  ])
  assert_eq(restored.watermark(), 20L)
}

///|
test "zero width interval join includes equal timestamps" {
  let task = Task::new("equal-join", "keyed-sum", OperatorKind::KeyedSum, 0, 0).with_event_time(
    TaskEventTimeConfig::new(join_before_ms=0L, join_after_ms=0L),
  )
  task.start()
  ignore(
    task.process(0L, TaskInput::new(timed_record(0L, 5L, 1L, input_id="left"))),
  )
  ignore(
    task.process(1L, TaskInput::new(timed_record(1L, 5L, 2L, input_id="right"))),
  )
  assert_eq(task.process_watermark(6L).length(), 1)
}

///|
test "window total overflow rejects the input without moving progress" {
  let task = window_task()
  task.start()
  ignore(
    task.process(0L, TaskInput::new(timed_record(0L, 1L, 9223372036854775807L))),
  )
  let before = task.snapshot()
  try task.process(1L, TaskInput::new(timed_record(1L, 2L, 1L))) catch {
    TaskError::Custom(_) => ()
    _ => fail("expected overflow")
  } noraise {
    _ => fail("overflow accepted")
  }
  assert_eq(task.snapshot(), before)
}
