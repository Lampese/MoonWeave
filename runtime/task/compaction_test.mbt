///|
test "running window Task recovers materialized buffers after repeated history reclamation" {
  let mut task = Task::new(
    "compact-window",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
    max_state_events=32,
  ).with_event_time(TaskEventTimeConfig::new(window_size_ms=10L))
  task.start()
  for cycle in 0..<100 {
    let ids = []
    for index in 0..<5 {
      let sequence = (cycle * 5 + index).to_int64()
      let record = timed_record(sequence, cycle.to_int64() * 10L + 1L, 2L)
      ids.push(record.id())
      ignore(task.process(sequence, TaskInput::new(record)))
    }
    assert_eq(task.process_watermark((cycle + 1).to_int64() * 10L), [
      TaskOutput::WindowedSum({
        key: "account",
        start_ms: cycle.to_int64() * 10L,
        end_ms: (cycle + 1).to_int64() * 10L,
        total: 10L,
      }),
    ])
    task.compact(
      @causal.CausalRetirement::from_ids(
        "window-checkpoint-" + cycle.to_string(),
        ids,
      ),
    )
    assert_eq(task.state_memory_count(), 1)
    assert_eq(task.phase(), TaskPhase::Running)
    let snapshot = task.snapshot()
    let restored = Task::new(
      "compact-window",
      "keyed-sum",
      OperatorKind::KeyedSum,
      0,
      0,
      max_state_events=32,
    ).with_event_time(TaskEventTimeConfig::new(window_size_ms=10L))
    restored.start()
    restored.restore(snapshot)
    assert_eq(restored.snapshot(), snapshot)
    task = restored
  }
  assert_eq(task.snapshot().emitted_outputs(), 100)
  assert_eq(task.process(500L, TaskInput::new(timed_record(0L, 1L, 2L))), [])
  assert_eq(task.snapshot().late_events(), 0)
}

///|
test "join retains unmatched input payload when its causal identity is retired" {
  let task = Task::new(
    "compact-join",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
  ).with_event_time(
    TaskEventTimeConfig::new(join_before_ms=5L, join_after_ms=5L),
  )
  task.start()
  let left = timed_record(0L, 10L, 2L, input_id="left")
  ignore(task.process(0L, TaskInput::new(left)))
  ignore(task.process_watermark(10L))
  task.compact(@causal.CausalRetirement::from_ids("join-left", [left.id()]))
  let restored = Task::new(
    "compact-join",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
  )
  restored.start()
  restored.restore(task.snapshot())
  let right = timed_record(1L, 12L, 3L, input_id="right", parents=[left.id()])
  let result = restored.process(1L, TaskInput::new(right))
  assert_eq(result, [
    TaskOutput::Custom({ "left": { "value": "2" }, "right": { "value": "3" } }),
  ])
  assert_eq(restored.process(2L, TaskInput::new(left)), [])
  assert_eq(restored.process_watermark(30L), [])
  assert_eq(restored.snapshot().join_emitted().length(), 0)
}

///|
test "running aggregation resumes from totals instead of retired input events" {
  let task = Task::new(
    "compact-sum",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
    max_state_events=16,
  )
  task.start()
  let first = timed_record(0L, 1L, 2L)
  ignore(task.process(0L, TaskInput::new(first)))
  task.compact(
    @causal.CausalRetirement::from_ids("sum-checkpoint", [first.id()]),
  )
  let restored = Task::new(
    "compact-sum",
    "keyed-sum",
    OperatorKind::KeyedSum,
    0,
    0,
    max_state_events=16,
  )
  restored.start()
  restored.restore(task.snapshot())
  assert_eq(
    restored.process(
      1L,
      TaskInput::new(timed_record(1L, 2L, 3L, parents=[first.id()])),
    ),
    [TaskOutput::KeyedSum(@aggregation.KeyedSum::new("account", 5L))],
  )
  assert_eq(restored.snapshot().emitted_outputs(), 2)
}
