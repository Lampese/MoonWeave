///|
test "graph witnesses survive pending checkpoint and suppress foreign-key updates" {
  let own = @plan.key_partition("a", 2)
  let task = @task.Task::new(
    "graph-task",
    "keyed-sum",
    @task.OperatorKind::KeyedSum,
    own,
    0,
  ).with_graph_input({ partition_count: 2, emit_packets: false, })
  task.start()
  let parent = @protocol.EventRecord::new(
    @causal.EventId::new("source", 0L),
    [],
    "b",
    @task.graph_skip_payload(),
  )
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("source", 1L),
    [parent.id()],
    "a",
    { "value": "7" },
  )
  assert_eq(task.process(0L, @task.TaskInput::new(child)), [])
  let snapshot = task.snapshot()
  let restored = @task.Task::new(
    "graph-task",
    "keyed-sum",
    @task.OperatorKind::KeyedSum,
    own,
    0,
  ).with_graph_input({ partition_count: 2, emit_packets: false, })
  restored.start()
  restored.restore(snapshot)
  assert_eq(restored.pending_count(), 1)
  assert_eq(restored.process(1L, @task.TaskInput::new(parent)), [
    @task.TaskOutput::KeyedSum({ key: "a", total: 7L, }),
  ])
  restored.compact(
    @causal.CausalRetirement::new("checkpoint", [
      @causal.EventRange::new("source", 0L, 1L),
    ]),
  )
  task.restore(restored.snapshot())
  assert_eq(task.pending_count(), 0)
  let next = @protocol.EventRecord::new(
    @causal.EventId::new("source", 2L),
    [child.id()],
    "a",
    { "value": "5" },
  )
  assert_eq(task.process(2L, @task.TaskInput::new(next)), [
    @task.TaskOutput::KeyedSum({ key: "a", total: 12L, }),
  ])
}
