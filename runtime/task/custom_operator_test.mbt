///|
test "custom operator executes causal inputs and restores from the v1 snapshot" {
  let operator = CustomOperator::new(
    "uppercase",
    1,
    "custom:uppercase:v1",
    "uppercase-state:v1",
    "json-event:v1",
    "json-output:v1",
    "moonweave-task-json-v1",
    "test-deps-v1",
    true,
    false,
    _ => (),
    input => {
      [
        Json::object({
          "key": Json::string(input.event().key()),
          "value": input.event().payload(),
        }),
      ]
    },
  )
  let task = Task::new_custom("custom-task", "uppercase", operator, 0, 0)
  task.start()
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("custom", 0),
    [],
    "hello",
    Json::object({ "value": Json::string("world") }),
  )
  let expected = [
    TaskOutput::Custom(
      Json::object({
        "key": Json::string("hello"),
        "value": Json::object({ "value": Json::string("world") }),
      }),
    ),
  ]
  assert_eq(
    task.process_batch(@protocol.EventBatch::new(0, 0, 0L, [event])),
    expected,
  )
  let snapshot = task.snapshot()
  let restored = Task::new_custom("custom-task", "uppercase", operator, 0, 0)
  restored.start()
  restored.restore(snapshot)
  assert_eq(restored.outputs(), expected)
  assert_eq(restored.event_count(), 1)
}

///|
test "custom operator preserves causally pending inputs" {
  let operator = CustomOperator::new(
    "pending",
    1,
    "custom:pending:v1",
    "pending-state:v1",
    "json-event:v1",
    "json-output:v1",
    "moonweave-task-json-v1",
    "test-deps-v1",
    true,
    false,
    _ => (),
    input => [Json::string(input.event().id().origin())],
  )
  let task = Task::new_custom("pending-task", "pending", operator, 0, 0)
  task.start()
  let root = @protocol.EventRecord::new(
    @causal.EventId::new("pending", 0),
    [],
    "k",
    Json::null(),
  )
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("pending", 1),
    [root.id()],
    "k",
    Json::null(),
  )
  assert_eq(task.process(0L, TaskInput::new(child)), [])
  assert_eq(task.pending_count(), 1)
  assert_eq(task.process(1L, TaskInput::new(root)).length(), 2)
  assert_eq(task.pending_count(), 0)
}
