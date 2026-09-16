///|
test "watermarks wait for unseen inputs and use the active minimum" {
  let progress = SourceWatermarks::new(["left", "right"], 0L)
  progress.observe("left", 100L, 1L)
  assert_eq(progress.advance(1L), -1L)
  progress.observe("right", 80L, 2L)
  assert_eq(progress.advance(2L), 80L)
  progress.observe("right", 120L, 3L)
  assert_eq(progress.advance(3L), 100L)
  progress.observe("left", 20L, 4L)
  assert_eq(progress.advance(4L), 100L)
}

///|
test "idle input reactivation and restart preserve the published frontier" {
  let progress = SourceWatermarks::new(
    ["left", "right"],
    0L,
    idle_timeout_ms=10L,
  )
  progress.observe("right", 10L, 0L)
  progress.observe("left", 100L, 9L)
  assert_eq(progress.advance(10L), 100L)
  let restored = SourceWatermarks::restore(progress.snapshot())
  restored.observe("right", 20L, 11L)
  assert_eq(restored.advance(11L), 100L)
  restored.observe("right", 120L, 12L)
  restored.observe("left", 150L, 12L)
  assert_eq(restored.advance(12L), 120L)
  assert_eq(restored.advance(100L), 120L)
}

///|
test "watermark input validation preserves prior progress" {
  let progress = SourceWatermarks::new(["main"], 0L)
  let before = progress.snapshot()
  try progress.observe("missing", 1L, 1L) catch {
    FrameIoError::Handler(_) => ()
    _ => fail("unexpected error")
  } noraise {
    _ => fail("unknown input accepted")
  }
  assert_eq(progress.snapshot(), before)
}

///|
test "Kafka watermark waits for every assigned partition across idle and expansion" {
  let progress = SourceWatermarks::new(["main"], 0L, idle_timeout_ms=10L)
  progress.assign_partitions("main", ["kafka/sales/0", "kafka/sales/1"], 0L)
  progress.observe("main", 100L, 1L, source_id="kafka/sales/0")
  assert_eq(progress.advance(1L), -1L)
  progress.observe("main", 20L, 2L, source_id="kafka/sales/1")
  assert_eq(progress.advance(2L), 20L)
  progress.observe("main", 200L, 11L, source_id="kafka/sales/0")
  assert_eq(progress.advance(12L), 200L)
  let restored = SourceWatermarks::restore(progress.snapshot())
  restored.observe("main", 30L, 13L, source_id="kafka/sales/1")
  assert_eq(restored.advance(13L), 200L)
  restored.assign_partitions(
    "main",
    ["kafka/sales/0", "kafka/sales/1", "kafka/sales/2"],
    14L,
  )
  restored.observe("main", 300L, 15L, source_id="kafka/sales/0")
  restored.observe("main", 300L, 15L, source_id="kafka/sales/1")
  assert_eq(restored.advance(15L), 200L)
  restored.observe("main", 250L, 16L, source_id="kafka/sales/2")
  assert_eq(restored.advance(16L), 250L)
}

///|
test "one Kafka stream can multiplex both join inputs without a silent partition" {
  let progress = SourceWatermarks::new(["left", "right"], 0L)
  for input in ["left", "right"] {
    progress.assign_partitions(input, ["kafka/both/0", "kafka/both/1"], 0L)
  }
  progress.observe("left", 20L, 1L, source_id="kafka/both/0")
  assert_eq(progress.advance(1L), -1L)
  progress.observe("right", 15L, 2L, source_id="kafka/both/1")
  assert_eq(progress.advance(2L), 15L)
  let snapshot = progress.snapshot()
  let inputs = snapshot.inputs.copy()
  inputs.push({
    input_id: "left",
    source_id: None,
    maximum_timestamp: -1L,
    last_activity: 0L,
  })
  let malformed = { ..snapshot, inputs, }
  try SourceWatermarks::restore(malformed) catch {
    FrameIoError::Handler(_) => ()
    _ => fail("unexpected error")
  } noraise {
    _ => fail("mixed placeholder and partition state accepted")
  }
}
