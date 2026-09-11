///|
test "output delivery checkpoints are isolated by assignment epoch" {
  assert_eq(
    output_consumer_checkpoint_path("/state/sink-kafka-p3", 0),
    "/state/sink-kafka-p3-e0.wal",
  )
  assert_eq(
    output_consumer_checkpoint_path("/state/sink-kafka-p3", 7),
    "/state/sink-kafka-p3-e7.wal",
  )
}
