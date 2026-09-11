///|
async test "SourceLog keeps large adjacent identities distinct after replay" {
  let directory = @fs.tmpdir(prefix="moonweave-source-int64-")
  defer @fs.rmdir(directory, recursive=true)
  for initial in [2147483648L, 9007199254740992L, 9223372036854775805L] {
    let path = directory + "/" + initial.to_string() + ".wal"
    let first = source_event("kafka/events/0", initial, "key", "B")
    let second = source_event("kafka/events/0", initial + 1L, "key", "C")
    let log = SourceLog::open(path)
    defer log.close()
    assert_eq(log.append(0, initial, first), initial)
    assert_eq(log.append(0, initial, second), initial + 1L)
    log.close()
    let restored = SourceLog::open(path)
    defer restored.close()
    assert_eq(restored.event_count(), 2)
    assert_eq(restored.next_offset(0), Some(initial + 2L))
    assert_eq(restored.append(0, initial, second), initial + 1L)
    assert_eq(restored.append(0, initial, first), initial)
    assert_eq(restored.batches(0, 1, initial)[0].events(), [first, second])
    let conflict = source_event("kafka/events/0", initial + 1L, "key", "D")
    try restored.append(0, initial, conflict) catch {
      SourceLogError::IdentityConflict(id) => assert_eq(id, second.id())
      _ => fail("expected source identity conflict")
    } noraise {
      _ => fail("large identity lost conflict detection")
    }
  }
}

///|
async test "SourceLog rejects offset exhaustion before WAL append and after replay" {
  let directory = @fs.tmpdir(prefix="moonweave-source-exhausted-")
  defer @fs.rmdir(directory, recursive=true)
  let path = directory + "/source.wal"
  let first = source_event("source", 0L, "key", "B")
  let second = source_event("source", 1L, "key", "C")
  let log = SourceLog::open(path)
  defer log.close()
  assert_eq(log.append(0, 9223372036854775806L, first), 9223372036854775806L)
  let before = @fs.read_file(path).binary()
  try log.append(0, 0L, second) catch {
    SourceLogError::InvalidOffset(9223372036854775807L) => ()
    _ => fail("expected source offset exhaustion")
  } noraise {
    _ => fail("source offset wrapped")
  }
  assert_eq(@fs.read_file(path).binary(), before)
  assert_eq(log.event_count(), 1)
  log.close()
  let restored = SourceLog::open(path)
  defer restored.close()
  assert_eq(restored.next_offset(0), Some(9223372036854775807L))
  assert_eq(restored.append(0, 0L, first), 9223372036854775806L)
  try restored.append(0, 0L, second) catch {
    SourceLogError::InvalidOffset(9223372036854775807L) => ()
    _ => fail("expected replayed source offset exhaustion")
  } noraise {
    _ => fail("replayed source offset wrapped")
  }
  assert_eq(@fs.read_file(path).binary(), before)
}

///|
async test "Worker checkpoint restores large causal identities and pending parents" {
  let directory = @fs.tmpdir(prefix="moonweave-checkpoint-int64-")
  defer @fs.rmdir(directory, recursive=true)
  let start = 9007199254740992L
  let parent_id = @causal.EventId::new("kafka/events/0", 9007199254740993L)
  let child = @protocol.EventRecord::new(
    @causal.EventId::new("kafka/events/0", 9007199254740992L),
    [parent_id],
    "key",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  let parent = @protocol.EventRecord::new(
    parent_id,
    [],
    "key",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let task = @task.Task::new(
    "task",
    "reachability",
    @task.OperatorKind::Reachability,
    0,
    0,
    start_offset=start,
  )
  task.start()
  assert_eq(task.process(start, @task.TaskInput::new(child)), [])
  let snapshot = task.snapshot()
  let checkpoint = Checkpoint::new(
    0,
    0,
    0L,
    start,
    start,
    snapshot.frontier_digest(),
    state_schema=snapshot.state_schema(),
    operator_state=@json.to_json(snapshot),
  )
  checkpoint.save(directory + "/worker.json")
  let loaded = Checkpoint::load(directory + "/worker.json").unwrap()
  assert_eq(loaded.applied_offset(), start)
  let restored_snapshot : @task.TaskSnapshot = @json.from_json(
    loaded.operator_state().unwrap(),
  )
  assert_eq(restored_snapshot, snapshot)
  let restored = @task.Task::new(
    "task",
    "reachability",
    @task.OperatorKind::Reachability,
    0,
    0,
    start_offset=start,
  )
  restored.start()
  restored.restore(restored_snapshot)
  assert_eq(restored.pending_count(), 1)
  assert_eq(restored.process(start + 1L, @task.TaskInput::new(parent)), [
    @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "B")),
    @task.TaskOutput::Reachable(@reachability.Reachable::new("A", "C")),
    @task.TaskOutput::Reachable(@reachability.Reachable::new("B", "C")),
  ])
  assert_eq(restored.offset(), start + 1L)
  assert_eq(restored.pending_count(), 0)
  assert_eq(restored.process(start + 2L, @task.TaskInput::new(parent)), [])
}

///|
async test "compacted outbox keeps large event identities in duplicate fingerprints" {
  let directory = @fs.tmpdir(prefix="moonweave-outbox-int64-")
  defer @fs.rmdir(directory, recursive=true)
  let path = directory + "/outbox.wal"
  let event = source_event("kafka/events/0", 9007199254740992L, "key", "B")
  let other = source_event("kafka/events/0", 9007199254740993L, "key", "B")
  let delta = @protocol.DeltaBatch::new("delta", 0, 0, 1, 0, [event])
  let conflict = @protocol.DeltaBatch::new("delta", 0, 0, 1, 0, [other])
  let outbox = DurableExchangeOutbox::open(path, 1)
  defer outbox.close()
  outbox.push(delta)
  outbox.ack(delta.delta_id())
  outbox.compact()
  outbox.close()
  let restored = DurableExchangeOutbox::open(path, 1)
  defer restored.close()
  restored.push(delta)
  assert_eq(restored.pending_count(), 0)
  try restored.push(conflict) catch {
    DurableOutboxError::IdentityConflict(_) => ()
    _ => fail("expected large identity fingerprint conflict")
  } noraise {
    _ => fail("adjacent large identities shared a duplicate fingerprint")
  }
}
