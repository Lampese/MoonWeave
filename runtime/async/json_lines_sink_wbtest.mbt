///|
async test "JSONL bounded history preserves gaps, concurrent writers and torn suffix recovery" {
  let root = @fs.tmpdir(prefix="moonweave-jsonl-bounded")
  defer @fs.rmdir(root, recursive=true)
  let path = root + "/output.jsonl"
  let record = (sequence : Int64) => TaskOutputRecord::{
    version: 1,
    task_id: "job:一/p0",
    partition: 0,
    epoch: 0,
    sequence,
    output: @task.TaskOutput::Custom({ "value": sequence.to_string() }),
  }
  let sink = JsonLinesSink::open(path, maximum_index_ranges=2)
  for sequence in 0..<600 {
    if sequence != 300 {
      sink.write(record(sequence.to_int64()))
    }
  }
  assert_eq(sink.written_count(), 599L)
  assert_eq(sink.ranges.length(), 2)
  assert_eq(sink.known.length(), 256)
  assert_false(sink.contains(record(300L).idempotency_key()))
  sink.close()
  // Explicit OpenOrCreate is essential: fs.write_file defaults to truncate
  // even with append=true. This injection must preserve the durable prefix.
  @fs.write_file(
    path,
    "{\"task_id\":",
    append=true,
    create_mode=@fs.OpenOrCreate,
  )
  let a = JsonLinesSink::open(path, maximum_index_ranges=2)
  defer a.close()
  let b = JsonLinesSink::open(path, maximum_index_ranges=2)
  defer b.close()
  assert_eq(a.written_count(), 599L)
  a.write(record(0L))
  a.write(record(300L))
  b.write(record(300L))
  b.write(record(600L))
  a.write({ ..record(600L), epoch: 4, })
  assert_eq(a.written_count(), 601L)
  assert_eq(b.written_count(), 601L)
  assert_eq(a.ranges.length(), 1)
  assert_eq(a.known.length(), 256)
  assert_eq(@fs.read_file(path).text().split("\n").count(), 602)
  let conflict = {
    ..record(600L),
    output: @task.TaskOutput::Custom({ "different": true }),
  }
  try a.write(conflict) catch {
    IdentityConflict(_) => ()
    _ => fail("expected recent identity conflict")
  } noraise {
    _ => fail("conflict accepted")
  }
}
