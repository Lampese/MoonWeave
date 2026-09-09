///|
async test "JSONL source commits and reopens from its physical cursor" {
  let directory = @fs.tmpdir(prefix="moonweave-jsonl-source")
  let path = directory + "/events.jsonl"
  defer {
    for name in @fs.readdir(directory) {
      @fs.remove(directory + "/" + name) catch {
        _ => ()
      }
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  let first = Event::new(
    @causal.EventId::new("jsonl", 0),
    [],
    "a",
    Json::string("one"),
  )
  let second = Event::new(
    @causal.EventId::new("jsonl", 1),
    [],
    "b",
    Json::string("two"),
  )
  @fs.write_file(
    path,
    @json.to_json(first).stringify() +
    "\n" +
    @json.to_json(second).stringify() +
    "\n",
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  )
  let source = JsonLinesSource::open(path)
  let (batch, next_offset) = source.poll_with_offset(maximum_events=1)
  assert_eq(batch, [first])
  assert_eq(source.offset(), 0L)
  assert_eq(next_offset, 1L)
  source.commit(next_offset)
  assert_eq(source.offset(), 1L)
  source.close()
  let reopened = JsonLinesSource::open(path)
  assert_eq(reopened.poll(), [second])
  reopened.close()
}
