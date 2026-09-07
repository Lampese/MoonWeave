///|
async fn with_checkpoint_path(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-checkpoint")
  let path = directory + "/worker.json"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".tmp") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  f(path)
}

///|
async test "checkpoint saves atomically and loads validated progress" {
  with_checkpoint_path <| path => {
    let checkpoint = Checkpoint::new(
      2,
      9,
      128L,
      12L,
      10L,
      "frontier-10",
      delta_wal_offset=64L,
      state_schema="reachability-state:v1",
      operator_state=Json::object({ "pending": Json::number(1) }),
    )
    checkpoint.save(path)
    guard Checkpoint::load(path) is Some(loaded) else {
      fail("checkpoint should load")
    }
    assert_eq(loaded, checkpoint)
    assert_eq(loaded.version(), 1)
    assert_eq(loaded.delta_wal_offset(), 64L)
    assert_eq(loaded.state_schema(), Some("reachability-state:v1"))
    assert_eq(
      loaded.operator_state(),
      Some(Json::object({ "pending": Json::number(1) })),
    )
    assert_true(@fs.exists(path))
    assert_false(@fs.exists(path + ".tmp"))
  }
}

///|
async test "checkpoint accepts legacy manifests without a state schema" {
  with_checkpoint_path <| path => {
    let current = Checkpoint::new(0, 2, 0L, -1L, -1L, "frontier-empty")
    current.save(path)
    let json = @json.parse(@fs.read_file(path).text())
    guard json is Object(fields) else { fail("checkpoint was not an object") }
    fields.remove("state_schema")
    @fs.write_file(
      path,
      @utf8.encode(json.stringify()),
      create_mode=@fs.CreateOrTruncate,
      sync=@fs.Full,
    )
    guard Checkpoint::load(path) is Some(loaded) else {
      fail("legacy checkpoint should load")
    }
    assert_eq(loaded.state_schema(), None)
    assert_eq(loaded.operator_state(), None)
  }
}

///|
async test "missing checkpoint is not an error" {
  with_checkpoint_path <| path => { assert_eq(Checkpoint::load(path), None) }
}

///|
async test "checkpoint rejects invalid progress ordering" {
  try Checkpoint::new(0, 0, 10L, 4L, 5L, "frontier") catch {
    CheckpointError::InvalidField => ()
    _ => fail("expected invalid checkpoint")
  } noraise {
    _ => fail("expected invalid checkpoint")
  }
}

///|
async test "checkpoint rejects an empty state schema" {
  try
    Checkpoint::new(0, 0, 0L, -1L, -1L, "frontier-empty", state_schema="")
  catch {
    CheckpointError::InvalidField => ()
    _ => fail("expected empty schema to be rejected")
  } noraise {
    _ => fail("expected empty schema to be rejected")
  }
}

///|
async test "checkpoint rejects malformed or unsupported version" {
  with_checkpoint_path <| path => {
    @fs.write_file(
      path,
      "{\"version\":2}",
      create_mode=@fs.CreateOrTruncate,
      sync=@fs.Full,
    )
    try Checkpoint::load(path) catch {
      CheckpointError::Malformed => ()
      _ => fail("expected malformed checkpoint")
    } noraise {
      _ => fail("expected malformed checkpoint")
    }
  }
}
