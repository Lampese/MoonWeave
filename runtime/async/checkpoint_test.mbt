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
    guard @json.parse(@fs.read_file(path).text()) is Object(fields) else {
      fail("checkpoint was not an object")
    }
    assert_eq(fields.get("version"), None)
    assert_eq(loaded.delta_wal_offset(), 64L)
    assert_eq(loaded.state_schema(), "reachability-state:v1")
    assert_eq(
      loaded.operator_state(),
      Some(Json::object({ "pending": Json::number(1) })),
    )
    assert_true(@fs.exists(path))
    assert_false(@fs.exists(path + ".tmp"))
  }
}

///|
async test "checkpoint rejects manifests without a state schema" {
  with_checkpoint_path <| path => {
    let current = Checkpoint::new(
      0,
      2,
      0L,
      -1L,
      -1L,
      "frontier-empty",
      state_schema="moonweave:transport:v1",
    )
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
    try Checkpoint::load(path) catch {
      CheckpointError::Malformed => ()
      _ => fail("expected missing schema to be rejected")
    } noraise {
      _ => fail("expected missing schema to be rejected")
    }
  }
}

///|
async test "checkpoint rejects removed version fields" {
  with_checkpoint_path <| path => {
    let checkpoint = Checkpoint::new(
      0,
      0,
      0L,
      -1L,
      -1L,
      "frontier-empty",
      state_schema="moonweave:transport:v1",
    )
    checkpoint.save(path)
    let json = @json.parse(@fs.read_file(path).text())
    guard json is Object(fields) else { fail("checkpoint was not an object") }
    fields["version"] = Json::number(1.0)
    @fs.write_file(
      path,
      @utf8.encode(json.stringify()),
      create_mode=@fs.CreateOrTruncate,
      sync=@fs.Full,
    )
    try Checkpoint::load(path) catch {
      CheckpointError::Malformed => ()
      _ => fail("expected removed version field to be rejected")
    } noraise {
      _ => fail("expected removed version field to be rejected")
    }
  }
}

///|
async test "missing checkpoint is not an error" {
  with_checkpoint_path <| path => { assert_eq(Checkpoint::load(path), None) }
}

///|
async test "checkpoint rejects invalid progress ordering" {
  try
    Checkpoint::new(
      0,
      0,
      10L,
      4L,
      5L,
      "frontier",
      state_schema="moonweave:transport:v1",
    )
  catch {
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
async test "checkpoint rejects an incomplete manifest" {
  with_checkpoint_path <| path => {
    @fs.write_file(path, "{}", create_mode=@fs.CreateOrTruncate, sync=@fs.Full)
    try Checkpoint::load(path) catch {
      CheckpointError::Malformed => ()
      _ => fail("expected malformed checkpoint")
    } noraise {
      _ => fail("expected malformed checkpoint")
    }
  }
}
