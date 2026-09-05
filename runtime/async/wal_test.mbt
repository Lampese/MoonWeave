///|
async fn with_wal_path(f : async (String) -> Unit) -> Unit {
  let directory = @fs.tmpdir(prefix="moonweave-wal")
  let path = directory + "/events.wal"
  defer {
    @fs.remove(path) catch {
      _ => ()
    }
    @fs.remove(path + ".lock") catch {
      _ => ()
    }
    @fs.rmdir(directory) catch {
      _ => ()
    }
  }
  f(path)
}

///|
async test "wal appends synchronized records and replays them" {
  with_wal_path <| path => {
    let (wal, initial) = Wal::open(path, maximum_record_bytes=64)
    assert_eq(initial.records(), [])
    assert_eq(initial.valid_bytes(), 0L)
    let first_offset = wal.append(b"alpha")
    let second_offset = wal.append(b"beta")
    assert_eq(first_offset, 0L)
    assert_eq(second_offset > first_offset, true)
    let size = wal.write_offset()
    wal.close()
    let (reopened, replay) = Wal::open(path, maximum_record_bytes=64)
    defer reopened.close()
    assert_eq(replay.records(), [b"alpha", b"beta"])
    assert_eq(replay.repaired_tail_bytes(), 0)
    assert_eq(reopened.write_offset(), size)
    assert_eq(replay.record_count_before(0L), 0)
    assert_eq(replay.record_count_before(second_offset), 1)
    assert_eq(replay.record_count_before(size), 2)
    try replay.record_count_before(1L) catch {
      WalError::InvalidReplayOffset(offset) => assert_eq(offset, 1L)
      _ => fail("expected invalid WAL boundary")
    } noraise {
      _ => fail("expected invalid WAL boundary")
    }
  }
}

///|
async test "wal repairs an incomplete trailing record" {
  with_wal_path <| path => {
    {
      let (wal, _) = Wal::open(path)
      ignore(wal.append(b"committed"))
      wal.close()
    }
    let corrupt_tail = @fs.open(
      path,
      mode=@fs.WriteOnly,
      append=true,
      create_mode=@fs.OpenExisting,
      sync=@fs.Full,
    )
    corrupt_tail.write(b"MWAL\x01\x00")
    corrupt_tail.close()
    {
      let (reopened, replay) = Wal::open(path)
      assert_eq(replay.records(), [b"committed"])
      assert_true(replay.repaired_tail_bytes() > 0)
      let tail_replay = reopened.append(b"after-repair")
      assert_eq(tail_replay, replay.valid_bytes())
      reopened.close()
    }
    let (final_wal, final_replay) = Wal::open(path)
    assert_eq(final_replay.records(), [b"committed", b"after-repair"])
    final_wal.close()
  }
}

///|
async test "wal fails closed on checksum corruption" {
  with_wal_path <| path => {
    {
      let (wal, _) = Wal::open(path)
      ignore(wal.append(b"protected"))
      wal.close()
    }
    let file = @fs.open(
      path,
      mode=@fs.ReadWrite,
      create_mode=@fs.OpenExisting,
      sync=@fs.Full,
    )
    file.write_at(b"X", position=13L)
    file.close()
    try Wal::open(path) catch {
      WalError::ChecksumMismatch(position~) => assert_eq(position, 0L)
      _ => fail("expected checksum failure")
    } noraise {
      _ => fail("expected checksum failure")
    }
  }
}

///|
async test "wal enforces record size" {
  with_wal_path <| path => {
    let (wal, _) = Wal::open(path, maximum_record_bytes=4)
    try wal.append(b"12345") catch {
      WalError::RecordTooLarge(actual~, limit~) => {
        assert_eq(actual, 5)
        assert_eq(limit, 4)
      }
      _ => fail("expected record size failure")
    } noraise {
      _ => fail("expected record size failure")
    }
    wal.close()
  }
}
