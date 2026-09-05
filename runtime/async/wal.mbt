///|
/// Errors at the append-only WAL boundary.
pub(all) suberror WalError {
  EmptyPath
  InvalidMaximumRecordBytes(Int)
  RecordTooLarge(actual~ : Int, limit~ : Int)
  InvalidRecordLength(position~ : Int64)
  CorruptMagic(position~ : Int64)
  UnsupportedVersion(version~ : Int, position~ : Int64)
  ChecksumMismatch(position~ : Int64)
  InvalidReplayOffset(Int64)
  Io(String)
} derive(Debug, Eq)

///|
const WAL_MAGIC : Bytes = b"MWAL"

///|
const WAL_VERSION : Int = 1

///|
const WAL_HEADER_BYTES : Int = 13

///|
/// Valid records recovered from a WAL file. A nonzero trailing byte count means
/// an incomplete final record was discarded and the file was repaired before
/// Wal::open returned.
pub struct WalReplay {
  records : ReadOnlyArray[Bytes]
  boundaries : ReadOnlyArray[Int64]
  valid_bytes : Int64
  repaired_tail_bytes : Int
}

///|
pub fn WalReplay::records(self : WalReplay) -> ArrayView[Bytes] {
  self.records[:]
}

///|
pub fn WalReplay::valid_bytes(self : WalReplay) -> Int64 {
  self.valid_bytes
}

///|
/// Return whether `offset` is an exact boundary between verified WAL records.
pub fn WalReplay::is_valid_offset(self : WalReplay, offset : Int64) -> Bool {
  self.boundaries.contains(offset)
}

///|
/// Return the verified records after an exact WAL byte boundary. This keeps
/// checkpoint replay independent of the physical record header format.
pub fn WalReplay::records_after(
  self : WalReplay,
  offset : Int64,
) -> Array[Bytes] raise WalError {
  let mut index = 0
  while index < self.boundaries.length() {
    if self.boundaries[index] == offset {
      return self.records[index:].to_owned()
    }
    index += 1
  }
  raise WalError::InvalidReplayOffset(offset)
}

///|
/// Return the number of verified records ending at an exact WAL boundary.
/// This lets a checkpoint describe a logical prefix without exposing headers.
pub fn WalReplay::record_count_before(
  self : WalReplay,
  offset : Int64,
) -> Int raise WalError {
  for index, boundary in self.boundaries {
    if boundary == offset {
      return index
    }
  }
  raise WalError::InvalidReplayOffset(offset)
}

///|
pub fn WalReplay::repaired_tail_bytes(self : WalReplay) -> Int {
  self.repaired_tail_bytes
}

///|
/// One process owns a WAL at a time. The separate lock file remains stable if
/// recovery repairs the WAL via atomic rename.
pub struct Wal {
  path : String
  lock_path : String
  file : @fs.File
  lock_file : @fs.File
  maximum_record_bytes : Int
  mut write_offset : Int64
}

///|
pub fn Wal::path(self : Wal) -> String {
  self.path
}

///|
pub fn Wal::write_offset(self : Wal) -> Int64 {
  self.write_offset
}

///|
pub fn Wal::maximum_record_bytes(self : Wal) -> Int {
  self.maximum_record_bytes
}

///|
pub fn Wal::close(self : Wal) -> Unit {
  self.file.close()
  self.lock_file.close()
}

///|
fn checksum(payload : Bytes) -> UInt {
  let mut hash : UInt = 2166136261U
  for byte in payload {
    hash = (hash ^ byte.to_int().reinterpret_as_uint()) * 16777619U
  }
  hash
}

///|
fn encode_record(
  payload : Bytes,
  maximum_record_bytes : Int,
) -> Bytes raise WalError {
  if payload.length() > maximum_record_bytes {
    raise WalError::RecordTooLarge(
      actual=payload.length(),
      limit=maximum_record_bytes,
    )
  }
  let buffer = Buffer(size_hint=WAL_HEADER_BYTES + payload.length())
  buffer.write_bytes(WAL_MAGIC[:])
  buffer.write_byte(WAL_VERSION.to_byte())
  buffer.write_uint_be(payload.length().reinterpret_as_uint())
  buffer.write_uint_be(checksum(payload))
  buffer.write_bytes(payload[:])
  buffer.to_bytes()
}

///|
fn parse_records(
  data : Bytes,
  maximum_record_bytes : Int,
) -> WalReplay raise WalError {
  let records = []
  let boundaries = [0L]
  let mut position = 0
  while position < data.length() {
    let remaining = data.length() - position
    if remaining < WAL_HEADER_BYTES {
      return {
        records: ReadOnlyArray::from_array(records),
        boundaries: ReadOnlyArray::from_array(boundaries),
        valid_bytes: position.to_int64(),
        repaired_tail_bytes: remaining,
      }
    }
    let magic = data[position:position + 4]
    if !magic.equal_to_bytes(WAL_MAGIC) {
      raise WalError::CorruptMagic(position=position.to_int64())
    }
    let version = data[position + 4].to_int()
    if version != WAL_VERSION {
      raise WalError::UnsupportedVersion(version~, position=position.to_int64())
    }
    guard data[position + 5:position + 9] is [u32be(payload_len_bits)] else {
      raise WalError::InvalidRecordLength(position=position.to_int64())
    }
    let payload_len = payload_len_bits.reinterpret_as_int()
    if payload_len < 0 {
      raise WalError::InvalidRecordLength(position=position.to_int64())
    }
    if payload_len > maximum_record_bytes {
      raise WalError::RecordTooLarge(
        actual=payload_len,
        limit=maximum_record_bytes,
      )
    }
    guard data[position + 9:position + 13] is [u32be(checksum_expected)] else {
      raise WalError::InvalidRecordLength(position=position.to_int64())
    }
    let payload_start = position + WAL_HEADER_BYTES
    if data.length() - payload_start < payload_len {
      return {
        records: ReadOnlyArray::from_array(records),
        boundaries: ReadOnlyArray::from_array(boundaries),
        valid_bytes: position.to_int64(),
        repaired_tail_bytes: remaining,
      }
    }
    let payload = data[payload_start:payload_start + payload_len].to_owned()
    if checksum(payload) != checksum_expected {
      raise WalError::ChecksumMismatch(position=position.to_int64())
    }
    records.push(payload)
    position = payload_start + payload_len
    boundaries.push(position.to_int64())
  }
  {
    records: ReadOnlyArray::from_array(records),
    boundaries: ReadOnlyArray::from_array(boundaries),
    valid_bytes: position.to_int64(),
    repaired_tail_bytes: 0,
  }
}

///|
fn encode_records(
  records : ArrayView[Bytes],
  maximum_record_bytes : Int,
) -> Bytes raise WalError {
  let buffer = Buffer()
  for record in records {
    buffer.write_bytes(encode_record(record, maximum_record_bytes))
  }
  buffer.to_bytes()
}

///|
async fn open_locked_file(path : String) -> @fs.File raise WalError {
  @fs.open(
    path,
    mode=@fs.ReadWrite,
    append=true,
    create_mode=@fs.OpenOrCreate,
    sync=@fs.NoSync,
  ) catch {
    error => raise WalError::Io(@debug.to_string(error))
  }
}

///|
/// Open a WAL, recover the valid prefix, and retain an exclusive process lock.
pub async fn Wal::open(
  path : String,
  maximum_record_bytes? : Int = 1024 * 1024,
) -> (Wal, WalReplay) raise WalError {
  if path.is_empty() {
    raise WalError::EmptyPath
  }
  if maximum_record_bytes <= 0 {
    raise WalError::InvalidMaximumRecordBytes(maximum_record_bytes)
  }
  let lock_path = path + ".lock"
  let lock_file = @fs.open(
    lock_path,
    mode=@fs.ReadWrite,
    create_mode=@fs.OpenOrCreate,
    sync=@fs.NoSync,
  ) catch {
    error => raise WalError::Io(@debug.to_string(error))
  }
  errdefer lock_file.close()
  lock_file.lock(@fs.Exclusive) catch {
    error => raise WalError::Io(@debug.to_string(error))
  }
  let mut file = open_locked_file(path)
  let data = file.read_all().binary() catch {
      error => raise WalError::Io(@debug.to_string(error))
    }
  let replay = parse_records(data, maximum_record_bytes)
  if replay.repaired_tail_bytes() > 0 {
    let repair_path = path + ".repair"
    let repaired = encode_records(replay.records(), maximum_record_bytes)
    @fs.write_file(
      repair_path,
      repaired,
      create_mode=@fs.CreateOrTruncate,
      sync=@fs.Full,
    ) catch {
      error => raise WalError::Io(@debug.to_string(error))
    }
    file.close()
    @fs.rename(repair_path, path, replace=true) catch {
      error => raise WalError::Io(@debug.to_string(error))
    }
    file = open_locked_file(path)
  }
  (
    {
      path,
      lock_path,
      file,
      lock_file,
      maximum_record_bytes,
      write_offset: replay.valid_bytes(),
    },
    replay,
  )
}

///|
/// Append one payload and synchronize it before reporting its record offset.
pub async fn Wal::append(self : Wal, payload : Bytes) -> Int64 raise WalError {
  let record = encode_record(payload, self.maximum_record_bytes)
  let offset = self.write_offset
  self.file.write(record) catch {
    error => raise WalError::Io(@debug.to_string(error))
  }
  self.file.sync() catch {
    error => raise WalError::Io(@debug.to_string(error))
  }
  self.write_offset += record.length().to_int64()
  offset
}
