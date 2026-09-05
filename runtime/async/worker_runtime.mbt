///|
/// Summary of one worker connection run, including records replayed before the
/// live session and the final logical offsets.
pub struct WorkerRunSummary {
  replayed_batches : Int
  replayed_deltas : Int
  live_batches : Int
  persisted_offset : Int64
  applied_offset : Int64
}

///|
pub fn WorkerRunSummary::replayed_batches(self : WorkerRunSummary) -> Int {
  self.replayed_batches
}

///|
pub fn WorkerRunSummary::replayed_deltas(self : WorkerRunSummary) -> Int {
  self.replayed_deltas
}

///|
pub fn WorkerRunSummary::live_batches(self : WorkerRunSummary) -> Int {
  self.live_batches
}

///|
pub fn WorkerRunSummary::persisted_offset(self : WorkerRunSummary) -> Int64 {
  self.persisted_offset
}

///|
pub fn WorkerRunSummary::applied_offset(self : WorkerRunSummary) -> Int64 {
  self.applied_offset
}

///|
fn frontier_digest(frontier : @causal.Frontier) -> String {
  frontier.digest()
}

///|
/// Admit protocol records into the worker's local causal log solely for
/// frontier accounting. The application handler remains the authoritative
/// operator boundary.
fn admit_causal_batch(
  log : @causal.CausalLog[Json],
  batch : @protocol.EventBatch,
) -> Unit raise FrameIoError {
  for record in batch.events() {
    let event = @causal.Event::new(
      record.id(),
      record.parents(),
      record.key(),
      record.payload(),
    ) catch {
      error => raise FrameIoError::Handler(@debug.to_string(error))
    }
    ignore(
      log.submit(event) catch {
        error => raise FrameIoError::Handler(@debug.to_string(error))
      },
    )
  }
}

///|
fn admit_causal_delta(
  log : @causal.CausalLog[Json],
  delta : @protocol.DeltaBatch,
) -> Unit raise FrameIoError {
  for record in delta.events() {
    let event = @causal.Event::new(
      record.id(),
      record.parents(),
      record.key(),
      record.payload(),
    ) catch {
      error => raise FrameIoError::Handler(@debug.to_string(error))
    }
    ignore(
      log.submit(event) catch {
        error => raise FrameIoError::Handler(@debug.to_string(error))
      },
    )
  }
}

///|
/// Raw read used only inside `with_timeout_opt`. It deliberately leaves async
/// cancellation and lower-level errors open so the timeout combinator can
/// distinguish its own deadline from a peer closing the socket.
async fn read_frame_for_timeout(
  connection : @socket.Tcp,
  maximum_frame_bytes : Int,
) -> @protocol.Frame {
  let header = connection.read_exactly(4)
  let length = (header[0].to_int() << 24) +
    (header[1].to_int() << 16) +
    (header[2].to_int() << 8) +
    header[3].to_int()
  if length < 0 || length > maximum_frame_bytes {
    raise FrameIoError::FrameTooLarge(actual=length, limit=maximum_frame_bytes)
  }
  let body = connection.read_exactly(length)
  @protocol.Frame::decode_payload(body, maximum_frame_bytes~)
}

///|
fn decode_batch_payload(
  payload : String,
) -> @protocol.EventBatch raise FrameIoError {
  @protocol.EventBatch::from_payload(payload) catch {
    error => raise FrameIoError::Message(error)
  }
}

///|
/// Exchange WAL records retain the stable delta identity because a delta has no
/// primary source offset on the target assignment.
struct DeltaWalRecord {
  delta_id : String
  delta : @protocol.DeltaBatch
} derive(Debug, Eq, ToJson, FromJson)

///|
fn load_wal_deltas(
  assignment : @protocol.Assignment,
  replay : WalReplay,
) -> (Array[DeltaWalRecord], Map[String, @protocol.DeltaBatch]) raise FrameIoError {
  let records = []
  let known : Map[String, @protocol.DeltaBatch] = Map([])
  for encoded in replay.records() {
    let payload = @utf8.decode(encoded) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
    let json = @json.parse(payload) catch {
      _ => raise FrameIoError::Handler("Delta WAL contains malformed JSON")
    }
    let raw : DeltaWalRecord = @json.from_json(json) catch {
      _ => raise FrameIoError::Handler("Delta WAL contains malformed record")
    }
    if raw.delta_id.is_empty() || raw.delta_id != raw.delta.delta_id() {
      raise FrameIoError::Handler("Delta WAL contains an invalid delta ID")
    }
    let delta = @protocol.DeltaBatch::from_payload(raw.delta.to_payload()) catch {
      error => raise FrameIoError::Message(error)
    }
    if delta.target_partition() != assignment.partition() ||
      delta.target_epoch() != assignment.epoch() {
      raise FrameIoError::Handler(
        "Delta WAL record does not match the active assignment",
      )
    }
    match known.get(raw.delta_id) {
      Some(previous) =>
        if previous != delta {
          raise FrameIoError::Handler("Delta WAL contains conflicting records")
        }
      None => {
        let record = DeltaWalRecord::{ delta_id: raw.delta_id, delta, }
        records.push(record)
        known[raw.delta_id] = delta
      }
    }
  }
  (records, known)
}

///|
fn replay_wal_deltas(
  records : Array[DeltaWalRecord],
  delta_handler : (@protocol.DeltaBatch) -> Unit raise,
) -> Int raise FrameIoError {
  let mut replayed = 0
  for record in records {
    delta_handler(record.delta) catch {
      error => raise FrameIoError::Handler(@debug.to_string(error))
    }
    replayed += 1
  }
  replayed
}

///|
fn load_wal_batches(
  session : @protocol.WorkerSession,
  replay : WalReplay,
) -> (Array[@protocol.EventBatch], Int64, Map[Int64, @protocol.EventBatch]) raise FrameIoError {
  guard session.state().assignment() is Some(assignment) else {
    raise FrameIoError::MissingAssignment
  }
  let mut last_offset = assignment.next_offset() - 1L
  let batches = []
  let known_batches : Map[Int64, @protocol.EventBatch] = Map([])
  for encoded in replay.records() {
    let payload = @utf8.decode(encoded) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
    let batch = decode_batch_payload(payload)
    if batch.partition() != assignment.partition() ||
      batch.epoch() != assignment.epoch() {
      raise FrameIoError::Handler(
        "WAL batch does not match the active assignment",
      )
    }
    if batch.first_offset() <= last_offset {
      if batch.end_offset() <= last_offset {
        match known_batches.get(batch.first_offset()) {
          Some(previous) => {
            if previous != batch {
              raise FrameIoError::Handler("WAL contains conflicting batches")
            }
            continue
          }
          None =>
            raise FrameIoError::Handler("WAL contains overlapping batches")
        }
      }
      raise FrameIoError::Handler("WAL contains overlapping batches")
    }
    if batch.first_offset() != last_offset + 1L {
      raise FrameIoError::Handler("WAL contains a gap between batches")
    }
    batches.push(batch)
    known_batches[batch.first_offset()] = batch
    last_offset = batch.end_offset()
  }
  (batches, last_offset, known_batches)
}

///|
/// Replay only after the complete WAL and checkpoint metadata have been
/// validated. This prevents a bad manifest from triggering application side
/// effects before the worker rejects the recovery attempt.
fn replay_wal_batches(
  batches : Array[@protocol.EventBatch],
  handler : (@protocol.EventBatch) -> Unit raise,
) -> Int raise FrameIoError {
  let mut replayed = 0
  for batch in batches {
    handler(batch) catch {
      error => raise FrameIoError::Handler(@debug.to_string(error))
    }
    replayed += 1
  }
  replayed
}

///|
/// Validate a manifest against the assignment and the WAL prefix just opened.
/// A checkpoint may lag the WAL because the process can stop between two
/// atomic manifest replacements, but it must never claim data that is absent.
fn validate_checkpoint(
  checkpoint : Checkpoint,
  assignment : @protocol.Assignment,
  wal : Wal,
  replay : WalReplay,
  delta_replay : WalReplay?,
  delta_wal_offset : Int64,
  wal_batches : ArrayView[@protocol.EventBatch],
  delta_records : ArrayView[DeltaWalRecord],
  restored_offset : Int64,
) -> Unit raise FrameIoError {
  if checkpoint.partition() != assignment.partition() ||
    checkpoint.epoch() != assignment.epoch() {
    raise FrameIoError::Handler(
      "checkpoint does not match the active assignment",
    )
  }
  if checkpoint.wal_offset() > wal.write_offset() ||
    checkpoint.delta_wal_offset() > delta_wal_offset ||
    checkpoint.persisted_offset() > restored_offset ||
    checkpoint.applied_offset() > restored_offset ||
    checkpoint.applied_offset() > checkpoint.persisted_offset() {
    raise FrameIoError::Handler("checkpoint is ahead of the recovered WAL")
  }
  match delta_replay {
    None if checkpoint.delta_wal_offset() != 0L =>
      raise FrameIoError::Handler(
        "checkpoint is ahead of the recovered Delta WAL",
      )
    Some(replay) if !replay.is_valid_offset(checkpoint.delta_wal_offset()) =>
      raise FrameIoError::Handler("checkpoint is not on a Delta WAL boundary")
    _ => ()
  }
  let primary_count = replay.record_count_before(checkpoint.wal_offset()) catch {
    _ => raise FrameIoError::Handler("checkpoint is not on a WAL boundary")
  }
  let delta_count = match delta_replay {
    None => 0
    Some(value) =>
      value.record_count_before(checkpoint.delta_wal_offset()) catch {
        _ =>
          raise FrameIoError::Handler(
            "checkpoint is not on a Delta WAL boundary",
          )
      }
  }
  if primary_count > wal_batches.length() ||
    delta_count > delta_records.length() {
    raise FrameIoError::Handler("checkpoint WAL prefix is inconsistent")
  }
  let checkpoint_log : @causal.CausalLog[Json] = @causal.CausalLog::new() catch {
    error => raise FrameIoError::Handler(@debug.to_string(error))
  }
  for batch in wal_batches[0:primary_count] {
    admit_causal_batch(checkpoint_log, batch)
  }
  for record in delta_records[0:delta_count] {
    admit_causal_delta(checkpoint_log, record.delta)
  }
  if checkpoint.frontier_digest() != frontier_digest(checkpoint_log.frontier()) {
    raise FrameIoError::Handler("checkpoint frontier digest mismatch")
  }
}

///|
/// Persist the worker's logical progress after the corresponding WAL and
/// application barriers have completed.
async fn save_checkpoint(
  checkpoint_path : String?,
  assignment : @protocol.Assignment,
  wal_offset : Int64,
  persisted_offset : Int64,
  applied_offset : Int64,
  frontier_digest : String,
  delta_wal_offset : Int64,
) -> Unit raise FrameIoError {
  match checkpoint_path {
    None => ()
    Some(path) => {
      let checkpoint = Checkpoint::new(
        assignment.partition(),
        assignment.epoch(),
        wal_offset,
        persisted_offset,
        applied_offset,
        frontier_digest,
        delta_wal_offset~,
      ) catch {
        error => raise FrameIoError::Checkpoint(error)
      }
      checkpoint.save(path) catch {
        error => raise FrameIoError::Checkpoint(error)
      }
    }
  }
}

///|
async fn send_worker_ack(
  connection : @socket.Tcp,
  outbound : @protocol.FrameBuilder,
  partition : Int,
  epoch : Int,
  offset : Int64,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  let ack = @protocol.AckPersisted::new(partition, epoch, offset) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(
    @protocol.FrameKind::AckPersisted,
    ack.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
async fn send_worker_progress(
  connection : @socket.Tcp,
  outbound : @protocol.FrameBuilder,
  partition : Int,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
  frontier_digest : String,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  let progress = @protocol.Progress::new(
    partition, epoch, persisted_offset, applied_offset, frontier_digest,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(
    @protocol.FrameKind::Progress,
    progress.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
/// Return capacity after a batch has crossed the WAL and application
/// barriers. Credit is additive, so this keeps the worker's receive window
/// bounded while allowing a stream longer than the initial grant.
async fn send_worker_credit(
  connection : @socket.Tcp,
  outbound : @protocol.FrameBuilder,
  partition : Int,
  epoch : Int,
  events : Int64,
  bytes : Int64,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  let credit = @protocol.Credit::new(partition, epoch, events, bytes) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(@protocol.FrameKind::Credit, credit.to_payload()) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
async fn send_worker_probe_reply(
  connection : @socket.Tcp,
  outbound : @protocol.FrameBuilder,
  probe : @protocol.Probe,
  reached_offset : Int64,
  frontier_digest : String,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  let reply = @protocol.ProbeReply::new(
    probe.query_id(),
    probe.partition(),
    probe.epoch(),
    reached_offset,
    frontier_digest,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(
    @protocol.FrameKind::ProbeReply,
    reply.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
async fn send_worker_heartbeat(
  connection : @socket.Tcp,
  outbound : @protocol.FrameBuilder,
  partition : Int,
  epoch : Int,
  persisted_offset : Int64,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  let heartbeat = @protocol.Heartbeat::new(
    partition, epoch, 0, persisted_offset,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(
    @protocol.FrameKind::Heartbeat,
    heartbeat.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
async fn send_worker_delta_ack(
  connection : @socket.Tcp,
  outbound : @protocol.FrameBuilder,
  delta_id : String,
  partition : Int,
  epoch : Int,
  maximum_frame_bytes : Int,
) -> Unit raise FrameIoError {
  let ack = @protocol.DeltaAck::new(delta_id, partition, epoch) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(@protocol.FrameKind::DeltaAck, ack.to_payload()) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
/// Run one worker connection with durable input admission and idempotent
/// replay. The callback is the pure operator/application boundary.
pub async fn run_worker(
  connection : @socket.Tcp,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  incarnation : String,
  capabilities : Array[String],
  wal_path : String,
  handler : (@protocol.EventBatch) -> Unit raise,
  checkpoint_path? : String,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
  initial_credit_events? : Int64 = 128L,
  initial_credit_bytes? : Int64 = 4L * 1024L * 1024L,
  heartbeat_interval_ms? : Int = 0,
  delta_handler? : (@protocol.DeltaBatch) -> Unit raise,
  delta_wal_path? : String,
) -> WorkerRunSummary raise FrameIoError {
  if heartbeat_interval_ms < 0 {
    raise FrameIoError::Handler("heartbeat interval cannot be negative")
  }
  defer connection.close()
  let (wal, replay) = Wal::open(wal_path) catch {
    error => raise FrameIoError::Wal(error)
  }
  defer wal.close()
  let (delta_wal, delta_replay) = match delta_wal_path {
    None => (None, None)
    Some(path) => {
      let (opened, replay) = Wal::open(path) catch {
        error => raise FrameIoError::Wal(error)
      }
      (Some(opened), Some(replay))
    }
  }
  defer (match delta_wal {
    Some(wal) => wal.close()
    None => ()
  })
  let delta_wal_offset = match delta_wal {
    Some(value) => value.write_offset()
    None => 0L
  }
  let checkpoint = match checkpoint_path {
    None => None
    Some(path) =>
      Checkpoint::load(path) catch {
        error => raise FrameIoError::Checkpoint(error)
      }
  }
  let (session, outbound) = worker_handshake(
    connection,
    cluster_id,
    coordinator_id,
    worker_id,
    incarnation,
    capabilities,
    maximum_frame_bytes~,
  )
  guard session.state().assignment() is Some(assignment) else {
    raise FrameIoError::MissingAssignment
  }
  let causal_log : @causal.CausalLog[Json] = @causal.CausalLog::new() catch {
    error => raise FrameIoError::Handler(@debug.to_string(error))
  }
  let (wal_batches, restored_offset, known_batches) = load_wal_batches(
    session, replay,
  )
  let delta_records : Array[DeltaWalRecord] = []
  let known_deltas : Map[String, @protocol.DeltaBatch] = Map([])
  match delta_replay {
    None => ()
    Some(delta_replay_value) => {
      let (records, known) = load_wal_deltas(assignment, delta_replay_value)
      for record in records {
        delta_records.push(record)
      }
      for entry in known.to_array() {
        let (delta_id, delta) = entry
        known_deltas[delta_id] = delta
      }
    }
  }
  for batch in wal_batches {
    admit_causal_batch(causal_log, batch)
  }
  for record in delta_records {
    admit_causal_delta(causal_log, record.delta)
  }
  let recovered_frontier_digest = frontier_digest(causal_log.frontier())
  match checkpoint {
    None => ()
    Some(value) =>
      validate_checkpoint(
        value, assignment, wal, replay, delta_replay, delta_wal_offset, wal_batches,
        delta_records, restored_offset,
      )
  }
  let replayed_batches = replay_wal_batches(wal_batches, handler)
  let replayed_deltas = if delta_records.is_empty() {
    0
  } else {
    match delta_handler {
      None => raise FrameIoError::Handler("delta handler is not configured")
      Some(callback) => replay_wal_deltas(delta_records, callback)
    }
  }
  session.restore_progress(restored_offset, restored_offset, restored_offset) catch {
    error => raise FrameIoError::Session(error)
  }
  save_checkpoint(
    checkpoint_path,
    assignment,
    wal.write_offset(),
    restored_offset,
    restored_offset,
    recovered_frontier_digest,
    delta_wal_offset,
  )
  let resume_message = @protocol.Resume::new(
    assignment.partition(),
    assignment.epoch(),
    restored_offset,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let resume_frame = outbound.build(
    @protocol.FrameKind::Resume,
    resume_message.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, resume_frame, maximum_frame_bytes~)
  send_worker_progress(
    connection,
    outbound,
    assignment.partition(),
    assignment.epoch(),
    restored_offset,
    restored_offset,
    recovered_frontier_digest,
    maximum_frame_bytes,
  )
  let credit = @protocol.Credit::new(
    assignment.partition(),
    assignment.epoch(),
    initial_credit_events,
    initial_credit_bytes,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let credit_frame = outbound.build(
    @protocol.FrameKind::Credit,
    credit.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, credit_frame, maximum_frame_bytes~)
  let mut live_batches = 0
  for ;; {
    let frame = match heartbeat_interval_ms {
      0 => read_frame(connection, maximum_frame_bytes~)
      interval => {
        let incoming = @async.with_timeout_opt(interval, () => {
          read_frame_for_timeout(connection, maximum_frame_bytes)
        }) catch {
          error => raise FrameIoError::Io(@debug.to_string(error))
        }
        match incoming {
          None => {
            send_worker_heartbeat(
              connection,
              outbound,
              assignment.partition(),
              assignment.epoch(),
              session.last_persisted_offset(),
              maximum_frame_bytes,
            )
            continue
          }
          Some(value) => value
        }
      }
    }
    let batch = match frame.kind() {
      @protocol.FrameKind::EventBatch =>
        Some(decode_batch_payload(frame.payload()))
      _ => None
    }
    let delta = match frame.kind() {
      @protocol.FrameKind::DeltaBatch =>
        Some(
          @protocol.DeltaBatch::from_payload(frame.payload()) catch {
            error => raise FrameIoError::Message(error)
          },
        )
      _ => None
    }
    let duplicate_by_id = session.has_seen(frame.message_id())
    let duplicate_by_range = match batch {
      Some(value) =>
        if value.end_offset() <= session.last_received_offset() {
          match known_batches.get(value.first_offset()) {
            Some(previous) =>
              if previous != value {
                raise FrameIoError::Handler(
                  "resent batch conflicts with the recovered WAL",
                )
              }
            None =>
              raise FrameIoError::Handler(
                "resent batch has no matching recovered WAL record",
              )
          }
          true
        } else {
          false
        }
      None => false
    }
    let duplicate_by_delta_wal = match delta {
      Some(value) =>
        match known_deltas.get(value.delta_id()) {
          Some(previous) => {
            if previous != value {
              raise FrameIoError::Handler(
                "resent delta conflicts with the recovered Delta WAL",
              )
            }
            true
          }
          None => false
        }
      None => false
    }
    let duplicate = duplicate_by_id ||
      duplicate_by_range ||
      duplicate_by_delta_wal
    session.accept_frame(frame) catch {
      error => raise FrameIoError::Session(error)
    }
    match frame.kind() {
      @protocol.FrameKind::EventBatch => {
        guard batch is Some(batch_value) else {
          raise FrameIoError::Message(@protocol.MessageError::Malformed)
        }
        if duplicate {
          send_worker_ack(
            connection,
            outbound,
            batch_value.partition(),
            batch_value.epoch(),
            session.last_persisted_offset(),
            maximum_frame_bytes,
          )
          send_worker_progress(
            connection,
            outbound,
            batch_value.partition(),
            batch_value.epoch(),
            session.last_persisted_offset(),
            session.last_applied_offset(),
            frontier_digest(causal_log.frontier()),
            maximum_frame_bytes,
          )
          if !duplicate_by_id && duplicate_by_range {
            send_worker_credit(
              connection,
              outbound,
              batch_value.partition(),
              batch_value.epoch(),
              batch_value.events().length().to_int64(),
              @utf8.encode(frame.payload()).length().to_int64(),
              maximum_frame_bytes,
            )
          }
        } else {
          let candidate = causal_log.copy()
          admit_causal_batch(candidate, batch_value)
          let persisted_bytes = @utf8.encode(frame.payload())
          ignore(
            wal.append(persisted_bytes) catch {
              error => raise FrameIoError::Wal(error)
            },
          )
          known_batches[batch_value.first_offset()] = batch_value
          admit_causal_batch(causal_log, batch_value)
          session.mark_persisted(batch_value.end_offset()) catch {
            error => raise FrameIoError::Session(error)
          }
          send_worker_ack(
            connection,
            outbound,
            batch_value.partition(),
            batch_value.epoch(),
            batch_value.end_offset(),
            maximum_frame_bytes,
          )
          handler(batch_value) catch {
            error => raise FrameIoError::Handler(@debug.to_string(error))
          }
          session.mark_applied(batch_value.end_offset()) catch {
            error => raise FrameIoError::Session(error)
          }
          send_worker_progress(
            connection,
            outbound,
            batch_value.partition(),
            batch_value.epoch(),
            session.last_persisted_offset(),
            session.last_applied_offset(),
            frontier_digest(causal_log.frontier()),
            maximum_frame_bytes,
          )
          save_checkpoint(
            checkpoint_path,
            assignment,
            wal.write_offset(),
            session.last_persisted_offset(),
            session.last_applied_offset(),
            frontier_digest(causal_log.frontier()),
            match delta_wal {
              Some(value) => value.write_offset()
              None => 0L
            },
          )
          send_worker_credit(
            connection,
            outbound,
            batch_value.partition(),
            batch_value.epoch(),
            batch_value.events().length().to_int64(),
            persisted_bytes.length().to_int64(),
            maximum_frame_bytes,
          )
          live_batches += 1
        }
      }
      @protocol.FrameKind::Probe => {
        let probe = @protocol.Probe::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        let probe_digest = frontier_digest(causal_log.frontier())
        send_worker_probe_reply(
          connection,
          outbound,
          probe,
          session.last_applied_offset(),
          probe_digest,
          maximum_frame_bytes,
        )
      }
      @protocol.FrameKind::DeltaBatch => {
        guard delta is Some(delta_value) else {
          raise FrameIoError::Message(@protocol.MessageError::Malformed)
        }
        if !duplicate {
          match delta_wal {
            Some(wal) => {
              let candidate = causal_log.copy()
              admit_causal_delta(candidate, delta_value)
              let record = DeltaWalRecord::{
                delta_id: delta_value.delta_id(),
                delta: delta_value,
              }
              let encoded = @utf8.encode(@json.to_json(record).stringify())
              ignore(
                wal.append(encoded) catch {
                  error => raise FrameIoError::Wal(error)
                },
              )
            }
            None =>
              raise FrameIoError::Handler(
                "delta WAL is required before acknowledging exchange data",
              )
          }
          known_deltas[delta_value.delta_id()] = delta_value
          admit_causal_delta(causal_log, delta_value)
          match delta_handler {
            None =>
              raise FrameIoError::Handler("delta handler is not configured")
            Some(handler) =>
              handler(delta_value) catch {
                error => raise FrameIoError::Handler(@debug.to_string(error))
              }
          }
          save_checkpoint(
            checkpoint_path,
            assignment,
            wal.write_offset(),
            session.last_persisted_offset(),
            session.last_applied_offset(),
            frontier_digest(causal_log.frontier()),
            match delta_wal {
              Some(value) => value.write_offset()
              None => 0L
            },
          )
        }
        send_worker_delta_ack(
          connection,
          outbound,
          delta_value.delta_id(),
          delta_value.target_partition(),
          delta_value.target_epoch(),
          maximum_frame_bytes,
        )
        if !duplicate_by_id {
          send_worker_credit(
            connection,
            outbound,
            assignment.partition(),
            assignment.epoch(),
            delta_value.events().length().to_int64(),
            @utf8.encode(frame.payload()).length().to_int64(),
            maximum_frame_bytes,
          )
        }
      }
      @protocol.FrameKind::Seal => ()
      @protocol.FrameKind::Drain => break
      _ =>
        raise FrameIoError::Session(
          @protocol.SessionError::WrongPhase(
            expected=@protocol.SessionPhase::Running,
            actual=session.phase(),
          ),
        )
    }
  }
  {
    replayed_batches,
    replayed_deltas,
    live_batches,
    persisted_offset: session.last_persisted_offset(),
    applied_offset: session.last_applied_offset(),
  }
}
