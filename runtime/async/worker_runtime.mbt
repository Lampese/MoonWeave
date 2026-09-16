///|
/// Summary of one worker connection run, including records replayed before the
/// live session and the final logical offsets.
pub struct WorkerRunSummary {
  replayed_batches : Int
  replayed_deltas : Int
  live_batches : Int
  persisted_offset : Int64
  applied_offset : Int64
} derive(Debug, Eq)

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
/// Bound the Worker-local causal index before it becomes durable. Pending
/// descendants are state too, so they count against the same Job budget.
fn check_causal_state_limit(
  log : @causal.CausalLog[Json],
  limit : Int,
  operator_count? : Int = 0,
) -> Unit raise FrameIoError {
  if limit > 0 {
    let actual = log.state_count() + operator_count
    if actual > limit {
      raise FrameIoError::StateLimitExceeded(limit~, actual~)
    }
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
/// Watermarks use a tagged record in the same WAL as source batches.  Keeping
/// one sequence is necessary to preserve event/control ordering on replay.
struct WatermarkWalRecord {
  kind : String
  watermark : @protocol.Watermark
} derive(Debug, Eq, ToJson, FromJson)

///|
/// The payload remains in the exchange WAL. This marker records its exact
/// position relative to source batches and watermarks in the primary journal.
struct DeltaOrderWalRecord {
  kind : String
  delta_id : String
} derive(Debug, Eq, ToJson, FromJson)

///|
enum WorkerWalRecord {
  Checkpoint(WorkerJournalCheckpoint)
  EmbeddedDelta(@protocol.DeltaBatch)
  Batch(@protocol.EventBatch)
  Watermark(@protocol.Watermark)
  Delta(String)
} derive(Debug, Eq, ToJson, FromJson)

///|
fn decode_worker_record(encoded : Bytes) -> WorkerWalRecord raise FrameIoError {
  let payload = @utf8.decode(encoded) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  let json = @json.parse(payload) catch {
    _ => raise FrameIoError::Handler("WAL contains malformed JSON")
  }
  guard json is Object(fields) else {
    raise FrameIoError::Handler("WAL record is not an object")
  }
  match fields.get("kind") {
    Some(Json::String("checkpoint")) => {
      let saved : WorkerJournalCheckpoint = @json.from_json(json) catch {
        _ => raise FrameIoError::Handler("malformed journal checkpoint")
      }
      WorkerWalRecord::Checkpoint(saved)
    }
    Some(Json::String("exchange")) => {
      let raw : EmbeddedDeltaRecord = @json.from_json(json) catch {
        _ => raise FrameIoError::Handler("malformed journal exchange")
      }
      WorkerWalRecord::EmbeddedDelta(
        @protocol.DeltaBatch::from_payload(raw.delta.to_payload()) catch {
          error => raise FrameIoError::Message(error)
        },
      )
    }
    Some(Json::String("delta-order")) => {
      let raw : DeltaOrderWalRecord = @json.from_json(json) catch {
        _ => raise FrameIoError::Handler("WAL delta order marker is malformed")
      }
      if raw.delta_id.is_empty() {
        raise FrameIoError::Handler("empty exchange journal identity")
      }
      WorkerWalRecord::Delta(raw.delta_id)
    }
    Some(Json::String("watermark")) => {
      let raw : WatermarkWalRecord = @json.from_json(json) catch {
        _ => raise FrameIoError::Handler("WAL watermark is malformed")
      }
      let watermark = @protocol.Watermark::from_payload(
        raw.watermark.to_payload(),
      ) catch {
        error => raise FrameIoError::Message(error)
      }
      WorkerWalRecord::Watermark(watermark)
    }
    _ => WorkerWalRecord::Batch(decode_batch_payload(payload))
  }
}

///|
fn encode_worker_record(record : WorkerWalRecord) -> Bytes {
  @utf8.encode(
    match record {
      WorkerWalRecord::Checkpoint(saved) => @json.to_json(saved).stringify()
      WorkerWalRecord::EmbeddedDelta(delta) =>
        @json.to_json(EmbeddedDeltaRecord::{ kind: "exchange", delta, }).stringify()
      WorkerWalRecord::Batch(batch) => batch.to_payload()
      WorkerWalRecord::Delta(id) =>
        @json.to_json(DeltaOrderWalRecord::{
          kind: "delta-order",
          delta_id: id,
        }).stringify()
      WorkerWalRecord::Watermark(watermark) =>
        @json.to_json(WatermarkWalRecord::{ kind: "watermark", watermark, }).stringify()
    },
  )
}

///|
fn load_wal_records(
  session : @protocol.WorkerSession,
  replay : WalReplay,
) -> (Array[WorkerWalRecord], Int64, Map[Int64, @protocol.EventBatch]) raise FrameIoError {
  guard session.state().assignment() is Some(assignment) else {
    raise FrameIoError::MissingAssignment
  }
  let records : Array[WorkerWalRecord] = []
  let known : Map[Int64, @protocol.EventBatch] = Map([])
  let mut last_offset = assignment.next_offset() - 1L
  let mut last_watermark = -1L
  for encoded in replay.records() {
    let record = decode_worker_record(encoded)
    match record {
      WorkerWalRecord::Checkpoint(saved) => {
        if !records.is_empty() ||
          saved.checkpoint.partition() != assignment.partition() ||
          saved.checkpoint.epoch() != assignment.epoch() ||
          saved.checkpoint.persisted_offset() < last_offset {
          raise FrameIoError::Handler(
            "journal checkpoint does not match assignment",
          )
        }
        last_offset = saved.checkpoint.persisted_offset()
        last_watermark = saved.checkpoint.watermark()
      }
      WorkerWalRecord::EmbeddedDelta(delta) =>
        if delta.target_partition() != assignment.partition() ||
          delta.target_epoch() != assignment.epoch() {
          raise FrameIoError::Handler(
            "journal exchange does not match assignment",
          )
        }
      WorkerWalRecord::Delta(_) => ()
      WorkerWalRecord::Watermark(watermark) => {
        if watermark.partition() != assignment.partition() ||
          watermark.epoch() != assignment.epoch() ||
          watermark.timestamp() < last_watermark {
          raise FrameIoError::Handler("WAL watermark is invalid or regresses")
        }
        last_watermark = watermark.timestamp()
      }
      WorkerWalRecord::Batch(batch) => {
        if batch.partition() != assignment.partition() ||
          batch.epoch() != assignment.epoch() {
          raise FrameIoError::Handler("WAL batch does not match assignment")
        }
        if batch.first_offset() != last_offset + 1L {
          raise FrameIoError::Handler("WAL contains a gap or overlap")
        }
        last_offset = batch.end_offset()
        known[batch.first_offset()] = batch
      }
    }
    records.push(record)
  }
  (records, last_offset, known)
}

///|
fn replay_wal_records(
  records : Array[WorkerWalRecord],
  handler : (@protocol.EventBatch) -> Unit raise,
  watermark_handler : ((Int64) -> Unit raise FrameIoError)?,
  delta_handler : ((@protocol.DeltaBatch) -> Unit raise)?,
  known_deltas : Map[String, @protocol.DeltaBatch],
) -> (Int, Int) raise FrameIoError {
  let mut replayed = 0
  let mut replayed_deltas = 0
  for record in records {
    match record {
      WorkerWalRecord::Checkpoint(_) =>
        raise FrameIoError::Handler("checkpoint must be restored before replay")
      WorkerWalRecord::EmbeddedDelta(delta) => {
        guard delta_handler is Some(callback) else {
          raise FrameIoError::Handler("delta handler is not configured")
        }
        callback(delta) catch {
          error => raise FrameIoError::Handler(@debug.to_string(error))
        }
        replayed_deltas += 1
      }
      WorkerWalRecord::Batch(batch) => {
        handler(batch) catch {
          error => raise FrameIoError::Handler(@debug.to_string(error))
        }
        replayed += 1
      }
      WorkerWalRecord::Watermark(watermark) =>
        match watermark_handler {
          None => ()
          Some(callback) => callback(watermark.timestamp())
        }
      WorkerWalRecord::Delta(id) => {
        guard known_deltas.get(id) is Some(delta) else {
          raise FrameIoError::Handler(
            "journal references missing exchange data",
          )
        }
        guard delta_handler is Some(callback) else {
          raise FrameIoError::Handler("delta handler is not configured")
        }
        callback(delta) catch {
          error => raise FrameIoError::Handler(@debug.to_string(error))
        }
        replayed_deltas += 1
      }
    }
  }
  (replayed, replayed_deltas)
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
  wal_records : ArrayView[WorkerWalRecord],
  delta_records : ArrayView[DeltaWalRecord],
  restored_offset : Int64,
  state_schema : String,
) -> Unit raise FrameIoError {
  if checkpoint.partition() != assignment.partition() ||
    checkpoint.epoch() != assignment.epoch() {
    raise FrameIoError::Handler(
      "checkpoint does not match the active assignment",
    )
  }
  if checkpoint.state_schema() != state_schema {
    raise FrameIoError::Handler("checkpoint state schema mismatch")
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
  if primary_count > wal_records.length() ||
    delta_count > delta_records.length() {
    raise FrameIoError::Handler("checkpoint WAL prefix is inconsistent")
  }
  let checkpoint_log : @causal.CausalLog[Json] = @causal.CausalLog::new() catch {
    error => raise FrameIoError::Handler(@debug.to_string(error))
  }
  let mut watermark = -1L
  let mut ordered_deltas = 0
  for record in wal_records[0:primary_count] {
    match record {
      WorkerWalRecord::Checkpoint(_) | WorkerWalRecord::EmbeddedDelta(_) =>
        raise FrameIoError::Handler(
          "journal checkpoint is authoritative after reclamation",
        )
      WorkerWalRecord::Batch(batch) => admit_causal_batch(checkpoint_log, batch)
      WorkerWalRecord::Watermark(value) => watermark = value.timestamp()
      WorkerWalRecord::Delta(id) => {
        if ordered_deltas >= delta_count ||
          delta_records[ordered_deltas].delta_id != id {
          raise FrameIoError::Handler(
            "checkpoint exchange journal prefix mismatch",
          )
        }
        ordered_deltas += 1
      }
    }
  }
  for record in delta_records[0:delta_count] {
    admit_causal_delta(checkpoint_log, record.delta)
  }
  if checkpoint.frontier_digest() != frontier_digest(checkpoint_log.frontier()) {
    raise FrameIoError::Handler("checkpoint frontier digest mismatch")
  }
  if checkpoint.watermark() != watermark {
    raise FrameIoError::Handler(
      "checkpoint watermark does not match journal prefix",
    )
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
  state_schema : String,
  operator_state? : Json,
  watermark? : Int64 = -1L,
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
        state_schema~,
        operator_state?,
        watermark?=if watermark < 0L { None } else { Some(watermark) },
      ) catch {
        error => raise FrameIoError::Checkpoint(error)
      }
      checkpoint.save(path) catch {
        error => raise FrameIoError::Checkpoint(error)
      }
      if applied_offset >= 0L {
        @crash_boundary.hit("worker.checkpoint.saved", path) catch {
          error => raise FrameIoError::Io(@debug.to_string(error))
        }
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
  watermark? : Int64,
) -> Unit raise FrameIoError {
  let progress = @protocol.Progress::new(
    partition,
    epoch,
    persisted_offset,
    applied_offset,
    frontier_digest,
    watermark?,
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
/// Internal Worker implementation with an optional post-handshake assignment
/// validator used by the typed Task adapter.
async fn run_worker_impl(
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
  assignment_validator? : (@protocol.Assignment) -> Unit raise FrameIoError,
  state_restore? : (Json, Int64, String) -> Unit raise FrameIoError,
  state_snapshot? : () -> Json raise FrameIoError,
  watermark_handler? : (Int64) -> Unit raise FrameIoError,
  output_barrier? : async () -> Unit raise FrameIoError,
  requested_partition? : Int,
  state_schema? : String = "moonweave:transport:v1",
  assignment_setup? : () -> Unit raise FrameIoError,
  drain_handler? : () -> Unit raise FrameIoError,
  output_summary? : () -> (
    String,
    Int64,
    String,
    String,
    String?,
    Int64?,
    Int64?,
  ) raise FrameIoError,
  terminal_barrier? : async () -> Unit raise FrameIoError,
  auth_token? : String,
  job_id? : String,
  storage_prepare? : async (@protocol.Assignment) -> (String, String?, String?) raise FrameIoError,
  storage_budget_bytes? : Int64,
  causal_state_limit? : Int = 0,
  primary_admission_validator? : (@protocol.EventBatch) -> Int raise FrameIoError,
  delta_admission_validator? : (@protocol.DeltaBatch) -> Int raise FrameIoError,
  operator_state_count? : () -> Int,
  state_compact? : (@causal.CausalRetirement) -> Unit raise FrameIoError,
  state_watermark? : () -> Int64,
  reclamation_barrier? : async () -> Unit raise FrameIoError,
  execution_lane? : ExecutionLane,
) -> WorkerRunSummary raise FrameIoError {
  if heartbeat_interval_ms < 0 {
    raise FrameIoError::Handler("heartbeat interval cannot be negative")
  }
  defer connection.close()
  let (session, outbound) = worker_handshake(
    connection,
    cluster_id,
    coordinator_id,
    worker_id,
    incarnation,
    capabilities,
    maximum_frame_bytes~,
    requested_partition?,
    auth_token?,
    job_id?,
  )
  guard session.state().assignment() is Some(assignment) else {
    raise FrameIoError::MissingAssignment
  }
  match assignment_validator {
    None => ()
    Some(validator) => validator(assignment)
  }
  match assignment_setup {
    None => ()
    Some(setup) => setup()
  }
  let (wal_path, checkpoint_path, delta_wal_path) = match storage_prepare {
    None => (wal_path, checkpoint_path, delta_wal_path)
    Some(prepare) => prepare(assignment)
  }
  let mut permit : ExecutionPermit? = None
  let release = () => {
    if permit is Some(held) {
      held.release()
      permit = None
    }
  }
  defer release()
  let acquire : async () -> Unit raise FrameIoError = () => {
    if execution_lane is Some(lane) {
      permit = Some(lane.acquire())
    }
  }
  let flush_outputs : async () -> Unit raise FrameIoError = () => {
    // A sibling shard may be needed to unblock downstream admission. The
    // shard still has one owner while its slot is available to other shards.
    release()
    if output_barrier is Some(barrier) {
      barrier()
    }
    acquire()
  }
  acquire()
  let (wal, replay) = Wal::open(wal_path, maximum_bytes?=storage_budget_bytes) catch {
    error => raise FrameIoError::Wal(error)
  }
  defer wal.close()
  let (delta_wal, delta_replay) = match delta_wal_path {
    None => (None, None)
    Some(path) => {
      let (opened, replay) = Wal::open(
        path,
        maximum_bytes?=storage_budget_bytes,
      ) catch {
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
  let (wal_records, restored_offset, known_batches) = load_wal_records(
    session, replay,
  )
  let journal_checkpoint = match wal_records.get(0) {
    Some(WorkerWalRecord::Checkpoint(saved)) => Some(saved)
    _ => None
  }
  // The embedded checkpoint supersedes sidecars with obsolete byte offsets.
  let checkpoint = match (journal_checkpoint, checkpoint_path) {
    (Some(_), _) | (_, None) => None
    (_, Some(path)) =>
      Checkpoint::load(path) catch {
        error => raise FrameIoError::Checkpoint(error)
      }
  }
  let mut causal_log : @causal.CausalLog[Json] = match journal_checkpoint {
    Some(saved) => saved.validate(assignment, state_schema)
    None =>
      @causal.CausalLog::new() catch {
        error => raise FrameIoError::Handler(@debug.to_string(error))
      }
  }
  let mut recovered_watermark = -1L
  for record in wal_records {
    match record {
      WorkerWalRecord::Checkpoint(saved) =>
        recovered_watermark = saved.checkpoint.watermark()
      WorkerWalRecord::EmbeddedDelta(_) => ()
      WorkerWalRecord::Batch(_) | WorkerWalRecord::Delta(_) => ()
      WorkerWalRecord::Watermark(value) =>
        recovered_watermark = value.timestamp()
    }
  }
  let delta_records : Array[DeltaWalRecord] = []
  let known_deltas : Map[String, @protocol.DeltaBatch] = Map([])
  match (journal_checkpoint, delta_replay) {
    (Some(_), _) | (_, None) => ()
    (_, Some(delta_replay_value)) => {
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
  // A crash may leave one durable exchange payload before its order marker.
  // It is necessarily a tail record because the Worker processes frames
  // serially and acknowledges only after writing the marker. Complete that
  // append before replaying or announcing Resume.
  let mut referenced_deltas = 0
  for record in wal_records {
    match record {
      WorkerWalRecord::Delta(id) => {
        if referenced_deltas >= delta_records.length() ||
          delta_records[referenced_deltas].delta_id != id {
          raise FrameIoError::Handler("exchange journal order mismatch")
        }
        referenced_deltas += 1
      }
      _ => ()
    }
  }
  for record in delta_records[referenced_deltas:] {
    let marker : DeltaOrderWalRecord = {
      kind: "delta-order",
      delta_id: record.delta_id,
    }
    ignore(
      wal.append(@utf8.encode(@json.to_json(marker).stringify())) catch {
        error => raise FrameIoError::Wal(error)
      },
    )
    wal_records.push(WorkerWalRecord::Delta(record.delta_id))
  }
  for record in wal_records {
    match record {
      WorkerWalRecord::Batch(batch) => admit_causal_batch(causal_log, batch)
      WorkerWalRecord::EmbeddedDelta(delta) => {
        admit_causal_delta(causal_log, delta)
        known_deltas[delta.delta_id()] = delta
      }
      WorkerWalRecord::Checkpoint(_) => ()
      WorkerWalRecord::Watermark(_) | WorkerWalRecord::Delta(_) => ()
    }
  }
  for record in delta_records {
    admit_causal_delta(causal_log, record.delta)
  }
  check_causal_state_limit(causal_log, causal_state_limit)
  let recovered_frontier_digest = frontier_digest(causal_log.frontier())
  match checkpoint {
    None => ()
    Some(value) =>
      validate_checkpoint(
        value, assignment, wal, replay, delta_replay, delta_wal_offset, wal_records,
        delta_records, restored_offset, state_schema,
      )
  }
  let mut primary_replay_start = match journal_checkpoint {
    None => 0
    Some(saved) => {
      guard state_restore is Some(restore) else {
        raise FrameIoError::Handler(
          "journal checkpoint requires an operator restorer",
        )
      }
      restore(
        saved.checkpoint.operator_state().unwrap(),
        saved.checkpoint.persisted_offset(),
        saved.checkpoint.frontier_digest(),
      )
      1
    }
  }
  match (checkpoint, state_restore) {
    (Some(saved), Some(restore)) =>
      match saved.operator_state() {
        None => ()
        Some(state) => {
          restore(state, saved.persisted_offset(), saved.frontier_digest())
          primary_replay_start = replay.record_count_before(saved.wal_offset()) catch {
            _ =>
              raise FrameIoError::Handler("checkpoint is not on a WAL boundary")
          }
        }
      }
    _ => ()
  }
  let (replayed_batches, replayed_deltas) = replay_wal_records(
    wal_records[primary_replay_start:].to_owned(),
    handler,
    watermark_handler,
    delta_handler,
    known_deltas,
  )
  session.restore_watermark(recovered_watermark) catch {
    error => raise FrameIoError::Session(error)
  }
  // Primary, exchange, and watermark records share a replay order. Restoring
  // a checkpoint skips exactly the journal prefix represented in its state.
  check_causal_state_limit(
    causal_log,
    causal_state_limit,
    operator_count=match operator_state_count {
      None => 0
      Some(count) => count()
    },
  )
  flush_outputs()
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
    state_schema,
    watermark=session.watermark(),
    operator_state?=match state_snapshot {
      None => None
      Some(snapshot) => Some(snapshot())
    },
  )
  release()
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
  let mut reclaimed_offset = journal_checkpoint
    .map(saved => saved.checkpoint.persisted_offset())
    .unwrap_or(-1L)
  let mut journal_compacted = journal_checkpoint is Some(_)
  let mut reported_watermark = -1L
  for ;; {
    acquire()
    defer release()
    if state_watermark is Some(current) && current() > reported_watermark {
      reported_watermark = current()
      send_worker_progress(
        connection,
        outbound,
        assignment.partition(),
        assignment.epoch(),
        session.last_persisted_offset(),
        session.last_applied_offset(),
        frontier_digest(causal_log.frontier()),
        maximum_frame_bytes,
        watermark=reported_watermark,
      )
    }
    let operator_count = operator_state_count.map(count => count()).unwrap_or(0)
    let compact_due = wal.stats().record_count() >= 16 ||
      (
        wal.stats().record_count() > 1 &&
        causal_state_limit > 0 &&
        causal_log.state_count() + operator_count > causal_state_limit / 2
      ) ||
      (
        wal.stats().record_count() > 1 &&
        wal.maximum_bytes() > 0L &&
        wal.write_offset() > wal.maximum_bytes() / 2L
      )
    if compact_due {
      match (state_compact, state_snapshot) {
        (Some(compact), Some(snapshot)) => {
          causal_log = reclaim_worker_journal(
            wal, delta_wal, assignment, session, causal_log, state_schema, compact,
            snapshot,
          )
          journal_compacted = true
          reclaimed_offset = session.last_applied_offset()
          known_batches.clear()
          known_deltas.clear()
          match reclamation_barrier {
            None => ()
            Some(barrier) => barrier()
          }
        }
        _ => ()
      }
    }
    release()
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
    acquire()
    let batch = match frame.kind() {
      @protocol.FrameKind::EventBatch =>
        Some(decode_batch_payload(frame.payload()))
      _ => None
    }
    let watermark = match frame.kind() {
      @protocol.FrameKind::Watermark =>
        Some(
          @protocol.Watermark::from_payload(frame.payload()) catch {
            error => raise FrameIoError::Message(error)
          },
        )
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
            None if value.end_offset() <= reclaimed_offset => ()
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
    let duplicate_watermark = match watermark {
      None => false
      Some(value) => value.timestamp() <= session.watermark()
    }
    let duplicate = duplicate_by_id ||
      duplicate_by_range ||
      duplicate_by_delta_wal ||
      duplicate_watermark
    // Reject a new batch before WorkerSession advances its receive cursor. A
    // budget refusal must be fully fail-closed: no WAL append, ACK, or cursor
    // movement is allowed for the refused input.
    if !duplicate {
      match batch {
        Some(batch_value) => {
          let candidate = causal_log.copy()
          admit_causal_batch(candidate, batch_value)
          let operator_count = match primary_admission_validator {
            None => 0
            Some(validate) => validate(batch_value)
          }
          check_causal_state_limit(
            candidate,
            causal_state_limit,
            operator_count~,
          )
        }
        None => ()
      }
      match delta {
        Some(delta_value) => {
          let candidate = causal_log.copy()
          admit_causal_delta(candidate, delta_value)
          let operator_count = match delta_admission_validator {
            None => 0
            Some(validate) => validate(delta_value)
          }
          check_causal_state_limit(
            candidate,
            causal_state_limit,
            operator_count~,
          )
        }
        None => ()
      }
    }
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
          let persisted_bytes = @utf8.encode(frame.payload())
          ignore(
            wal.append(persisted_bytes) catch {
              error => raise FrameIoError::Wal(error)
            },
          )
          @crash_boundary.hit("worker.input.persisted", wal_path) catch {
            error => raise FrameIoError::Io(@debug.to_string(error))
          }
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
          flush_outputs()
          session.mark_applied(batch_value.end_offset()) catch {
            error => raise FrameIoError::Session(error)
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
            state_schema,
            watermark=session.watermark(),
            operator_state?=match state_snapshot {
              None => None
              Some(snapshot) => Some(snapshot())
            },
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
          if journal_compacted {
            ignore(
              wal.append(
                encode_worker_record(
                  WorkerWalRecord::EmbeddedDelta(delta_value),
                ),
              ) catch {
                error => raise FrameIoError::Wal(error)
              },
            )
          } else {
            match delta_wal {
              Some(wal) => {
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
            let marker : DeltaOrderWalRecord = {
              kind: "delta-order",
              delta_id: delta_value.delta_id(),
            }
            ignore(
              wal.append(@utf8.encode(@json.to_json(marker).stringify())) catch {
                error => raise FrameIoError::Wal(error)
              },
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
          flush_outputs()
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
            state_schema,
            watermark=session.watermark(),
            operator_state?=match state_snapshot {
              None => None
              Some(snapshot) => Some(snapshot())
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
      @protocol.FrameKind::Retire => {
        let request = @protocol.Retire::from_payload(frame.payload()) catch {
          error => raise FrameIoError::Message(error)
        }
        match (state_compact, state_snapshot) {
          (Some(compact), Some(snapshot)) => {
            causal_log = reclaim_worker_journal(
              wal,
              delta_wal,
              assignment,
              session,
              causal_log,
              state_schema,
              compact,
              snapshot,
              global_certificate=request.certificate,
            )
            journal_compacted = true
            reclaimed_offset = session.last_applied_offset()
            known_batches.clear()
            known_deltas.clear()
            match reclamation_barrier {
              None => ()
              Some(barrier) => barrier()
            }
          }
          _ => ()
        }
        let ack : @protocol.RetireAck = {
          partition: request.partition,
          epoch: request.epoch,
          offset: request.offset,
          checkpoint_id: request.certificate.checkpoint_id(),
        }
        let reply = outbound.build(
          @protocol.FrameKind::RetireAck,
          ack.to_payload(),
        ) catch {
          error => raise FrameIoError::Protocol(error)
        }
        write_frame(connection, reply, maximum_frame_bytes~)
      }
      @protocol.FrameKind::Watermark => {
        guard watermark is Some(value) else {
          raise FrameIoError::Message(@protocol.MessageError::Malformed)
        }
        if !duplicate {
          let record = WatermarkWalRecord::{
            kind: "watermark",
            watermark: value,
          }
          ignore(
            wal.append(@utf8.encode(@json.to_json(record).stringify())) catch {
              error => raise FrameIoError::Wal(error)
            },
          )
        }
        if !duplicate {
          match watermark_handler {
            None => ()
            Some(handler) => handler(value.timestamp())
          }
          flush_outputs()
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
            state_schema,
            watermark=session.watermark(),
            operator_state?=match state_snapshot {
              None => None
              Some(snapshot) => Some(snapshot())
            },
          )
        }
        if state_watermark is Some(current) && current() >= 0L {
          reported_watermark = current()
          send_worker_progress(
            connection,
            outbound,
            assignment.partition(),
            assignment.epoch(),
            session.last_persisted_offset(),
            session.last_applied_offset(),
            frontier_digest(causal_log.frontier()),
            maximum_frame_bytes,
            watermark=reported_watermark,
          )
        }
      }
      @protocol.FrameKind::Seal => ()
      @protocol.FrameKind::DrainRequest => {
        let pending_events = causal_log.pending_count()
        if pending_events == 0 {
          match watermark_handler {
            None => ()
            Some(handler) => {
              if session.watermark() < 9223372036854775807L {
                let record = WatermarkWalRecord::{
                  kind: "watermark",
                  watermark: @protocol.Watermark::new(
                    assignment.partition(),
                    assignment.epoch(),
                    9223372036854775807L,
                  ) catch {
                    error => raise FrameIoError::Protocol(error)
                  },
                }
                ignore(
                  wal.append(@utf8.encode(@json.to_json(record).stringify())) catch {
                    error => raise FrameIoError::Wal(error)
                  },
                )
                session.restore_watermark(9223372036854775807L) catch {
                  error => raise FrameIoError::Session(error)
                }
              }
              handler(9223372036854775807L)
            }
          }
          flush_outputs()
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
            state_schema,
            watermark=session.watermark(),
            operator_state?=match state_snapshot {
              None => None
              Some(snapshot) => Some(snapshot())
            },
          )
          match drain_handler {
            None => ()
            Some(finish) => finish()
          }
          match terminal_barrier {
            None => ()
            Some(barrier) => barrier()
          }
        }
        let ack = match output_summary {
          None =>
            @protocol.DrainAck::new(
              frame.message_id(),
              assignment.partition(),
              assignment.epoch(),
              session.last_persisted_offset(),
              session.last_applied_offset(),
              pending_events,
              frontier_digest(causal_log.frontier()),
            ) catch {
              error => raise FrameIoError::Protocol(error)
            }
          Some(make_summary) => {
            let (
              task_id,
              output_count,
              output_digest,
              task_state_digest,
              output_consumer_id,
              output_acked_sequence,
              output_retained_from,
            ) = make_summary()
            @protocol.DrainAck::new(
              frame.message_id(),
              assignment.partition(),
              assignment.epoch(),
              session.last_persisted_offset(),
              session.last_applied_offset(),
              pending_events,
              frontier_digest(causal_log.frontier()),
              task_id~,
              output_count~,
              output_digest~,
              task_state_digest~,
              output_consumer_id?,
              output_acked_sequence?,
              output_retained_from?,
            ) catch {
              error => raise FrameIoError::Protocol(error)
            }
          }
        }
        let ack_frame = outbound.build(
          @protocol.FrameKind::DrainAck,
          ack.to_payload(),
        ) catch {
          error => raise FrameIoError::Protocol(error)
        }
        write_frame(connection, ack_frame, maximum_frame_bytes~)
        if pending_events > 0 {
          raise FrameIoError::PendingEvents(pending_events)
        }
        break
      }
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
  requested_partition? : Int,
  auth_token? : String,
) -> WorkerRunSummary raise FrameIoError {
  run_worker_impl(
    connection,
    cluster_id,
    coordinator_id,
    worker_id,
    incarnation,
    capabilities,
    wal_path,
    handler,
    checkpoint_path?,
    maximum_frame_bytes~,
    initial_credit_events~,
    initial_credit_bytes~,
    heartbeat_interval_ms~,
    delta_handler?,
    delta_wal_path?,
    requested_partition?,
    auth_token?,
  )
}
