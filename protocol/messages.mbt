///|
/// Errors raised while decoding a typed payload carried by a valid frame.
pub(all) suberror MessageError {
  Malformed
  WrongKind(expected~ : FrameKind, actual~ : FrameKind)
  Protocol(ProtocolError)
} derive(Debug, Eq)

///|
/// A causal event record used on the first data-plane workload.
pub(all) struct EventRecord {
  id : @causal.EventId
  parents : Array[@causal.EventId]
  key : String
  payload : Json
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn EventRecord::new(
  id : @causal.EventId,
  parents : ArrayView[@causal.EventId],
  key : String,
  payload : Json,
) -> EventRecord raise ProtocolError {
  let event = @causal.Event::new(id, parents, key, payload) catch {
    error => raise ProtocolError::Causal(error)
  }
  {
    id: event.id(),
    parents: event.parents().to_owned(),
    key: event.key(),
    payload: event.payload(),
  }
}

///|
pub fn EventRecord::id(self : EventRecord) -> @causal.EventId {
  self.id
}

///|
pub fn EventRecord::parents(self : EventRecord) -> ArrayView[@causal.EventId] {
  self.parents[:]
}

///|
pub fn EventRecord::key(self : EventRecord) -> String {
  self.key
}

///|
pub fn EventRecord::payload(self : EventRecord) -> Json {
  self.payload
}

///|
pub fn EventRecord::to_event(
  self : EventRecord,
) -> @causal.Event[Json] raise ProtocolError {
  @causal.Event::new(self.id, self.parents[:], self.key, self.payload) catch {
    error => raise ProtocolError::Causal(error)
  }
}

///|
pub(all) struct Hello {
  worker_id : String
  incarnation : String
  capabilities : Array[String]
  last_session_id : String?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Hello::new(
  worker_id : String,
  incarnation : String,
  capabilities : Array[String],
  last_session_id? : String,
) -> Hello raise ProtocolError {
  if worker_id.is_empty() {
    raise ProtocolError::EmptyWorkerId
  }
  if incarnation.is_empty() {
    raise ProtocolError::EmptyIncarnation
  }
  { worker_id, incarnation, capabilities, last_session_id, }
}

///|
pub fn Hello::worker_id(self : Hello) -> String {
  self.worker_id
}

///|
pub fn Hello::incarnation(self : Hello) -> String {
  self.incarnation
}

///|
pub fn Hello::capabilities(self : Hello) -> ArrayView[String] {
  self.capabilities[:]
}

///|
pub fn Hello::last_session_id(self : Hello) -> String? {
  self.last_session_id
}

///|
pub(all) struct AssignmentSpec {
  partition : Int
  epoch : Int
  incarnation : String
  start_offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn AssignmentSpec::new(
  partition : Int,
  epoch : Int,
  incarnation : String,
  start_offset? : Int64 = 0L,
) -> AssignmentSpec raise ProtocolError {
  if partition < 0 {
    raise ProtocolError::UnknownAssignment
  }
  if epoch < 0 {
    raise ProtocolError::InvalidEpoch(epoch)
  }
  if incarnation.is_empty() {
    raise ProtocolError::EmptyIncarnation
  }
  if start_offset < 0L {
    raise ProtocolError::InvalidOffset(start_offset)
  }
  { partition, epoch, incarnation, start_offset, }
}

///|
pub fn AssignmentSpec::partition(self : AssignmentSpec) -> Int {
  self.partition
}

///|
pub fn AssignmentSpec::epoch(self : AssignmentSpec) -> Int {
  self.epoch
}

///|
pub fn AssignmentSpec::incarnation(self : AssignmentSpec) -> String {
  self.incarnation
}

///|
pub fn AssignmentSpec::start_offset(self : AssignmentSpec) -> Int64 {
  self.start_offset
}

///|
pub(all) struct Welcome {
  session_id : String
  assignment : AssignmentSpec?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Welcome::new(
  session_id : String,
  assignment? : AssignmentSpec,
) -> Welcome raise ProtocolError {
  if session_id.is_empty() {
    raise ProtocolError::InvalidFrameField
  }
  { session_id, assignment, }
}

///|
pub fn Welcome::session_id(self : Welcome) -> String {
  self.session_id
}

///|
pub fn Welcome::assignment(self : Welcome) -> AssignmentSpec? {
  self.assignment
}

///|
pub(all) struct Resume {
  partition : Int
  epoch : Int
  persisted_offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Resume::new(
  partition : Int,
  epoch : Int,
  persisted_offset : Int64,
) -> Resume raise ProtocolError {
  if partition < 0 {
    raise ProtocolError::UnknownAssignment
  }
  if epoch < 0 {
    raise ProtocolError::InvalidEpoch(epoch)
  }
  if persisted_offset < -1L {
    raise ProtocolError::InvalidOffset(persisted_offset)
  }
  { partition, epoch, persisted_offset, }
}

///|
pub fn Resume::partition(self : Resume) -> Int {
  self.partition
}

///|
pub fn Resume::epoch(self : Resume) -> Int {
  self.epoch
}

///|
pub fn Resume::persisted_offset(self : Resume) -> Int64 {
  self.persisted_offset
}

///|
pub(all) struct EventBatch {
  partition : Int
  epoch : Int
  first_offset : Int64
  events : Array[EventRecord]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn EventBatch::new(
  partition : Int,
  epoch : Int,
  first_offset : Int64,
  events : Array[EventRecord],
) -> EventBatch raise ProtocolError {
  if events.is_empty() {
    raise ProtocolError::EmptyBatch
  }
  if partition < 0 || epoch < 0 || first_offset < 0L {
    raise ProtocolError::InvalidBatchOffset
  }
  { partition, epoch, first_offset, events, }
}

///|
pub fn EventBatch::partition(self : EventBatch) -> Int {
  self.partition
}

///|
pub fn EventBatch::epoch(self : EventBatch) -> Int {
  self.epoch
}

///|
pub fn EventBatch::first_offset(self : EventBatch) -> Int64 {
  self.first_offset
}

///|
pub fn EventBatch::events(self : EventBatch) -> ArrayView[EventRecord] {
  self.events[:]
}

///|
pub fn EventBatch::end_offset(self : EventBatch) -> Int64 {
  self.first_offset + self.events.length().to_int64() - 1L
}

///|
/// A derived event transfer between different Worker assignments. It has its
/// own idempotence domain (the enclosing frame message ID), so it does not
/// consume the target assignment's source offset cursor.
pub(all) struct DeltaBatch {
  delta_id : String
  source_partition : Int
  source_epoch : Int
  target_partition : Int
  target_epoch : Int
  events : Array[EventRecord]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn DeltaBatch::new(
  delta_id : String,
  source_partition : Int,
  source_epoch : Int,
  target_partition : Int,
  target_epoch : Int,
  events : Array[EventRecord],
) -> DeltaBatch raise ProtocolError {
  if delta_id.is_empty() {
    raise ProtocolError::InvalidDeltaId
  }
  if source_partition < 0 ||
    target_partition < 0 ||
    source_epoch < 0 ||
    target_epoch < 0 ||
    events.is_empty() {
    raise ProtocolError::InvalidBatchOffset
  }
  {
    delta_id,
    source_partition,
    source_epoch,
    target_partition,
    target_epoch,
    events,
  }
}

///|
pub fn DeltaBatch::delta_id(self : DeltaBatch) -> String {
  self.delta_id
}

///|
pub fn DeltaBatch::source_partition(self : DeltaBatch) -> Int {
  self.source_partition
}

///|
pub fn DeltaBatch::source_epoch(self : DeltaBatch) -> Int {
  self.source_epoch
}

///|
pub fn DeltaBatch::target_partition(self : DeltaBatch) -> Int {
  self.target_partition
}

///|
pub fn DeltaBatch::target_epoch(self : DeltaBatch) -> Int {
  self.target_epoch
}

///|
pub fn DeltaBatch::events(self : DeltaBatch) -> ArrayView[EventRecord] {
  self.events[:]
}

///|
pub(all) struct Credit {
  partition : Int
  epoch : Int
  events : Int64
  bytes : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Credit::new(
  partition : Int,
  epoch : Int,
  events : Int64,
  bytes : Int64,
) -> Credit raise ProtocolError {
  if partition < 0 ||
    epoch < 0 ||
    events < 0L ||
    bytes < 0L ||
    (events == 0L && bytes == 0L) {
    raise ProtocolError::InvalidCredit
  }
  { partition, epoch, events, bytes, }
}

///|
pub fn Credit::partition(self : Credit) -> Int {
  self.partition
}

///|
pub fn Credit::epoch(self : Credit) -> Int {
  self.epoch
}

///|
pub fn Credit::events(self : Credit) -> Int64 {
  self.events
}

///|
pub fn Credit::bytes(self : Credit) -> Int64 {
  self.bytes
}

///|
pub(all) struct AckPersisted {
  partition : Int
  epoch : Int
  offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn AckPersisted::new(
  partition : Int,
  epoch : Int,
  offset : Int64,
) -> AckPersisted raise ProtocolError {
  if partition < 0 || epoch < 0 || offset < -1L {
    raise ProtocolError::InvalidOffset(offset)
  }
  { partition, epoch, offset, }
}

///|
pub fn AckPersisted::partition(self : AckPersisted) -> Int {
  self.partition
}

///|
pub fn AckPersisted::epoch(self : AckPersisted) -> Int {
  self.epoch
}

///|
pub fn AckPersisted::offset(self : AckPersisted) -> Int64 {
  self.offset
}

///|
/// Acknowledgement for one cross-Worker delta frame.
pub(all) struct DeltaAck {
  delta_id : String
  target_partition : Int
  target_epoch : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn DeltaAck::new(
  delta_id : String,
  target_partition : Int,
  target_epoch : Int,
) -> DeltaAck raise ProtocolError {
  if delta_id.is_empty() {
    raise ProtocolError::InvalidDeltaId
  }
  if target_partition < 0 {
    raise ProtocolError::UnknownAssignment
  }
  if target_epoch < 0 {
    raise ProtocolError::InvalidEpoch(target_epoch)
  }
  { delta_id, target_partition, target_epoch, }
}

///|
pub fn DeltaAck::delta_id(self : DeltaAck) -> String {
  self.delta_id
}

///|
pub fn DeltaAck::target_partition(self : DeltaAck) -> Int {
  self.target_partition
}

///|
pub fn DeltaAck::target_epoch(self : DeltaAck) -> Int {
  self.target_epoch
}

///|
pub(all) struct Heartbeat {
  partition : Int
  epoch : Int
  queue_depth : Int
  persisted_offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Heartbeat::new(
  partition : Int,
  epoch : Int,
  queue_depth : Int,
  persisted_offset : Int64,
) -> Heartbeat raise ProtocolError {
  if partition < 0 || epoch < 0 || queue_depth < 0 || persisted_offset < -1L {
    raise ProtocolError::InvalidFrameField
  }
  { partition, epoch, queue_depth, persisted_offset, }
}

///|
pub fn Heartbeat::partition(self : Heartbeat) -> Int {
  self.partition
}

///|
pub fn Heartbeat::epoch(self : Heartbeat) -> Int {
  self.epoch
}

///|
pub fn Heartbeat::queue_depth(self : Heartbeat) -> Int {
  self.queue_depth
}

///|
pub fn Heartbeat::persisted_offset(self : Heartbeat) -> Int64 {
  self.persisted_offset
}

///|
pub(all) struct Progress {
  partition : Int
  epoch : Int
  persisted_offset : Int64
  applied_offset : Int64
  frontier_digest : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Progress::new(
  partition : Int,
  epoch : Int,
  persisted_offset : Int64,
  applied_offset : Int64,
  frontier_digest : String,
) -> Progress raise ProtocolError {
  if partition < 0 ||
    epoch < 0 ||
    persisted_offset < -1L ||
    applied_offset < -1L ||
    applied_offset > persisted_offset ||
    frontier_digest.is_empty() {
    raise ProtocolError::InvalidFrameField
  }
  { partition, epoch, persisted_offset, applied_offset, frontier_digest, }
}

///|
pub fn Progress::partition(self : Progress) -> Int {
  self.partition
}

///|
pub fn Progress::epoch(self : Progress) -> Int {
  self.epoch
}

///|
pub fn Progress::persisted_offset(self : Progress) -> Int64 {
  self.persisted_offset
}

///|
pub fn Progress::applied_offset(self : Progress) -> Int64 {
  self.applied_offset
}

///|
pub fn Progress::frontier_digest(self : Progress) -> String {
  self.frontier_digest
}

///|
/// Ask one worker to prove progress for a query obligation.
pub(all) struct Probe {
  query_id : String
  partition : Int
  epoch : Int
  target_offset : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Probe::new(
  query_id : String,
  partition : Int,
  epoch : Int,
  target_offset : Int64,
) -> Probe raise ProtocolError {
  if query_id.is_empty() {
    raise ProtocolError::InvalidQueryId
  }
  if partition < 0 {
    raise ProtocolError::UnknownAssignment
  }
  if epoch < 0 {
    raise ProtocolError::InvalidEpoch(epoch)
  }
  if target_offset < -1L {
    raise ProtocolError::InvalidOffset(target_offset)
  }
  { query_id, partition, epoch, target_offset, }
}

///|
pub fn Probe::query_id(self : Probe) -> String {
  self.query_id
}

///|
pub fn Probe::partition(self : Probe) -> Int {
  self.partition
}

///|
pub fn Probe::epoch(self : Probe) -> Int {
  self.epoch
}

///|
pub fn Probe::target_offset(self : Probe) -> Int64 {
  self.target_offset
}

///|
/// Evidence returned by a worker for one probe.
pub(all) struct ProbeReply {
  query_id : String
  partition : Int
  epoch : Int
  reached_offset : Int64
  frontier_digest : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn ProbeReply::new(
  query_id : String,
  partition : Int,
  epoch : Int,
  reached_offset : Int64,
  frontier_digest : String,
) -> ProbeReply raise ProtocolError {
  if query_id.is_empty() {
    raise ProtocolError::InvalidQueryId
  }
  if partition < 0 {
    raise ProtocolError::UnknownAssignment
  }
  if epoch < 0 {
    raise ProtocolError::InvalidEpoch(epoch)
  }
  if reached_offset < -1L {
    raise ProtocolError::InvalidOffset(reached_offset)
  }
  if frontier_digest.is_empty() {
    raise ProtocolError::InvalidDigest
  }
  { query_id, partition, epoch, reached_offset, frontier_digest, }
}

///|
pub fn ProbeReply::query_id(self : ProbeReply) -> String {
  self.query_id
}

///|
pub fn ProbeReply::partition(self : ProbeReply) -> Int {
  self.partition
}

///|
pub fn ProbeReply::epoch(self : ProbeReply) -> Int {
  self.epoch
}

///|
pub fn ProbeReply::reached_offset(self : ProbeReply) -> Int64 {
  self.reached_offset
}

///|
pub fn ProbeReply::frontier_digest(self : ProbeReply) -> String {
  self.frontier_digest
}

///|
/// Confirm that a query obligation is closed at a verified frontier.
pub(all) struct Seal {
  query_id : String
  partition : Int
  epoch : Int
  sealed_offset : Int64
  frontier_digest : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Seal::new(
  query_id : String,
  partition : Int,
  epoch : Int,
  sealed_offset : Int64,
  frontier_digest : String,
) -> Seal raise ProtocolError {
  if query_id.is_empty() {
    raise ProtocolError::InvalidQueryId
  }
  if partition < 0 {
    raise ProtocolError::UnknownAssignment
  }
  if epoch < 0 {
    raise ProtocolError::InvalidEpoch(epoch)
  }
  if sealed_offset < -1L {
    raise ProtocolError::InvalidOffset(sealed_offset)
  }
  if frontier_digest.is_empty() {
    raise ProtocolError::InvalidDigest
  }
  { query_id, partition, epoch, sealed_offset, frontier_digest, }
}

///|
pub fn Seal::query_id(self : Seal) -> String {
  self.query_id
}

///|
pub fn Seal::partition(self : Seal) -> Int {
  self.partition
}

///|
pub fn Seal::epoch(self : Seal) -> Int {
  self.epoch
}

///|
pub fn Seal::sealed_offset(self : Seal) -> Int64 {
  self.sealed_offset
}

///|
pub fn Seal::frontier_digest(self : Seal) -> String {
  self.frontier_digest
}

///|
fn[T : ToJson] encode_payload(value : T) -> String {
  @json.to_json(value).stringify()
}

///|
fn[T : FromJson] decode_payload(text : String) -> T raise MessageError {
  let json = @json.parse(text) catch { _ => raise MessageError::Malformed }
  @json.from_json(json) catch {
    _ => raise MessageError::Malformed
  }
}

///|
fn protocol_error(error : ProtocolError) -> MessageError {
  MessageError::Protocol(error)
}

///|
fn validate_event_record(value : EventRecord) -> EventRecord raise MessageError {
  EventRecord::new(value.id, value.parents[:], value.key, value.payload) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Hello::to_payload(self : Hello) -> String {
  encode_payload(self)
}

///|
pub fn Hello::from_payload(text : String) -> Hello raise MessageError {
  let value : Hello = decode_payload(text)
  Hello::new(
    value.worker_id,
    value.incarnation,
    value.capabilities,
    last_session_id?=value.last_session_id,
  ) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Welcome::to_payload(self : Welcome) -> String {
  encode_payload(self)
}

///|
pub fn Welcome::from_payload(text : String) -> Welcome raise MessageError {
  let value : Welcome = decode_payload(text)
  let assignment = match value.assignment {
    None => None
    Some(spec) =>
      Some(
        AssignmentSpec::new(
          spec.partition,
          spec.epoch,
          spec.incarnation,
          start_offset=spec.start_offset,
        ) catch {
          error => raise protocol_error(error)
        },
      )
  }
  Welcome::new(value.session_id, assignment?) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn AssignmentSpec::to_payload(self : AssignmentSpec) -> String {
  encode_payload(self)
}

///|
pub fn AssignmentSpec::from_payload(
  text : String,
) -> AssignmentSpec raise MessageError {
  let value : AssignmentSpec = decode_payload(text)
  AssignmentSpec::new(
    value.partition,
    value.epoch,
    value.incarnation,
    start_offset=value.start_offset,
  ) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Resume::to_payload(self : Resume) -> String {
  encode_payload(self)
}

///|
pub fn Resume::from_payload(text : String) -> Resume raise MessageError {
  let value : Resume = decode_payload(text)
  Resume::new(value.partition, value.epoch, value.persisted_offset) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn EventBatch::to_payload(self : EventBatch) -> String {
  encode_payload(self)
}

///|
pub fn EventBatch::from_payload(text : String) -> EventBatch raise MessageError {
  let value : EventBatch = decode_payload(text)
  let events = value.events.map(validate_event_record)
  EventBatch::new(value.partition, value.epoch, value.first_offset, events) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn DeltaBatch::to_payload(self : DeltaBatch) -> String {
  encode_payload(self)
}

///|
pub fn DeltaBatch::from_payload(text : String) -> DeltaBatch raise MessageError {
  let value : DeltaBatch = decode_payload(text)
  let events = value.events.map(validate_event_record)
  DeltaBatch::new(
    value.delta_id,
    value.source_partition,
    value.source_epoch,
    value.target_partition,
    value.target_epoch,
    events,
  ) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Credit::to_payload(self : Credit) -> String {
  encode_payload(self)
}

///|
pub fn Credit::from_payload(text : String) -> Credit raise MessageError {
  let value : Credit = decode_payload(text)
  Credit::new(value.partition, value.epoch, value.events, value.bytes) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn AckPersisted::to_payload(self : AckPersisted) -> String {
  encode_payload(self)
}

///|
pub fn AckPersisted::from_payload(
  text : String,
) -> AckPersisted raise MessageError {
  let value : AckPersisted = decode_payload(text)
  AckPersisted::new(value.partition, value.epoch, value.offset) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn DeltaAck::to_payload(self : DeltaAck) -> String {
  encode_payload(self)
}

///|
pub fn DeltaAck::from_payload(text : String) -> DeltaAck raise MessageError {
  let value : DeltaAck = decode_payload(text)
  DeltaAck::new(value.delta_id, value.target_partition, value.target_epoch) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Heartbeat::to_payload(self : Heartbeat) -> String {
  encode_payload(self)
}

///|
pub fn Heartbeat::from_payload(text : String) -> Heartbeat raise MessageError {
  let value : Heartbeat = decode_payload(text)
  Heartbeat::new(
    value.partition,
    value.epoch,
    value.queue_depth,
    value.persisted_offset,
  ) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Progress::to_payload(self : Progress) -> String {
  encode_payload(self)
}

///|
pub fn Progress::from_payload(text : String) -> Progress raise MessageError {
  let value : Progress = decode_payload(text)
  Progress::new(
    value.partition,
    value.epoch,
    value.persisted_offset,
    value.applied_offset,
    value.frontier_digest,
  ) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Probe::to_payload(self : Probe) -> String {
  encode_payload(self)
}

///|
pub fn Probe::from_payload(text : String) -> Probe raise MessageError {
  let value : Probe = decode_payload(text)
  Probe::new(value.query_id, value.partition, value.epoch, value.target_offset) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn ProbeReply::to_payload(self : ProbeReply) -> String {
  encode_payload(self)
}

///|
pub fn ProbeReply::from_payload(text : String) -> ProbeReply raise MessageError {
  let value : ProbeReply = decode_payload(text)
  ProbeReply::new(
    value.query_id,
    value.partition,
    value.epoch,
    value.reached_offset,
    value.frontier_digest,
  ) catch {
    error => raise protocol_error(error)
  }
}

///|
pub fn Seal::to_payload(self : Seal) -> String {
  encode_payload(self)
}

///|
pub fn Seal::from_payload(text : String) -> Seal raise MessageError {
  let value : Seal = decode_payload(text)
  Seal::new(
    value.query_id,
    value.partition,
    value.epoch,
    value.sealed_offset,
    value.frontier_digest,
  ) catch {
    error => raise protocol_error(error)
  }
}
