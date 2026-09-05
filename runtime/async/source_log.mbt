///|
/// Errors raised by the durable Coordinator-side source log.
pub(all) suberror SourceLogError {
  EmptyPath
  InvalidPartition(Int)
  InvalidOffset(Int64)
  InvalidBatchSize(Int)
  EmptySessionPrefix
  DuplicateWorkerConnection(String)
  DuplicateOutbox(Int)
  MissingOutbox(Int)
  ClosedOutbox(Int)
  OffsetGap(expected~ : Int64, actual~ : Int64)
  IdentityConflict(@causal.EventId)
  Malformed
  UnsupportedVersion(Int)
  Plan(@plan.PlanError)
  Protocol(@protocol.ProtocolError)
  Wal(WalError)
  Io(String)
} derive(Debug, Eq)

///|
/// One durable source event. Assignment epochs are deliberately absent: the
/// same logical source offset can be re-enveloped under a newer fencing epoch.
struct SourceEntry {
  version : Int
  partition : Int
  offset : Int64
  event : @protocol.EventRecord
} derive(Debug, Eq, ToJson, FromJson)

///|
struct SourceLocation {
  partition : Int
  offset : Int64
  event : @protocol.EventRecord
}

///|
/// Coordinator-side source-of-truth for input content and logical offsets.
/// It complements CoordinatorStore, which journals assignment/send intent.
pub struct SourceLog {
  wal : Wal
  entries : Map[Int, Array[SourceEntry]]
  locations : Map[@causal.EventId, SourceLocation]
  next_offsets : Map[Int, Int64]
  write_lock : @async.Mutex
}

///|
fn validate_source_event(
  event : @protocol.EventRecord,
) -> @protocol.EventRecord raise SourceLogError {
  @protocol.EventRecord::new(
    event.id(),
    event.parents(),
    event.key(),
    event.payload(),
  ) catch {
    error => raise SourceLogError::Protocol(error)
  }
}

///|
fn prepare_source_entry(
  locations : Map[@causal.EventId, SourceLocation],
  next_offsets : Map[Int, Int64],
  entry : SourceEntry,
) -> (SourceEntry, Bool) raise SourceLogError {
  if entry.version != 1 {
    raise SourceLogError::UnsupportedVersion(entry.version)
  }
  if entry.partition < 0 {
    raise SourceLogError::InvalidPartition(entry.partition)
  }
  if entry.offset < 0L {
    raise SourceLogError::InvalidOffset(entry.offset)
  }
  let event = validate_source_event(entry.event)
  match locations.get(event.id()) {
    Some(previous) => {
      if previous.partition == entry.partition &&
        previous.offset == entry.offset &&
        previous.event == event {
        return (
          SourceEntry::{
            version: 1,
            partition: entry.partition,
            offset: entry.offset,
            event,
          },
          false,
        )
      }
      raise SourceLogError::IdentityConflict(event.id())
    }
    None => ()
  }
  let expected = match next_offsets.get(entry.partition) {
    Some(value) => value
    None => entry.offset
  }
  if entry.offset != expected {
    raise SourceLogError::OffsetGap(expected~, actual=entry.offset)
  }
  (
    SourceEntry::{
      version: 1,
      partition: entry.partition,
      offset: entry.offset,
      event,
    },
    true,
  )
}

///|
fn commit_source_entry(
  entries : Map[Int, Array[SourceEntry]],
  locations : Map[@causal.EventId, SourceLocation],
  next_offsets : Map[Int, Int64],
  canonical : SourceEntry,
) -> Unit {
  let partition_entries = entries.get_or_init(canonical.partition, () => [])
  partition_entries.push(canonical)
  locations[canonical.event.id()] = SourceLocation::{
    partition: canonical.partition,
    offset: canonical.offset,
    event: canonical.event,
  }
  next_offsets[canonical.partition] = canonical.offset + 1L
}

///|
fn install_source_entry(
  entries : Map[Int, Array[SourceEntry]],
  locations : Map[@causal.EventId, SourceLocation],
  next_offsets : Map[Int, Int64],
  entry : SourceEntry,
) -> Unit raise SourceLogError {
  let (canonical, should_install) = prepare_source_entry(
    locations, next_offsets, entry,
  )
  if should_install {
    commit_source_entry(entries, locations, next_offsets, canonical)
  }
}

///|
pub async fn SourceLog::open(path : String) -> SourceLog raise SourceLogError {
  if path.is_empty() {
    raise SourceLogError::EmptyPath
  }
  let (wal, replay) = Wal::open(path) catch {
    error => raise SourceLogError::Wal(error)
  }
  errdefer wal.close()
  let entries : Map[Int, Array[SourceEntry]] = Map([])
  let locations : Map[@causal.EventId, SourceLocation] = Map([])
  let next_offsets : Map[Int, Int64] = Map([])
  for encoded in replay.records() {
    let text = @utf8.decode(encoded) catch {
      error => raise SourceLogError::Io(@debug.to_string(error))
    }
    let json = @json.parse(text) catch { _ => raise SourceLogError::Malformed }
    let entry : SourceEntry = @json.from_json(json) catch {
      _ => raise SourceLogError::Malformed
    }
    install_source_entry(entries, locations, next_offsets, entry)
  }
  { wal, entries, locations, next_offsets, write_lock: @async.Mutex::Mutex(), }
}

///|
pub fn SourceLog::close(self : SourceLog) -> Unit {
  self.wal.close()
}

///|
pub fn SourceLog::event_count(self : SourceLog) -> Int {
  self.locations.length()
}

///|
pub fn SourceLog::next_offset(self : SourceLog, partition : Int) -> Int64? {
  self.next_offsets.get(partition)
}

///|
fn SourceLog::entries_from(
  self : SourceLog,
  partition : Int,
  start_offset : Int64,
) -> Array[SourceEntry] raise SourceLogError {
  if partition < 0 {
    raise SourceLogError::InvalidPartition(partition)
  }
  if start_offset < 0L {
    raise SourceLogError::InvalidOffset(start_offset)
  }
  match self.entries.get(partition) {
    None => []
    Some(values) => values.filter(entry => entry.offset >= start_offset)
  }
}

///|
/// Append one input event and synchronize it before returning its offset.
/// Equal EventId retransmissions are idempotent; conflicting reuse fails.
pub async fn SourceLog::append(
  self : SourceLog,
  partition : Int,
  initial_offset : Int64,
  event : @protocol.EventRecord,
) -> Int64 raise SourceLogError {
  if partition < 0 {
    raise SourceLogError::InvalidPartition(partition)
  }
  if initial_offset < 0L {
    raise SourceLogError::InvalidOffset(initial_offset)
  }
  let event = validate_source_event(event)
  self.write_lock.acquire() catch {
    error => raise SourceLogError::Io(@debug.to_string(error))
  }
  defer self.write_lock.release()
  match self.locations.get(event.id()) {
    Some(previous) => {
      if previous.partition == partition && previous.event == event {
        return previous.offset
      }
      raise SourceLogError::IdentityConflict(event.id())
    }
    None => ()
  }
  let offset = self.next_offsets.get(partition).unwrap_or(initial_offset)
  let entry = SourceEntry::{ version: 1, partition, offset, event, }
  let (canonical, should_install) = prepare_source_entry(
    self.locations,
    self.next_offsets,
    entry,
  )
  if !should_install {
    return offset
  }
  let encoded = @utf8.encode(@json.to_json(canonical).stringify())
  ignore(
    self.wal.append(encoded) catch {
      error => raise SourceLogError::Wal(error)
    },
  )
  commit_source_entry(
    self.entries,
    self.locations,
    self.next_offsets,
    canonical,
  )
  offset
}

///|
/// Materialize durable source entries as contiguous EventBatches under the
/// assignment epoch active at send time.
pub fn SourceLog::batches(
  self : SourceLog,
  partition : Int,
  epoch : Int,
  start_offset : Int64,
  maximum_batch_events? : Int = 128,
) -> Array[@protocol.EventBatch] raise SourceLogError {
  if partition < 0 {
    raise SourceLogError::InvalidPartition(partition)
  }
  if epoch < 0 || start_offset < 0L {
    raise SourceLogError::InvalidOffset(start_offset)
  }
  if maximum_batch_events <= 0 {
    raise SourceLogError::InvalidBatchSize(maximum_batch_events)
  }
  let selected = self.entries_from(partition, start_offset)
  let result = []
  let mut index = 0
  while index < selected.length() {
    let first = selected[index].offset
    let events = []
    let end = Int::min(index + maximum_batch_events, selected.length())
    for entry in selected[index:end] {
      events.push(entry.event)
    }
    let batch = @protocol.EventBatch::new(partition, epoch, first, events) catch {
      error => raise SourceLogError::Protocol(error)
    }
    result.push(batch)
    index = end
  }
  result
}

///|
pub struct RoutedIngress {
  worker_id : String
  partition : Int
  epoch : Int
  offset : Int64
  event : @protocol.EventRecord
}

///|
pub fn RoutedIngress::worker_id(self : RoutedIngress) -> String {
  self.worker_id
}

///|
pub fn RoutedIngress::partition(self : RoutedIngress) -> Int {
  self.partition
}

///|
pub fn RoutedIngress::epoch(self : RoutedIngress) -> Int {
  self.epoch
}

///|
pub fn RoutedIngress::offset(self : RoutedIngress) -> Int64 {
  self.offset
}

///|
pub fn RoutedIngress::event(self : RoutedIngress) -> @protocol.EventRecord {
  self.event
}

///|
pub struct IngressRouter {
  plan : @plan.JobSpec
  control : @control.CoordinatorState
  source : SourceLog
  outboxes : Map[Int, ExchangeOutbox]
}

///|
/// One routed input plus all derived transfers implied by the JobSpec.
pub struct IngressDispatch {
  ingress : RoutedIngress
  batch : @protocol.EventBatch
  deltas : Array[@protocol.DeltaBatch]
}

///|
pub fn IngressDispatch::ingress(self : IngressDispatch) -> RoutedIngress {
  self.ingress
}

///|
pub fn IngressDispatch::batch(self : IngressDispatch) -> @protocol.EventBatch {
  self.batch
}

///|
pub fn IngressDispatch::deltas(
  self : IngressDispatch,
) -> ArrayView[@protocol.DeltaBatch] {
  self.deltas[:]
}

///|
pub async fn IngressRouter::open(
  plan : @plan.JobSpec,
  control : @control.CoordinatorState,
  source_path : String,
) -> IngressRouter raise SourceLogError {
  let source = SourceLog::open(source_path)
  { plan, control, source, outboxes: Map([]), }
}

///|
pub fn IngressRouter::close(self : IngressRouter) -> Unit {
  self.source.close()
}

///|
/// Route and durably append one ingress event. Assignment ownership is read at
/// call time so a fenced/reassigned partition immediately uses its new epoch.
pub async fn IngressRouter::route(
  self : IngressRouter,
  event : @protocol.EventRecord,
) -> RoutedIngress raise SourceLogError {
  let target = self.plan.route_event(event)
  let assignment = match self.control.assignment(target.partition()) {
    Some(value) => value
    None => raise SourceLogError::InvalidPartition(target.partition())
  }
  let offset = self.source.append(
    assignment.partition(),
    assignment.start_offset(),
    event,
  )
  let current = match self.control.assignment(target.partition()) {
    Some(value) => value
    None => raise SourceLogError::InvalidPartition(target.partition())
  }
  {
    worker_id: current.worker_id(),
    partition: current.partition(),
    epoch: current.epoch(),
    offset,
    event,
  }
}

///|
/// Route one event, persist it, and derive all explicit cross-partition
/// transfers using the current assignment epochs.
pub async fn IngressRouter::dispatch(
  self : IngressRouter,
  event : @protocol.EventRecord,
) -> IngressDispatch raise SourceLogError {
  let routed = self.route(event)
  let assignment = match self.control.assignment(routed.partition()) {
    Some(value) => value
    None => raise SourceLogError::InvalidPartition(routed.partition())
  }
  let routed = if routed.worker_id() != assignment.worker_id() ||
    routed.epoch() != assignment.epoch() {
    RoutedIngress::{
      worker_id: assignment.worker_id(),
      partition: assignment.partition(),
      epoch: assignment.epoch(),
      offset: routed.offset(),
      event,
    }
  } else {
    routed
  }
  let batch = @protocol.EventBatch::new(
    routed.partition(),
    routed.epoch(),
    routed.offset(),
    [event],
  ) catch {
    error => raise SourceLogError::Protocol(error)
  }
  let deltas = []
  let targets = self.plan.exchange_targets(assignment.partition()) catch {
    error => raise SourceLogError::Plan(error)
  }
  for target in targets {
    let target_assignment = match self.control.assignment(target.partition()) {
      Some(value) => value
      None => raise SourceLogError::InvalidPartition(target.partition())
    }
    deltas.push(
      self.plan.delta(
        assignment.partition(),
        assignment.epoch(),
        target.partition(),
        target_assignment.epoch(),
        [event],
      ) catch {
        error => raise SourceLogError::Plan(error)
      },
    )
  }
  { ingress: routed, batch, deltas, }
}

///|
/// Register the target outbox for one partition. Registration is explicit so
/// missing Worker connections fail before any event is partially dispatched.
pub fn IngressRouter::register_outbox(
  self : IngressRouter,
  partition : Int,
  outbox : ExchangeOutbox,
) -> Unit raise SourceLogError {
  if partition < 0 || self.control.assignment(partition) is None {
    raise SourceLogError::InvalidPartition(partition)
  }
  if self.outboxes.contains(partition) {
    raise SourceLogError::DuplicateOutbox(partition)
  }
  self.outboxes[partition] = outbox
}

///|
pub fn IngressRouter::outbox(
  self : IngressRouter,
  partition : Int,
) -> ExchangeOutbox? {
  self.outboxes.get(partition)
}

///|
/// Persist and enqueue every derived DeltaBatch. Route endpoints are checked
/// before the source WAL append. If a queue closes during the async append,
/// the durable source remains authoritative and a retry rebuilds identical
/// deltas.
pub async fn IngressRouter::dispatch_and_enqueue(
  self : IngressRouter,
  event : @protocol.EventRecord,
) -> IngressDispatch raise SourceLogError {
  let source = self.plan.route_event(event)
  let targets = self.plan.exchange_targets(source.partition()) catch {
    error => raise SourceLogError::Plan(error)
  }
  for target in targets {
    let outbox = match self.outboxes.get(target.partition()) {
      Some(value) => value
      None => raise SourceLogError::MissingOutbox(target.partition())
    }
    if outbox.is_closed() {
      raise SourceLogError::ClosedOutbox(target.partition())
    }
  }
  let dispatch = self.dispatch(event)
  for delta in dispatch.deltas() {
    let outbox = self.outboxes.get(delta.target_partition()).unwrap()
    outbox.push(delta) catch {
      error => raise SourceLogError::Io(@debug.to_string(error))
    }
  }
  dispatch
}

///|
/// Recreate all inbound exchange deltas for one target assignment from the
/// durable source log. This is the recovery path for process-local outboxes.
fn IngressRouter::inbound_deltas(
  self : IngressRouter,
  target_partition : Int,
  maximum_batch_events : Int,
) -> Array[@protocol.DeltaBatch] raise SourceLogError {
  if maximum_batch_events <= 0 {
    raise SourceLogError::InvalidBatchSize(maximum_batch_events)
  }
  let target_assignment = match self.control.assignment(target_partition) {
    Some(value) => value
    None => raise SourceLogError::InvalidPartition(target_partition)
  }
  let result = []
  for source_partition in 0..<self.plan.partition_count() {
    let targets = self.plan.exchange_targets(source_partition) catch {
      error => raise SourceLogError::Plan(error)
    }
    if !targets.any(target => target.partition() == target_partition) {
      continue
    }
    let source_assignment = match self.control.assignment(source_partition) {
      Some(value) => value
      None => raise SourceLogError::InvalidPartition(source_partition)
    }
    let entries = self.source.entries_from(
      source_partition,
      source_assignment.start_offset(),
    )
    let mut index = 0
    while index < entries.length() {
      let events = []
      let end = Int::min(index + maximum_batch_events, entries.length())
      for entry in entries[index:end] {
        events.push(entry.event)
      }
      result.push(
        self.plan.delta(
          source_partition,
          source_assignment.epoch(),
          target_partition,
          target_assignment.epoch(),
          events,
        ) catch {
          error => raise SourceLogError::Plan(error)
        },
      )
      index = end
    }
  }
  result
}

///|
/// Rebuild one SupervisorWork from durable source content and the current
/// assignment epoch. Worker Resume selects the acknowledged prefix at runtime.
pub fn IngressRouter::supervisor_work(
  self : IngressRouter,
  partition : Int,
  session_id : String,
  maximum_batch_events? : Int = 128,
  probe? : @protocol.Probe,
) -> SupervisorWork raise SourceLogError {
  let assignment = match self.control.assignment(partition) {
    Some(value) => value
    None => raise SourceLogError::InvalidPartition(partition)
  }
  let batches = self.source.batches(
    partition,
    assignment.epoch(),
    assignment.start_offset(),
    maximum_batch_events~,
  )
  SupervisorWork::new(
    assignment.worker_id(),
    session_id,
    partition,
    batches,
    probe?,
    deltas=self.inbound_deltas(partition, maximum_batch_events),
  )
}

///|
/// Build the complete deterministic connection plan after startup or fencing.
/// A Worker may own multiple partitions; each Task session advertises its
/// partition hint in Hello so the Supervisor can route connections precisely.
pub fn IngressRouter::supervisor_works(
  self : IngressRouter,
  session_prefix : String,
  maximum_batch_events? : Int = 128,
) -> Array[SupervisorWork] raise SourceLogError {
  if session_prefix.is_empty() {
    raise SourceLogError::EmptySessionPrefix
  }
  if maximum_batch_events <= 0 {
    raise SourceLogError::InvalidBatchSize(maximum_batch_events)
  }
  let works = []
  for partition in 0..<self.plan.partition_count() {
    match self.control.assignment(partition) {
      Some(_) => ()
      None => raise SourceLogError::InvalidPartition(partition)
    }
    works.push(
      self.supervisor_work(
        partition,
        session_prefix + "-p" + partition.to_string(),
        maximum_batch_events~,
      ),
    )
  }
  works
}
