///|
/// Lifecycle of one deterministic task. Transport and persistence adapters may
/// drive these transitions, but cannot bypass them.
pub(all) enum TaskPhase {
  Created
  Running
  Draining
  Stopped
  Failed
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// Errors raised by the synchronous execution layer.
pub(all) suberror TaskError {
  EmptyTaskId
  EmptyOperatorId
  UnknownOperator(String)
  OperatorMismatch(expected~ : String, actual~ : String)
  OperatorContractMismatch
  InvalidPartition(Int)
  PartitionMismatch(expected~ : Int, actual~ : Int)
  InvalidEpoch(Int)
  InvalidOffset(Int64)
  PendingEvents(Int)
  WrongPhase(expected~ : TaskPhase, actual~ : TaskPhase)
  InvalidTransition
  Reachability(@reachability.EngineError)
  Security(@security.SecurityError)
  Causal(@causal.CausalError)
  Protocol(@protocol.ProtocolError)
} derive(Debug, Eq)

///|
/// One operator input accepted by a Task. Payload stays typed at the protocol
/// boundary as JSON; the selected built-in operator validates its own shape.
pub(all) struct TaskInput {
  event : @protocol.EventRecord
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn TaskInput::new(event : @protocol.EventRecord) -> TaskInput {
  { event, }
}

///|
pub fn TaskInput::event(self : TaskInput) -> @protocol.EventRecord {
  self.event
}

///|
/// A newly emitted operator result. Facts and alerts are intentionally separate
/// so callers cannot confuse a security alert with a graph reachability fact.
pub(all) enum TaskOutput {
  Reachable(@reachability.Reachable)
  SecurityAlert(@security.SecurityAlert)
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
/// Monotone progress exported by a Task after a successful input step.
pub(all) struct TaskProgress {
  task_id : String
  partition : Int
  epoch : Int
  offset : Int64
  phase : TaskPhase
  frontier_digest : String
  emitted_outputs : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn TaskProgress::task_id(self : TaskProgress) -> String {
  self.task_id
}

///|
pub fn TaskProgress::partition(self : TaskProgress) -> Int {
  self.partition
}

///|
pub fn TaskProgress::epoch(self : TaskProgress) -> Int {
  self.epoch
}

///|
pub fn TaskProgress::offset(self : TaskProgress) -> Int64 {
  self.offset
}

///|
pub fn TaskProgress::phase(self : TaskProgress) -> TaskPhase {
  self.phase
}

///|
pub fn TaskProgress::frontier_digest(self : TaskProgress) -> String {
  self.frontier_digest
}

///|
pub fn TaskProgress::emitted_outputs(self : TaskProgress) -> Int {
  self.emitted_outputs
}

///|
/// The synchronous execution object. It owns exactly one built-in operator and
/// one assignment identity; no sockets, timers, or filesystem state are kept.
pub struct Task {
  task_id : String
  operator_id : String
  operator : OperatorKind
  partition : Int
  epoch : Int
  start_offset : Int64
  mut phase : TaskPhase
  mut offset : Int64
  mut frontier : @causal.Frontier
  mut emitted_outputs : Int
  mut reachability : @reachability.Engine?
  mut security : @security.Engine?
}

///|
pub fn Task::new(
  task_id : String,
  operator_id : String,
  operator : OperatorKind,
  partition : Int,
  epoch : Int,
  start_offset? : Int64 = 0L,
) -> Task raise TaskError {
  if task_id.is_empty() {
    raise TaskError::EmptyTaskId
  }
  if operator_id.is_empty() {
    raise TaskError::EmptyOperatorId
  }
  if operator_id != operator.id() {
    raise TaskError::OperatorMismatch(
      expected=operator.id(),
      actual=operator_id,
    )
  }
  if partition < 0 {
    raise TaskError::InvalidPartition(partition)
  }
  if epoch < 0 {
    raise TaskError::InvalidEpoch(epoch)
  }
  if start_offset < 0L {
    raise TaskError::InvalidOffset(start_offset)
  }
  let reachability = match operator {
    OperatorKind::Reachability =>
      Some(
        @reachability.Engine::new() catch {
          error => raise TaskError::Causal(error)
        },
      )
    OperatorKind::SecurityChain => None
  }
  let security = match operator {
    OperatorKind::Reachability => None
    OperatorKind::SecurityChain =>
      Some(
        @security.Engine::new() catch {
          error => raise TaskError::Causal(error)
        },
      )
  }
  {
    task_id,
    operator_id,
    operator,
    partition,
    epoch,
    start_offset,
    phase: TaskPhase::Created,
    offset: start_offset - 1L,
    frontier: @causal.Frontier::empty(),
    emitted_outputs: 0,
    reachability,
    security,
  }
}

///|
/// Construct a Task directly from the protocol assignment envelope used by a
/// Worker. The assignment remains data; no transport state is retained here.
pub fn Task::from_assignment(
  task_id : String,
  operator_id : String,
  operator : OperatorKind,
  assignment : @protocol.AssignmentSpec,
) -> Task raise TaskError {
  Task::new(
    task_id,
    operator_id,
    operator,
    assignment.partition(),
    assignment.epoch(),
    start_offset=assignment.start_offset(),
  )
}

///|
pub fn Task::from_spec(spec : TaskSpec) -> Task raise TaskError {
  Task::new(
    spec.task_id(),
    spec.operator_id(),
    spec.operator(),
    spec.partition(),
    spec.epoch(),
    start_offset=spec.start_offset(),
  )
}

///|
/// Validate that the transport assignment matches this Task before WAL replay.
pub fn Task::validate_assignment(
  self : Task,
  assignment : @protocol.Assignment,
) -> Unit raise TaskError {
  if assignment.partition() != self.partition {
    raise TaskError::PartitionMismatch(
      expected=self.partition,
      actual=assignment.partition(),
    )
  }
  if assignment.epoch() != self.epoch {
    raise TaskError::Protocol(
      @protocol.ProtocolError::StaleEpoch(
        expected=self.epoch,
        actual=assignment.epoch(),
      ),
    )
  }
  if assignment.next_offset() != self.start_offset {
    raise TaskError::InvalidOffset(assignment.next_offset())
  }
}

///|
pub fn Task::task_id(self : Task) -> String {
  self.task_id
}

///|
pub fn Task::operator_id(self : Task) -> String {
  self.operator_id
}

///|
pub fn Task::operator(self : Task) -> OperatorKind {
  self.operator
}

///|
pub fn Task::partition(self : Task) -> Int {
  self.partition
}

///|
pub fn Task::epoch(self : Task) -> Int {
  self.epoch
}

///|
pub fn Task::start_offset(self : Task) -> Int64 {
  self.start_offset
}

///|
pub fn Task::phase(self : Task) -> TaskPhase {
  self.phase
}

///|
pub fn Task::offset(self : Task) -> Int64 {
  self.offset
}

///|
pub fn Task::frontier(self : Task) -> @causal.Frontier {
  self.frontier
}

///|
pub fn Task::event_count(self : Task) -> Int {
  match self.operator {
    OperatorKind::Reachability => self.reachability.unwrap().event_count()
    OperatorKind::SecurityChain => self.security.unwrap().event_count()
  }
}

///|
pub fn Task::pending_count(self : Task) -> Int {
  match self.operator {
    OperatorKind::Reachability => self.reachability.unwrap().pending_count()
    OperatorKind::SecurityChain => self.security.unwrap().pending_count()
  }
}

///|
/// Return the current monotone operator outputs in deterministic order.
pub fn Task::outputs(self : Task) -> Array[TaskOutput] {
  let outputs = match self.operator {
    OperatorKind::Reachability =>
      self.reachability
      .unwrap()
      .facts()
      .map(fact => TaskOutput::Reachable(fact))
    OperatorKind::SecurityChain =>
      self.security
      .unwrap()
      .alerts()
      .map(alert => TaskOutput::SecurityAlert(alert))
  }
  outputs.sort()
  outputs
}

///|
pub fn Task::progress(self : Task) -> TaskProgress {
  {
    task_id: self.task_id,
    partition: self.partition,
    epoch: self.epoch,
    offset: self.offset,
    phase: self.phase,
    frontier_digest: self.frontier.digest(),
    emitted_outputs: self.emitted_outputs,
  }
}

///|
fn task_snapshot_event(
  id : @causal.EventId,
  parents : ArrayView[@causal.EventId],
  key : String,
  payload : Json,
) -> @protocol.EventRecord raise TaskError {
  @protocol.EventRecord::new(id, parents, key, payload) catch {
    error => raise TaskError::Protocol(error)
  }
}

///|
/// Capture the complete synchronous operator state. This method performs no
/// filesystem or transport work; an async adapter decides when and where the
/// returned value is persisted.
pub fn Task::snapshot(self : Task) -> TaskSnapshot raise TaskError {
  let events : Array[@protocol.EventRecord] = []
  match self.operator {
    OperatorKind::Reachability => {
      let engine = self.reachability.unwrap()
      for event in engine.events() {
        events.push(
          task_snapshot_event(
            event.id(),
            event.parents(),
            event.key(),
            event.payload().to_json(),
          ),
        )
      }
      for event in engine.pending_events() {
        events.push(
          task_snapshot_event(
            event.id(),
            event.parents(),
            event.key(),
            event.payload().to_json(),
          ),
        )
      }
    }
    OperatorKind::SecurityChain => {
      let engine = self.security.unwrap()
      for event in engine.events() {
        events.push(
          task_snapshot_event(
            event.id(),
            event.parents(),
            event.key(),
            event.payload().to_json(),
          ),
        )
      }
      for event in engine.pending_events() {
        events.push(
          task_snapshot_event(
            event.id(),
            event.parents(),
            event.key(),
            event.payload().to_json(),
          ),
        )
      }
    }
  }
  events.sort_by((left, right) => Compare::compare(left.id(), right.id()))
  {
    task_id: self.task_id,
    operator_id: self.operator_id,
    operator: self.operator,
    partition: self.partition,
    epoch: self.epoch,
    offset: self.offset,
    frontier_digest: self.frontier.digest(),
    emitted_outputs: self.emitted_outputs,
    events,
    operator_capability: Some(self.operator.capability()),
  }
}

///|
/// Restore a snapshot into this Task by replaying its pure event set. The
/// current lifecycle phase is retained so a running Worker can restore after
/// its transport handshake has validated the assignment.
pub fn Task::restore(
  self : Task,
  snapshot : TaskSnapshot,
) -> Unit raise TaskError {
  match snapshot.operator_capability {
    Some(capability) if capability != self.operator.capability() =>
      raise TaskError::OperatorContractMismatch
    _ => ()
  }
  if snapshot.task_id() != self.task_id ||
    snapshot.operator_id() != self.operator_id ||
    snapshot.operator() != self.operator ||
    snapshot.partition() != self.partition ||
    snapshot.epoch() != self.epoch ||
    snapshot.offset() < self.start_offset - 1L ||
    snapshot.frontier_digest().is_empty() ||
    snapshot.emitted_outputs() < 0 {
    raise TaskError::InvalidTransition
  }
  let candidate = Task::new(
    self.task_id,
    self.operator_id,
    self.operator,
    self.partition,
    self.epoch,
    start_offset=self.start_offset,
  )
  for event in snapshot.events() {
    let restored_outputs = candidate.apply_input(TaskInput::new(event))
    ignore(restored_outputs)
  }
  candidate.refresh_frontier()
  if candidate.frontier.digest() != snapshot.frontier_digest() {
    raise TaskError::InvalidTransition
  }
  candidate.offset = snapshot.offset()
  candidate.emitted_outputs = snapshot.emitted_outputs()
  self.reachability = candidate.reachability
  self.security = candidate.security
  self.offset = candidate.offset
  self.frontier = candidate.frontier
  self.emitted_outputs = candidate.emitted_outputs
}

///|
pub fn Task::start(self : Task) -> Unit raise TaskError {
  if self.phase != TaskPhase::Created {
    raise TaskError::WrongPhase(expected=TaskPhase::Created, actual=self.phase)
  }
  self.phase = TaskPhase::Running
}

///|
pub fn Task::drain(self : Task) -> Unit raise TaskError {
  if self.phase != TaskPhase::Running {
    raise TaskError::WrongPhase(expected=TaskPhase::Running, actual=self.phase)
  }
  self.phase = TaskPhase::Draining
}

///|
pub fn Task::stop(self : Task) -> Unit raise TaskError {
  if self.phase != TaskPhase::Draining {
    raise TaskError::WrongPhase(expected=TaskPhase::Draining, actual=self.phase)
  }
  if self.pending_count() > 0 {
    raise TaskError::PendingEvents(self.pending_count())
  }
  self.phase = TaskPhase::Stopped
}

///|
fn Task::advance_offset(self : Task, offset : Int64) -> Unit raise TaskError {
  if offset < self.offset || offset != self.offset + 1L {
    raise TaskError::InvalidOffset(offset)
  }
  self.offset = offset
}

///|
fn Task::validate_next_offset(
  self : Task,
  offset : Int64,
) -> Unit raise TaskError {
  if offset != self.offset + 1L {
    raise TaskError::InvalidOffset(offset)
  }
}

///|
fn Task::apply_input(
  self : Task,
  input : TaskInput,
) -> Array[TaskOutput] raise TaskError {
  match self.operator {
    OperatorKind::Reachability => {
      let engine = match self.reachability {
        Some(value) => value
        None => raise TaskError::InvalidTransition
      }
      let record = input.event()
      self.operator.validate_payload(record.payload())
      let facts = engine.apply_json_event(
        record.id(),
        record.parents(),
        record.key(),
        record.payload(),
      ) catch {
        error => raise TaskError::Reachability(error)
      }
      facts.map(fact => TaskOutput::Reachable(fact))
    }
    OperatorKind::SecurityChain => {
      let engine = match self.security {
        Some(value) => value
        None => raise TaskError::InvalidTransition
      }
      let record = input.event()
      self.operator.validate_payload(record.payload())
      let alerts = engine.apply_json_event(
        record.id(),
        record.parents(),
        record.key(),
        record.payload(),
      ) catch {
        error => raise TaskError::Security(error)
      }
      alerts.map(alert => TaskOutput::SecurityAlert(alert))
    }
  }
}

///|
fn Task::refresh_frontier(self : Task) -> Unit {
  self.frontier = match self.operator {
    OperatorKind::Reachability => self.reachability.unwrap().frontier()
    OperatorKind::SecurityChain => self.security.unwrap().frontier()
  }
}

///|
fn Task::copy(self : Task) -> Task {
  {
    task_id: self.task_id,
    operator_id: self.operator_id,
    operator: self.operator,
    partition: self.partition,
    epoch: self.epoch,
    start_offset: self.start_offset,
    phase: self.phase,
    offset: self.offset,
    frontier: self.frontier,
    emitted_outputs: self.emitted_outputs,
    reachability: self.reachability.map(engine => engine.copy()),
    security: self.security.map(engine => engine.copy()),
  }
}

///|
fn Task::commit(self : Task, candidate : Task) -> Unit {
  self.reachability = candidate.reachability
  self.security = candidate.security
  self.offset = candidate.offset
  self.frontier = candidate.frontier
  self.emitted_outputs = candidate.emitted_outputs
}

///|
/// Apply one contiguous input. A Task never advances its offset when the
/// selected operator rejects the event, so the caller can retry or fail closed.
pub fn Task::process(
  self : Task,
  offset : Int64,
  input : TaskInput,
) -> Array[TaskOutput] raise TaskError {
  if self.phase != TaskPhase::Running {
    raise TaskError::WrongPhase(expected=TaskPhase::Running, actual=self.phase)
  }
  self.validate_next_offset(offset)
  let outputs = self.apply_input(input)
  self.advance_offset(offset)
  self.refresh_frontier()
  self.emitted_outputs += outputs.length()
  outputs
}

///|
/// Convenience adapter for a protocol EventBatch. It requires contiguous
/// offsets and processes each event in batch order.
pub fn Task::process_batch(
  self : Task,
  batch : @protocol.EventBatch,
) -> Array[TaskOutput] raise TaskError {
  if batch.partition() != self.partition {
    raise TaskError::PartitionMismatch(
      expected=self.partition,
      actual=batch.partition(),
    )
  }
  if batch.epoch() != self.epoch {
    raise TaskError::Protocol(
      @protocol.ProtocolError::StaleEpoch(
        expected=self.epoch,
        actual=batch.epoch(),
      ),
    )
  }
  let candidate = self.copy()
  let outputs = []
  let mut offset = batch.first_offset()
  for event in batch.events() {
    outputs.push_iter(candidate.process(offset, TaskInput::new(event)).iter())
    offset += 1L
  }
  outputs.sort()
  self.commit(candidate)
  outputs
}

///|
/// Apply an inbound exchange batch to the same operator state without
/// advancing the target partition's primary source offset.
pub fn Task::process_delta(
  self : Task,
  delta : @protocol.DeltaBatch,
) -> Array[TaskOutput] raise TaskError {
  if self.phase != TaskPhase::Running {
    raise TaskError::WrongPhase(expected=TaskPhase::Running, actual=self.phase)
  }
  if delta.target_partition() != self.partition {
    raise TaskError::PartitionMismatch(
      expected=self.partition,
      actual=delta.target_partition(),
    )
  }
  if delta.target_epoch() != self.epoch {
    raise TaskError::Protocol(
      @protocol.ProtocolError::StaleEpoch(
        expected=self.epoch,
        actual=delta.target_epoch(),
      ),
    )
  }
  let candidate = self.copy()
  let outputs = []
  for event in delta.events() {
    outputs.push_iter(candidate.apply_input(TaskInput::new(event)).iter())
  }
  candidate.refresh_frontier()
  candidate.emitted_outputs += outputs.length()
  outputs.sort()
  self.commit(candidate)
  outputs
}

///|
pub fn Task::fail(self : Task) -> Unit {
  self.phase = TaskPhase::Failed
}
