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
  StateLimitExceeded(limit~ : Int, actual~ : Int)
  WrongPhase(expected~ : TaskPhase, actual~ : TaskPhase)
  InvalidTransition
  Aggregation(@aggregation.AggregationError)
  Reachability(@reachability.EngineError)
  Security(@security.SecurityError)
  Custom(String)
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
  WindowedSum(WindowedSum)
  KeyedSum(@aggregation.KeyedSum)
  Reachable(@reachability.Reachable)
  SecurityAlert(@security.SecurityAlert)
  Custom(Json)
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Keep output ordering deterministic even though arbitrary JSON does not
/// implement the ordering trait required by `Array::sort`.
pub impl Compare for TaskOutput with fn compare(self, other) {
  match (self, other) {
    (TaskOutput::WindowedSum(left), TaskOutput::WindowedSum(right)) =>
      left.compare(right)
    (TaskOutput::WindowedSum(_), _) => -1
    (_, TaskOutput::WindowedSum(_)) => 1
    (TaskOutput::KeyedSum(left), TaskOutput::KeyedSum(right)) =>
      Compare::compare(left, right)
    (TaskOutput::Reachable(left), TaskOutput::Reachable(right)) =>
      Compare::compare(left, right)
    (TaskOutput::SecurityAlert(left), TaskOutput::SecurityAlert(right)) =>
      Compare::compare(left, right)
    (TaskOutput::Custom(left), TaskOutput::Custom(right)) =>
      @json.to_json(left).stringify().compare(@json.to_json(right).stringify())
    (TaskOutput::KeyedSum(_), _) => -1
    (TaskOutput::Reachable(_), TaskOutput::KeyedSum(_)) => 1
    (TaskOutput::Reachable(_), _) => -1
    (TaskOutput::SecurityAlert(_), TaskOutput::Custom(_)) => -1
    (TaskOutput::SecurityAlert(_), _) => 1
    (TaskOutput::Custom(_), _) => 1
  }
}

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
/// Event-time inputs need to retain their transport metadata while the
/// derived window/join state is compacted.  The causal log stores this payload
/// so a snapshot can reconstruct the same frontier and pending set.
struct TaskEventPayload {
  payload : Json
  event_time : Int64?
  input_id : String
} derive(Debug, Eq, ToJson, FromJson)

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
  mut epoch : Int
  mut start_offset : Int64
  mut phase : TaskPhase
  mut offset : Int64
  mut frontier : @causal.Frontier
  mut emitted_outputs : Int
  mut aggregation : @aggregation.Engine?
  mut reachability : @reachability.Engine?
  mut security : @security.Engine?
  custom_operator : CustomOperator?
  mut custom_log : @causal.CausalLog[TaskEventPayload]?
  mut custom_outputs : Array[Json]
  max_state_events : Int
  mut event_time_config : TaskEventTimeConfig?
  mut event_time_log : @causal.CausalLog[TaskEventPayload]?
  mut watermark : Int64
  mut watermark_requested : Int64
  mut late_events : Int
  mut window_buckets : Array[TaskWindowBucket]
  mut join_left : Array[@protocol.EventRecord]
  mut join_right : Array[@protocol.EventRecord]
  mut join_emitted : Set[(@causal.EventId, @causal.EventId)]
  mut graph_config : TaskGraphConfig?
  mut graph_log : @causal.CausalLog[TaskEventPayload]?
}

///|
pub fn Task::new(
  task_id : String,
  operator_id : String,
  operator : OperatorKind,
  partition : Int,
  epoch : Int,
  start_offset? : Int64 = 0L,
  max_state_events? : Int = 0,
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
  if max_state_events < 0 {
    raise TaskError::StateLimitExceeded(limit=max_state_events, actual=0)
  }
  let aggregation = match operator {
    OperatorKind::KeyedSum =>
      Some(
        @aggregation.Engine::new() catch {
          error => raise TaskError::Causal(error)
        },
      )
    OperatorKind::Reachability | OperatorKind::SecurityChain => None
    OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) =>
      raise TaskError::Custom(
        "custom tasks require a registered implementation",
      )
  }
  let reachability = match operator {
    OperatorKind::Reachability =>
      Some(
        @reachability.Engine::new() catch {
          error => raise TaskError::Causal(error)
        },
      )
    OperatorKind::KeyedSum | OperatorKind::SecurityChain => None
    OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) => None
  }
  let security = match operator {
    OperatorKind::KeyedSum | OperatorKind::Reachability => None
    OperatorKind::SecurityChain =>
      Some(
        @security.Engine::new() catch {
          error => raise TaskError::Causal(error)
        },
      )
    OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) => None
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
    aggregation,
    reachability,
    security,
    custom_operator: None,
    custom_log: None,
    custom_outputs: [],
    max_state_events,
    event_time_config: None,
    event_time_log: None,
    watermark: -1L,
    watermark_requested: -1L,
    late_events: 0,
    window_buckets: [],
    join_left: [],
    join_right: [],
    join_emitted: Set([]),
    graph_config: None,
    graph_log: None,
  }
}

///|
/// Construct a Task for a compile-time registered custom implementation.
/// Checkpoint recovery reuses the same event replay contract as built-ins.
pub fn Task::new_custom(
  task_id : String,
  operator_id : String,
  custom_operator : CustomOperator,
  partition : Int,
  epoch : Int,
  start_offset? : Int64 = 0L,
  max_state_events? : Int = 0,
) -> Task raise TaskError {
  if task_id.is_empty() {
    raise TaskError::EmptyTaskId
  }
  if operator_id.is_empty() {
    raise TaskError::EmptyOperatorId
  }
  if operator_id != custom_operator.id() {
    raise TaskError::OperatorMismatch(
      expected=custom_operator.id(),
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
  if max_state_events < 0 {
    raise TaskError::StateLimitExceeded(limit=max_state_events, actual=0)
  }
  let custom_log = @causal.CausalLog::new() catch {
    error => raise TaskError::Causal(error)
  }
  {
    task_id,
    operator_id,
    operator: custom_operator.kind(),
    partition,
    epoch,
    start_offset,
    phase: TaskPhase::Created,
    offset: start_offset - 1L,
    frontier: @causal.Frontier::empty(),
    emitted_outputs: 0,
    aggregation: None,
    reachability: None,
    security: None,
    custom_operator: Some(custom_operator),
    custom_log: Some(custom_log),
    custom_outputs: [],
    max_state_events,
    event_time_config: None,
    event_time_log: None,
    watermark: -1L,
    watermark_requested: -1L,
    late_events: 0,
    window_buckets: [],
    join_left: [],
    join_right: [],
    join_emitted: Set([]),
    graph_config: None,
    graph_log: None,
  }
}

///|
/// Enable the live event-time path before a Task is started.
pub fn Task::with_event_time(
  self : Task,
  config : TaskEventTimeConfig,
) -> Task raise TaskError {
  if self.phase != TaskPhase::Created {
    raise TaskError::WrongPhase(expected=TaskPhase::Created, actual=self.phase)
  }
  config.validate()
  if config.window_size_ms() > 0L && self.operator != OperatorKind::KeyedSum {
    raise TaskError::Custom("event-time windows require keyed-sum")
  }
  let log : @causal.CausalLog[TaskEventPayload] = @causal.CausalLog::new() catch {
    error => raise TaskError::Causal(error)
  }
  { ..self, event_time_config: Some(config), event_time_log: Some(log), }
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
  let task = Task::new(
    spec.task_id(),
    spec.operator_id(),
    spec.operator(),
    spec.partition(),
    spec.epoch(),
    start_offset=spec.start_offset(),
  )
  match spec.event_time_config() {
    None => task
    Some(config) => task.with_event_time(config)
  }
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
/// Adopt a newly fenced lease before a dynamic Worker starts replay. This is
/// only valid while the Task is still Created, before any operator state has
/// been applied.
pub fn Task::adopt_assignment(
  self : Task,
  assignment : @protocol.Assignment,
) -> Unit raise TaskError {
  if self.phase != TaskPhase::Created {
    raise TaskError::WrongPhase(expected=TaskPhase::Created, actual=self.phase)
  }
  if assignment.partition() != self.partition {
    raise TaskError::PartitionMismatch(
      expected=self.partition,
      actual=assignment.partition(),
    )
  }
  self.epoch = assignment.epoch()
  self.start_offset = assignment.next_offset()
  self.offset = self.start_offset - 1L
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
  if self.graph_log is Some(log) {
    return log.admitted_count()
  }
  match self.event_time_log {
    Some(log) => log.admitted_count()
    None =>
      match self.operator {
        OperatorKind::KeyedSum => self.aggregation.unwrap().event_count()
        OperatorKind::Reachability => self.reachability.unwrap().event_count()
        OperatorKind::SecurityChain => self.security.unwrap().event_count()
        OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) =>
          self.custom_log.unwrap().admitted_count()
      }
  }
}

///|
pub fn Task::pending_count(self : Task) -> Int {
  if self.graph_log is Some(log) {
    return log.pending_count()
  }
  match self.event_time_log {
    Some(log) => log.pending_count()
    None =>
      match self.operator {
        OperatorKind::KeyedSum => self.aggregation.unwrap().pending_count()
        OperatorKind::Reachability => self.reachability.unwrap().pending_count()
        OperatorKind::SecurityChain => self.security.unwrap().pending_count()
        OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) =>
          self.custom_log.unwrap().pending_count()
      }
  }
}

///|
pub fn Task::state_event_count(self : Task) -> Int {
  self.event_count() + self.pending_count()
}

///|
/// Return the configured in-memory state budget. Zero means unlimited for the
/// pure Task API; production adapters normally supply a finite Job default.
pub fn Task::max_state_events(self : Task) -> Int {
  self.max_state_events
}

///|
pub fn Task::watermark(self : Task) -> Int64 {
  self.watermark
}

///|
pub fn Task::late_events(self : Task) -> Int {
  self.late_events
}

///|
fn Task::check_state_limit(self : Task) -> Unit raise TaskError {
  let actual = self.state_memory_count()
  if self.max_state_events > 0 && actual > self.max_state_events {
    raise TaskError::StateLimitExceeded(limit=self.max_state_events, actual~)
  }
}

///|
/// Count all durable and derived structures retained by a Task. The causal
/// event count alone is insufficient for reachability (derived closure),
/// aggregation (historical updates), and custom operators (emitted JSON).
/// This is deliberately conservative: rejecting a candidate before commit is
/// preferable to allowing a production process to grow without a bound.
pub fn Task::state_memory_count(self : Task) -> Int {
  let graph_count = self.graph_log.map(log => log.state_count()).unwrap_or(0)
  if self.event_time_log is Some(log) {
    return graph_count +
      log.state_count() +
      self.window_buckets.length() +
      self.join_left.length() +
      self.join_right.length() +
      self.join_emitted.length()
  }
  let base = match self.operator {
    OperatorKind::KeyedSum => {
      let engine = self.aggregation.unwrap()
      let keys = engine.totals().length()
      let retained_values = if keys > engine.output_count() {
        keys
      } else {
        engine.output_count()
      }
      engine.causal_state_count() + retained_values
    }
    OperatorKind::Reachability => {
      let engine = self.reachability.unwrap()
      engine.state_count()
    }
    OperatorKind::SecurityChain => {
      let engine = self.security.unwrap()
      engine.state_count()
    }
    OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) =>
      self.custom_log.unwrap().state_count() + self.custom_outputs.length()
  }
  let event_time_count = match self.event_time_log {
    None => 0
    Some(log) => log.state_count()
  }
  graph_count +
  base +
  event_time_count +
  self.window_buckets.length() +
  self.join_left.length() +
  self.join_right.length() +
  self.join_emitted.length()
}

///|
/// Return the current monotone operator outputs in deterministic order.
pub fn Task::outputs(self : Task) -> Array[TaskOutput] {
  let outputs = match self.operator {
    OperatorKind::KeyedSum =>
      self.aggregation
      .unwrap()
      .outputs()
      .map(value => TaskOutput::KeyedSum(value))
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
    OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) =>
      self.custom_outputs.map(output => TaskOutput::Custom(output))
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
  event_time? : Int64,
  input_id? : String = "main",
) -> @protocol.EventRecord raise TaskError {
  @protocol.EventRecord::new(
    id,
    parents,
    key,
    payload,
    event_time?,
    input_id?=Some(input_id),
  ) catch {
    error => raise TaskError::Protocol(error)
  }
}

///|
fn task_event_payload(record : @protocol.EventRecord) -> TaskEventPayload {
  {
    payload: record.payload(),
    event_time: record.event_time(),
    input_id: record.input_id(),
  }
}

///|
fn task_event_causal(
  record : @protocol.EventRecord,
) -> @causal.Event[TaskEventPayload] raise TaskError {
  let payload = task_event_payload(record)
  @causal.Event::new(record.id(), record.parents(), record.key(), payload) catch {
    error => raise TaskError::Causal(error)
  }
}

///|
fn task_event_record(
  event : @causal.Event[TaskEventPayload],
) -> @protocol.EventRecord raise TaskError {
  @protocol.EventRecord::new(
    event.id(),
    event.parents(),
    event.key(),
    event.payload().payload,
    event_time?=event.payload().event_time,
    input_id?=Some(event.payload().input_id),
  ) catch {
    error => raise TaskError::Protocol(error)
  }
}

///|
fn sorted_join_pairs(
  values : Array[(@causal.EventId, @causal.EventId)],
) -> Array[(@causal.EventId, @causal.EventId)] {
  values.sort()
  values
}

///|
/// Capture the complete synchronous operator state. This method performs no
/// filesystem or transport work; an async adapter decides when and where the
/// returned value is persisted.
pub fn Task::snapshot(self : Task) -> TaskSnapshot raise TaskError {
  let events : Array[@protocol.EventRecord] = []
  match self.operator {
    OperatorKind::KeyedSum => {
      let engine = self.aggregation.unwrap()
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
    OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) => {
      let log = match self.custom_log {
        Some(value) => value
        None => raise TaskError::InvalidTransition
      }
      for event in log.events() {
        events.push(task_event_record(event))
      }
      for event in log.pending_events() {
        events.push(task_event_record(event))
      }
    }
  }
  match self.event_time_log {
    None => ()
    Some(log) => {
      events.clear()
      for event in log.events() {
        events.push(task_event_record(event))
      }
      for event in log.pending_events() {
        events.push(task_event_record(event))
      }
    }
  }
  if self.graph_log is Some(log) {
    events.clear()
    for event in log.events() {
      events.push(task_event_record(event))
    }
    for event in log.pending_events() {
      events.push(task_event_record(event))
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
    operator_capability: self.operator.capability(),
    materialized_state: self.materialized_state(),
    graph_config: self.graph_config,
    graph_state: self.graph_log.map(log => @json.to_json(log.snapshot())),
    watermark: Some(self.watermark),
    watermark_requested: Some(self.watermark_requested),
    late_events: Some(self.late_events),
    event_time_config: self.event_time_config,
    window_buckets: Some(self.window_buckets.copy()),
    join_left: Some(self.join_left.copy()),
    join_right: Some(self.join_right.copy()),
    join_emitted: Some(sorted_join_pairs(self.join_emitted.to_array())),
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
  if snapshot.operator_capability != self.operator.capability() {
    raise TaskError::OperatorContractMismatch
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
  let candidate = match self.custom_operator {
    None =>
      Task::new(
        self.task_id,
        self.operator_id,
        self.operator,
        self.partition,
        self.epoch,
        start_offset=self.start_offset,
        max_state_events=self.max_state_events,
      )
    Some(operator) =>
      Task::new_custom(
        self.task_id,
        self.operator_id,
        operator,
        self.partition,
        self.epoch,
        start_offset=self.start_offset,
        max_state_events=self.max_state_events,
      )
  }
  if snapshot.graph_config != self.graph_config {
    raise TaskError::OperatorContractMismatch
  }
  candidate.restore_graph_state(snapshot)
  match snapshot.event_time_config() {
    None => candidate.restore_materialized_state(snapshot)
    Some(config) => {
      config.validate()
      if self.event_time_config is Some(expected) && expected != config {
        raise TaskError::OperatorContractMismatch
      }
      guard snapshot.materialized_state is Some(state) else {
        raise TaskError::InvalidTransition
      }
      let saved : @causal.CausalLogSnapshot[TaskEventPayload] = @json.from_json(
        state,
      ) catch {
        _ => raise TaskError::InvalidTransition
      }
      let log = @causal.CausalLog::restore(saved) catch {
        error => raise TaskError::Causal(error)
      }
      candidate.event_time_config = Some(config)
      candidate.event_time_log = Some(log)
      let records = log.events().map(event => task_event_record(event))
      records.push_iter(
        log.pending_events().map(event => task_event_record(event)).iter(),
      )
      for record in records {
        guard record.event_time() is Some(timestamp) &&
          timestamp >= 0L &&
          timestamp < 9223372036854775807L else {
          raise TaskError::InvalidTransition
        }
        if config.window_size_ms() > 0L {
          if record.input_id() != "main" {
            raise TaskError::InvalidTransition
          }
          self.operator.validate_payload(record.payload())
        } else if record.input_id() != "left" && record.input_id() != "right" {
          raise TaskError::InvalidTransition
        }
      }
    }
  }
  if candidate.snapshot().events() != snapshot.events() {
    raise TaskError::InvalidTransition
  }
  candidate.refresh_frontier()
  if candidate.frontier.digest() != snapshot.frontier_digest() {
    raise TaskError::InvalidTransition
  }
  candidate.offset = snapshot.offset()
  candidate.emitted_outputs = snapshot.emitted_outputs()
  candidate.watermark = snapshot.watermark()
  candidate.watermark_requested = snapshot.watermark_requested()
  if candidate.watermark < -1L ||
    candidate.watermark_requested < candidate.watermark ||
    snapshot.late_events() < 0 {
    raise TaskError::InvalidTransition
  }
  candidate.late_events = snapshot.late_events()
  candidate.window_buckets = snapshot.window_buckets().to_owned()
  candidate.join_left = snapshot.join_left().to_owned()
  candidate.join_right = snapshot.join_right().to_owned()
  candidate.join_emitted = Set(snapshot.join_emitted().to_owned())
  if candidate.join_emitted.length() != snapshot.join_emitted().length() {
    raise TaskError::InvalidTransition
  }
  candidate.validate_event_time_state()
  candidate.check_state_limit()
  self.commit(candidate)
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
  if self.offset == 9223372036854775807L ||
    offset < self.offset ||
    offset != self.offset + 1L {
    raise TaskError::InvalidOffset(offset)
  }
  self.offset = offset
}

///|
fn Task::validate_next_offset(
  self : Task,
  offset : Int64,
) -> Unit raise TaskError {
  if self.offset == 9223372036854775807L || offset != self.offset + 1L {
    raise TaskError::InvalidOffset(offset)
  }
}

///|
fn Task::apply_input(
  self : Task,
  input : TaskInput,
  callback_input? : TaskInput,
) -> Array[TaskOutput] raise TaskError {
  match self.operator {
    OperatorKind::KeyedSum => {
      let engine = match self.aggregation {
        Some(value) => value
        None => raise TaskError::InvalidTransition
      }
      let record = input.event()
      self.operator.validate_payload(record.payload())
      let sums = engine.apply_json_event(
        record.id(),
        record.parents(),
        record.key(),
        record.payload(),
      ) catch {
        error => raise TaskError::Aggregation(error)
      }
      sums.map(value => TaskOutput::KeyedSum(value))
    }
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
    OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) => {
      let operator = match self.custom_operator {
        Some(value) => value
        None => raise TaskError::InvalidTransition
      }
      let log = match self.custom_log {
        Some(value) => value
        None => raise TaskError::InvalidTransition
      }
      let record = input.event()
      operator.validate_payload(record.payload())
      let event = task_event_causal(record)
      let admission = log.submit(event) catch {
        error => raise TaskError::Causal(error)
      }
      let outputs : Array[TaskOutput] = []
      for admitted in admission.admitted() {
        let admitted_record = task_event_record(admitted)
        let emitted = operator.apply(
          callback_input.unwrap_or(TaskInput::new(admitted_record)),
        )
        for output in emitted {
          self.custom_outputs.push(output)
          outputs.push(TaskOutput::Custom(output))
        }
      }
      outputs
    }
  }
}

///|
fn Task::refresh_frontier(self : Task) -> Unit {
  if self.graph_log is Some(log) {
    self.frontier = log.frontier()
    return
  }
  self.frontier = match self.event_time_log {
    Some(log) => log.frontier()
    None =>
      match self.operator {
        OperatorKind::KeyedSum => self.aggregation.unwrap().frontier()
        OperatorKind::Reachability => self.reachability.unwrap().frontier()
        OperatorKind::SecurityChain => self.security.unwrap().frontier()
        OperatorKind::Custom(_, _, _, _, _, _, _, _, _, _) =>
          self.custom_log.unwrap().frontier()
      }
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
    aggregation: self.aggregation.map(engine => engine.copy()),
    reachability: self.reachability.map(engine => engine.copy()),
    security: self.security.map(engine => engine.copy()),
    custom_operator: self.custom_operator,
    custom_log: self.custom_log.map(log => log.copy()),
    custom_outputs: self.custom_outputs.copy(),
    max_state_events: self.max_state_events,
    event_time_config: self.event_time_config,
    event_time_log: self.event_time_log.map(log => log.copy()),
    watermark: self.watermark,
    watermark_requested: self.watermark_requested,
    late_events: self.late_events,
    window_buckets: self.window_buckets.copy(),
    join_left: self.join_left.copy(),
    join_right: self.join_right.copy(),
    join_emitted: self.join_emitted.copy(),
    graph_config: self.graph_config,
    graph_log: self.graph_log.map(log => log.copy()),
  }
}

///|
fn Task::commit(self : Task, candidate : Task) -> Unit {
  self.aggregation = candidate.aggregation
  self.reachability = candidate.reachability
  self.security = candidate.security
  self.custom_log = candidate.custom_log
  self.custom_outputs = candidate.custom_outputs
  self.watermark = candidate.watermark
  self.watermark_requested = candidate.watermark_requested
  self.late_events = candidate.late_events
  self.window_buckets = candidate.window_buckets
  self.join_left = candidate.join_left
  self.join_right = candidate.join_right
  self.join_emitted = candidate.join_emitted
  self.event_time_config = candidate.event_time_config
  self.event_time_log = candidate.event_time_log
  self.graph_config = candidate.graph_config
  self.graph_log = candidate.graph_log
  self.offset = candidate.offset
  self.frontier = candidate.frontier
  self.emitted_outputs = candidate.emitted_outputs
}

///|
/// Apply one input to a private candidate. The public wrapper commits this
/// candidate only after operator and state-limit validation succeeds.
fn Task::process_unchecked(
  self : Task,
  offset : Int64,
  input : TaskInput,
) -> Array[TaskOutput] raise TaskError {
  if self.phase != TaskPhase::Running {
    raise TaskError::WrongPhase(expected=TaskPhase::Running, actual=self.phase)
  }
  self.validate_next_offset(offset)
  let outputs = self.dispatch_input(input)
  self.check_state_limit()
  self.advance_offset(offset)
  self.refresh_frontier()
  self.emitted_outputs += outputs.length()
  outputs
}

///|
/// Apply one contiguous input atomically. A Task never advances its offset or
/// mutates operator state when validation or the state limit rejects the event.
pub fn Task::process(
  self : Task,
  offset : Int64,
  input : TaskInput,
) -> Array[TaskOutput] raise TaskError {
  let candidate = self.copy()
  let outputs = candidate.process_unchecked(offset, input)
  self.commit(candidate)
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
    outputs.push_iter(
      candidate.process_unchecked(offset, TaskInput::new(event)).iter(),
    )
    if offset != 9223372036854775807L {
      offset += 1L
    }
  }
  outputs.sort()
  self.commit(candidate)
  outputs
}

///|
/// Validate a primary batch transactionally without changing this Task. The
/// Worker adapter uses this before persisting a transport WAL.
pub fn Task::validate_batch(
  self : Task,
  batch : @protocol.EventBatch,
) -> Int raise TaskError {
  let candidate = self.copy()
  ignore(candidate.process_batch(batch))
  candidate.state_memory_count()
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
    let event_outputs = candidate.dispatch_input(TaskInput::new(event))
    outputs.push_iter(event_outputs.iter())
  }
  candidate.check_state_limit()
  candidate.refresh_frontier()
  candidate.emitted_outputs += outputs.length()
  outputs.sort()
  self.commit(candidate)
  outputs
}

///|
/// Validate an exchange batch transactionally without changing this Task.
pub fn Task::validate_delta(
  self : Task,
  delta : @protocol.DeltaBatch,
) -> Int raise TaskError {
  let candidate = self.copy()
  ignore(candidate.process_delta(delta))
  candidate.state_memory_count()
}

///|
fn Task::buffer_event_time_input(
  self : Task,
  input : TaskInput,
  flush? : Bool = true,
) -> Array[TaskOutput] raise TaskError {
  let record = input.event()
  let config = self.event_time_config.unwrap()
  let timestamp = record.event_time().unwrap_or(-1L)
  if timestamp < 0L {
    raise TaskError::Custom(
      "event-time operator requires a non-negative timestamp",
    )
  }
  if config.window_size_ms() > 0L {
    let start = timestamp / config.window_size_ms() * config.window_size_ms()
    if start > 9223372036854775807L - config.window_size_ms() {
      raise TaskError::Custom("window end exceeds Int64 timestamp range")
    }
  } else if record.input_id() != "left" && record.input_id() != "right" {
    raise TaskError::Custom("join input must be left or right")
  }
  let event = task_event_causal(record)
  let admission = self.event_time_log.unwrap().submit(event) catch {
      error => raise TaskError::Causal(error)
    }
  for admitted in admission.admitted() {
    self.buffer_admitted_event_time(task_event_record(admitted))
  }
  if flush {
    self.advance_event_time()
  } else {
    []
  }
}

///|
fn Task::buffer_admitted_event_time(
  self : Task,
  record : @protocol.EventRecord,
) -> Unit raise TaskError {
  let timestamp = match record.event_time() {
    Some(value) => value
    None => raise TaskError::Custom("event-time operator requires event_time")
  }
  let config = self.event_time_config.unwrap()
  let window_closed = if config.window_size_ms() > 0L {
    let start = timestamp / config.window_size_ms() * config.window_size_ms()
    start + config.window_size_ms() <= self.watermark
  } else {
    false
  }
  let join_closed = timestamp < self.watermark
  if window_closed || (config.window_size_ms() == 0L && join_closed) {
    self.late_events += 1
    return
  }
  if config.window_size_ms() > 0L {
    if self.operator != OperatorKind::KeyedSum {
      raise TaskError::Custom("event-time windows require keyed-sum")
    }
    let contribution = @aggregation.Contribution::from_json(
      record.key(),
      record.payload(),
    ) catch {
      error => raise TaskError::Aggregation(error)
    }
    let start = timestamp / config.window_size_ms() * config.window_size_ms()
    if start > 9223372036854775807L - config.window_size_ms() {
      raise TaskError::Custom("window end exceeds Int64 timestamp range")
    }
    let mut found = -1
    for i in 0..<self.window_buckets.length() {
      let bucket = self.window_buckets[i]
      if bucket.key == record.key() && bucket.start_ms == start {
        found = i
        break
      }
    }
    let target = if found >= 0 {
      found
    } else {
      self.window_buckets.push({
        key: record.key(),
        start_ms: start,
        total: 0L,
      })
      self.window_buckets.length() - 1
    }
    let bucket = self.window_buckets[target]
    let value = contribution.value()
    if (value > 0L && bucket.total > 9223372036854775807L - value) ||
      (value < 0L && bucket.total < -9223372036854775808L - value) {
      raise TaskError::Custom("window total exceeds Int64 range")
    }
    self.window_buckets[target] = { ..bucket, total: bucket.total + value, }
  }
  if config.window_size_ms() == 0L {
    if record.input_id() == "left" {
      self.join_left.push(record)
    } else if record.input_id() == "right" {
      self.join_right.push(record)
    } else {
      raise TaskError::Custom("join input must be left or right")
    }
  }
}

///|
fn Task::flush_event_time(
  self : Task,
  target : Int64,
) -> Array[TaskOutput] raise TaskError {
  let config = match self.event_time_config {
    None => return []
    Some(value) => value
  }
  let outputs : Array[TaskOutput] = []
  if config.window_size_ms() > 0L {
    let ready = self.window_buckets.filter(bucket => {
      bucket.start_ms + config.window_size_ms() <= target
    })
    self.window_buckets = self.window_buckets.filter(bucket => {
      bucket.start_ms + config.window_size_ms() > target
    })
    let ordered = ready
    ordered.sort_by((left, right) => {
      if left.start_ms != right.start_ms {
        left.start_ms.compare(right.start_ms)
      } else {
        left.key.compare(right.key)
      }
    })
    for bucket in ordered {
      outputs.push(
        TaskOutput::WindowedSum({
          key: bucket.key,
          start_ms: bucket.start_ms,
          end_ms: bucket.start_ms + config.window_size_ms(),
          total: bucket.total,
        }),
      )
    }
  }
  if config.window_size_ms() == 0L {
    // Match before retiring either side: a watermark may close both records
    // at once, including the final end-of-input watermark.
    for left in self.join_left {
      for right in self.join_right {
        let lt = left.event_time().unwrap()
        let rt = right.event_time().unwrap()
        if left.key() == right.key() &&
          (
            (rt >= lt && rt - lt <= config.join_after_ms()) ||
            (lt > rt && lt - rt <= config.join_before_ms())
          ) {
          let pair = (left.id(), right.id())
          if self.join_emitted.add_and_check(pair) {
            outputs.push(
              TaskOutput::Custom(
                if self.graph_config
                  .map(config => config.emit_packets)
                  .unwrap_or(false) {
                  Json::object({
                    "left": left.payload(),
                    "right": right.payload(),
                    "key": Json::string(left.key()),
                    "event_time": @json.to_json(if lt > rt { lt } else { rt }),
                  })
                } else {
                  Json::object({
                    "left": left.payload(),
                    "right": right.payload(),
                  })
                },
              ),
            )
          }
        }
      }
    }
    let retained_left : Array[@protocol.EventRecord] = []
    for left in self.join_left {
      let left_expiry = if left.event_time().unwrap() >
        9223372036854775807L - config.join_after_ms() {
        9223372036854775807L
      } else {
        left.event_time().unwrap() + config.join_after_ms()
      }
      if target != 9223372036854775807L && left_expiry >= target {
        retained_left.push(left)
      }
    }
    let retained_right : Array[@protocol.EventRecord] = []
    for right in self.join_right {
      let right_expiry = if right.event_time().unwrap() >
        9223372036854775807L - config.join_before_ms() {
        9223372036854775807L
      } else {
        right.event_time().unwrap() + config.join_before_ms()
      }
      if target != 9223372036854775807L && right_expiry >= target {
        retained_right.push(right)
      }
    }
    self.join_left = retained_left
    self.join_right = retained_right
    let left_ids = Set(retained_left.map(record => record.id()))
    let right_ids = Set(retained_right.map(record => record.id()))
    self.join_emitted = Set(
      self.join_emitted
      .to_array()
      .filter(pair => left_ids.contains(pair.0) && right_ids.contains(pair.1)),
    )
  }
  outputs
}

///|
pub fn Task::process_watermark(
  self : Task,
  watermark : Int64,
) -> Array[TaskOutput] raise TaskError {
  if self.phase != TaskPhase::Running {
    raise TaskError::WrongPhase(expected=TaskPhase::Running, actual=self.phase)
  }
  if watermark < 0L {
    raise TaskError::InvalidOffset(watermark)
  }
  if watermark < self.watermark_requested {
    raise TaskError::InvalidOffset(watermark)
  }
  let candidate = self.copy()
  candidate.watermark_requested = watermark
  let outputs = candidate.advance_event_time()
  candidate.check_state_limit()
  candidate.emitted_outputs += outputs.length()
  outputs.sort()
  self.commit(candidate)
  outputs
}

///|
/// Validate a frontier advance without mutating live state.
pub fn Task::validate_watermark(
  self : Task,
  timestamp : Int64,
) -> Unit raise TaskError {
  ignore(self.copy().process_watermark(timestamp))
}

///|
/// A pending causal descendant holds the event-time frontier until its parents
/// arrive. Keep the requested frontier so processing its final parent resumes
/// window completion without requiring another watermark from the source.
fn Task::advance_event_time(self : Task) -> Array[TaskOutput] raise TaskError {
  let watermark = self.watermark_requested
  if watermark < 0L {
    return []
  }
  let config = self.event_time_config
  let mut effective = match config {
    None => watermark
    Some(value) =>
      if watermark == 9223372036854775807L {
        watermark
      } else if watermark <= value.allowed_lateness_ms() {
        0L
      } else {
        watermark - value.allowed_lateness_ms()
      }
  }
  match self.event_time_log {
    Some(log) =>
      for pending in log.pending_events() {
        let timestamp = pending.payload().event_time.unwrap()
        if timestamp < effective {
          effective = timestamp
        }
      }
    None => ()
  }
  if self.graph_log is Some(log) {
    for pending in log.pending_events() {
      if pending.payload().event_time is Some(timestamp) &&
        timestamp < effective {
        effective = timestamp
      }
    }
  }
  if effective < self.watermark {
    effective = self.watermark
  }
  let outputs = self.flush_event_time(effective)
  self.watermark = effective
  outputs
}

///|
pub fn Task::fail(self : Task) -> Unit {
  self.phase = TaskPhase::Failed
}
