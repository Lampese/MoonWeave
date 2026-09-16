///|
/// Immutable execution manifest derived from one JobSpec assignment.
pub(all) struct TaskSpec {
  job_id : String
  task_id : String
  operator_id : String
  operator : OperatorKind
  partition : Int
  epoch : Int
  start_offset : Int64
  event_time_config : TaskEventTimeConfig?
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Configuration shared by the live event-time operators. A missing value
/// keeps the legacy offset-driven operator behavior.
pub(all) struct TaskEventTimeConfig {
  window_size_ms : Int64
  allowed_lateness_ms : Int64
  join_before_ms : Int64
  join_after_ms : Int64
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn TaskEventTimeConfig::new(
  window_size_ms? : Int64,
  allowed_lateness_ms? : Int64 = 0L,
  join_before_ms? : Int64,
  join_after_ms? : Int64,
) -> TaskEventTimeConfig raise TaskError {
  let window = window_size_ms.unwrap_or(0L)
  let before = join_before_ms.unwrap_or(0L)
  let after = join_after_ms.unwrap_or(0L)
  if window < 0L ||
    allowed_lateness_ms < 0L ||
    before < 0L ||
    after < 0L ||
    (window == 0L && join_before_ms is None && join_after_ms is None) ||
    (window > 0L && (join_before_ms is Some(_) || join_after_ms is Some(_))) {
    raise TaskError::Custom("invalid event-time configuration")
  }
  {
    window_size_ms: window,
    allowed_lateness_ms,
    join_before_ms: before,
    join_after_ms: after,
  }
}

///|
pub fn TaskEventTimeConfig::validate(
  self : TaskEventTimeConfig,
) -> Unit raise TaskError {
  if self.window_size_ms < 0L ||
    self.allowed_lateness_ms < 0L ||
    self.join_before_ms < 0L ||
    self.join_after_ms < 0L ||
    (
      self.window_size_ms > 0L &&
      (self.join_before_ms != 0L || self.join_after_ms != 0L)
    ) {
    raise TaskError::Custom("invalid event-time configuration")
  }
}

///|
pub fn TaskEventTimeConfig::window_size_ms(self : TaskEventTimeConfig) -> Int64 {
  self.window_size_ms
}

///|
pub fn TaskEventTimeConfig::allowed_lateness_ms(
  self : TaskEventTimeConfig,
) -> Int64 {
  self.allowed_lateness_ms
}

///|
pub fn TaskEventTimeConfig::join_before_ms(self : TaskEventTimeConfig) -> Int64 {
  self.join_before_ms
}

///|
pub fn TaskEventTimeConfig::join_after_ms(self : TaskEventTimeConfig) -> Int64 {
  self.join_after_ms
}

///|
pub fn OperatorKind::from_id(
  operator_id : String,
) -> OperatorKind raise TaskError {
  match operator_id {
    "keyed-sum" => OperatorKind::KeyedSum
    "reachability" => OperatorKind::Reachability
    "security-chain" => OperatorKind::SecurityChain
    _ => raise TaskError::UnknownOperator(operator_id)
  }
}

///|
pub fn TaskSpec::from_job(
  job : @plan.JobSpec,
  assignment : @protocol.AssignmentSpec,
) -> TaskSpec raise TaskError {
  if assignment.partition() < 0 ||
    assignment.partition() >= job.partition_count() {
    raise TaskError::InvalidPartition(assignment.partition())
  }
  let operator = OperatorKind::from_id(job.operator_id())
  let task_id = job.job_id() +
    "/p" +
    assignment.partition().to_string() +
    "/e" +
    assignment.epoch().to_string()
  {
    job_id: job.job_id(),
    task_id,
    operator_id: job.operator_id(),
    operator,
    partition: assignment.partition(),
    epoch: assignment.epoch(),
    start_offset: assignment.start_offset(),
    event_time_config: None,
  }
}

///|
pub fn TaskSpec::job_id(self : TaskSpec) -> String {
  self.job_id
}

///|
pub fn TaskSpec::task_id(self : TaskSpec) -> String {
  self.task_id
}

///|
pub fn TaskSpec::operator_id(self : TaskSpec) -> String {
  self.operator_id
}

///|
pub fn TaskSpec::operator(self : TaskSpec) -> OperatorKind {
  self.operator
}

///|
pub fn TaskSpec::partition(self : TaskSpec) -> Int {
  self.partition
}

///|
pub fn TaskSpec::epoch(self : TaskSpec) -> Int {
  self.epoch
}

///|
pub fn TaskSpec::start_offset(self : TaskSpec) -> Int64 {
  self.start_offset
}

///|
pub fn TaskSpec::event_time_config(self : TaskSpec) -> TaskEventTimeConfig? {
  self.event_time_config
}
