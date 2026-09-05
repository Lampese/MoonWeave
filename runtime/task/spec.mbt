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
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn OperatorKind::from_id(
  operator_id : String,
) -> OperatorKind raise TaskError {
  match operator_id {
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
