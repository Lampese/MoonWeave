///|
/// Validate checkpoint structure on a transfer host, which need not contain
/// the custom operator's executable code. This checks causal state, graph and
/// event-time state, identity and frontier consistency. Payload schemas are
/// checked separately by the registered implementation before installation
/// and again by the Worker on restore. No operator callback is replayed here.
pub fn TaskSnapshot::validate_transfer(
  self : TaskSnapshot,
  start_offset : Int64,
) -> Unit raise TaskError {
  let task = match self.operator {
    Custom(_, _, _, _, _, _, _, _, _, _) => {
      let structural = CustomOperator::{
        kind: self.operator,
        validate: _ => (),
        process: _ => {
          raise TaskError::Custom(
            "transfer validation cannot execute an operator",
          )
        },
      }
      Task::new_custom(
        self.task_id,
        self.operator_id,
        structural,
        self.partition,
        self.epoch,
        start_offset~,
      )
    }
    _ =>
      Task::new(
        self.task_id,
        self.operator_id,
        self.operator,
        self.partition,
        self.epoch,
        start_offset~,
      )
  }
  let task = match self.graph_config {
    Some(config) => task.with_graph_input(config)
    None => task
  }
  task.restore(self)
}
