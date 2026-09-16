///|
fn CompiledJob::initial_parallelism(self : CompiledJob) -> Int {
  self.graph_stages
  .get(0)
  .map(value => value.1.definition.parallelism())
  .unwrap_or(self.definition.parallelism())
}

///|
fn CompiledJob::maximum_parallelism(self : CompiledJob) -> Int {
  self.graph_stages
  .iter()
  .map(value => value.1.partition_count())
  .fold(init=2147483647, Int::min)
  .min(if self.is_graph() { 2147483647 } else { self.partition_count() })
}
