///|
/// Resource limits can change without changing the meaning of checkpointed
/// state or replay. Graphs apply this contract recursively to every stage.
fn CompiledJob::validate_compatible_update(
  self : CompiledJob,
  next : CompiledJob,
) -> Unit raise ApiError {
  let before = self.definition
  let after = next.definition
  if { ..before, max_state_events: after.max_state_events, graph: after.graph, } !=
    after ||
    self.state_schema() != next.state_schema() {
    raise ApiError::InvalidDriverCommand(
      "update changes the running topology or state schema",
    )
  }
  match (before.graph, after.graph) {
    (None, None) => ()
    (Some(old), Some(new)) => {
      if old.edges != new.edges ||
        self.graph_stages.map(value => value.0) !=
        next.graph_stages.map(value => value.0) {
        raise ApiError::InvalidDriverCommand(
          "update changes the graph topology",
        )
      }
      for index, (_, stage) in self.graph_stages {
        stage.validate_compatible_update(next.graph_stages[index].1)
      }
    }
    _ =>
      raise ApiError::InvalidDriverCommand("update changes the graph topology")
  }
}

///|
async fn DriverStore::begin_graph_update(
  self : DriverStore,
  candidate : JobSubmission,
) -> Unit raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock graph update intent", error)
  }
  defer self.write_lock.release()
  ignore(self.validate_update(candidate))
  match self.graph_update {
    Some(existing) if existing == candidate => return
    Some(_) =>
      raise ApiError::InvalidDriverCommand("another graph update is pending")
    None => ()
  }
  let entry = {
    ..driver_entry("graph_update"),
    graph_update: Some(driver_owned(candidate)),
  }
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist graph update intent", error)
    },
  )
  self.install_entry(entry)
}

///|
async fn DriverStore::complete_graph_update(
  self : DriverStore,
) -> Unit raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock graph update completion", error)
  }
  defer self.write_lock.release()
  self.check_open()
  let entry = driver_entry("graph_updated")
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist graph update completion", error)
    },
  )
  self.install_entry(entry)
}
