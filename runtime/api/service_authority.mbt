///|
async fn CoordinatorService::replicate_authority(
  self : CoordinatorService,
) -> Unit raise ApiError {
  guard self.network_ha is Some(ha) else { return }
  self.authority_lock.acquire() catch {
    error => raise driver_error("lock authority replication", error)
  }
  defer self.authority_lock.release()
  let snapshot = self.authoritative_snapshot()
  ha.propose(
    snapshot,
    expected_index=self.authority_index,
    expected_term=self.authority_term,
  ) catch {
    error => raise driver_error("replicate Coordinator authority", error)
  }
  self.authority_index = ha.commit_index()
  self.authority_term = ha.state_term()
}

///|
async fn CoordinatorService::network_active(
  self : CoordinatorService,
) -> Bool raise ApiError {
  match self.network_ha {
    None => true
    Some(ha) => {
      let status = ha.tick() catch {
        error => raise driver_error("check execution leadership", error)
      }
      status.leader() == ha.node_id() &&
      status.available_peers() >= status.quorum()
    }
  }
}
