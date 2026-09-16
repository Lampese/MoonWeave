///|
async fn authority_node_config(root : String) -> NetworkHaConfig {
  let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let endpoint = NetworkHaEndpoint::new("127.0.0.1", server.addr.port())
  server.close()
  NetworkHaConfig::new(
    "c",
    "coordinator",
    "a",
    endpoint,
    [
      NetworkHaPeer::new("a", endpoint, root),
      NetworkHaPeer::new(
        "b",
        NetworkHaEndpoint::new("127.0.0.1", 1),
        root + "/b",
      ),
      NetworkHaPeer::new(
        "d",
        NetworkHaEndpoint::new("127.0.0.1", 2),
        root + "/d",
      ),
    ],
    root,
    lease_ms=50L,
  )
}

///|
async test "quorum election promise survives restart and rejects a stale candidate" {
  let root = @fs.tmpdir(prefix="moonweave-network-vote")
  defer @fs.rmdir(root, recursive=true)
  let config = authority_node_config(root)
  let node = NetworkHaNode::open(config)
  let vote : NetworkHaRequest = {
    version: 1,
    cluster_id: "c",
    coordinator_id: "coordinator",
    kind: "vote",
    node_id: "b",
    term: 1L,
    index: 0L,
    leader: "b",
    state: None,
    state_term: Some(0L),
  }
  assert_true(node.handle(vote).ok)
  assert_false(node.handle({ ..vote, leader: "d", node_id: "d", }).ok)
  node.close()
  let restored = NetworkHaNode::open(config)
  defer restored.close()
  assert_false(restored.handle({ ..vote, leader: "d", node_id: "d", }).ok)
  restored.install_snapshot(2L, "b", 1L, { "revision": "committed" })
  assert_false(restored.handle({ ..vote, term: 3L, }).ok)
}

///|
async test "quorum replay chooses later term at equal index without regressing an election" {
  let root = @fs.tmpdir(prefix="moonweave-network-authority")
  defer @fs.rmdir(root, recursive=true)
  let config = authority_node_config(root)
  let node = NetworkHaNode::open(config)
  node.install_snapshot(1L, "a", 1L, { "revision": "unconfirmed" })
  node.install_snapshot(2L, "b", 1L, { "revision": "committed" })
  assert_eq(node.state(), Some({ "revision": "committed" }))
  node.term = 5L
  node.leader = "d"
  node.voted_for = "d"
  node.persist_meta()
  node.install_snapshot(3L, "b", 2L, { "revision": "next" })
  assert_eq(node.term, 5L)
  assert_eq(node.leader, "d")
  node.close()
  let restored = NetworkHaNode::open(config)
  defer restored.close()
  assert_eq(restored.term, 5L)
  assert_eq(restored.leader, "d")
  assert_eq(restored.state_term(), 3L)
  assert_eq(restored.state(), Some({ "revision": "next" }))
}

///|
async test "identical quorum retry still rejects a minority" {
  let root = @fs.tmpdir(prefix="moonweave-network-minority")
  defer @fs.rmdir(root, recursive=true)
  let node = NetworkHaNode::open(authority_node_config(root))
  defer node.close()
  let state : Json = { "revision": "one" }
  node.install_snapshot(1L, "a", 1L, state)
  try node.propose(state) catch {
    NetworkHaError::NoQuorum(..) => ()
    _ => fail("wrong minority error")
  } noraise {
    _ => fail("minority acknowledged an identical write")
  }
}
