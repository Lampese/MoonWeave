///|
fn option(args : Array[String], name : String, fallback : String) -> String {
  let mut index = 0
  while index + 1 < args.length() {
    if args[index] == name {
      return args[index + 1]
    }
    index += 1
  }
  fallback
}

///|
async fn main {
  let args = @env.args()
  let worker_id = option(args, "--worker", "worker-1")
  let incarnation_text = option(args, "--incarnation", "boot-cli")
  let port_text = option(args, "--port", "7801")
  let journal_path = option(args, "--journal", "moonweave-coordinator.journal")
  let checkpoint_path = option(
    args, "--checkpoint", "moonweave-coordinator.json",
  )
  let port : Int = @strconv.from_str(port_text) catch {
    _ => {
      println("--port must be an integer")
      panic()
    }
  }
  let store = @transport.CoordinatorStore::open(
    "cli-cluster",
    "coordinator",
    journal_path,
    checkpoint_path~,
  ) catch {
    error => {
      println(@debug.to_string(error))
      panic()
    }
  }
  let incarnation = @protocol.Incarnation::new(incarnation_text)
  ignore(store.register_worker(worker_id, incarnation, ["reachability"]))
  let assignment = store.assign(0, worker_id)
  let event = @protocol.EventRecord::new(
    @causal.EventId::new("cli-source", 0),
    [],
    "cli",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let batch = @protocol.EventBatch::new(0, assignment.epoch(), 0L, [event])
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:" + port.to_string()),
    reuse_addr=true,
  )
  println("coordinator listening on \{server.addr}; worker=\{worker_id}")
  let (connection, _) = server.accept()
  let summary = @transport.run_durable_managed_coordinator(
    connection,
    store,
    worker_id,
    "cli-session",
    0,
    [batch],
  ) catch {
    error => {
      println(@debug.to_string(error))
      panic()
    }
  }
  println(
    "coordinator sent=\{summary.sent_batches()} applied=\{summary.applied_offset()}",
  )
  server.close()
}
