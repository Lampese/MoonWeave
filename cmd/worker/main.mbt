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
  let host = option(args, "--host", "127.0.0.1")
  let port_text = option(args, "--port", "7801")
  let worker_id = option(args, "--worker", "worker-1")
  let incarnation = option(args, "--incarnation", "boot-cli")
  let wal_path = option(args, "--wal", "moonweave-worker.wal")
  let port : Int = @strconv.from_str(port_text) catch {
    _ => {
      println("--port must be an integer")
      panic()
    }
  }
  let connection = @socket.Tcp::connect_to_host(host, port~)
  let handler : (@protocol.EventBatch) -> Unit raise = batch => {
    println(
      "worker received batch offset=\{batch.first_offset()} events=\{batch.events().length()}",
    )
  }
  let summary = @transport.run_worker(
    connection,
    "cli-cluster",
    "coordinator",
    worker_id,
    incarnation,
    ["reachability"],
    wal_path,
    handler,
    initial_credit_events=8L,
    initial_credit_bytes=1024L,
  ) catch {
    error => {
      println(@debug.to_string(error))
      panic()
    }
  }
  println(
    "worker live=\{summary.live_batches()} replayed=\{summary.replayed_batches()} applied=\{summary.applied_offset()}",
  )
}
