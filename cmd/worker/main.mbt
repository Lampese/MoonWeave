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
  let port : Int = @strconv.from_str(option(args, "--port", "7801")) catch {
    _ => {
      println("--port must be an integer")
      panic()
    }
  }
  let worker_id = option(args, "--worker", "worker-1")
  let incarnation = option(args, "--incarnation", "boot-cli")
  let storage = option(args, "--storage", "moonweave-worker")
  let job_id = option(args, "--job", "cli-job")
  let partition : Int = @strconv.from_str(option(args, "--partition", "0")) catch {
    _ => {
      println("--partition must be an integer")
      panic()
    }
  }
  let job = @flow.JobDefinition::new(job_id, @flow.OperatorRef::reachability())
    .with_workers([worker_id])
    .compile()
  let config = @flow.WorkerNodeConfig::new(
    "cli-cluster",
    "coordinator",
    @flow.NodeEndpoint::new(host, port),
    worker_id,
    incarnation,
    storage,
    partition,
  )
  let summary = job.run_worker_node(config)
  println("worker=\{summary.worker_id()} partition=\{summary.partition()}")
  println(
    "live_batches=\{summary.live_batches()} replayed_batches=\{summary.replayed_batches()} applied=\{summary.applied_offset()}",
  )
}
