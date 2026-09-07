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
fn parse_endpoint(value : String) -> @flow.NodeEndpoint raise {
  let parts = value.split(":").to_array()
  if parts.length() != 2 {
    raise @flow.ApiError::InvalidEndpoint(value)
  }
  let port : Int = @strconv.from_str(parts[1]) catch {
    _ => raise @flow.ApiError::InvalidEndpoint(value)
  }
  @flow.NodeEndpoint::new(parts[0].to_owned(), port)
}

///|
async fn main {
  let args = @env.args()
  let bind = parse_endpoint(option(args, "--bind", "0.0.0.0:7801"))
  let advertised = parse_endpoint(option(args, "--advertise", "127.0.0.1:7801"))
  let worker_id = option(args, "--worker", "worker-1")
  let incarnation = option(args, "--incarnation", "boot-cli")
  let storage = option(args, "--storage", "moonweave-coordinator")
  let job_id = option(args, "--job", "cli-job")
  let job = @flow.JobDefinition::new(job_id, @flow.OperatorRef::reachability())
    .with_workers([worker_id])
    .compile()
  let config = @flow.CoordinatorNodeConfig::new(
    "cli-cluster",
    "coordinator",
    bind,
    storage,
    [@flow.WorkerSlot::new(worker_id, incarnation)],
  ).with_advertised_endpoint(advertised)
  let event = @flow.Event::new(
    @causal.EventId::new("cli-source", 0),
    [],
    "cli",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let summary = job.run_coordinator_node(config, [event])
  println("coordinator endpoint=\{summary.endpoint().to_string()}")
  println(
    "completed=\{summary.completed_partitions()} failed=\{summary.failed_partitions()}",
  )
  println("sent_batches=\{summary.sent_batches()}")
  for failure in summary.failures() {
    println("failure=\{failure}")
  }
}
