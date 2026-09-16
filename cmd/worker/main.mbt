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
fn storage_budget(args : Array[String]) -> Int64 raise @flow.ApiError {
  let text = option(args, "--storage-budget-bytes", "268435456")
  let value : Int = @string.parse_int(text) catch {
    _ => raise @flow.ApiError::Cluster("invalid --storage-budget-bytes")
  }
  if value < 0 {
    raise @flow.ApiError::Cluster("--storage-budget-bytes cannot be negative")
  }
  value.to_int64()
}

///|
fn partition_hint(args : Array[String]) -> Int? raise @flow.ApiError {
  let text = option(args, "--partition", "")
  if text.is_empty() {
    return None
  }
  let value : Int = @string.parse_int(text) catch {
    _ => raise @flow.ApiError::Cluster("invalid --partition")
  }
  if value < 0 {
    raise @flow.ApiError::InvalidPartitionCount(value)
  }
  Some(value)
}

///|
fn required_option(args : Array[String], name : String) -> String raise {
  let value = option(args, name, "")
  if value.is_empty() {
    raise @flow.ApiError::Cluster(name + " is required")
  }
  value
}

///|
fn parse_endpoint(value : String) -> @flow.NodeEndpoint raise {
  let parts = value.split(":").to_array()
  if parts.length() != 2 {
    raise @flow.ApiError::InvalidEndpoint(value)
  }
  let port : Int = @string.parse_int(parts[1]) catch {
    _ => raise @flow.ApiError::InvalidEndpoint(value)
  }
  @flow.NodeEndpoint::new(parts[0].to_owned(), port)
}

///|
async fn load_submission(path : String) -> @flow.JobSubmission {
  let text = @fs.read_file(path).text() catch {
    error =>
      raise @flow.ApiError::Cluster(
        "read Job config: " + @debug.to_string(error),
      )
  }
  @json.from_json(
    @json.parse(text) catch {
      error =>
        raise @flow.ApiError::Cluster(
          "parse Job config: " + @debug.to_string(error),
        )
    },
  ) catch {
    error =>
      raise @flow.ApiError::Cluster(
        "decode Job config: " + @debug.to_string(error),
      )
  }
}

///|
/// Run a configured Worker process. The JobSubmission JSON contains the same
/// static OperatorArtifact that the Coordinator validates; executable operator
/// code remains in this Worker binary and is never loaded from the network.
async fn main {
  let args = @env.args()
  let cluster_id = option(args, "--cluster", "moonweave-cluster")
  let coordinator_id = option(args, "--coordinator", "coordinator-0")
  let endpoint_text = option(args, "--coordinator-endpoint", "")
  let endpoints_text = option(args, "--coordinator-endpoints", "")
  let endpoint = if !endpoint_text.is_empty() {
    parse_endpoint(endpoint_text)
  } else if !endpoints_text.is_empty() {
    parse_endpoint(endpoints_text.split(",").to_array()[0].to_owned())
  } else {
    parse_endpoint("127.0.0.1:7801")
  }
  let worker_id = required_option(args, "--worker")
  let incarnation = option(args, "--incarnation", "worker-boot-0")
  let storage = required_option(args, "--storage")
  let job_path = required_option(args, "--job-config")
  let budget = storage_budget(args)
  let partition = partition_hint(args)
  let auth_token = option(args, "--auth-token", "")
  let submission = load_submission(job_path)
  let job = submission.job().compile()
  let config = (match partition {
    None =>
      @flow.WorkerNodeConfig::new_auto(
        cluster_id, coordinator_id, endpoint, worker_id, incarnation, storage,
      )
    Some(value) =>
      @flow.WorkerNodeConfig::new_auto_for_partition(
        cluster_id, coordinator_id, endpoint, worker_id, incarnation, storage, value,
      )
  }).with_storage_budget(budget)
  let config = if endpoints_text.is_empty() {
    config
  } else {
    config.with_coordinator_endpoints(
      endpoints_text
      .split(",")
      .to_array()
      .map(value => parse_endpoint(value.to_owned())),
    )
  }
  let config = if auth_token.is_empty() {
    config
  } else {
    config.with_auth_token(auth_token)
  }
  if job.is_graph() {
    let control = parse_endpoint(required_option(args, "--control-endpoint"))
    let summary = job.run_graph_worker(config, control)
    println(
      "worker=\{summary.worker_id()} completed_tasks=\{summary.completed_tasks()}",
    )
    return
  }
  if option(args, "--managed", "false") == "true" {
    let summary = job.run_worker_service(config)
    println(
      "worker=\{summary.worker_id()} completed_tasks=\{summary.completed_tasks()}",
    )
    return
  }
  match partition {
    _ if partition is Some(_) || job.partition_count() == 1 => {
      let summary = job.run_worker_node(config)
      println("worker=\{summary.worker_id()} partition=\{summary.partition()}")
      println(
        "live_batches=\{summary.live_batches()} replayed_batches=\{summary.replayed_batches()} applied=\{summary.applied_offset()}",
      )
    }
    _ => {
      let set_config = @flow.WorkerNodeSetConfig::new(
        cluster_id, coordinator_id, endpoint, worker_id, incarnation, storage,
      ).with_storage_budget(budget)
      let set_config = if endpoints_text.is_empty() {
        set_config
      } else {
        set_config.with_coordinator_endpoints(
          endpoints_text
          .split(",")
          .to_array()
          .map(value => parse_endpoint(value.to_owned())),
        )
      }
      let set_config = if auth_token.is_empty() {
        set_config
      } else {
        set_config.with_auth_token(auth_token)
      }
      let summary = job.run_worker_node_set(set_config)
      println(
        "worker=\{summary.worker_id()} completed_tasks=\{summary.completed_tasks()} failed_tasks=\{summary.failed_tasks()}",
      )
      println(
        "live_batches=\{summary.live_batches()} replayed_batches=\{summary.replayed_batches()}",
      )
    }
  }
}
