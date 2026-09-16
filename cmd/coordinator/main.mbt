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
fn driver_event_limit(args : Array[String]) -> Int raise @flow.ApiError {
  let text = option(args, "--driver-event-limit", "100000")
  let value : Int = @string.parse_int(text) catch {
    _ => raise @flow.ApiError::Cluster("invalid --driver-event-limit")
  }
  if value < 0 {
    raise @flow.ApiError::Cluster("--driver-event-limit cannot be negative")
  }
  value
}

///|
fn parse_endpoint(value : String) -> @flow.NodeEndpoint raise @flow.ApiError {
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
fn parse_network_ha(
  args : Array[String],
  cluster_id : String,
  coordinator_id : String,
  default_storage : String,
) -> @runtime.NetworkHaConfig? raise @flow.ApiError {
  let nodes = option(args, "--ha-nodes", "")
  if nodes.is_empty() {
    return None
  }
  let self_id = option(args, "--ha-node", "node-0")
  let peers : Array[@runtime.NetworkHaPeer] = []
  for item in nodes.split(",") {
    let fields = item.split("@").to_array()
    if fields.length() != 3 {
      raise @flow.ApiError::Cluster("invalid --ha-nodes entry")
    }
    let endpoint = parse_endpoint(fields[1].to_owned())
    peers.push(
      @runtime.NetworkHaPeer::new(
        fields[0].to_owned(),
        @runtime.NetworkHaEndpoint::new(endpoint.host(), endpoint.port()),
        fields[2].to_owned(),
      ) catch {
        error => raise @flow.ApiError::Cluster(@debug.to_string(error))
      },
    )
  }
  let bind = parse_endpoint(option(args, "--ha-bind", "127.0.0.1:7901"))
  Some(
    @runtime.NetworkHaConfig::new(
      cluster_id,
      coordinator_id,
      self_id,
      @runtime.NetworkHaEndpoint::new(bind.host(), bind.port()),
      peers[:],
      default_storage + "/network-ha-" + self_id,
    ) catch {
      error => raise @flow.ApiError::Cluster(@debug.to_string(error))
    },
  )
}

///|
fn worker_slots(value : String) -> Array[@flow.WorkerSlot] raise {
  if value.is_empty() {
    raise @flow.ApiError::EmptyWorkerList
  }
  let result : Array[@flow.WorkerSlot] = []
  for item in value.split(",") {
    let fields = item.split("@").to_array()
    if fields.length() < 2 || fields.length() > 3 {
      raise @flow.ApiError::InvalidWorkerAssignment(item.to_owned())
    }
    let slot = @flow.WorkerSlot::new(fields[0].to_owned(), fields[1].to_owned())
    result.push(
      match fields.length() {
        2 => slot
        _ => slot.with_storage_dir(fields[2].to_owned())
      },
    )
  }
  result
}

///|
/// Long-lived Coordinator/Driver service. Job definitions and input records
/// arrive through the Driver protocol; this process contains no sample Job or
/// sample event and can therefore be supervised as a real service.
async fn main {
  @signal.set_global_cancellation_signals([
    @signal.Signal::SIGTERM,
    @signal.Signal::SIGINT,
  ])
  let args = @env.args()
  let cluster_id = option(args, "--cluster", "moonweave-cluster")
  let coordinator_id = option(args, "--coordinator", "coordinator-0")
  let bind = parse_endpoint(option(args, "--bind", "0.0.0.0:7801"))
  let advertise = parse_endpoint(option(args, "--advertise", "127.0.0.1:7801"))
  let driver_bind = parse_endpoint(
    option(args, "--driver-bind", "0.0.0.0:7802"),
  )
  let driver_advertise = parse_endpoint(
    option(args, "--driver-advertise", "127.0.0.1:7802"),
  )
  let storage = option(args, "--storage", "moonweave-coordinator")
  let workers = worker_slots(option(args, "--workers", "worker-0@boot-0"))
  let auth_token = option(args, "--auth-token", "")
  let budget = storage_budget(args)
  let event_limit = driver_event_limit(args)
  let storage_exists = @fs.exists(storage)
  if !storage_exists {
    @fs.mkdir(storage, recursive=true)
  }
  let coordinator_config = @flow.CoordinatorNodeConfig::new(
      cluster_id, coordinator_id, bind, storage, workers,
    )
    .with_advertised_endpoint(advertise)
    .with_storage_budget(budget)
  let coordinator_config = match
    parse_network_ha(args, cluster_id, coordinator_id, storage) {
    Some(ha) => coordinator_config.with_network_ha(ha)
    None => coordinator_config
  }
  let coordinator_config = if auth_token.is_empty() {
    coordinator_config
  } else {
    coordinator_config.with_auth_token(auth_token)
  }
  let driver_config = @flow.DriverControlConfig::new(
    cluster_id, coordinator_id, driver_bind,
  ).with_advertised_endpoint(driver_advertise)
  let driver_config = if auth_token.is_empty() {
    driver_config
  } else {
    driver_config.with_auth_token(auth_token)
  }
  let coordinator = @flow.CoordinatorService::bind(
    coordinator_config,
    driver_config,
    maximum_events=event_limit,
  )
  println(
    "coordinator data=\{coordinator.data_endpoint().to_string()} driver=\{coordinator.control_endpoint().to_string()}",
  )
  let mut completed = false
  let mut statuses : Array[@flow.JobRunStatus] = []
  try
    @async.with_task_group <| group => {
      let service = group.spawn() <| () => {
        @async.protect_from_cancel <| () => { coordinator.run() }
      }
      @async.with_cancellation_handler(
        handler=() => {
          @async.protect_from_cancel <| () => {
            coordinator.shutdown_gracefully() catch {
              error =>
                println("graceful shutdown failed: \{@debug.to_string(error)}")
            }
            statuses = coordinator.statuses()
            coordinator.close()
            service.wait()
            completed = true
          }
        },
        () => {
          service.wait()
          completed = true
        },
      )
    }
  catch {
    error if @async.is_cancellation_error(error) && completed => ()
    error => {
      println("coordinator service failed: " + @debug.to_string(error))
      raise error
    }
  }
  for status in statuses {
    println("job=\{status.job_id()} phase=\{@debug.to_string(status.phase())}")
    match status.failure() {
      Some(reason) => println("failure=\{reason}")
      None => ()
    }
  }
}
