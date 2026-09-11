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
async fn load_events(path : String) -> Array[@flow.Event] {
  let text = @fs.read_file(path).text() catch {
    error =>
      raise @flow.ApiError::Cluster(
        "read input config: " + @debug.to_string(error),
      )
  }
  @json.from_json(
    @json.parse(text) catch {
      error =>
        raise @flow.ApiError::Cluster(
          "parse input config: " + @debug.to_string(error),
        )
    },
  ) catch {
    error =>
      raise @flow.ApiError::Cluster(
        "decode input config: " + @debug.to_string(error),
      )
  }
}

///|
fn print_status(status : @flow.JobRunStatus) -> Unit {
  println(
    "job=\{status.job_id()} phase=\{@debug.to_string(status.phase())} accepted=\{status.accepted_events()} digest=\{status.plan_digest()}",
  )
  match status.failure() {
    Some(reason) => println("failure=\{reason}")
    None => ()
  }
}

///|
/// Submit and control a Job through the Coordinator's durable Driver endpoint.
/// Every invocation submits the same idempotent submission before executing the
/// requested command, so a CLI restart does not address an unknown request.
async fn main {
  let args = @env.args()
  let endpoint = parse_endpoint(required_option(args, "--endpoint"))
  let cluster_id = required_option(args, "--cluster")
  let coordinator_id = required_option(args, "--coordinator")
  let driver_id = option(args, "--driver", "driver-cli")
  let command = option(args, "--command", "submit")
  let config_path = required_option(args, "--job-config")
  let submission = load_submission(config_path)
  let config = @flow.DriverConfig::new(
    cluster_id, coordinator_id, driver_id, endpoint,
  )
  let auth_token = option(args, "--auth-token", "")
  let config = if auth_token.is_empty() {
    config
  } else {
    config.with_auth_token(auth_token)
  }
  let client = @flow.DriverClient::connect(config)
  defer client.close()
  let submitted = client.submit(submission)
  match command {
    "submit" => print_status(submitted)
    "status" => print_status(client.status())
    "drain" => print_status(client.drain())
    "cancel" => print_status(client.cancel())
    "input" => {
      let path = required_option(args, "--events")
      print_status(client.input(load_events(path)))
    }
    _ => raise @flow.ApiError::InvalidDriverCommand(command)
  }
}
