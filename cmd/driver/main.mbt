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
fn print_status(status : @flow.JobRunStatus, json? : Bool = false) -> Unit {
  if json {
    println(@json.to_json(status).stringify())
    return
  }
  println(
    "job=\{status.job_id()} phase=\{@debug.to_string(status.phase())} paused=\{status.is_paused()} accepted=\{status.accepted_events()} digest=\{status.plan_digest()}",
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
async fn run_driver() -> Unit {
  let args = @env.args()
  let command = option(args, "--command", "submit")
  let config_path = required_option(args, "--job-config")
  if command == "compile" {
    let definition : @flow.JobDefinition = @json.from_json(
      @json.parse(@fs.read_file(config_path).text()),
    )
    let job = definition.compile()
    let id = option(args, "--submission-id", job.job_id())
    println(@json.to_json(job.submission_with_id(id)).stringify())
    return
  }
  let endpoint_text = option(args, "--endpoint", "")
  let seed_text = option(args, "--coordinator-endpoints", "")
  let endpoint = if !endpoint_text.is_empty() {
    parse_endpoint(endpoint_text)
  } else if !seed_text.is_empty() {
    parse_endpoint(seed_text.split(",").to_array()[0].to_owned())
  } else {
    raise @flow.ApiError::Cluster(
      "--endpoint or --coordinator-endpoints is required",
    )
  }
  let cluster_id = required_option(args, "--cluster")
  let coordinator_id = required_option(args, "--coordinator")
  let driver_id = option(args, "--driver", "driver-cli")
  let json = option(args, "--format", "text") == "json"
  let submission = load_submission(config_path)
  let update_submission = if command == "update" {
    let update_path = required_option(args, "--update-job-config")
    load_submission(update_path)
  } else {
    submission
  }
  let config = @flow.DriverConfig::new(
    cluster_id, coordinator_id, driver_id, endpoint,
  )
  let config = if seed_text.is_empty() {
    config
  } else {
    config.with_coordinator_endpoints(
      seed_text
      .split(",")
      .to_array()
      .map(value => parse_endpoint(value.to_owned())),
    )
  }
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
    "submit" => print_status(submitted, json~)
    "status" => print_status(client.status(), json~)
    "pause" => print_status(client.pause(), json~)
    "resume" => print_status(client.resume(), json~)
    "rescale" => {
      let workers_text = required_option(args, "--workers")
      let workers = workers_text
        .split(",")
        .to_array()
        .map(value => value.to_owned())
      let value = option(args, "--parallelism", "")
      let parallelism = if value.is_empty() {
        None
      } else {
        Some(@string.parse_int(value))
      }
      print_status(client.rescale(workers[:], parallelism?), json~)
    }
    "update" => print_status(client.update(update_submission), json~)
    "drain" => print_status(client.drain(), json~)
    "cancel" => print_status(client.cancel(), json~)
    "input" => {
      let path = required_option(args, "--events")
      print_status(client.input(load_events(path)), json~)
    }
    _ => raise @flow.ApiError::InvalidDriverCommand(command)
  }
}

///|
async fn main {
  run_driver() catch {
    error => {
      @stdio.stderr.write(@debug.to_string(error) + "\n") catch {
        _ => ()
      }
      raise error
    }
  }
}
