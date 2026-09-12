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
fn integer_option(
  args : Array[String],
  name : String,
  fallback : Int,
) -> Int raise {
  @string.parse_int(option(args, name, fallback.to_string())) catch {
    _ => raise @flow.ApiError::Cluster("invalid " + name)
  }
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
fn performance_event_with_origin(
  origin : String,
  sequence : Int,
) -> @flow.Event raise {
  @flow.Event::new(
    @causal.EventId::new(origin, sequence.to_int64()),
    [],
    "account",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
}

///|
fn p99(values : Array[Int64]) -> Int64 {
  values.sort()
  let position = (values.length() * 99 + 99) / 100 - 1
  values[position.max(0)]
}

///|
async fn main {
  let args = @env.args()
  let endpoint = parse_endpoint(required_option(args, "--endpoint"))
  let cluster_id = required_option(args, "--cluster")
  let coordinator_id = required_option(args, "--coordinator")
  let config_path = required_option(args, "--job-config")
  let iterations = integer_option(args, "--iterations", 64)
  let duration_ms = integer_option(args, "--duration-ms", 0)
  let batch_size = integer_option(args, "--batch-size", 16)
  let paced_ms = integer_option(args, "--paced-ms", 0)
  let origin = option(args, "--origin", "production-performance")
  if iterations <= 0 || duration_ms < 0 || batch_size <= 0 || paced_ms < 0 {
    raise @flow.ApiError::Cluster(
      "iterations, duration and batch size are invalid",
    )
  }
  let submission = load_submission(config_path)
  let client = @flow.DriverClient::connect(
    @flow.DriverConfig::new(
      cluster_id, coordinator_id, "performance-fixture", endpoint,
    ),
  )
  defer client.close()
  let started = @async.now()
  let initial_accepted = client.submit(submission).accepted_events()
  let mut sequence = 0
  let mut count = 0
  let latencies : Array[Int64] = []
  if duration_ms > 0 {
    let deadline = started + duration_ms.to_int64()
    while @async.now() < deadline || count == 0 {
      let events : Array[@flow.Event] = []
      for _ in 0..<batch_size {
        events.push(performance_event_with_origin(origin, sequence))
        sequence += 1
      }
      let before = @async.now()
      let status = client.input(events)
      let elapsed = @async.now() - before
      if status.accepted_events() <
        initial_accepted + count.to_int64() + events.length().to_int64() {
        raise @flow.ApiError::Cluster("performance input was rejected")
      }
      latencies.push(elapsed)
      count += events.length()
      if paced_ms > 0 {
        @async.sleep(paced_ms)
      }
    }
  } else {
    while count < iterations {
      let before = @async.now()
      let status = client.input([
        performance_event_with_origin(origin, sequence),
      ])
      let elapsed = @async.now() - before
      if status.accepted_events() != initial_accepted + (count + 1).to_int64() {
        raise @flow.ApiError::Cluster("performance input was rejected")
      }
      latencies.push(elapsed)
      sequence += 1
      count += 1
      if paced_ms > 0 {
        @async.sleep(paced_ms)
      }
    }
  }
  let elapsed_total = (@async.now() - started).max(1L)
  let throughput_x1000 = count.to_int64() * 1000L * 1000L / elapsed_total
  println("performance_events=" + count.to_string())
  println("performance_elapsed_ms=" + elapsed_total.to_string())
  println(
    "performance_throughput_events_per_sec_x1000=" +
    throughput_x1000.to_string(),
  )
  println("performance_input_p99_ms=" + p99(latencies).to_string())
}
