///|
struct HaRequest {
  version : Int
  id : String
  kind : String
  worker_id : String
  incarnation : String
  partition : Int
} derive(ToJson, FromJson)

///|
struct HaResponse {
  version : Int
  id : String
  ok : Bool
  node : String
  term : Int64
  message : String
} derive(ToJson, FromJson)

///|
/// Each native fixture process owns these four durable stores. Their current
/// snapshots are committed as one HA state bundle after every control write.
struct FixtureState {
  job : @ha.JobStore
  driver : @flow.DriverStore
  source : @ha.SourceLog
  submission : @flow.JobSubmission
}

///|
fn fixture_error(stage : String, error : Error) -> @flow.ApiError {
  @flow.ApiError::Cluster(stage + ": " + @debug.to_string(error))
}

///|
async fn open_fixture_state(
  root : String,
  node_id : String,
) -> FixtureState raise @flow.ApiError {
  let definition = @flow.JobDefinition::new(
    "ha-state-job",
    @flow.OperatorRef::reachability(),
  ).with_workers(["worker-a"])
  let compiled = definition.compile()
  let submission = compiled.submission_with_id("ha-state-submission")
  let spec = @plan.JobSpec::new(
    "ha-state-job",
    "reachability",
    1,
    ["worker-a"],
    [],
  ) catch {
    error => raise fixture_error("create job spec", error)
  }
  let job = @ha.JobStore::open(
    spec,
    root + "/replicas/" + node_id + ".job.wal",
    maximum_bytes=16777216L,
  ) catch {
    error => raise fixture_error("open JobStore", error)
  }
  errdefer job.close()
  if job.phase() == @control.JobPhase::Created {
    ignore(
      job.assign(0, "worker-a", 0, 0L) catch {
        error => raise fixture_error("assign JobStore", error)
      },
    )
    job.start() catch {
      error => raise fixture_error("start JobStore", error)
    }
  }
  let driver = @flow.DriverStore::open(
    root + "/replicas/" + node_id + ".driver.wal",
    maximum_bytes=16777216L,
  ) catch {
    error => raise fixture_error("open DriverStore", error)
  }
  errdefer driver.close()
  ignore(
    driver.submit(submission) catch {
      error => raise fixture_error("submit DriverStore", error)
    },
  )
  let record = @protocol.EventRecord::new(
    @causal.EventId::new("ha-state", 0L),
    [],
    "account",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  ) catch {
    error => raise fixture_error("create state event", error)
  }
  let event = @flow.Event::new(
    record.id(),
    record.parents(),
    record.key(),
    record.payload(),
  ) catch {
    error => raise fixture_error("create Driver event", error)
  }
  if driver.accepted_events() == 0L {
    ignore(
      driver.input([event]) catch {
        error => raise fixture_error("input DriverStore", error)
      },
    )
  }
  let source = @ha.SourceLog::open(
    root + "/replicas/" + node_id + ".source.wal",
    maximum_bytes=16777216L,
  ) catch {
    error => raise fixture_error("open SourceLog", error)
  }
  errdefer source.close()
  if source.event_count() == 0 {
    ignore(
      source.append(0, 0L, record) catch {
        error => raise fixture_error("append SourceLog", error)
      },
    )
  }
  { job, driver, source, submission, }
}

///|
fn fixture_state_bundle(
  state : FixtureState,
  node : @ha.CoordinatorHaNode,
) -> @ha.CoordinatorHaStateBundle {
  {
    coordinator: @json.to_json(node.snapshot()),
    job: @json.to_json(state.job.snapshot()),
    driver: Json::object({
      "version": Json::string("1"),
      "submission": @json.to_json(state.submission),
      "phase": @json.to_json(state.driver.phase()),
      "accepted_events": Json::string(
        state.driver.accepted_events().to_string(),
      ),
    }),
    source: Json::object({
      "version": Json::string("1"),
      "event_count": Json::string(state.source.event_count().to_string()),
      "next_offset": Json::string(
        state.source.next_offset(0).unwrap_or(0L).to_string(),
      ),
    }),
  }
}

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
fn parse_nodes(value : String) -> Array[String] {
  value.split(",").map(item => item.to_owned()).to_array()
}

///|
async fn write_response(root : String, response : HaResponse) -> Unit {
  @fs.write_file(
    root + "/responses/" + response.id + ".json",
    @utf8.encode(@json.to_json(response).stringify()),
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    error => println("response write failed: \{@debug.to_string(error)}")
  }
}

///|
async fn write_state(root : String, node : @ha.CoordinatorHaNode) -> Unit {
  let snapshot = node.snapshot()
  @fs.write_file(
    root + "/state/" + node.node_id() + ".txt",
    "term=" +
    node.term().to_string() +
    "\nleader=" +
    (node.leader_id() catch { _ => "<error>" }) +
    "\nworkers=" +
    snapshot.workers().length().to_string() +
    "\nassignments=" +
    snapshot.assignments().length().to_string() +
    "\n",
    create_mode=@fs.CreateOrTruncate,
    sync=@fs.Full,
  ) catch {
    _ => ()
  }
}

///|
async fn process_request(
  root : String,
  node : @ha.CoordinatorHaNode,
  state : FixtureState,
) -> Unit {
  let request_path = root + "/request.json"
  let response_dir = root + "/responses"
  @fs.mkdir(response_dir, recursive=true) catch {
    _ => ()
  }
  let request : HaRequest = @json.from_json(
    @json.parse(@fs.read_file(request_path).text()) catch {
      _ => return
    },
  ) catch {
    _ => return
  }
  let response_path = response_dir + "/" + request.id + ".json"
  if @fs.exists(response_path) {
    return
  }
  if !(node.is_leader() catch { _ => false }) {
    return
  }
  let commit_bundle = () => {
    node.propose_state(fixture_state_bundle(state, node)) catch {
      error => raise fixture_error("replicate state bundle", error)
    }
  }
  let result = match request.kind {
    "register" => {
      let incarnation = @protocol.Incarnation::new(request.incarnation) catch {
        _ => return
      }
      try {
        ignore(node.register_worker(request.worker_id, incarnation, []))
        commit_bundle()
        Ok(())
      } catch {
        error => Err(@debug.to_string(error))
      }
    }
    "assign" =>
      try {
        ignore(node.assign(request.partition, request.worker_id))
        commit_bundle()
        Ok(())
      } catch {
        error => Err(@debug.to_string(error))
      }
    _ => Err("unknown request kind")
  }
  match result {
    Ok(_) =>
      write_response(root, {
        version: 1,
        id: request.id,
        ok: true,
        node: node.node_id(),
        term: node.term(),
        message: "committed",
      })
    Err(message) =>
      write_response(root, {
        version: 1,
        id: request.id,
        ok: false,
        node: node.node_id(),
        term: node.term(),
        message,
      })
  }
}

///|
async fn main {
  let args = @env.args()
  let root = option(args, "--root", "")
  let node_id = option(args, "--node", "")
  let nodes = parse_nodes(option(args, "--nodes", "node-0,node-1,node-2"))
  let config = @ha.CoordinatorHaNodeConfig::new(
    option(args, "--cluster", "ha-fixture-cluster"),
    option(args, "--coordinator", "coordinator-0"),
    node_id,
    nodes,
    root,
    lease_ms=400L,
    maximum_bytes=16777216L,
  )
  let node = @ha.CoordinatorHaNode::open(config) catch {
    error => {
      println("open_error=\{@debug.to_string(error)}")
      return
    }
  }
  defer node.close()
  let fixture_state = open_fixture_state(root, node_id) catch {
    error => {
      println("state_open_error=\{@debug.to_string(error)}")
      return
    }
  }
  defer {
    fixture_state.job.close()
    fixture_state.driver.close()
    fixture_state.source.close()
  }
  @fs.mkdir(root + "/state", recursive=true) catch {
    _ => ()
  }
  println("node=\{node.node_id()} started")
  for ;; {
    if @fs.exists(root + "/stop") {
      break
    }
    node.tick() catch {
      error => {
        println("node=\{node.node_id()} error=\{@debug.to_string(error)}")
        @async.sleep(25)
      }
    }
    write_state(root, node)
    process_request(root, node, fixture_state)
    @async.sleep(25)
  }
  println("node=\{node.node_id()} stopped term=\{node.term()}")
}
