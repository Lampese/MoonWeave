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
fn parse_endpoint(value : String) -> @runtime.NetworkHaEndpoint raise {
  let fields = value.split(":").to_array()
  if fields.length() != 2 {
    raise @runtime.NetworkHaError::InvalidConfig
  }
  let port : Int = @string.parse_int(fields[1]) catch {
    _ => raise @runtime.NetworkHaError::InvalidConfig
  }
  @runtime.NetworkHaEndpoint::new(fields[0].to_owned(), port)
}

///|
fn parse_peers(value : String) -> Array[@runtime.NetworkHaPeer] raise {
  let peers : Array[@runtime.NetworkHaPeer] = []
  for item in value.split(",") {
    let fields = item.split("@").to_array()
    if fields.length() != 3 {
      raise @runtime.NetworkHaError::InvalidConfig
    }
    peers.push(
      @runtime.NetworkHaPeer::new(
        fields[0].to_owned(),
        parse_endpoint(fields[1].to_owned()),
        fields[2].to_owned(),
      ),
    )
  }
  peers
}

///|
fn make_config(args : Array[String]) -> @runtime.NetworkHaConfig raise {
  let cluster = option(args, "--cluster", "network-ha")
  let coordinator = option(args, "--coordinator", "coordinator")
  let node = option(args, "--node", "")
  let storage = option(args, "--storage", "")
  let peers = parse_peers(option(args, "--nodes", ""))
  @runtime.NetworkHaConfig::new(
    cluster,
    coordinator,
    node,
    parse_endpoint(option(args, "--ha-bind", "127.0.0.1:7901")),
    peers[:],
    storage,
    lease_ms=300L,
  )
}

///|
async fn serve(args : Array[String]) -> Unit {
  let config = make_config(args)
  let node = @runtime.NetworkHaNode::open(config)
  defer node.close()
  @async.with_task_group <| group => {
    group.spawn_bg(allow_failure=true) <| () => { node.serve() }
    let root = option(args, "--observe", "")
    if !root.is_empty() {
      @fs.mkdir(root, recursive=true) catch {
        _ => ()
      }
    }
    for ;; {
      if !option(args, "--stop-file", "").is_empty() &&
        @fs.exists(option(args, "--stop-file", "")) {
        return
      }
      let status = node.tick()
      if !root.is_empty() {
        @fs.write_file(
          root + "/" + node.node_id() + ".json",
          @json.to_json(status).stringify(),
          sync=@fs.Full,
        )
      }
      @async.sleep(25)
    }
  }
}

///|
async fn propose(args : Array[String]) -> Unit {
  let peers = parse_peers(option(args, "--nodes", ""))
  let client = @runtime.NetworkHaClient::new(
    option(args, "--cluster", "network-ha"),
    option(args, "--coordinator", "coordinator"),
    peers[:],
  )
  let state = @json.parse(option(args, "--state", "{}"))
  client.propose(state)
  println("proposed")
}

///|
async fn read_state(args : Array[String]) -> Unit {
  let peers = parse_peers(option(args, "--nodes", ""))
  let client = @runtime.NetworkHaClient::new(
    option(args, "--cluster", "network-ha"),
    option(args, "--coordinator", "coordinator"),
    peers[:],
  )
  match client.read() {
    Some(state) => println(state.stringify())
    None => println("null")
  }
}

///|
async fn main {
  let args = @env.args()
  match option(args, "--mode", "serve") {
    "serve" => serve(args)
    "propose" => propose(args)
    "read" => read_state(args)
    _ => raise @runtime.NetworkHaError::InvalidConfig
  }
}
