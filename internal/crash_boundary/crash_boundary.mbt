///|
let selected_point : String = @env.get_env_var("MOONWEAVE_CRASH_POINT").unwrap_or(
  "",
)

///|
let gate_directory : String = @env.get_env_var("MOONWEAVE_CRASH_GATE").unwrap_or(
  "",
)

///|
/// An opt-in acceptance barrier. The harness owns gate.lock and kills the
/// process after reached.json is durable. Normal processes do no filesystem IO.
pub async fn hit(point : String, context : String) -> Unit {
  if selected_point != point || gate_directory.is_empty() {
    return
  }
  if !@fs.exists(gate_directory + "/armed") {
    return
  }
  let gate = @fs.open(gate_directory + "/gate.lock", mode=@fs.ReadWrite)
  defer gate.close()
  let marker : Json = { "point": point, "context": context }
  @fs.write_file(
    gate_directory + "/reached.json.tmp",
    marker.stringify(),
    sync=@fs.Full,
  )
  @fs.rename(
    gate_directory + "/reached.json.tmp",
    gate_directory + "/reached.json",
    replace=true,
  )
  gate.lock(@fs.Shared)
}
