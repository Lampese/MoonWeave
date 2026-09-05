///|
fn event_count(args : Array[String]) -> Int {
  let mut index = 0
  while index + 1 < args.length() {
    if args[index] == "--events" {
      return @strconv.from_str(args[index + 1]) catch { _ => 100 }
    }
    index += 1
  }
  100
}

///|
fn main raise {
  let metrics = @benchmarks.run_reachability_benchmark(event_count(@env.args()))
  println("events=\{metrics.events()}")
  println("candidate_step=\{metrics.candidate_step()}")
  println("stable_step=\{metrics.stable_step()}")
  println("barrier_rounds=\{metrics.barrier_rounds()}")
  println("barrier_messages=\{metrics.barrier_messages()}")
  println("targeted_probes=\{metrics.targeted_probes()}")
  println("targeted_messages=\{metrics.targeted_messages()}")
}
