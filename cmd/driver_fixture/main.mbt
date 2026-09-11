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
async fn main {
  let args = @env.args()
  let events_path = option(args, "--events", "")
  if !events_path.is_empty() {
    let events_text = @fs.read_file(events_path).text()
    let events : Array[@flow.Event] = @json.from_json(@json.parse(events_text))
    println(@json.to_json(events).stringify())
    return
  }
  let job_id = option(args, "--job", "driver-job")
  let submission_id = option(args, "--submission", "driver-submission")
  let job = @flow.JobDefinition::new(job_id, @flow.OperatorRef::reachability())
    .with_workers(["worker-0"])
    .compile()
  let submission = job.submission_with_id(submission_id)
  println(@json.to_json(submission).stringify())
}
