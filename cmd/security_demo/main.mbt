///|
fn main raise {
  let engine = @security.Engine::new()
  let login_id = @causal.EventId::new("auth", 0)
  let admin_id = @causal.EventId::new("admin", 0)
  let export_id = @causal.EventId::new("storage", 0)
  let login_payload = Json::object({
    "user_id": Json::string("user-42"),
    "kind": Json::string("login_success"),
  })
  let admin_payload = Json::object({
    "user_id": Json::string("user-42"),
    "kind": Json::string("grant_admin"),
  })
  let export_payload = Json::object({
    "user_id": Json::string("user-42"),
    "kind": Json::string("bulk_export"),
  })
  ignore(
    engine.apply_json_event(export_id, [admin_id], "user-42", export_payload),
  )
  println(
    "after export pending=\{engine.pending_count()} alerts=\{engine.alerts().length()}",
  )
  ignore(
    engine.apply_json_event(admin_id, [login_id], "user-42", admin_payload),
  )
  println(
    "after admin pending=\{engine.pending_count()} alerts=\{engine.alerts().length()}",
  )
  let alerts = engine.apply_json_event(login_id, [], "user-42", login_payload)
  println(
    "after login pending=\{engine.pending_count()} alerts=\{@debug.to_string(alerts)}",
  )
}
