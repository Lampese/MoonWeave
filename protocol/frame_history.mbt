///|
/// TCP sessions retain a bounded retry window. Older sequence numbers still
/// fail the monotonic sequence check after their payload leaves this cache;
/// a peer reconnects and resumes durable offsets to retry older work.
fn remember_recent_frame(history : Map[String, Frame], frame : Frame) -> Unit {
  if history.length() >= 256 {
    for key in history.keys().to_array() {
      history.remove(key)
      break
    }
  }
  history[frame.message_id()] = frame
}
