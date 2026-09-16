///|
/// A completed event-time window. Bounds are milliseconds and describe the
/// half-open interval [start_ms, end_ms).
pub(all) struct WindowedSum {
  key : String
  start_ms : Int64
  end_ms : Int64
  total : Int64
} derive(Debug, Eq, Compare, ToJson, FromJson)

///|
pub fn WindowedSum::key(self : WindowedSum) -> String {
  self.key
}

///|
pub fn WindowedSum::start_ms(self : WindowedSum) -> Int64 {
  self.start_ms
}

///|
pub fn WindowedSum::end_ms(self : WindowedSum) -> Int64 {
  self.end_ms
}

///|
pub fn WindowedSum::total(self : WindowedSum) -> Int64 {
  self.total
}
