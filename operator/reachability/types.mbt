///|
pub(all) suberror ReachabilityError {
  EmptyNode
  MalformedPayload
} derive(Debug, Eq)

///|
/// One directed input edge.
pub struct Edge {
  source : String
  target : String
} derive(Debug, Eq, Compare, Hash)

///|
pub fn Edge::new(
  source : String,
  target : String,
) -> Edge raise ReachabilityError {
  if source.is_empty() || target.is_empty() {
    raise ReachabilityError::EmptyNode
  }
  { source, target, }
}

///|
pub fn Edge::source(self : Edge) -> String {
  self.source
}

///|
pub fn Edge::target(self : Edge) -> String {
  self.target
}

///|
/// JSON shape used by the first protocol workload. Keeping this adapter next
/// to the operator makes the wire format explicit without coupling the
/// protocol package to a particular operator.
pub(all) struct EdgePayload {
  source : String
  target : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn EdgePayload::new(source : String, target : String) -> EdgePayload {
  { source, target, }
}

///|
pub fn EdgePayload::source(self : EdgePayload) -> String {
  self.source
}

///|
pub fn EdgePayload::target(self : EdgePayload) -> String {
  self.target
}

///|
pub fn Edge::from_json(value : Json) -> Edge raise ReachabilityError {
  let payload : EdgePayload = @json.from_json(value) catch {
    _ => raise ReachabilityError::MalformedPayload
  }
  Edge::new(payload.source(), payload.target())
}

///|
/// A non-empty directed path derived from the input edge set.
pub struct Reachable {
  source : String
  target : String
} derive(Debug, Eq, Compare, Hash)

///|
pub fn Reachable::new(source : String, target : String) -> Reachable {
  { source, target, }
}

///|
pub fn Reachable::source(self : Reachable) -> String {
  self.source
}

///|
pub fn Reachable::target(self : Reachable) -> String {
  self.target
}
