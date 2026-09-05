///|
/// Incremental transitive closure for an append-only directed graph.
///
/// The state is monotone: edges and derived reachability facts are only added.
/// Applying the same edge or merging the same remote state is idempotent.
pub struct State {
  edges : Set[(String, String)]
  closure : Set[(String, String)]
}

///|
pub fn State::new() -> State {
  { edges: Set([]), closure: Set([]), }
}

///|
pub fn State::copy(self : State) -> State {
  { edges: self.edges.copy(), closure: self.closure.copy(), }
}

///|
pub fn State::edge_count(self : State) -> Int {
  self.edges.length()
}

///|
pub fn State::fact_count(self : State) -> Int {
  self.closure.length()
}

///|
pub fn State::is_reachable(
  self : State,
  source : String,
  target : String,
) -> Bool {
  self.closure.contains((source, target))
}

///|
pub fn State::edges(self : State) -> Array[Edge] {
  let result = self.edges
    .iter()
    .map(pair => Edge::{ source: pair.0, target: pair.1, })
    .to_array()
  result.sort()
  result
}

///|
pub fn State::facts(self : State) -> Array[Reachable] {
  let result = self.closure
    .iter()
    .map(pair => Reachable::new(pair.0, pair.1))
    .to_array()
  result.sort()
  result
}

///|
/// Add one edge and return only newly derived reachability facts.
pub fn State::apply(self : State, edge : Edge) -> Array[Reachable] {
  let input = (edge.source(), edge.target())
  if !self.edges.add_and_check(input) {
    return []
  }
  let predecessors : Set[String] = Set([edge.source()])
  let successors : Set[String] = Set([edge.target()])
  for pair in self.closure {
    let (source, target) = pair
    if target == edge.source() {
      predecessors.add(source)
    }
    if source == edge.target() {
      successors.add(target)
    }
  }
  let added = []
  for source in predecessors {
    for target in successors {
      if self.closure.add_and_check((source, target)) {
        added.push(Reachable::new(source, target))
      }
    }
  }
  added.sort()
  added
}

///|
/// Join another state by applying its grow-only edge set.
pub fn State::merge(self : State, other : State) -> Array[Reachable] {
  let added = []
  for edge in other.edges() {
    added.push_iter(self.apply(edge).iter())
  }
  added.sort()
  added
}
