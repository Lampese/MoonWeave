///|
/// Serializable operations executed by the statically compiled transform
/// operator. Numeric values remain Int64; overflow rejects the input.
pub(all) enum GraphTransform {
  SetField(String, Json)
  Multiply(String, Int64)
  KeepEqual(String, Json)
  KeyBy(String)
} derive(Debug, Eq, ToJson, FromJson)

///|
pub(all) struct GraphStage {
  id : String
  definition : JobDefinition
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn GraphStage::new(
  id : String,
  definition : JobDefinition,
) -> GraphStage raise ApiError {
  if id.is_empty() || id.contains("/") {
    raise ApiError::Cluster("invalid graph stage id")
  }
  { id, definition, }
}

///|
/// Every edge is a durable, independently acknowledged input. Distinct edges
/// preserve distinct identities at branch merges. `input_id` selects main,
/// left, or right at the destination operator.
pub(all) struct GraphEdge {
  from : String
  to : String
  input_id : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn GraphEdge::new(
  from : String,
  to : String,
  input_id? : String = "main",
) -> GraphEdge {
  { from, to, input_id, }
}

///|
pub(all) struct GraphDefinition {
  stages : Array[GraphStage]
  edges : Array[GraphEdge]
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn GraphDefinition::new(
  stages : ArrayView[GraphStage],
  edges : ArrayView[GraphEdge],
) -> GraphDefinition {
  { stages: stages.to_owned(), edges: edges.to_owned(), }
}

///|
/// Build a distributed graph with the regular submission and Driver APIs.
/// Stage ids identify manual source inputs; a single root also accepts main.
pub fn JobDefinition::dataflow(
  job_id : String,
  graph : GraphDefinition,
) -> JobDefinition raise ApiError {
  {
    ..JobDefinition::new(job_id, OperatorRef::keyed_sum()),
    graph: Some(graph),
  }
}

///|
pub fn JobDefinition::transform(
  job_id : String,
  operations : ArrayView[GraphTransform],
) -> JobDefinition raise ApiError {
  let operations = operations.to_owned()
  let reference = OperatorRef::new(
    "transform-" + api_digest(@json.to_json(operations).stringify()),
    1,
  )
  { ..JobDefinition::new(job_id, reference), transform: Some(operations), }
}

///|
pub fn CompiledJob::is_graph(self : CompiledJob) -> Bool {
  self.definition.graph is Some(_)
}

///|
fn GraphDefinition::ordered(
  self : GraphDefinition,
) -> Array[GraphStage] raise ApiError {
  if self.stages.is_empty() {
    raise ApiError::Cluster("graph has no stages")
  }
  let by_id : Map[String, GraphStage] = Map([])
  for stage in self.stages {
    ignore(GraphStage::new(stage.id, stage.definition))
    if stage.definition.graph is Some(_) || by_id.contains(stage.id) {
      raise ApiError::Cluster("nested or duplicate graph stage")
    }
    by_id[stage.id] = stage
  }
  let seen_edges : Set[String] = Set([])
  for edge in self.edges {
    if !by_id.contains(edge.from) ||
      !by_id.contains(edge.to) ||
      edge.input_id.is_empty() ||
      edge.input_id.contains("/") ||
      !seen_edges.add_and_check(edge.identity()) {
      raise ApiError::Cluster("invalid or duplicate graph edge")
    }
  }
  let remaining = self.stages.copy()
  remaining.sort_by((a, b) => a.id.compare(b.id))
  let ordered : Array[GraphStage] = []
  let done : Set[String] = Set([])
  while ordered.length() < remaining.length() {
    let before = ordered.length()
    for stage in remaining {
      if !done.contains(stage.id) &&
        self.edges.all(edge => edge.to != stage.id || done.contains(edge.from)) {
        ordered.push(stage)
        done.add(stage.id)
      }
    }
    if before == ordered.length() {
      raise ApiError::Cluster("graph contains a cycle")
    }
  }
  ordered
}

///|
fn GraphEdge::identity(self : GraphEdge) -> String {
  self.from + "/" + self.to + "/" + self.input_id
}

///|
fn JobDefinition::compile_graph(
  self : JobDefinition,
  registry : OperatorRegistry,
) -> JobDefinition raise ApiError {
  guard self.graph is Some(graph) else { return self }
  let ordered = graph.ordered()
  let timed = ordered.any(stage => stage.definition.event_time_config is Some(_))
  let stages = []
  let workers : Set[String] = Set([])
  for stage in ordered {
    let incoming = graph.edges.filter(edge => edge.to == stage.id)
    let outgoing = graph.edges.filter(edge => edge.from == stage.id)
    let routes = []
    for from in 0..<stage.definition.partition_count {
      for to in 0..<stage.definition.partition_count {
        if from != to {
          routes.push(
            @plan.ExchangeRoute::new(from, to) catch {
              error => raise ApiError::Plan(error)
            },
          )
        }
      }
    }
    if !incoming.is_empty() &&
      stage.definition.source != Events &&
      stage.definition.graph_input is None {
      raise ApiError::Source(
        "a graph stage uses either incoming edges or an external source",
      )
    }
    let inputs : Set[String] = Set([])
    for edge in incoming {
      inputs.add(edge.input_id)
    }
    let input_ids = inputs.to_array()
    input_ids.sort()
    let definition = {
      ..stage.definition,
      job_id: self.job_id + "/stage/" + stage.id,
      routes,
      graph_input: Some(@task.TaskGraphConfig::{
        partition_count: stage.definition.partition_count,
        emit_packets: !outgoing.is_empty(),
      }),
      graph_watermarks: if timed {
        Some(incoming.map(edge => edge.identity()))
      } else {
        None
      },
      source: if incoming.is_empty() {
        stage.definition.source
      } else {
        SourceSpec::Inputs(input_ids.map(id => NamedSource::new(id, Events)))
      },
      sink: if outgoing.is_empty() {
        stage.definition.sink
      } else {
        SinkSpec::Durable("graph-" + stage.id)
      },
    }
    let compiled = definition.compile(registry~)
    for worker in compiled.definition.workers {
      workers.add(worker)
    }
    stages.push({ id: stage.id, definition: compiled.definition, })
  }
  let all_workers = workers.to_array()
  all_workers.sort()
  let edges = graph.edges.copy()
  edges.sort_by((a, b) => a.identity().compare(b.identity()))
  { ..self, workers: all_workers, graph: Some({ stages, edges, }), }
}

///|
fn CompiledJob::graph_root(
  self : CompiledJob,
  input_id : String,
) -> GraphStage raise ApiError {
  let graph = self.definition.graph.unwrap()
  let roots = graph.stages.filter(stage => {
    !graph.edges.any(edge => edge.to == stage.id)
  })
  let root = if roots.length() == 1 && input_id == "main" {
    Some(roots[0])
  } else {
    roots.search_by(stage => stage.id == input_id).map(index => roots[index])
  }
  guard root is Some(stage) && stage.definition.source == Events else {
    raise ApiError::Source("Driver input must select a graph Events root")
  }
  stage
}
