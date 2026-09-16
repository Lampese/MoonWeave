///|
/// A transform's output carries the new key and preserves input time. The
/// graph transport supplies stable identities and causal parent witnesses.
struct GraphValue {
  key : String
  payload : Json
  event_time : Int64?
} derive(Debug, Eq, ToJson, FromJson)

///|
fn transform_descriptor(
  definition : JobDefinition,
) -> OperatorDescriptor raise ApiError {
  let operations = definition.transform.unwrap()
  for operation in operations {
    let field = match operation {
      SetField(name, _)
      | Multiply(name, _)
      | KeepEqual(name, _)
      | KeyBy(name) => name
    }
    if field.is_empty() {
      raise ApiError::InvalidEvent("transform field is empty")
    }
  }
  let identity = api_digest(@json.to_json(operations).stringify())
  if definition.operator.id() != "transform-" + identity {
    raise ApiError::ArtifactMismatch("transform configuration")
  }
  let implementation = @task.CustomOperator::new(
    definition.operator.id(),
    1,
    identity,
    "graph-transform:v1",
    "json:v1",
    "graph-value:v1",
    "moonweave-task:v1",
    "builtin:v1",
    true,
    false,
    payload => {
      if !(payload is Object(_)) {
        raise @task.TaskError::Custom("transform expects an object")
      }
    },
    input => {
      let event = input.event()
      guard event.payload() is Object(original) else {
        raise @task.TaskError::Custom("transform expects an object")
      }
      let fields = original.copy()
      let mut key = event.key()
      for operation in operations {
        match operation {
          SetField(field, value) => fields[field] = value
          KeepEqual(field, expected) =>
            if fields.get(field) != Some(expected) {
              return [
                @json.to_json(GraphValue::{
                  key,
                  payload: @task.graph_skip_payload(),
                  event_time: event.event_time(),
                }),
              ]
            }
          KeyBy(field) => {
            guard fields.get(field) is Some(String(value)) && !value.is_empty() else {
              raise @task.TaskError::Custom(
                "key field must be a non-empty string",
              )
            }
            key = value
          }
          Multiply(field, factor) => {
            guard fields.get(field) is Some(json) else {
              raise @task.TaskError::Custom("missing numeric field")
            }
            let value : Int64 = @json.from_json(json) catch {
              _ => raise @task.TaskError::Custom("invalid Int64 field")
            }
            if (value == -9223372036854775808L && factor == -1L) ||
              (factor == -9223372036854775808L && value == -1L) {
              raise @task.TaskError::Custom("transform integer overflow")
            }
            let product = value * factor
            if factor != 0L && product / factor != value {
              raise @task.TaskError::Custom("transform integer overflow")
            }
            fields[field] = @json.to_json(product)
          }
        }
      }
      [
        @json.to_json(GraphValue::{
          key,
          payload: Json::object(fields),
          event_time: event.event_time(),
        }),
      ]
    },
  ) catch {
    error => raise ApiError::TaskFailed(@debug.to_string(error))
  }
  let registry = OperatorRegistry::new()
  registry.register_custom(implementation)
  registry.resolve(definition.operator)
}
