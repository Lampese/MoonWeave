///|
test "job definitions compile to deterministic built-in plans" {
  let route = @plan.ExchangeRoute::new(0, 1)
  let first = JobDefinition::new("api-job", OperatorRef::reachability())
    .with_partition_count(2)
    .with_workers(["worker-b", "worker-a"])
    .with_routes([route])
    .with_sink(SinkSpec::durable("alerts"))
  let second = JobDefinition::new("api-job", OperatorRef::reachability())
    .with_partition_count(2)
    .with_workers(["worker-a", "worker-b"])
    .with_routes([route])
    .with_sink(SinkSpec::durable("alerts"))
  let compiled = first.compile()
  assert_eq(compiled.job_id(), "api-job")
  assert_eq(compiled.partition_count(), 2)
  assert_eq(compiled.operator(), OperatorRef::reachability())
  assert_eq(compiled.sink(), SinkSpec::Durable("alerts"))
  assert_true(compiled.plan_digest().has_prefix("plan-"))
  assert_eq(compiled.plan_digest(), second.compile().plan_digest())
}

///|
test "builtin operator registry exposes stable execution metadata" {
  let registry = OperatorRegistry::builtins()
  let descriptor = registry.resolve(OperatorRef::reachability())
  assert_eq(descriptor.code_digest(), "builtin:reachability:v1")
  assert_eq(descriptor.state_schema(), "reachability-state:v1")
  assert_true(descriptor.deterministic())
  assert_true(descriptor.supports_proof())
  assert_true(descriptor.capability().has_prefix("moonweave-operator:"))
  assert_eq(registry.capabilities().length(), 2)
  let isolated = OperatorRegistry::new()
  isolated.register(descriptor)
  assert_eq(isolated.entries().length(), 1)
  try isolated.register(descriptor) catch {
    ApiError::DuplicateOperator(key) =>
      assert_true(key.has_prefix("reachability#"))
    _ => fail("expected duplicate operator registration")
  } noraise {
    _ => fail("expected duplicate operator registration")
  }
  try registry.resolve(OperatorRef::new("reachability", 2)) catch {
    ApiError::UnsupportedOperatorVersion(id~, version~) => {
      assert_eq(id, "reachability")
      assert_eq(version, 2)
    }
    _ => fail("expected unknown builtin version")
  } noraise {
    _ => fail("expected unknown builtin version")
  }
}

///|
test "matching artifact metadata is accepted and mismatches are rejected" {
  let artifact = OperatorArtifact::new(
    OperatorRef::reachability(),
    "builtin:reachability:v1",
    "moonweave-task-json-v1",
    "native",
    "reachability-state:v1",
    "builtin-core-v1",
    signature="test-signature",
  )
  let compiled = JobDefinition::new("artifact-job", OperatorRef::reachability())
    .with_artifact(artifact)
    .compile()
  assert_eq(
    compiled.operator_descriptor().code_digest(),
    "builtin:reachability:v1",
  )
  let mismatch = OperatorArtifact::new(
    OperatorRef::reachability(),
    "artifact:other",
    "moonweave-task-json-v1",
    "native",
    "reachability-state:v1",
    "builtin-core-v1",
  )
  try
    JobDefinition::new("artifact-job", OperatorRef::reachability())
    .with_artifact(mismatch)
    .compile()
  catch {
    ApiError::ArtifactMismatch(id) => assert_eq(id, "reachability")
    _ => fail("expected artifact mismatch")
  } noraise {
    _ => fail("expected artifact mismatch")
  }
}

///|
test "operator descriptors validate payloads before transport" {
  let descriptor = OperatorRegistry::builtins().resolve(
    OperatorRef::reachability(),
  )
  let valid = Event::new(
    @causal.EventId::new("valid", 0),
    [],
    "key",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  descriptor.validate_event(valid)
  let invalid = Event::new(
    @causal.EventId::new("invalid", 0),
    [],
    "key",
    Json::object({ "kind": Json::string("not-an-edge") }),
  )
  try descriptor.validate_event(invalid) catch {
    ApiError::InvalidEvent(_) => ()
    _ => fail("expected operator input schema rejection")
  } noraise {
    _ => fail("expected operator input schema rejection")
  }
}

///|
test "cluster submissions round trip and reject plan drift" {
  let compiled = JobDefinition::new(
    "submission-job",
    OperatorRef::security_chain(),
  ).compile()
  let submission = compiled.submission()
  let signed_artifact = OperatorArtifact::new(
    OperatorRef::security_chain(),
    "builtin:security-chain:v1",
    "moonweave-task-json-v1",
    "native",
    "security-chain-state:v1",
    "builtin-core-v1",
    signature="signed-envelope",
  )
  let signed_submission = ClusterSubmission::{
    ..submission,
    operator: signed_artifact,
  }
  assert_eq(signed_submission.compile().plan_digest(), compiled.plan_digest())
  let encoded = @json.to_json(submission)
  let decoded : ClusterSubmission = @json.from_json(encoded)
  assert_eq(decoded.compile().plan_digest(), compiled.plan_digest())
  let changed = JobDefinition::new(
    "submission-job-changed",
    OperatorRef::security_chain(),
  )
  let drifted = ClusterSubmission::{ ..submission, definition: changed, }
  try drifted.compile() catch {
    ApiError::SubmissionMismatch(reason) => assert_eq(reason, "plan digest")
    _ => fail("expected submission plan mismatch")
  } noraise {
    _ => fail("expected submission plan mismatch")
  }
}

///|
test "job definitions reject invalid high-level configuration" {
  try OperatorRef::new("reachability", 0) catch {
    ApiError::InvalidOperatorVersion(version) => assert_eq(version, 0)
    _ => fail("expected invalid operator version")
  } noraise {
    _ => fail("expected invalid operator version")
  }
  try
    JobDefinition::new("api-job", OperatorRef::reachability()).with_workers([
      "worker", "worker",
    ])
  catch {
    ApiError::DuplicateWorker(worker) => assert_eq(worker, "worker")
    _ => fail("expected duplicate worker rejection")
  } noraise {
    _ => fail("expected duplicate worker rejection")
  }
  try SinkSpec::durable("") catch {
    ApiError::EmptySinkId => ()
    _ => fail("expected empty sink rejection")
  } noraise {
    _ => fail("expected empty sink rejection")
  }
}

///|
test "compiler rejects unsupported operator versions and identities" {
  let versioned = OperatorRef::new("reachability", 2)
  try JobDefinition::new("api-job", versioned).compile() catch {
    ApiError::UnsupportedOperatorVersion(id~, version~) => {
      assert_eq(id, "reachability")
      assert_eq(version, 2)
    }
    _ => fail("expected unsupported operator version")
  } noraise {
    _ => fail("expected unsupported operator version")
  }
  let unknown = OperatorRef::new("custom", 1)
  try JobDefinition::new("api-job", unknown).compile() catch {
    ApiError::UnsupportedOperator(id) => assert_eq(id, "custom")
    _ => fail("expected unsupported operator")
  } noraise {
    _ => fail("expected unsupported operator")
  }
}

///|
test "local runner executes the same causal Task semantics" {
  let root_id = @causal.EventId::new("root", 0)
  let child_id = @causal.EventId::new("child", 0)
  let root = Event::new(
    root_id,
    [],
    "user-1",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let child = Event::new(
    child_id,
    [root_id],
    "user-1",
    Json::object({ "source": Json::string("B"), "target": Json::string("C") }),
  )
  let run = JobDefinition::new("local-job", OperatorRef::reachability())
    .compile()
    .run_local()
  assert_eq(run.submit([child]), [])
  let outputs = run.submit([root])
  assert_true(outputs.length() > 0)
  assert_eq(run.progress().offset(), 1L)
  assert_eq(run.progresses().length(), 1)
  assert_eq(run.phase(), LocalRunPhase::Running)
  run.close()
  assert_eq(run.phase(), LocalRunPhase::Stopped)
}

///|
test "local runner supports deterministic multi-partition key routing" {
  let compiled = JobDefinition::new("local-job", OperatorRef::reachability())
    .with_partition_count(2)
    .compile()
  let run = compiled.run_local()
  let first = Event::new(
    @causal.EventId::new("first", 0),
    [],
    "key-a",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let second = Event::new(
    @causal.EventId::new("second", 0),
    [],
    "key-b",
    Json::object({ "source": Json::string("X"), "target": Json::string("Y") }),
  )
  ignore(run.submit([first, second]))
  assert_eq(run.progresses().length(), 2)
  run.close()
}

///|
test "local runner executes explicit exchange routes" {
  let route = @plan.ExchangeRoute::new(0, 1)
  let compiled = JobDefinition::new("local-job", OperatorRef::reachability())
    .with_partition_count(2)
    .with_routes([route])
    .compile()
  let run = compiled.run_local()
  let event = Event::new(
    @causal.EventId::new("exchange", 0),
    [],
    "source-key",
    Json::object({ "source": Json::string("A"), "target": Json::string("B") }),
  )
  let outputs = run.submit([event])
  assert_true(outputs.length() > 0)
  assert_eq(run.progresses().length(), 2)
  run.close()
}
