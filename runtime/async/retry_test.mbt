///|
async test "transport retry uses bounded exponential backoff" {
  let policy = RetryPolicy::new(4, 1, 4)
  assert_eq(policy.backoff_ms(1), 1)
  assert_eq(policy.backoff_ms(2), 2)
  assert_eq(policy.backoff_ms(3), 4)
  assert_eq(policy.backoff_ms(4), 4)
  let mut attempts = 0
  let value = retry_transport(
    () => 0,
    _ => {
      attempts += 1
      if attempts < 3 {
        raise FrameIoError::Io("temporary")
      }
      42
    },
    policy,
  )
  assert_eq(value, 42)
  assert_eq(attempts, 3)
}

///|
async test "transport retry does not retry protocol failures" {
  let policy = RetryPolicy::new(4, 0, 0)
  let mut attempts = 0
  try {
    let _ : Unit = retry_transport(
      () => 0,
      _ => {
        attempts += 1
        raise FrameIoError::Handler("deterministic")
      },
      policy,
    )
  } catch {
    FrameIoError::Handler(message) => assert_eq(message, "deterministic")
    _ => fail("expected handler error")
  } noraise {
    _ => fail("expected handler error")
  }
  assert_eq(attempts, 1)
}

///|
async test "transport retry also retries connector failures" {
  let policy = RetryPolicy::new(3, 0, 0)
  let mut connects = 0
  let value = retry_transport(
    () => {
      connects += 1
      if connects < 3 {
        raise FrameIoError::Io("connect")
      }
      7
    },
    _ => 7,
    policy,
  )
  assert_eq(value, 7)
  assert_eq(connects, 3)
}
