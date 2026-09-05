///|
/// Retry configuration for transient transport failures.
pub(all) suberror RetryError {
  InvalidAttempts(Int)
  InvalidBackoff(Int)
  InvalidBackoffLimit(Int)
} derive(Debug, Eq)

///|
pub struct RetryPolicy {
  max_attempts : Int
  initial_backoff_ms : Int
  max_backoff_ms : Int
}

///|
pub fn RetryPolicy::new(
  max_attempts : Int,
  initial_backoff_ms : Int,
  max_backoff_ms : Int,
) -> RetryPolicy raise RetryError {
  if max_attempts <= 0 {
    raise RetryError::InvalidAttempts(max_attempts)
  }
  if initial_backoff_ms < 0 {
    raise RetryError::InvalidBackoff(initial_backoff_ms)
  }
  if max_backoff_ms < initial_backoff_ms {
    raise RetryError::InvalidBackoffLimit(max_backoff_ms)
  }
  { max_attempts, initial_backoff_ms, max_backoff_ms, }
}

///|
pub fn RetryPolicy::max_attempts(self : RetryPolicy) -> Int {
  self.max_attempts
}

///|
pub fn RetryPolicy::initial_backoff_ms(self : RetryPolicy) -> Int {
  self.initial_backoff_ms
}

///|
pub fn RetryPolicy::max_backoff_ms(self : RetryPolicy) -> Int {
  self.max_backoff_ms
}

///|
pub fn RetryPolicy::backoff_ms(self : RetryPolicy, attempt : Int) -> Int {
  if attempt <= 1 || self.initial_backoff_ms == 0 {
    return self.initial_backoff_ms
  }
  let mut delay = self.initial_backoff_ms
  let mut index = 1
  while index < attempt && delay < self.max_backoff_ms {
    delay = Int::min(delay * 2, self.max_backoff_ms)
    index += 1
  }
  delay
}

///|
/// Retry only errors explicitly classified as transient transport I/O. A
/// protocol, WAL, checkpoint, or handler error is deterministic and fails
/// closed immediately.
pub async fn[X, Y] retry_transport(
  connector : async () -> X,
  operation : async (X) -> Y raise FrameIoError,
  policy : RetryPolicy,
) -> Y raise FrameIoError {
  let mut attempt = 1
  for ;; {
    let resource_result : Result[X, FrameIoError] = Ok(connector()) catch {
      error => Err(FrameIoError::Io(@debug.to_string(error)))
    }
    let result : Result[Y, FrameIoError] = match resource_result {
      Ok(resource) => Ok(operation(resource)) catch { error => Err(error) }
      Err(error) => Err(error)
    }
    match result {
      Ok(value) => return value
      Err(error) =>
        match error {
          FrameIoError::Io(_) => {
            if attempt >= policy.max_attempts() {
              raise error
            }
            let delay = policy.backoff_ms(attempt)
            if delay > 0 {
              @async.sleep(delay) catch {
                error => raise FrameIoError::Io(@debug.to_string(error))
              }
            }
            attempt += 1
          }
          _ => raise error
        }
    }
  }
}
