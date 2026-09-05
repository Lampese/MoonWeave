///|
/// Errors raised by a bounded asynchronous Exchange outbox.
pub(all) suberror OutboxError {
  InvalidCapacity(Int)
  Closed(String)
} derive(Debug, Eq)

///|
/// A bounded async queue for derived deltas. `push` applies backpressure when
/// the queue is full; it never silently drops a DeltaBatch.
pub struct ExchangeOutbox {
  queue : @async.Queue[@protocol.DeltaBatch]
  capacity : Int
  mut closed : Bool
}

///|
pub fn ExchangeOutbox::new(capacity : Int) -> ExchangeOutbox raise OutboxError {
  if capacity <= 0 {
    raise OutboxError::InvalidCapacity(capacity)
  }
  {
    queue: @aqueue.Queue(kind=@aqueue.Blocking(capacity)),
    capacity,
    closed: false,
  }
}

///|
pub fn ExchangeOutbox::capacity(self : ExchangeOutbox) -> Int {
  self.capacity
}

///|
pub fn ExchangeOutbox::is_closed(self : ExchangeOutbox) -> Bool {
  self.closed
}

///|
pub async fn ExchangeOutbox::push(
  self : ExchangeOutbox,
  delta : @protocol.DeltaBatch,
) -> Unit raise OutboxError {
  self.queue.put(delta) catch {
    error => raise OutboxError::Closed(@debug.to_string(error))
  }
}

///|
/// Enqueue a deterministic batch of routed deltas, applying backpressure to
/// each target stream in input order.
pub async fn ExchangeOutbox::push_all(
  self : ExchangeOutbox,
  deltas : ArrayView[@protocol.DeltaBatch],
) -> Unit raise OutboxError {
  for delta in deltas {
    self.push(delta)
  }
}

///|
pub fn ExchangeOutbox::try_push(
  self : ExchangeOutbox,
  delta : @protocol.DeltaBatch,
) -> Bool raise OutboxError {
  self.queue.try_put(delta) catch {
    error => raise OutboxError::Closed(@debug.to_string(error))
  }
}

///|
pub async fn ExchangeOutbox::pop(
  self : ExchangeOutbox,
) -> @protocol.DeltaBatch raise OutboxError {
  self.queue.get() catch {
    error => raise OutboxError::Closed(@debug.to_string(error))
  }
}

///|
/// Try to consume one delta without waiting for a producer.
pub fn ExchangeOutbox::try_pop(
  self : ExchangeOutbox,
) -> @protocol.DeltaBatch? raise OutboxError {
  self.queue.try_get() catch {
    error => raise OutboxError::Closed(@debug.to_string(error))
  }
}

///|
pub fn ExchangeOutbox::close(self : ExchangeOutbox) -> Unit {
  self.closed = true
  self.queue.close()
}
