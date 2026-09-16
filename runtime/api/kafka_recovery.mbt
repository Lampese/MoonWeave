///|
/// Only transport failures, broker-declared transient errors, and membership
/// changes may restart a connector. Invalid data and authorization failures
/// remain visible; they must never turn into an infinite silent retry.
fn kafka_retryable(error : Error) -> Bool {
  match error {
    ApiError::KafkaUnavailable(_) => true
    @moonkafka.TransportError::ConnectionClosed(_)
    | @moonkafka.TransportError::RequestTimeout(_) => true
    @moonkafka.BrokerError::BrokerError(code, _) =>
      @moonkafka.error_retriable(code) ||
      code == 22 ||
      code == 25 ||
      code == 27 ||
      code == 110 ||
      code == 113
    _ => false
  }
}

///|
async fn SourceBackedExecution::run_kafka_source(
  self : SourceBackedExecution,
  config : KafkaSourceConfig,
  input_id : String?,
) -> Unit raise ApiError {
  defer {
    self.active_source_pumps -= 1
  }
  let mut delay = 100
  while !self.kafka_stop_requested && self.phase() == Running {
    // Each connection owns a short-lived task group. Reconnecting joins
    // the old membership loop and releases all sockets before replacement.
    try
      @async_runtime.with_task_group <| group => {
        let source = KafkaSource::connect(group~, config)
        self.kafka_sources.push(source)
        defer {
          source.close()
          if self.kafka_sources.search_by(item => item.topic() == source.topic())
            is Some(index) {
            ignore(self.kafka_sources.remove(index))
          }
        }
        delay = 100
        self.pump_kafka_source(source, input_id)
      }
    catch {
      error => {
        if !kafka_retryable(error) {
          raise source_error("Kafka source runtime", error)
        }
        if self.kafka_stop_requested || self.phase() != Running {
          break
        }
        @async_runtime.sleep(delay) catch {
          error => raise source_error("Kafka reconnect", error)
        }
        delay = Int::min(delay * 2, 2000)
      }
    }
  }
}
