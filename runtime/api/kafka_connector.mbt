///|
/// Serializable source offset policy. Runtime consumer handles are kept out
/// of JobDefinition and its submission/WAL representation.
pub(all) enum KafkaStartFrom {
  Earliest
  Latest
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Serializable Kafka transport security selection. Credentials and TLS
/// handles are injected through `KafkaRuntimeConfig` at execution time.
pub(all) enum KafkaSecurityProtocol {
  Plaintext
  Ssl
  SaslPlaintext
  SaslSsl
} derive(Debug, Eq, ToJson, FromJson)

///|
/// Serializable configuration for one Kafka source partition. A source owns one explicit
/// partition because the Kafka package's public record type does not expose a
/// partition number; keeping that identity in the source preserves stable
/// causal event IDs.
pub(all) struct KafkaSourceSpec {
  bootstrap_servers : Array[String]
  topic : String
  group_id : String
  partition : Int
  start_from : KafkaStartFrom
  security_protocol : KafkaSecurityProtocol
  credential_ref : String?
  max_poll_records : Int
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn KafkaSourceSpec::new(
  bootstrap_servers : Array[String],
  topic : String,
  group_id : String,
  partition : Int,
) -> KafkaSourceSpec raise ApiError {
  if bootstrap_servers.is_empty() {
    raise ApiError::Source("Kafka bootstrap servers are required")
  }
  for server in bootstrap_servers {
    if server.is_empty() {
      raise ApiError::Source("Kafka bootstrap server cannot be empty")
    }
  }
  if topic.is_empty() {
    raise ApiError::Source("Kafka topic is required")
  }
  if group_id.is_empty() {
    raise ApiError::Source("Kafka group id is required")
  }
  if partition < 0 {
    raise ApiError::Source("Kafka partition must be non-negative")
  }
  {
    bootstrap_servers,
    topic,
    group_id,
    partition,
    start_from: KafkaStartFrom::Earliest,
    security_protocol: KafkaSecurityProtocol::Plaintext,
    credential_ref: None,
    max_poll_records: 128,
  }
}

///|
pub fn KafkaSourceSpec::with_start_from(
  self : KafkaSourceSpec,
  start_from : KafkaStartFrom,
) -> KafkaSourceSpec {
  { ..self, start_from, }
}

///|
pub fn KafkaSourceSpec::with_security_protocol(
  self : KafkaSourceSpec,
  security_protocol : KafkaSecurityProtocol,
) -> KafkaSourceSpec {
  { ..self, security_protocol, }
}

///|
pub fn KafkaSourceSpec::with_credential_ref(
  self : KafkaSourceSpec,
  credential_ref : String,
) -> KafkaSourceSpec raise ApiError {
  if credential_ref.is_empty() {
    raise ApiError::Source("Kafka credential reference cannot be empty")
  }
  { ..self, credential_ref: Some(credential_ref), }
}

///|
pub fn KafkaSourceSpec::with_max_poll_records(
  self : KafkaSourceSpec,
  maximum : Int,
) -> KafkaSourceSpec raise ApiError {
  if maximum <= 0 {
    raise ApiError::Source("Kafka max poll records must be positive")
  }
  { ..self, max_poll_records: maximum, }
}

///|
pub fn KafkaSourceSpec::topic(self : KafkaSourceSpec) -> String {
  self.topic
}

///|
pub fn KafkaSourceSpec::group_id(self : KafkaSourceSpec) -> String {
  self.group_id
}

///|
pub fn KafkaSourceSpec::partition(self : KafkaSourceSpec) -> Int {
  self.partition
}

///|
pub fn KafkaSourceSpec::bootstrap_servers(
  self : KafkaSourceSpec,
) -> ArrayView[String] {
  self.bootstrap_servers[:]
}

///|
pub fn KafkaSourceSpec::security_protocol(
  self : KafkaSourceSpec,
) -> KafkaSecurityProtocol {
  self.security_protocol
}

///|
pub fn KafkaSourceSpec::credential_ref(self : KafkaSourceSpec) -> String? {
  self.credential_ref
}

///|
pub(all) struct KafkaSinkSpec {
  bootstrap_servers : Array[String]
  topic : String
  security_protocol : KafkaSecurityProtocol
  credential_ref : String?
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn KafkaSinkSpec::new(
  bootstrap_servers : Array[String],
  topic : String,
) -> KafkaSinkSpec raise ApiError {
  if bootstrap_servers.is_empty() {
    raise ApiError::Sink("Kafka bootstrap servers are required")
  }
  for server in bootstrap_servers {
    if server.is_empty() {
      raise ApiError::Sink("Kafka bootstrap server cannot be empty")
    }
  }
  if topic.is_empty() {
    raise ApiError::Sink("Kafka topic is required")
  }
  {
    bootstrap_servers,
    topic,
    security_protocol: KafkaSecurityProtocol::Plaintext,
    credential_ref: None,
  }
}

///|
pub fn KafkaSinkSpec::with_security_protocol(
  self : KafkaSinkSpec,
  security_protocol : KafkaSecurityProtocol,
) -> KafkaSinkSpec {
  { ..self, security_protocol, }
}

///|
pub fn KafkaSinkSpec::with_credential_ref(
  self : KafkaSinkSpec,
  credential_ref : String,
) -> KafkaSinkSpec raise ApiError {
  if credential_ref.is_empty() {
    raise ApiError::Sink("Kafka credential reference cannot be empty")
  }
  { ..self, credential_ref: Some(credential_ref), }
}

///|
pub fn KafkaSinkSpec::topic(self : KafkaSinkSpec) -> String {
  self.topic
}

///|
pub fn KafkaSinkSpec::bootstrap_servers(
  self : KafkaSinkSpec,
) -> ArrayView[String] {
  self.bootstrap_servers[:]
}

///|
pub fn KafkaSinkSpec::security_protocol(
  self : KafkaSinkSpec,
) -> KafkaSecurityProtocol {
  self.security_protocol
}

///|
pub fn KafkaSinkSpec::credential_ref(self : KafkaSinkSpec) -> String? {
  self.credential_ref
}

///|
/// Runtime-only security material. This type is intentionally not part of a
/// serializable JobDefinition, submission envelope, checkpoint, or WAL.
pub struct KafkaRuntimeConfig {
  source_credential_ref : String?
  source_sasl : @moonkafka.SaslConfig?
  source_tls : @moonkafka.TlsClientOptions?
  sink_credential_ref : String?
  sink_sasl : @moonkafka.SaslConfig?
  sink_tls : @moonkafka.TlsClientOptions?
}

///|
pub fn KafkaRuntimeConfig::new() -> KafkaRuntimeConfig {
  {
    source_credential_ref: None,
    source_sasl: None,
    source_tls: None,
    sink_credential_ref: None,
    sink_sasl: None,
    sink_tls: None,
  }
}

///|
/// Inject source-side SASL/TLS handles under a non-secret reference name.
pub fn KafkaRuntimeConfig::with_source_security(
  self : KafkaRuntimeConfig,
  credential_ref : String,
  sasl : @moonkafka.SaslConfig?,
  tls : @moonkafka.TlsClientOptions?,
) -> KafkaRuntimeConfig raise ApiError {
  if credential_ref.is_empty() {
    raise ApiError::Source("Kafka source credential reference is required")
  }
  {
    ..self,
    source_credential_ref: Some(credential_ref),
    source_sasl: sasl,
    source_tls: tls,
  }
}

///|
/// Inject sink-side SASL/TLS handles under a non-secret reference name.
pub fn KafkaRuntimeConfig::with_sink_security(
  self : KafkaRuntimeConfig,
  credential_ref : String,
  sasl : @moonkafka.SaslConfig?,
  tls : @moonkafka.TlsClientOptions?,
) -> KafkaRuntimeConfig raise ApiError {
  if credential_ref.is_empty() {
    raise ApiError::Sink("Kafka sink credential reference is required")
  }
  {
    ..self,
    sink_credential_ref: Some(credential_ref),
    sink_sasl: sasl,
    sink_tls: tls,
  }
}

///|
/// Runtime source configuration retained for callers that build a connector
/// directly. Job-level code should use `KafkaSourceSpec` instead.
pub struct KafkaSourceConfig {
  bootstrap_servers : Array[String]
  topic : String
  group_id : String
  partition : Int
  start_from : @moonkafka.StartFrom
  security_protocol : @moonkafka.SecurityProtocol
  sasl : @moonkafka.SaslConfig?
  tls : @moonkafka.TlsClientOptions?
  max_poll_records : Int
}

///|
pub fn KafkaSourceConfig::new(
  bootstrap_servers : Array[String],
  topic : String,
  group_id : String,
  partition : Int,
) -> KafkaSourceConfig raise ApiError {
  if bootstrap_servers.is_empty() {
    raise ApiError::Source("Kafka bootstrap servers are required")
  }
  for server in bootstrap_servers {
    if server.is_empty() {
      raise ApiError::Source("Kafka bootstrap server cannot be empty")
    }
  }
  if topic.is_empty() {
    raise ApiError::Source("Kafka topic is required")
  }
  if group_id.is_empty() {
    raise ApiError::Source("Kafka group id is required")
  }
  if partition < 0 {
    raise ApiError::Source("Kafka partition must be non-negative")
  }
  {
    bootstrap_servers,
    topic,
    group_id,
    partition,
    start_from: @moonkafka.StartFrom::Earliest,
    security_protocol: @moonkafka.SecurityProtocol::Plaintext,
    sasl: None,
    tls: None,
    max_poll_records: 128,
  }
}

///|
pub fn KafkaSourceConfig::with_start_from(
  self : KafkaSourceConfig,
  start_from : @moonkafka.StartFrom,
) -> KafkaSourceConfig {
  { ..self, start_from, }
}

///|
pub fn KafkaSourceConfig::with_security(
  self : KafkaSourceConfig,
  security_protocol : @moonkafka.SecurityProtocol,
  sasl? : @moonkafka.SaslConfig? = None,
  tls? : @moonkafka.TlsClientOptions? = None,
) -> KafkaSourceConfig {
  { ..self, security_protocol, sasl, tls, }
}

///|
pub fn KafkaSourceConfig::with_max_poll_records(
  self : KafkaSourceConfig,
  maximum : Int,
) -> KafkaSourceConfig raise ApiError {
  if maximum <= 0 {
    raise ApiError::Source("Kafka max poll records must be positive")
  }
  { ..self, max_poll_records: maximum, }
}

///|
pub fn KafkaSourceConfig::topic(self : KafkaSourceConfig) -> String {
  self.topic
}

///|
pub fn KafkaSourceConfig::group_id(self : KafkaSourceConfig) -> String {
  self.group_id
}

///|
pub fn KafkaSourceConfig::partition(self : KafkaSourceConfig) -> Int {
  self.partition
}

///|
pub fn KafkaSourceConfig::bootstrap_servers(
  self : KafkaSourceConfig,
) -> ArrayView[String] {
  self.bootstrap_servers[:]
}

///|
pub fn KafkaSourceConfig::security_protocol(
  self : KafkaSourceConfig,
) -> @moonkafka.SecurityProtocol {
  self.security_protocol
}

///|
pub fn KafkaSourceConfig::sasl(
  self : KafkaSourceConfig,
) -> @moonkafka.SaslConfig? {
  self.sasl
}

///|
pub fn KafkaSourceConfig::tls(
  self : KafkaSourceConfig,
) -> @moonkafka.TlsClientOptions? {
  self.tls
}

///|
pub(all) struct KafkaSourceBatch {
  events : Array[Event]
  next_offset : Int64
} derive(Debug, Eq)

///|
pub fn KafkaSourceBatch::events(self : KafkaSourceBatch) -> ArrayView[Event] {
  self.events[:]
}

///|
pub fn KafkaSourceBatch::next_offset(self : KafkaSourceBatch) -> Int64 {
  self.next_offset
}

///|
/// A Kafka source with explicit offset commit semantics. Polling advances the
/// client position in memory; `commit` must be called only after the batch has
/// been durably processed.
pub struct KafkaSource {
  consumer : @moonkafka.Consumer
  topic : String
  partition : Int
  mut committed_offset : Int64
  mut pending_next_offset : Int64?
  mut closed : Bool
}

///|
fn kafka_source_error(stage : String, error : Error) -> ApiError {
  let detail = match error {
    @moonkafka.ProtocolError::ProtocolError(message) => message
    _ => @debug.to_string(error)
  }
  ApiError::Source("Kafka " + stage + ": " + detail)
}

///|
pub async fn KafkaSource::connect(
  group~ : @async_runtime.TaskGroup[Unit],
  config : KafkaSourceConfig,
) -> KafkaSource raise ApiError {
  let consumer_config = @moonkafka.ConsumerConfig::new(
    config.bootstrap_servers,
    config.topic,
    start_from=config.start_from,
    security_protocol=config.security_protocol,
    sasl=config.sasl,
    tls=config.tls,
    group_id=Some(config.group_id),
    enable_auto_commit=false,
    auto_offset_reset=Some(@moonkafka.AutoOffsetReset::ResetNone),
    max_poll_records=config.max_poll_records,
  ) catch {
    error => raise kafka_source_error("configure", error)
  }
  let consumer = @moonkafka.Consumer::connect_with_config(
    group~,
    consumer_config,
  ) catch {
    error => raise kafka_source_error("connect", error)
  }
  if consumer.position(config.partition) is None {
    consumer.close()
    raise ApiError::Source("Kafka partition does not exist in the topic")
  }
  consumer.assign([(config.topic, config.partition)]) catch {
    error => {
      consumer.close()
      raise kafka_source_error("assign", error)
    }
  }
  // `assign` intentionally treats committed-offset lookup as best effort in
  // moonkafka. Verify it here so a coordinator/protocol failure cannot
  // silently reset a production source to its start policy.
  let committed = consumer.committed(partitions=Some([config.partition])) catch {
    error => {
      consumer.close()
      raise kafka_source_error("read committed offset", error)
    }
  }
  let committed_offset = match committed.get(config.partition) {
    Some(offset) if offset >= 0L => {
      consumer.seek(config.partition, offset) catch {
        error => {
          consumer.close()
          raise kafka_source_error("seek committed offset", error)
        }
      }
      offset
    }
    Some(-1L) => consumer.position(config.partition).unwrap()
    _ => {
      consumer.close()
      raise ApiError::Source("Kafka committed offset response is incomplete")
    }
  }
  {
    consumer,
    topic: config.topic,
    partition: config.partition,
    committed_offset,
    pending_next_offset: None,
    closed: false,
  }
}

///|
pub fn KafkaSource::topic(self : KafkaSource) -> String {
  self.topic
}

///|
pub fn KafkaSource::partition(self : KafkaSource) -> Int {
  self.partition
}

///|
pub fn KafkaSource::position(self : KafkaSource) -> Int64? {
  self.consumer.position(self.partition)
}

///|
/// Decode one Kafka value containing the JSON form of a MoonWeave `Event`.
/// The Kafka topic/partition/offset becomes the new stable EventId origin and
/// sequence; the payload event supplies parents, key, and application data.
pub fn KafkaSource::event_from_record(
  self : KafkaSource,
  record : @moonkafka.Record,
) -> Event raise ApiError {
  kafka_record_to_event(self.topic, self.partition, record)
}

///|
fn kafka_record_to_event(
  topic : String,
  partition : Int,
  record : @moonkafka.Record,
) -> Event raise ApiError {
  guard record.value is Some(bytes) else {
    raise ApiError::Source("Kafka record has no value")
  }
  let value = @utf8.decode(bytes) catch {
    _ => raise ApiError::Source("Kafka record value is not valid UTF-8")
  }
  let json = @json.parse(value) catch {
    error =>
      raise ApiError::Source(
        "Kafka record value is not JSON: " + @debug.to_string(error),
      )
  }
  let input : Event = @json.from_json(json) catch {
    error =>
      raise ApiError::Source(
        "Kafka record value is not a MoonWeave Event: " +
        @debug.to_string(error),
      )
  }
  let offset = record.offset
  if offset < 0L || offset > 2147483647L {
    raise ApiError::Source(
      "Kafka record offset is outside the supported EventId range",
    )
  }
  let id = @causal.EventId::new(
    "kafka/" + topic + "/" + partition.to_string(),
    offset.to_int(),
  ) catch {
    error => raise ApiError::Source(@debug.to_string(error))
  }
  Event::new(id, input.parents(), input.key(), input.payload())
}

///|
pub async fn KafkaSource::poll(
  self : KafkaSource,
  max_wait_ms? : Int = 1000,
) -> KafkaSourceBatch raise ApiError {
  if self.closed {
    raise ApiError::Closed
  }
  // A failed poll or decode may already have advanced the client position.
  // Always retry from the last acknowledged offset and make a batch eligible
  // for commit only after every record decoded successfully.
  self.pending_next_offset = None
  self.consumer.seek(self.partition, self.committed_offset) catch {
    error => raise kafka_source_error("rewind", error)
  }
  let records = self.consumer.poll(max_wait_ms~) catch {
    error => raise kafka_source_error("poll", error)
  }
  let next_offset = match self.consumer.position(self.partition) {
    Some(offset) => offset
    None => self.committed_offset
  }
  let events : Array[Event] = []
  for record in records {
    events.push(self.event_from_record(record))
  }
  self.pending_next_offset = Some(next_offset)
  { events, next_offset, }
}

///|
/// Commit the next Kafka offset after successful processing of a polled batch.
pub async fn KafkaSource::commit(
  self : KafkaSource,
  next_offset : Int64,
) -> Unit raise ApiError {
  if self.closed {
    raise ApiError::Closed
  }
  if next_offset < 0L {
    raise ApiError::Source("Kafka commit offset must be non-negative")
  }
  match self.pending_next_offset {
    Some(expected) if expected != next_offset =>
      raise ApiError::Source(
        "Kafka commit offset does not match the pending poll",
      )
    None => raise ApiError::Source("Kafka commit requires a pending poll")
    Some(_) => ()
  }
  if next_offset < self.committed_offset {
    raise ApiError::Source("Kafka commit offset regressed")
  }
  let offsets : Map[Int, Int64] = Map([])
  offsets[self.partition] = next_offset
  self.consumer.commit(offsets~) catch {
    error => raise kafka_source_error("commit", error)
  }
  self.committed_offset = next_offset
  self.pending_next_offset = None
}

///|
pub fn KafkaSource::close(self : KafkaSource) -> Unit {
  if !self.closed {
    self.closed = true
    self.consumer.close()
  }
}

///|
/// Configuration for an idempotent Kafka output producer.
pub struct KafkaSinkConfig {
  bootstrap_servers : Array[String]
  topic : String
  security_protocol : @moonkafka.SecurityProtocol
  sasl : @moonkafka.SaslConfig?
  tls : @moonkafka.TlsClientOptions?
}

///|
pub fn KafkaSinkConfig::new(
  bootstrap_servers : Array[String],
  topic : String,
) -> KafkaSinkConfig raise ApiError {
  if bootstrap_servers.is_empty() {
    raise ApiError::Sink("Kafka bootstrap servers are required")
  }
  for server in bootstrap_servers {
    if server.is_empty() {
      raise ApiError::Sink("Kafka bootstrap server cannot be empty")
    }
  }
  if topic.is_empty() {
    raise ApiError::Sink("Kafka topic is required")
  }
  {
    bootstrap_servers,
    topic,
    security_protocol: @moonkafka.SecurityProtocol::Plaintext,
    sasl: None,
    tls: None,
  }
}

///|
pub fn KafkaSinkConfig::with_security(
  self : KafkaSinkConfig,
  security_protocol : @moonkafka.SecurityProtocol,
  sasl? : @moonkafka.SaslConfig? = None,
  tls? : @moonkafka.TlsClientOptions? = None,
) -> KafkaSinkConfig {
  { ..self, security_protocol, sasl, tls, }
}

///|
pub fn KafkaSinkConfig::topic(self : KafkaSinkConfig) -> String {
  self.topic
}

///|
pub fn KafkaSinkConfig::bootstrap_servers(
  self : KafkaSinkConfig,
) -> ArrayView[String] {
  self.bootstrap_servers[:]
}

///|
pub fn KafkaSinkConfig::security_protocol(
  self : KafkaSinkConfig,
) -> @moonkafka.SecurityProtocol {
  self.security_protocol
}

///|
pub fn KafkaSinkConfig::sasl(self : KafkaSinkConfig) -> @moonkafka.SaslConfig? {
  self.sasl
}

///|
pub fn KafkaSinkConfig::tls(
  self : KafkaSinkConfig,
) -> @moonkafka.TlsClientOptions? {
  self.tls
}

///|
pub struct KafkaSink {
  producer : @moonkafka.Producer
  topic : String
  mut closed : Bool
}

///|
fn kafka_sink_error(stage : String, error : Error) -> ApiError {
  let detail = match error {
    @moonkafka.ProtocolError::ProtocolError(message) => message
    _ => @debug.to_string(error)
  }
  ApiError::Sink("Kafka " + stage + ": " + detail)
}

///|
pub async fn KafkaSink::connect(
  group~ : @async_runtime.TaskGroup[Unit],
  config : KafkaSinkConfig,
) -> KafkaSink raise ApiError {
  let producer_config = @moonkafka.ProducerConfig::new(
    config.bootstrap_servers,
    config.topic,
    acks=-1,
    security_protocol=config.security_protocol,
    sasl=config.sasl,
    tls=config.tls,
    enable_idempotence=Some(true),
  ) catch {
    error => raise kafka_sink_error("configure", error)
  }
  let producer = @moonkafka.Producer::connect_with_config(
    group~,
    producer_config,
  ) catch {
    error => raise kafka_sink_error("connect", error)
  }
  { producer, topic: config.topic, closed: false, }
}

///|
pub fn KafkaSink::topic(self : KafkaSink) -> String {
  self.topic
}

///|
/// Send one output record using its stable idempotency key as the Kafka key.
/// The producer is idempotent, but delivery remains at-least-once across a
/// source/producer boundary because no cross-system transaction is claimed.
pub async fn KafkaSink::write(
  self : KafkaSink,
  record : @runtime.TaskOutputRecord,
) -> Unit raise ApiError {
  if self.closed {
    raise ApiError::Closed
  }
  let key = Some(@utf8.encode(record.idempotency_key()))
  let value = Some(@utf8.encode(@json.to_json(record).stringify()))
  let offset = self.producer.send(key?, value?) catch {
    error => raise kafka_sink_error("send", error)
  }
  if offset < 0L {
    raise ApiError::Sink("Kafka producer did not acknowledge the record")
  }
}

///|
/// Drain and ACK a durable output consumer only after Kafka acknowledges each
/// record. A repeated send after a crash is safe for consumers that use the
/// stable Kafka key/idempotency key.
pub async fn KafkaSink::consume(
  self : KafkaSink,
  consumer : @runtime.TaskOutputConsumer,
  maximum_records? : Int = 128,
) -> Int raise ApiError {
  consumer.consume(record => self.write(record), maximum_records~) catch {
    error => raise kafka_sink_error("consume", error)
  }
}

///|
pub fn KafkaSink::close(self : KafkaSink) -> Unit {
  if !self.closed {
    self.closed = true
    self.producer.close()
  }
}

///|
fn kafka_security_protocol(
  protocol : KafkaSecurityProtocol,
) -> @moonkafka.SecurityProtocol {
  match protocol {
    KafkaSecurityProtocol::Plaintext => @moonkafka.SecurityProtocol::Plaintext
    KafkaSecurityProtocol::Ssl => @moonkafka.SecurityProtocol::Ssl
    KafkaSecurityProtocol::SaslPlaintext =>
      @moonkafka.SecurityProtocol::SaslPlaintext
    KafkaSecurityProtocol::SaslSsl => @moonkafka.SecurityProtocol::SaslSsl
  }
}

///|
fn kafka_start_from(start_from : KafkaStartFrom) -> @moonkafka.StartFrom {
  match start_from {
    KafkaStartFrom::Earliest => @moonkafka.StartFrom::Earliest
    KafkaStartFrom::Latest => @moonkafka.StartFrom::Latest
  }
}

///|
fn validate_kafka_runtime_security(
  protocol : KafkaSecurityProtocol,
  sasl : @moonkafka.SaslConfig?,
  tls : @moonkafka.TlsClientOptions?,
  source : Bool,
) -> Unit raise ApiError {
  match protocol {
    KafkaSecurityProtocol::Plaintext => ()
    KafkaSecurityProtocol::Ssl =>
      if tls is None {
        raise ApiError::Cluster(
          if source {
            "Kafka source TLS options are required for SSL"
          } else {
            "Kafka sink TLS options are required for SSL"
          },
        )
      }
    KafkaSecurityProtocol::SaslPlaintext =>
      if sasl is None {
        raise ApiError::Cluster(
          if source {
            "Kafka source SASL credentials are required"
          } else {
            "Kafka sink SASL credentials are required"
          },
        )
      }
    KafkaSecurityProtocol::SaslSsl =>
      if sasl is None || tls is None {
        raise ApiError::Cluster(
          if source {
            "Kafka source SASL credentials and TLS options are required"
          } else {
            "Kafka sink SASL credentials and TLS options are required"
          },
        )
      }
  }
}

///|
fn check_kafka_credential_ref(
  expected : String?,
  actual : String?,
  source : Bool,
) -> Unit raise ApiError {
  match expected {
    None => ()
    Some(value) if actual != Some(value) =>
      raise ApiError::Cluster(
        if source {
          "Kafka source credential reference is not available at runtime"
        } else {
          "Kafka sink credential reference is not available at runtime"
        },
      )
    Some(_) => ()
  }
}

///|
fn KafkaSourceSpec::validate(self : KafkaSourceSpec) -> Unit raise ApiError {
  if self.bootstrap_servers.is_empty() {
    raise ApiError::Source("Kafka bootstrap servers are required")
  }
  for server in self.bootstrap_servers {
    if server.is_empty() {
      raise ApiError::Source("Kafka bootstrap server cannot be empty")
    }
  }
  if self.topic.is_empty() || self.group_id.is_empty() {
    raise ApiError::Source("Kafka topic and group id are required")
  }
  if self.partition < 0 {
    raise ApiError::Source("Kafka partition must be non-negative")
  }
  if self.max_poll_records <= 0 {
    raise ApiError::Source("Kafka max poll records must be positive")
  }
  match self.credential_ref {
    Some(value) if value.is_empty() =>
      raise ApiError::Source("Kafka credential reference cannot be empty")
    _ => ()
  }
}

///|
fn KafkaSinkSpec::validate(self : KafkaSinkSpec) -> Unit raise ApiError {
  if self.bootstrap_servers.is_empty() {
    raise ApiError::Sink("Kafka bootstrap servers are required")
  }
  for server in self.bootstrap_servers {
    if server.is_empty() {
      raise ApiError::Sink("Kafka bootstrap server cannot be empty")
    }
  }
  if self.topic.is_empty() {
    raise ApiError::Sink("Kafka topic is required")
  }
  match self.credential_ref {
    Some(value) if value.is_empty() =>
      raise ApiError::Sink("Kafka credential reference cannot be empty")
    _ => ()
  }
}

///|
fn KafkaRuntimeConfig::source_config(
  self : KafkaRuntimeConfig,
  spec : KafkaSourceSpec,
) -> KafkaSourceConfig raise ApiError {
  check_kafka_credential_ref(
    spec.credential_ref,
    self.source_credential_ref,
    true,
  )
  validate_kafka_runtime_security(
    spec.security_protocol,
    self.source_sasl,
    self.source_tls,
    true,
  )
  KafkaSourceConfig::new(
    spec.bootstrap_servers.copy(),
    spec.topic,
    spec.group_id,
    spec.partition,
  )
  .with_start_from(kafka_start_from(spec.start_from))
  .with_security(
    kafka_security_protocol(spec.security_protocol),
    sasl=self.source_sasl,
    tls=self.source_tls,
  )
  .with_max_poll_records(spec.max_poll_records)
}

///|
fn KafkaRuntimeConfig::sink_config(
  self : KafkaRuntimeConfig,
  spec : KafkaSinkSpec,
) -> KafkaSinkConfig raise ApiError {
  check_kafka_credential_ref(
    spec.credential_ref,
    self.sink_credential_ref,
    false,
  )
  validate_kafka_runtime_security(
    spec.security_protocol,
    self.sink_sasl,
    self.sink_tls,
    false,
  )
  KafkaSinkConfig::new(spec.bootstrap_servers.copy(), spec.topic).with_security(
    kafka_security_protocol(spec.security_protocol),
    sasl=self.sink_sasl,
    tls=self.sink_tls,
  )
}
