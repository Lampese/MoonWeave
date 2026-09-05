///|
/// Complete the coordinator side of the Hello/Welcome handshake.
///
/// Returns both the inbound session validator and the independent outbound
/// frame builder. The connection remains owned by the caller.
pub async fn coordinator_handshake(
  connection : @socket.Tcp,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  session_id : String,
  assignment? : @protocol.AssignmentSpec,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> (@protocol.CoordinatorSession, @protocol.FrameBuilder) raise FrameIoError {
  let session = @protocol.CoordinatorSession::new(
    cluster_id, coordinator_id, worker_id,
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  let hello_frame = read_frame(connection, maximum_frame_bytes~)
  let hello = session.accept_hello(hello_frame) catch {
    error => raise FrameIoError::Session(error)
  }
  let welcome = session.activate(session_id, assignment?) catch {
    error => raise FrameIoError::Session(error)
  }
  let epoch = match assignment {
    Some(spec) => spec.epoch()
    None => 0
  }
  let outbound = @protocol.FrameBuilder::new(
    cluster_id,
    coordinator_id,
    hello.incarnation(),
    session_id~,
    assignment_epoch=epoch,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(@protocol.FrameKind::Welcome, welcome.to_payload()) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
  (session, outbound)
}

///|
/// Complete Coordinator activation when the caller has already read Hello.
/// This is used by multi-connection supervisors that must route a socket by
/// worker ID before installing its assignment.
pub async fn coordinator_handshake_after_hello(
  connection : @socket.Tcp,
  hello_frame : @protocol.Frame,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  session_id : String,
  assignment? : @protocol.AssignmentSpec,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> (@protocol.CoordinatorSession, @protocol.FrameBuilder) raise FrameIoError {
  let session = @protocol.CoordinatorSession::new(
    cluster_id, coordinator_id, worker_id,
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  let hello = session.accept_hello(hello_frame) catch {
    error => raise FrameIoError::Session(error)
  }
  let welcome = session.activate(session_id, assignment?) catch {
    error => raise FrameIoError::Session(error)
  }
  let epoch = match assignment {
    Some(spec) => spec.epoch()
    None => 0
  }
  let outbound = @protocol.FrameBuilder::new(
    cluster_id,
    coordinator_id,
    hello.incarnation(),
    session_id~,
    assignment_epoch=epoch,
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let frame = outbound.build(@protocol.FrameKind::Welcome, welcome.to_payload()) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
  (session, outbound)
}

///|
/// Complete the worker side of the Hello/Welcome handshake.
pub async fn worker_handshake(
  connection : @socket.Tcp,
  cluster_id : String,
  coordinator_id : String,
  worker_id : String,
  incarnation : String,
  capabilities : Array[String],
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> (@protocol.WorkerSession, @protocol.FrameBuilder) raise FrameIoError {
  let session = @protocol.WorkerSession::new(
    cluster_id,
    worker_id,
    incarnation,
    coordinator_id~,
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  let outbound = @protocol.FrameBuilder::new(cluster_id, worker_id, incarnation) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let hello = @protocol.Hello::new(worker_id, incarnation, capabilities) catch {
    error => raise FrameIoError::Protocol(error)
  }
  let hello_frame = outbound.build(
    @protocol.FrameKind::Hello,
    hello.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, hello_frame, maximum_frame_bytes~)
  let welcome_frame = read_frame(connection, maximum_frame_bytes~)
  session.accept_frame(welcome_frame) catch {
    error => raise FrameIoError::Session(error)
  }
  outbound.set_session_id(session.session_id()) catch {
    error => raise FrameIoError::Protocol(error)
  }
  match session.state().assignment() {
    Some(assignment) =>
      outbound.set_assignment_epoch(assignment.epoch()) catch {
        error => raise FrameIoError::Protocol(error)
      }
    None => ()
  }
  (session, outbound)
}

///|
/// Send one coordinator-to-worker EventBatch after consuming worker credit.
pub async fn coordinator_send_batch(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  batch : @protocol.EventBatch,
  encoded_bytes : Int64,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> Unit raise FrameIoError {
  let expected_offset = session.next_offset(batch.partition(), batch.epoch()) catch {
    error => raise FrameIoError::Session(error)
  }
  if expected_offset != batch.first_offset() {
    raise FrameIoError::Protocol(
      @protocol.ProtocolError::InvalidOffset(batch.first_offset()),
    )
  }
  let first_offset = session.reserve_batch(
    batch.partition(),
    batch.epoch(),
    batch.events().length().to_int64(),
    encoded_bytes,
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  if first_offset != expected_offset {
    raise FrameIoError::Protocol(
      @protocol.ProtocolError::InvalidOffset(batch.first_offset()),
    )
  }
  let frame = outbound.build(
    @protocol.FrameKind::EventBatch,
    batch.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
/// Send a derived cross-Worker delta after consuming target-side credit. The
/// target source cursor is intentionally unchanged.
pub async fn coordinator_send_delta(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  delta : @protocol.DeltaBatch,
  encoded_bytes : Int64,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> String raise FrameIoError {
  session.reserve_delta(
    delta.target_partition(),
    delta.target_epoch(),
    delta.events().length().to_int64(),
    encoded_bytes,
  ) catch {
    error => raise FrameIoError::Session(error)
  }
  let frame = outbound.build(
    @protocol.FrameKind::DeltaBatch,
    delta.to_payload(),
  ) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
  delta.delta_id()
}

///|
/// Send a targeted query probe after the worker assignment is active.
pub async fn coordinator_send_probe(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  probe : @protocol.Probe,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> Unit raise FrameIoError {
  let next_offset = session.next_offset(probe.partition(), probe.epoch()) catch {
    error => raise FrameIoError::Session(error)
  }
  if probe.target_offset() >= next_offset {
    raise FrameIoError::Protocol(
      @protocol.ProtocolError::InvalidOffset(probe.target_offset()),
    )
  }
  let frame = outbound.build(@protocol.FrameKind::Probe, probe.to_payload()) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}

///|
/// Seal one query obligation only after the coordinator has verified its reply.
pub async fn coordinator_send_seal(
  connection : @socket.Tcp,
  session : @protocol.CoordinatorSession,
  outbound : @protocol.FrameBuilder,
  seal : @protocol.Seal,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> Unit raise FrameIoError {
  let next_offset = session.next_offset(seal.partition(), seal.epoch()) catch {
    error => raise FrameIoError::Session(error)
  }
  if seal.sealed_offset() >= next_offset {
    raise FrameIoError::Protocol(
      @protocol.ProtocolError::InvalidOffset(seal.sealed_offset()),
    )
  }
  let frame = outbound.build(@protocol.FrameKind::Seal, seal.to_payload()) catch {
    error => raise FrameIoError::Protocol(error)
  }
  write_frame(connection, frame, maximum_frame_bytes~)
}
