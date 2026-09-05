///|
/// Errors at the native async transport boundary.
pub(all) suberror FrameIoError {
  InvalidMaximumFrameBytes(Int)
  FrameTooLarge(actual~ : Int, limit~ : Int)
  Io(String)
  Protocol(@protocol.ProtocolError)
  Session(@protocol.SessionError)
  Coordinator(@control.CoordinatorError)
  CoordinatorStore(CoordinatorStoreError)
  SourceLog(SourceLogError)
  Task(@task.TaskError)
  TaskSet(@task.TaskSetError)
  OutputLog(OutputLogError)
  JobStore(JobStoreError)
  DurableOutbox(DurableOutboxError)
  SupervisorPlan(SupervisorPlanError)
  Message(@protocol.MessageError)
  Wal(WalError)
  Checkpoint(CheckpointError)
  MissingAssignment
  ProbeMismatch
  ProbeNotReached(target~ : Int64, actual~ : Int64)
  UnknownDeltaAck(String)
  DeltaMismatch
  DrainMismatch
  PendingEvents(Int)
  Handler(String)
} derive(Debug, Eq)

///|
/// Read one length-prefixed protocol frame from an async reader.
///
/// The transport owns framing; the protocol package owns payload validation.
/// Reading is bounded before allocation so a peer cannot request an unbounded
/// buffer.
pub async fn[R : @io.Reader] read_frame(
  reader : R,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> @protocol.Frame raise FrameIoError {
  if maximum_frame_bytes <= 0 {
    raise FrameIoError::InvalidMaximumFrameBytes(maximum_frame_bytes)
  }
  let header = reader.read_exactly(4) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  let length = (header[0].to_int() << 24) +
    (header[1].to_int() << 16) +
    (header[2].to_int() << 8) +
    header[3].to_int()
  if length < 0 || length > maximum_frame_bytes {
    raise FrameIoError::FrameTooLarge(actual=length, limit=maximum_frame_bytes)
  }
  let body = reader.read_exactly(length) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  @protocol.Frame::decode_payload(body, maximum_frame_bytes~) catch {
    error => raise FrameIoError::Protocol(error)
  }
}

///|
/// Write one complete protocol frame through an async writer.
pub async fn[W : @io.Writer] write_frame(
  writer : W,
  frame : @protocol.Frame,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> Unit raise FrameIoError {
  let encoded = frame.encode(maximum_frame_bytes~) catch {
    error => raise FrameIoError::Protocol(error)
  }
  writer.write(encoded) catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
}

///|
/// Serve one connection until the peer closes it. Each frame is echoed only
/// after it has passed the protocol decoder.
pub async fn serve_echo(
  connection : @socket.Tcp,
  maximum_frame_bytes? : Int = 4 * 1024 * 1024,
) -> Unit raise FrameIoError {
  defer connection.close()
  for ;; {
    let frame = read_frame(connection, maximum_frame_bytes~) catch {
      FrameIoError::Io(_) => return
      error => raise error
    }
    write_frame(connection, frame, maximum_frame_bytes~)
  }
}
