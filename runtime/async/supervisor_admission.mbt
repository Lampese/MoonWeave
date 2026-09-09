///|
/// A peer is not a Worker execution until its Hello has been validated.
/// Malformed, unauthenticated, and incomplete handshakes only close that peer.
async fn read_supervisor_hello(
  connection : @socket.Tcp,
  control : @control.CoordinatorState,
  maximum_frame_bytes : Int,
  timeout_ms : Int,
  auth_token : String?,
) -> (@protocol.Frame, @protocol.Hello)? raise FrameIoError {
  let frame = @async.with_timeout(timeout_ms, () => {
    read_frame(connection, maximum_frame_bytes~)
  }) catch {
    error => {
      if @async.is_being_cancelled() {
        raise FrameIoError::Io(@debug.to_string(error))
      }
      return None
    }
  }
  if frame.kind() != @protocol.FrameKind::Hello {
    return None
  }
  let hello = @protocol.Hello::from_payload(frame.payload()) catch {
    _ => return None
  }
  let validator = @protocol.CoordinatorSession::new(
    control.cluster_id(),
    control.coordinator_id(),
    hello.worker_id(),
  ) catch {
    _ => return None
  }
  ignore(validator.accept_hello(frame, auth_token?)) catch {
    _ => return None
  }
  Some((frame, hello))
}
