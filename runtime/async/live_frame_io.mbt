///|
/// Only the pump reads the socket. The consumer never cancels an operation
/// that removes a frame from the queue or consumes part of its wire encoding.
priv struct LiveFrameInbox {
  queue : @aqueue.Queue[Result[@protocol.Frame, FrameIoError]]
}

///|
fn LiveFrameInbox::new() -> LiveFrameInbox {
  { queue: @aqueue.Queue(kind=@aqueue.Blocking(1)), }
}

///|
async fn LiveFrameInbox::pump(
  self : LiveFrameInbox,
  connection : @socket.Tcp,
  maximum_frame_bytes : Int,
) -> Unit {
  for ;; {
    let frame : Result[@protocol.Frame, FrameIoError] = Ok(
      read_frame(connection, maximum_frame_bytes~),
    ) catch {
      error => Err(error)
    }
    self.queue.put(frame)
    if frame is Err(_) {
      return
    }
  }
}

///|
async fn LiveFrameInbox::poll(
  self : LiveFrameInbox,
  timeout_ms : Int,
) -> @protocol.Frame? raise FrameIoError {
  let mut incoming = self.queue.try_get() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  if incoming is None {
    @async.sleep(timeout_ms) catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
    incoming = self.queue.try_get() catch {
      error => raise FrameIoError::Io(@debug.to_string(error))
    }
  }
  match incoming {
    None => None
    Some(Ok(frame)) => Some(frame)
    Some(Err(error)) => raise error
  }
}

///|
async fn LiveFrameInbox::read(
  self : LiveFrameInbox,
) -> @protocol.Frame raise FrameIoError {
  let incoming = self.queue.get() catch {
    error => raise FrameIoError::Io(@debug.to_string(error))
  }
  match incoming {
    Ok(frame) => frame
    Err(error) => raise error
  }
}
