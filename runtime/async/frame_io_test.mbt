///|
fn frame() -> @protocol.Frame raise @protocol.ProtocolError {
  @protocol.Frame::new(
    1,
    "cluster-a",
    "session-a",
    "message-1",
    "worker-a",
    "boot-1",
    4L,
    7,
    @protocol.FrameKind::Heartbeat,
    "{\"queue_depth\":2}",
  )
}

///|
async test "async frame IO round trips through a native TCP socket" {
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  let addr = server.addr
  @async_runtime.with_task_group <| group => {
    group.spawn_bg() <| () => {
      let (connection, _) = server.accept()
      defer connection.close()
      let received = read_frame(connection)
      write_frame(connection, received)
    }
    let client = @socket.Tcp::connect(addr)
    defer client.close()
    let sent = frame()
    write_frame(client, sent)
    let received = read_frame(client)
    assert_eq(received, sent)
  }
}
