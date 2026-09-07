///|
async test "live frame polling preserves headers and bodies across deadlines" {
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:0"),
    reuse_addr=true,
  )
  defer server.close()
  @async.with_timeout(5000, () => {
    @async.with_task_group <| group => {
      let builder = @protocol.FrameBuilder::new("cluster", "worker", "boot")
      let frame = builder.build(@protocol.FrameKind::Heartbeat, "{}")
      let bytes = frame.encode()
      let sender = group.spawn() <| () => {
        let connection = @socket.Tcp::connect(server.addr)
        defer connection.close()
        connection.write(bytes[0:2])
        @async.sleep(20)
        connection.write(bytes[2:8])
        @async.sleep(20)
        connection.write(bytes[8:])
      }
      let (connection, _) = server.accept()
      defer connection.close()
      let inbox = LiveFrameInbox::new()
      group.spawn_bg(no_wait=true) <| () => { inbox.pump(connection, 4096) }
      let mut timeouts = 0
      for ;; {
        match inbox.poll(1) {
          None => timeouts += 1
          Some(value) => {
            assert_eq(value, frame)
            break
          }
        }
      }
      assert_true(timeouts > 0)
      sender.wait()
    }
  })
}
