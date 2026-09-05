///|
/// Minimal native worker transport smoke target. It validates framed async I/O
/// but intentionally does not implement scheduling or persistence yet.
async fn main {
  let server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:7800"),
    reuse_addr=true,
  )
  println("moonweave worker echo listening on 127.0.0.1:7800")
  server.run_forever(max_connections=64) <| ((connection, _) => {
    @transport.serve_echo(connection) catch {
      error => println("worker connection closed: \{@debug.to_string(error)}")
    }
  })
}
