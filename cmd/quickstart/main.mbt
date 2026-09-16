///|
/// Run: moon run --target native cmd/quickstart
/// Embedded Coordinator + two Workers in one process, using real TCP sessions.
async fn main {
  let dir = @fs.tmpdir(prefix="moonweave-quickstart-")
  defer @fs.rmdir(dir, recursive=true)
  let cluster = @flow.ClusterConfig::new("demo", "coordinator", dir)
  let orders = [("apple", 3L), ("banana", 2L), ("apple", 4L)].mapi((i, order) => {
    @flow.KeyedSumEvent::new(
      @causal.EventId::new("orders", i.to_int64()),
      order.0,
      order.1,
    )
  })
  let result = @flow.KeyedSumJob::new("sales")
    .with_partition_count(2)
    .with_workers(["worker-a", "worker-b"])
    .run_local_cluster(cluster, orders)
  for item in result.totals() {
    println("\{item.key()}: \{item.total()}")
  }
  println("completed partitions: \{result.completed_partitions()}")
}
