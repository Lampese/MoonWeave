<p align="center">
  <h1 align="center">MoonWeave</h1>
</p>

<p align="center">
  <strong>A distributed causal dataflow engine for stateful streaming jobs.</strong>
</p>

<p align="center">
  <a href="LICENSE">Apache-2.0</a>
</p>

MoonWeave turns an event stream into a partitioned, recoverable dataflow. A
Coordinator owns durable job state, Workers execute keyed partitions, and a
Driver submits and controls jobs. Events keep their causal dependencies while
state moves between Workers during rescaling and recovery.

## See It Run

The repository includes a small two-Worker example that runs through real
Coordinator and Worker TCP sessions:

```bash
moon run --target native cmd/quickstart
```

```text
apple: 7
banana: 2
completed partitions: 2
```

The complete program is intentionally small:

```moonbit
import {
  "moonweave/moonweave/causal",
  "moonweave/moonweave/runtime/api" @flow,
  "moonbitlang/async",
  "moonbitlang/async/fs",
}

async fn main {
  let storage = @fs.tmpdir(prefix="moonweave-quickstart-")
  defer @fs.rmdir(storage, recursive=true)

  let orders = [
    @flow.KeyedSumEvent::new(
      @causal.EventId::new("orders", 0L), "apple", 3L,
    ),
    @flow.KeyedSumEvent::new(
      @causal.EventId::new("orders", 1L), "banana", 2L,
    ),
    @flow.KeyedSumEvent::new(
      @causal.EventId::new("orders", 2L), "apple", 4L,
    ),
  ]

  let result = @flow.KeyedSumJob::new("sales")
    .with_partition_count(2)
    .with_workers(["worker-a", "worker-b"])
    .run_local_cluster(
      @flow.ClusterConfig::new("demo", "coordinator", storage),
      orders,
    )

  for total in result.totals() {
    println("\{total.key()}: \{total.total()}")
  }
  println("completed partitions: \{result.completed_partitions()}")
}
```

The same job model is used by the production Coordinator, Worker, and Driver
entrypoints under [`cmd/`](cmd/).

## Causal Events

An event can name a parent that has not arrived yet. MoonWeave keeps the child
pending, then releases it when the causal dependency is available:

```moonbit
let run = @flow.KeyedSumJob::new("orders").run_local()
let parent = @flow.KeyedSumEvent::new(
  @causal.EventId::new("orders", 0L), "account", 10L,
)
let child = @flow.KeyedSumEvent::new_with_parents(
  @causal.EventId::new("orders", 1L), [parent.id()], "account", 2L,
)

assert_eq(run.submit([child]), [])
assert_eq(run.submit([parent]), [
  @flow.KeyedSumUpdate::new("account", 10L),
  @flow.KeyedSumUpdate::new("account", 12L),
])
```

This is the core difference from a plain keyed reducer: transport order does
not become application order.

## What It Provides

### Distributed execution

- Keyed partitioning with deterministic routing and explicit exchange routes
- Multi-stage graphs with map, filter, rekey, branch, join, and aggregate stages
- Durable graph edges with downstream acknowledgement and backpressure
- Multiple independent jobs managed by one Coordinator

### Stateful streaming

- Event-time windows and watermark propagation
- Interval joins with bounded retention
- Causal frontiers, pending dependency release, and duplicate suppression
- Checkpoints, write-ahead logs, replay, and state reclamation

### Operations

- Online rescaling with state migration and epoch fencing
- Compatible job updates with durable update intent
- Coordinator HA and network failover
- Kafka, JSON Lines, and replayable event sources
- At-least-once durable sinks with stable idempotency keys

## Runtime Shape

```mermaid
flowchart LR
  Driver --> Coordinator
  Coordinator --> WorkerA[Worker A]
  Coordinator --> WorkerB[Worker B]
  WorkerA <--> WorkerB
  WorkerA --> Sink
  WorkerB --> Sink
```

The Coordinator assigns partition leases and records control-plane progress.
Workers own state and exchange data over framed TCP sessions. A failed Worker
can be fenced and its state can be replayed or migrated under a new epoch.

## Run The Services

Build the native production entrypoints:

```bash
moon build --target native cmd/coordinator cmd/worker cmd/driver
```

Use the Driver to submit a serialized `JobSubmission`, query status, provide
input, pause or resume a job, rescale Workers, apply a compatible update, and
drain the job. The entrypoint implementations are:

- [`cmd/coordinator`](cmd/coordinator)
- [`cmd/worker`](cmd/worker)
- [`cmd/driver`](cmd/driver)

The public typed API lives in [`runtime/api`](runtime/api). Built-in operators
are under [`operator/`](operator/).

## Delivery Model

Sources are replayable. Durable, JSON Lines, and Kafka sinks provide at-least-
once delivery and expose a stable output idempotency key for consumers. The
engine does not claim an external transaction unless the sink supplies one.

## Validation

The repository's acceptance scenarios exercise real Kafka graphs, event-time
windows, Coordinator failover, Worker rescaling, compatible updates, crash
boundaries, and multi-job execution.

## License

MoonWeave is available under the [Apache License 2.0](LICENSE).
