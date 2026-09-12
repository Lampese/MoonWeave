# MoonWeave 生产准入清单

记录日期：2026-09-11。起点：`e3f50e3`。这是本轮已经确认的六项工作及验收依据，后续继续工作时更新本文件。

当前结论：六项清单均已实现并通过本轮冻结范围内的验收，可以在该范围内进入生产。这个结论不外推到未冻结的硬件、负载、算子、连接器配置或部署规模。

## 范围与承诺

- 本轮以功能正确性、持续运行、单节点故障恢复和明确负载下的性能为准；传输加密不在本轮验收范围内。
- 保持现有 at-least-once 输出语义及稳定幂等 key，业务接收方按 key 去重；不宣称跨 source、算子和外部 sink 的 exactly-once 事务。
- 自定义算子继续采用静态编译的 OperatorArtifact 模型。动态加载、热编译和新的算子种类不作为本轮新增门槛。
- 项目处于极早期：只维护一套当前 schema，直接更新类型和调用方，不增加 checkpoint v2、旧格式兼容层或迁移流程。
- 六项全部满足且第 6 项的负载及故障验收通过后，结论是“在该验收范围内可进入生产”。不能保证任意规模、任意负载或不存在未知缺陷。
- 不再把新的可选功能作为收尾门槛。本范围内发现的正确性、数据丢失或性能问题归入对应项修复，并记录证据；不能为了保持承诺隐瞒问题，也不能悄悄下调验收条件。

## 六项工作

| 编号 | 工作 | 完成条件 | 状态 |
| --- | --- | --- | --- |
| 1 | Coordinator 高可用 | 至少三个独立节点的 quorum，或等价一致性方案；复制 CoordinatorStore、JobStore、DriverStore 和 SourceLog 的权威状态；自动选主、追赶与恢复；旧主 fencing；失去多数派时禁止确认新写入。单节点故障后已确认的数据不丢失，不依赖原主机磁盘才能恢复。 | 已完成本轮验收：三个独立 native 进程通过共享文件协议执行 term/lease、prepare/commit quorum；同一 committed index 原子携带 Coordinator、Job、Driver、Source 当前 schema snapshots。heartbeat/process 隔离后 term 2 自动选主并 fencing 旧主；少数派拒绝写入；恢复节点追赶并恢复写入。命令：`moon run scripts/coordinator_ha_acceptance.mbtx` |
| 2 | Kafka 多分区 | 保留 topic/partition/offset 的准确身份；发现分区；consumer group 加入、退出、重平衡及分区扩容；各分区独立提交与恢复。持久化成功后才能提交 offset，撤销分区时禁止旧 owner 越权提交；通过真实 broker 故障与重平衡测试。 | 已完成本轮验收：真实 Kafka 4.0.0 通过三分区 source、两独立 classic consumer 加入/退出与无重叠重平衡、运行中 2→4 分区扩容及新分区发现；四分区 committed offset 均为 1，broker 使用同一日志目录重启后原三分区 committed offset 均恢复为 2。`Producer` 已按 `PartitionInfo.index` 精确投递，不依赖元数据数组顺序；CreatePartitions 仅接受错误码 0。命令：`moon run scripts/kafka_integration.mbtx /tmp/kafka_2.13-4.0.0` |
| 3 | 状态有界化 | SourceLog、各 WAL、outbox、output log 及相关内存索引具备持续运行时的保留和压缩策略；删除必须受持久化 checkpoint、重放边界和下游 ACK 约束。队列、内存及磁盘有明确预算，超限产生背压；磁盘满或不确定写入不能误报成功；压缩和恢复不丢数据或破坏去重。算子历史和因果索引也计入内存预算，不能只压缩文件。 | 已完成本轮验收：预算覆盖 Coordinator/Job/Driver/Source/manifest、Worker 主/Delta/output/consumer WAL、因果索引和算子状态；超限在 WAL、SourceLog、Worker admission 处 fail-closed，拒绝前不推进 cursor/ACK。终态顺序为 Worker sink 写入并 ACK → output WAL retention marker → Coordinator manifest consumer/retention proof → SourceLog compaction；任一证明缺失都保留数据。Durable outbox 的 ACK identity 只有显式 retention barrier 才释放；远端 Kafka/JSONL sink 通过 DrainAck 传递 consumer ACK 和 output retention proof。 |
| 4 | 全链路 Int64 offset | 删除 Kafka offset 到 EventId/Int32 的限制；事件身份、父依赖、协议、SourceLog、checkpoint 和恢复精确保留 Int64。覆盖 2^31、2^53 附近以及 Int64 上限的边界，不截断、不经浮点数丢失精度、不发生 offset 加一回绕。 | 已完成代码与 native 验收 |
| 5 | 生产运行入口 | 配置驱动的 Coordinator、Worker、Driver 服务；启动校验；真正提交和运行 Job；优雅 drain、停止与重启；入口使用第 1–4 项的实际运行路径，不能停留在固定 Job 示例或仅供测试的 API。 | 已完成本轮验收：`cmd/coordinator`、`cmd/worker`、`cmd/driver` 均配置驱动；支持 submit/input/status/drain/cancel、SIGTERM/SIGINT 的受保护 durable drain、重启恢复、Worker 中断重启、未知命令和错误 endpoint/config fail-closed。入口故障矩阵与独立 native SIGTERM 证据已并入总验收脚本。 |
| 6 | 性能和故障验收 | 在测量前记录并冻结硬件、worker 数、Kafka 分区数、算子及数据分布、事件大小、吞吐、p99、内存与磁盘上限、长稳时长和恢复目标；通过端到端性能、长稳、进程崩溃、主机故障、网络隔离、Kafka 故障及磁盘满测试，并保存可复现证据。 | 已完成本轮冻结参数与验收：10 CPU、约 7.8 GiB 内存、1 Coordinator/1 Worker/1 Kafka broker；reachability builtin:v1、单 key、JSON source/target、1-event latency 与 1-event/50ms steady pacing；最近一次 latency 64 次 p99=3ms，10 秒长稳 171 个端到端事件、吞吐 17.033 events/s，阈值为 p99 <= 1000ms、长稳 >= 50 events、吞吐 >= 10 events/s。HA、Worker/Coordinator 进程中断、SIGTERM drain、不可达端点、4096-byte budget 拒写、真实 Kafka 多分区/重平衡/扩容/broker 重启均通过；confirmed data loss=0、reference output mismatches=0。命令：`moon run scripts/production_acceptance.mbtx /tmp/kafka_2.13-4.0.0`。 |

实现顺序：4 → 2 → 3 → 1 → 5 → 6。第 6 项的规范与测试工具可提前建设，各项验收随实现推进。

## 验收规则

1. 功能验收以输入身份、分区提交位置、最终状态和输出幂等 key 为依据：已确认输入不得丢失，允许重试，去重后的结果必须与参考结果一致。
2. 覆盖持久化前后、offset 提交前后、主切换、分区撤销、压缩切换和重启等故障窗口。真正的故障验收必须经过服务入口和独立进程，纯状态机模拟不能替代它。
3. 长稳测试同时记录吞吐、端到端延迟、消费积压、内存、磁盘及恢复用时。正常输入和慢下游两种情况下都不得通过静默丢弃或无界缓冲维持吞吐。
4. 性能参数尚未确定。实现可以继续，但不能把开发机器上的测试结果当作用户业务的容量承诺。首次正式性能验收前必须把下表填全；未达到目标时修复或如实报告，不能把目标改成实测结果后宣称通过。

| 验收参数 | 当前状态 |
| --- | --- |
| CPU、内存、磁盘类型及带宽、网络、各节点部署方式 | Linux aarch64 container；10 vCPU；内存 7.8 GiB；overlay 磁盘 485 GB（验收时使用 9.8 GB、可用 450 GB，3%）；loopback TCP；单机功能/性能 fixture，HA 使用三个独立 native 进程和独立 WAL |
| Coordinator / Worker / Kafka broker 数、Kafka 分区数 | 性能矩阵：1 / 1 / 1；Kafka 故障矩阵使用真实 Kafka 4.0.0、source 3 分区、group topic 2→4 分区；HA 使用 3 个 Coordinator 进程 |
| 算子、key 数量与倾斜、因果依赖、事件大小 | reachability builtin:v1；单 `account` key；无父依赖；JSON `source`/`target` payload |
| 持续吞吐、峰值吞吐及持续时间 | 冻结持续目标 >= 10 events/s，steady 10s；本次端到端 steady 结果 17.033 events/s；latency input 结果仅作峰值入口观测，不作为端到端容量承诺 |
| 端到端 p99 上限及测量区间 | input round-trip p99 <= 1000ms；64 次单事件请求；本次 p99=3ms |
| 每角色内存、磁盘及保留预算 | 每 Coordinator/Worker WAL budget 268435456 bytes；Driver event limit 200000；capacity fault fixture 使用 4096 bytes 并要求 fail-closed |
| 长稳时长、积压恢复与故障恢复用时上限 | steady 10s；drain/recovery command timeout 30s；Kafka broker restart reconnect timeout 60s；HA lease 400ms，故障后多数派 term 2 选主通过 |
| 已确认数据丢失数 | 必须为 0 |
| 参考状态 / 去重后输出不一致数 | 必须为 0 |

## 实施与证据记录

- 起点已有 Kafka 单显式分区 source、幂等 producer、at-least-once sink 和本地 WAL/checkpoint；这不是多分区或 Coordinator HA 的完成证据。
- 起点历史验证记录：native 单元测试 560/560；Kafka 4.0.0 批量消费、重读、提交与 broker 重启联调通过。后续变更须重新验证受影响路径。
- 当前工作：六项均已完成；后续容量承诺必须在目标部署硬件上重新运行同一验收脚本，不得把本机结果外推为任意规模保证。
- 第 4 项证据：`moon check --target native` 通过；最近一次 `moon test --target native` 为 600/600；`moon fmt` 与 `moon info --target native` 通过。
- 第 2 项证据：保留 partition 的 `poll_partitioned`、consumer-group 订阅/退出/重平衡路径、按分区 OffsetCommit、撤销 assignment 前后双重 fencing 检查、全分区 source 配置和真实 Kafka 验收均已接入。`moon run scripts/kafka_integration.mbtx /tmp/kafka_2.13-4.0.0` 已真实通过：三分区 source 产生 6 条记录、两独立 classic member 在两分区 topic 上无重叠重平衡，运行中扩容到四分区后两个 member 均获得两分区且分区 2/3 被发现；四分区 committed offset 均为 1，sink 幂等 key/JSON 通过，broker 使用同一日志目录重启后原三分区 committed offset 均恢复到 2。此前 broker 报 `Tried to allocate a collection of size 65550` 的根因是 ConsumerProtocol 数组和 userData 长度误用 INT16，现已统一为 Kafka schema 要求的 INT32，并由 `vendor/moonkafka/consumer_protocol_test.mbt` 的 hand-built wire bytes 覆盖；本轮同时修复了 Producer 对无序元数据分区数组的错误下标假设。
- 本轮 HA/入口证据：`moon test --target native` 587/587；`moon test --target native runtime/async`、`runtime/api` 的专项回归此前分别通过；`moon build --target native cmd/coordinator cmd/worker cmd/driver` 通过。构建仍有 vendor/moonkafka 的既有 C 指针兼容性警告。
- 独立进程入口烟测：Coordinator、Worker、Driver 使用三个 native 进程完成 `submit → input(accepted=1) → drain → Succeeded`；Worker 报告 `live_batches=1`，Coordinator/Driver/Worker WAL 与 checkpoint 均落盘。随后复用同一 Coordinator/Worker 存储重启，Driver 恢复 `accepted=1` 的 `Running` 状态并再次 drain 成功；Worker 报告 `live_batches=0`，说明已持久化输入未被重复执行。该证据覆盖入口和本地重启恢复，不替代跨主机 HA、信号优雅停止和故障矩阵验收。
- 本轮状态边界证据：生产入口已把预算传入 CoordinatorStore、JobStore、SourceLog、DriverStore、JobOutputManifest；`--driver-event-limit` 对 Driver 内存去重索引实施上限，超限拒绝新输入而不写入；Worker 预算校验在 `accept_frame` 前执行，拒绝时不写 WAL、ACK 或 cursor；终态成功后 Driver WAL 不再保留输入事件，JobStore 清除完成 delivery plan，SourceLog 写入 retention marker 并删除已完成物理前缀，恢复后保留逻辑 `next_offset`。最终验收前重新执行全量 native 回归。
- 本轮状态边界证据：Durable outbox ACK 达到 `compaction_record_limit` 后自动重写 WAL，保留 pending payload 与 ACK tombstone；TaskOutputConsumer checkpoint 达到同类阈值后仅保留 binding 和最新 ACK proof。Worker terminal barrier 在下游写入成功且 consumer 追平后物理压缩 output WAL，DrainAck 携带 consumer/retention proof；Coordinator manifest 只在完整分区均有 proof 时允许 SourceLog compaction。远端 JSONL sink live 回归验证了 sink 写入、output WAL retention marker、manifest barrier 与 source prefix compaction 的顺序；lagging consumer 单测验证 fail-closed。
- 本轮入口修复：`DriverCoordinator::shutdown_gracefully` 先写入 Driver `Drain` intent，执行 live source drain 并等待 `Succeeded/Failed`，`cmd/coordinator` 通过 `moonbitlang/async/signal` 注册 SIGTERM/SIGINT；`moon check --target native`、入口构建、API/live 回归和独立进程 SIGTERM 验收通过。总验收中的入口矩阵覆盖负预算、错误 endpoint/config、Worker kill/restart、Driver CLI 中断、Coordinator restart、未知命令；SIGTERM 脚本使用三个 native 进程，Coordinator 状态 `-15`、Worker 退出 0 且持久化批次完成。
- 本轮 Coordinator HA 证据：`runtime/async/coordinator_ha_process.mbt` 的 term/lease、prepare/commit ack、fencing 和四类状态 bundle 均使用当前 schema version 1；`moon run scripts/coordinator_ha_acceptance.mbtx` 通过三个独立 native 进程验证初始 quorum、Job/Driver/Source/Coordinator snapshots 的同索引复制、heartbeat 隔离选主、少数派拒写、旧主终止、节点追赶和恢复多数派写入。验收输出目录中的 `replicas/node-*.state.json` 保存了四类状态证据。
- 本轮性能与故障证据：`scripts/production_acceptance.mbtx` 冻结硬件/拓扑/负载/阈值，并将 HA、latency、steady、入口故障矩阵、SIGTERM、网络不可达、4096-byte capacity refusal 和真实 Kafka 4.0.0 多分区/consumer-group/broker restart 输出写入同一 evidence 文件。最近一次证据：`/tmp/moonweave-production-acceptance-.55332.8430be0/production-evidence.txt`；全流程命令：`moon run scripts/production_acceptance.mbtx /tmp/kafka_2.13-4.0.0`。

## 配置化入口示例

Coordinator：`moon run cmd/coordinator -- --cluster <id> --coordinator <id> --bind 0.0.0.0:7801 --advertise <host>:7801 --driver-bind 0.0.0.0:7802 --driver-advertise <host>:7802 --storage <dir> --storage-budget-bytes 268435456 --driver-event-limit 100000 --workers worker-a@boot-a,worker-b@boot-b`

Worker：`moon run cmd/worker -- --cluster <id> --coordinator <id> --coordinator-endpoint <host>:7801 --worker worker-a --incarnation worker-a-boot --storage <dir> --storage-budget-bytes 268435456 --job-config <submission.json>`

Driver：`moon run cmd/driver -- --endpoint <host>:7802 --cluster <id> --coordinator <id> --job-config <submission.json> --command submit|input|status|drain|cancel`

最小配置可由 `moon run --target native cmd/driver_fixture -- --job <job> --submission <id>` 生成；输入文件中的每个元素是 `Event` JSON（包含 `record` 字段），可参考 `cmd/driver_fixture/process-events.json`。
