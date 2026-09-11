# MoonWeave 生产准入清单

记录日期：2026-09-11。起点：`e3f50e3`。这是本轮已经确认的六项工作及验收依据，后续继续工作时更新本文件。

当前结论：尚未完成这六项，不能宣称已达到生产要求。单元测试通过、Kafka 联调通过、某个功能实现完成，均不等于生产验收通过。

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
| 1 | Coordinator 高可用 | 至少三个独立节点的 quorum，或等价一致性方案；复制 CoordinatorStore、JobStore、DriverStore 和 SourceLog 的权威状态；自动选主、追赶与恢复；旧主 fencing；失去多数派时禁止确认新写入。单节点故障后已确认的数据不丢失，不依赖原主机磁盘才能恢复。 | 未完成：当前只有同进程多 WAL；已补本地 leader 重选和失败写入回滚，不能替代跨进程 quorum/term/fencing |
| 2 | Kafka 多分区 | 保留 topic/partition/offset 的准确身份；发现分区；consumer group 加入、退出、重平衡及分区扩容；各分区独立提交与恢复。持久化成功后才能提交 offset，撤销分区时禁止旧 owner 越权提交；通过真实 broker 故障与重平衡测试。 | 实现中：代码路径与单元回归已完成，真实多分区 broker/重平衡验收待执行 |
| 3 | 状态有界化 | SourceLog、各 WAL、outbox、output log 及相关内存索引具备持续运行时的保留和压缩策略；删除必须受持久化 checkpoint、重放边界和下游 ACK 约束。队列、内存及磁盘有明确预算，超限产生背压；磁盘满或不确定写入不能误报成功；压缩和恢复不丢数据或破坏去重。算子历史和因果索引也计入内存预算，不能只压缩文件。 | 部分完成：生产入口已把预算传入 Coordinator/Job/Source/manifest WAL；Driver 有输入事件上限并在终态释放历史；成功排空后 Job plan 与 SourceLog 已完成前缀可安全压缩；Durable outbox 和 output consumer checkpoint WAL 在达到记录阈值后自动物理压缩，仍保留去重/最新 ACK 证明。持续运行中的 barrier 驱动 source/output/outbox 身份回收、算子/因果索引预算和端到端磁盘满背压验收仍待完成 |
| 4 | 全链路 Int64 offset | 删除 Kafka offset 到 EventId/Int32 的限制；事件身份、父依赖、协议、SourceLog、checkpoint 和恢复精确保留 Int64。覆盖 2^31、2^53 附近以及 Int64 上限的边界，不截断、不经浮点数丢失精度、不发生 offset 加一回绕。 | 已完成代码与 native 验收 |
| 5 | 生产运行入口 | 配置驱动的 Coordinator、Worker、Driver 服务；启动校验；真正提交和运行 Job；优雅 drain、停止与重启；入口使用第 1–4 项的实际运行路径，不能停留在固定 Job 示例或仅供测试的 API。 | 实现中：`cmd/coordinator` 已改为长期 Driver 服务，`cmd/worker` 从 JobSubmission JSON 启动，新增 `cmd/driver` 的 submit/input/status/drain/cancel；native build 通过；独立进程提交/重启/信号优雅停止验收仍待做 |
| 6 | 性能和故障验收 | 在测量前记录并冻结硬件、worker 数、Kafka 分区数、算子及数据分布、事件大小、吞吐、p99、内存与磁盘上限、长稳时长和恢复目标；通过端到端性能、长稳、进程崩溃、主机故障、网络隔离、Kafka 故障及磁盘满测试，并保存可复现证据。 | 待实现 |

实现顺序：4 → 2 → 3 → 1 → 5 → 6。第 6 项的规范与测试工具可提前建设，各项验收随实现推进。

## 验收规则

1. 功能验收以输入身份、分区提交位置、最终状态和输出幂等 key 为依据：已确认输入不得丢失，允许重试，去重后的结果必须与参考结果一致。
2. 覆盖持久化前后、offset 提交前后、主切换、分区撤销、压缩切换和重启等故障窗口。真正的故障验收必须经过服务入口和独立进程，纯状态机模拟不能替代它。
3. 长稳测试同时记录吞吐、端到端延迟、消费积压、内存、磁盘及恢复用时。正常输入和慢下游两种情况下都不得通过静默丢弃或无界缓冲维持吞吐。
4. 性能参数尚未确定。实现可以继续，但不能把开发机器上的测试结果当作用户业务的容量承诺。首次正式性能验收前必须把下表填全；未达到目标时修复或如实报告，不能把目标改成实测结果后宣称通过。

| 验收参数 | 当前状态 |
| --- | --- |
| CPU、内存、磁盘类型及带宽、网络、各节点部署方式 | 待确定并记录 |
| Coordinator / Worker / Kafka broker 数、Kafka 分区数 | 待确定；HA 至少覆盖三个独立 Coordinator 节点或等价方案 |
| 算子、key 数量与倾斜、因果依赖、事件大小 | 待确定 |
| 持续吞吐、峰值吞吐及持续时间 | 待确定 |
| 端到端 p99 上限及测量区间 | 待确定 |
| 每角色内存、磁盘及保留预算 | 待确定 |
| 长稳时长、积压恢复与故障恢复用时上限 | 待确定 |
| 已确认数据丢失数 | 必须为 0 |
| 参考状态 / 去重后输出不一致数 | 必须为 0 |

## 实施与证据记录

- 起点已有 Kafka 单显式分区 source、幂等 producer、at-least-once sink 和本地 WAL/checkpoint；这不是多分区或 Coordinator HA 的完成证据。
- 起点历史验证记录：native 单元测试 560/560；Kafka 4.0.0 批量消费、重读、提交与 broker 重启联调通过。后续变更须重新验证受影响路径。
- 当前工作：第 3 项，补齐生产入口预算和终态 retention 链路，并补充精度、持久化与恢复边界测试。
- 第 4 项证据：`moon check --target native` 通过；`moon test --target native` 573/573（后续多分区配置测试后为 574/574）；`moon fmt` 与 `moon info --target native` 通过。
- 第 2 项当前证据：保留 partition 的 `poll_partitioned`、consumer-group 订阅/退出/重平衡路径、按分区 OffsetCommit、撤销 assignment 前后双重 fencing 检查、全分区 source 配置和 integration Kafka 三分区场景已接入；需要 Kafka 发行版才能执行真实 broker 验收，当前环境未发现 `kafka-server-start.sh`。
- 本轮 HA/入口证据：`moon test --target native` 587/587；`moon test --target native runtime/async`、`runtime/api` 的专项回归此前分别通过；`moon build --target native cmd/coordinator cmd/worker cmd/driver` 通过。构建仍有 vendor/moonkafka 的既有 C 指针兼容性警告。
- 独立进程入口烟测：Coordinator、Worker、Driver 使用三个 native 进程完成 `submit → input(accepted=1) → drain → Succeeded`；Worker 报告 `live_batches=1`，Coordinator/Driver/Worker WAL 与 checkpoint 均落盘。随后复用同一 Coordinator/Worker 存储重启，Driver 恢复 `accepted=1` 的 `Running` 状态并再次 drain 成功；Worker 报告 `live_batches=0`，说明已持久化输入未被重复执行。该证据覆盖入口和本地重启恢复，不替代跨主机 HA、信号优雅停止和故障矩阵验收。
- 本轮状态边界证据：生产 Coordinator 将 `--storage-budget-bytes` 传入 CoordinatorStore、JobStore、SourceLog、DriverStore、JobOutputManifest；`--driver-event-limit` 对 Driver 内存去重索引实施上限，超限拒绝新输入而不写入；终态成功后 Driver WAL 不再保留输入事件，JobStore 清除完成 delivery plan，SourceLog 写入 retention marker 并删除已完成物理前缀，恢复后保留逻辑 `next_offset`。native 全量回归为 588/588。
- 本轮状态边界证据：Durable outbox ACK 达到 `compaction_record_limit` 后自动重写 WAL，保留 pending payload 与 ACK tombstone；TaskOutputConsumer checkpoint 达到同类阈值后仅保留 binding 和最新 ACK proof。两者均未自动释放去重身份或删除 source/output log，重启回归验证重复投递仍被识别。async 专项回归为 167/167；全量 native 回归为 590/590。

## 配置化入口示例

Coordinator：`moon run cmd/coordinator -- --cluster <id> --coordinator <id> --bind 0.0.0.0:7801 --advertise <host>:7801 --driver-bind 0.0.0.0:7802 --driver-advertise <host>:7802 --storage <dir> --storage-budget-bytes 268435456 --driver-event-limit 100000 --workers worker-a@boot-a,worker-b@boot-b`

Worker：`moon run cmd/worker -- --cluster <id> --coordinator <id> --coordinator-endpoint <host>:7801 --worker worker-a --incarnation worker-a-boot --storage <dir> --storage-budget-bytes 268435456 --job-config <submission.json>`

Driver：`moon run cmd/driver -- --endpoint <host>:7802 --cluster <id> --coordinator <id> --job-config <submission.json> --command submit|input|status|drain|cancel`

最小配置可由 `moon run --target native cmd/driver_fixture -- --job <job> --submission <id>` 生成；输入文件中的每个元素是 `Event` JSON（包含 `record` 字段），可参考 `cmd/driver_fixture/process-events.json`。
