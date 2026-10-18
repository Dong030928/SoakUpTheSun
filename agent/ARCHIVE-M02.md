# M02：RocketMQ 事务消息与异步会话归档

默认仍是 Redis 会话模式。显式启用后，完整轮次通过 RocketMQ 事务消息异步归档到 M01 的 MySQL 服务。前台不写 MySQL，不使用 Outbox；首次新会话和冷缓存恢复会查询数据库。

## 实现与框架职责

```text
租约与依赖准入 → Spring AI 对话/工具 → 最终回复
  → 不可变 ArchiveEvent → RocketMQ 半消息 ACK
  → Lua 校验租约、Epoch、head 和事件摘要
  → 单条 MSET 写热点快照和 COMMITTED 证据 → COMMIT
  → 消费端：M01 原始轮次＋Spring AI JDBC 快照＋memory_log 同事务提交
  → 消费成功；Redis 回执失败由后台对账修复
```

- Spring AI 继续负责 `ChatClient`、工具循环、`MessageChatMemoryAdvisor` 和 `MessageWindowChatMemory`；`JdbcChatMemoryRepository` 与原始轮次、消费日志加入同一个数据库事务。快照裁剪不删除原始轮次。
- 新增的 MQ 可靠性使用 RocketMQ **5.4.0 原生 remoting Java 客户端**：`TransactionMQProducer`、`TransactionListener`、`DefaultMQPushConsumer`，生命周期由 Spring 管理，不引入旧 Spring Boot Starter。
- 本地回调只在半消息成功后执行。`ArchiveEvent` 白名单只携带完整用户文本、最终助手文本、身份、Epoch、序号、事件 ID、时间及校验摘要。没有登录 Token、模型 Key、工具上下文或通话票据字段；自然语言正文不属于自动脱敏服务。
- 每个会话的锁、热点、事务账本沿用相同 Redis hash tag。脚本先验证/编码，再用单条 `MSET` 同时改变热点和提交证据，避开 Lua 运行错误不会自动撤销早先写入的问题。之后设置热点 TTL；若 TTL 设置结果不明，账本仍可证明提交。
- 首次启动主动加载路由并注册生产者心跳，其他实例无需先发送一条聊天消息才能接管回查。生产组必须跨实例一致，消费组在同一归档服务间一致。

## 必需的部署配置

M02 支持**独立的单节点 Redis 部署**；脚本同槽，但增量扫描尚未实现 Cluster 各主节点遍历，因此显式 Cluster 配置会拒绝启动。需要 Redis AOF、`appendfsync always`、正数 `maxmemory`、`maxmemory-policy noeviction`；启动会检查这些配置，账号须有对应 `INFO persistence` 和 `CONFIG GET` 权限。仅有副本不能替代持久化保证，磁盘损坏或永久丢失仍需恢复对账。

在**首次全新部署、尚无流量**时，由运维创建持久的 generation 标记，应用不会自动创建或覆盖：

```text
SETNX suts:agent:memory:transactions:generation <your-deployment-generation>
```

所有实例使用同一个 generation。标记丢失或变化会阻止新归档、使回查保持 UNKNOWN。Redis 永久丢失后不能直接重建标记放行，需要先排空/核查 MQ、数据库、备份和生产检查点，确定恢复范围；数据库最大序号或某一时刻积压为零不足以证明完整。

私有配置示例，保留值需按实际 Broker 回查、消费重试、系统处置 Topic 和审计期限填写，**不提供随意的保留天数**：

```yaml
suts:
  ai:
    memory:
      archive:
        enabled: true
        jdbc-url: ${AI_MEMORY_ARCHIVE_JDBC_URL}
        username: ${AI_MEMORY_ARCHIVE_USERNAME}
        password: ${AI_MEMORY_ARCHIVE_PASSWORD}
        migrate: false # 部署前已完成 M01 Schema 迁移；缺表/校验失败会拒绝启动
      transactions:
        enabled: true
        accepting: true
        consumer-enabled: true
        name-server: ${AI_MEMORY_MQ_NAME_SERVER}
        topic: ${AI_MEMORY_MQ_TOPIC}
        producer-group: ${AI_MEMORY_MQ_PRODUCER_GROUP}
        consumer-group: ${AI_MEMORY_MQ_CONSUMER_GROUP}
        generation: ${AI_MEMORY_REDIS_GENERATION}
        recovery-window-seconds: ${AI_MEMORY_RECOVERY_WINDOW_SECONDS}
        evidence-retention-seconds: ${AI_MEMORY_EVIDENCE_RETENTION_SECONDS}
```

Topic 预先创建，类型为 `TRANSACTION`，禁用自动创建业务 Topic；不要复用其他业务 Topic/组名。客户端适配固定版本的 Broker 协议，使用可信私网和 Broker 侧访问控制部署；本版没有新增 ACL 凭证/TLS 配置适配，不将本地无认证测试当作公开网络生产部署方案。MQ 异常只记录类型，正文及凭证不进入应用失败日志；Broker 自身可能记录完整失败消息，需单独管理日志权限和保留。

| 参数（`suts.ai.memory.transactions.*`） | 默认值或要求 |
| --- | --- |
| `enabled` | false；关闭时不创建 MQ 客户端和后台扫描器 |
| `accepting` | true；false 停止前台事务准入，消费者/回查/对账继续运行 |
| `consumer-enabled` | true；false 可部署生产/回查节点，须另有消费者 |
| `name-server/topic/producer-group/consumer-group/generation` | 必填 |
| `recovery-window-seconds/evidence-retention-seconds` | 必填正数；后者至少覆盖前者，运维保证其覆盖实际 Broker、重试、对账与审计窗口 |
| `max-pending-per-conversation` | 100；待落库轮次达到阈值先拒绝新请求 |
| `max-evidence-per-conversation` | 1000，且大于 pending 上限 |
| `max-ledger-bytes` | 4 MiB；准入保留一条消息所需空间，脚本再次校验实际大小 |
| `max-message-bytes` | 256 KiB，上限 1 MiB，须小于 Broker 实际消息限制 |
| `send-timeout-ms/max-reconsume-times` | 5000 ms / 16；半消息发送不自动重试 |
| `redrive-after-ms/reconcile-interval-ms/reconcile-batch` | 60000 / 5000 / 50 |

消息和账本的字节预算均独立于原有上下文字符预算。Lua/cjson 使用精确可序列化的整数范围，Epoch/head 上限为 9,999,999,999,999；达到上限即拒绝新增。原有一天滑动热点 TTL 保留，**不是模型厂商缓存 TTL**；厂商 TTL、Token 水位和摘要属于 M03。

证据保留与重投领取使用应用实例时间戳；部署须保持实例时钟同步，避免时钟大幅跳变导致提前清理或延迟重试。保留窗口是运行配置承诺，应用不会从厂商模型缓存 TTL 推导它。

## 状态与故障处理

响应 JSON 仍只有 `text/clientCommand/commandData`，可选响应头 `X-Agent-Memory-Status`：

| 值 | 含义 |
| --- | --- |
| `accepted` | 半消息流程已执行，Redis 本地提交已确认；不代表 MySQL 已落库，也不保证最终 COMMIT 单向通知必然送达 |
| `pending` | 发送返回不明确，但共享证据已证明本地提交，交由回查/对账继续 |
| `degraded` | 本轮记忆未确认接受；保留已完成的工具结果，并追加保存状态说明 |
| `cached` | 原 Redis 模式已保存热点快照，没有事务归档承诺 |
| `disabled` | 当前写入冻结 |

请求进入模型/工具前检查会话租约、generation、Topic 路由、真实 Broker RPC 和账本容量。依赖不可用或历史暂不完整时返回原 `Result` 包装中的业务码 `503`，HTTP 包装规则不变。预检不能保证稍后提交不失败；此时不重放已完成的求助或客户端动作。

回查只根据共享账本判断：同事件/内容/序号的 COMMITTED → COMMIT；明确 ABORTED → ROLLBACK；不存在、损坏、Redis 超时/不可用 → UNKNOWN。旧租约/版本冲突可写明确中止证据；无法安全记录中止则保持 UNKNOWN。回查不依赖原进程的 Map。

事务账本本身不设置热 TTL。未落库事件保留完整不可变消息体；SQL 确认后删除其中的正文，保留事件摘要与结果，保留期从确认时开始，重复回执不延长。后续提交只清理已确认归档或明确中止且超过保留期的证据。空闲账本元数据保留到受控释放；总内存由独立 Redis 的 maxmemory/noeviction 限定，容量耗尽会拒绝新请求，不能无限接收积压。

MySQL 故障返回 `RECONSUME_LATER`；数据库提交后 ACK 丢失，重复消费由 `memory_log` 去重。接近重试上限会记录 error/计数，耗尽由 Broker 进入 `%DLQ%<consumer-group>`，不静默确认坏消息。后台增量 SCAN 每轮最多处理配置数量的会话/事件，以共享 Lua 领取重驱动租约，避免多个实例同时不断重投。它先核查已提交消费日志：已落库则修复回执，否则仅对有 COMMITTED 证据的完整事件重新发送事务消息；事件 ID 和内容保持不变。原始 DLQ 消息仍需运维审计，不由对账器删除。

Broker 回查不是无限的。5.4.0 达到 `transactionCheckMax` 后将半消息移入 `TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC`（实际名称 `TRANS_CHECK_MAX_TIME_TOPIC`）；不要只按日志中省略下划线的名字查询。对账可以恢复仍有本地提交证据的事件，不能凭缺失记录猜测旧事务成功。

`TransactionMetrics.snapshot()` 和周期日志提供提交/中止/UNKNOWN、回查、消费失败、回执失败、重投、容量拒绝、重试耗尽风险、超恢复窗口观察次数和最大已观察 pending 年龄。它们是进程计数/峰值，不是部署全局实时积压。运维还需监控实际消费积压、DLQ、系统处置 Topic、Redis 内存/持久化和数据库 `highest_seen/archived_through` 差距；当前没有接入 Actuator/告警平台。

## 恢复、启用与回退

1. 完成 M01 Schema 和数据导入核验；旧窗口已经裁剪的历史无法补回。首次部署 generation 只在确认新建/恢复状态后设置。先启动 `transactions.enabled=true, accepting=false, writes-enabled=false` 的消费者/回查/对账节点，再开启前台接收。
2. 已有 M01/Redis 会话不会自动猜测生产 head。冻结所有写入/消费者/回查并确认排空后，对已独立核实的检查点执行下述 `ADOPT`。数据库必须连续无缺口且等于检查点；已有热点须与数据库快照一致；已有事务账本不允许覆盖。先 dry-run，再执行。
3. 普通热缓存过期、账本还在时，使用独立生产 head 核验 MySQL 连续水位，再仅恢复缺失热点；存在更新热点不回填旧快照。数据库仍落后时拒绝本次对话，等待消费/重驱动后重试。
4. 回退先设 `accepting=false`，排空未决事务、重试和消费，确认每个会话 `head=archivedThrough` 且没有未落库事件。保留证据直至已配置回查/重试/审计期限结束，使用 `RELEASE_TO_CACHE` 受控释放。热点不存在时从已核验数据库恢复；热点不同、未过保留期或检查点变化都拒绝释放。数据库历史保留。
5. 完成所有相关会话释放后关闭事务模式，才恢复 Redis-only 写入。当前版本阻止 Redis-only 写入和旧 M01 维护命令越过尚存的事务证据；不要同时运行两种写路径。重新启用需再次核验/ADOPT。旧二进制不具备新门禁，部署隔离由操作者保证。

维护命令在 **Agent 目录**，沿用私有配置，必需显式参数示例：

```sh
java -jar target/agent-0.0.1-SNAPSHOT.jar \
  --spring.config.additional-location=file:/absolute/private/agent-secrets.yaml \
  --spring.main.web-application-type=none \
  --spring.cloud.nacos.discovery.enabled=false \
  --suts.ai.embedding-enabled=false \
  --suts.ai.memory.writes-enabled=false \
  --suts.ai.memory.archive.enabled=true \
  --suts.ai.memory.transactions.enabled=false \
  --suts.ai.memory.transactions.maintenance.enabled=true \
  --suts.ai.memory.transactions.maintenance.action=ADOPT \
  --suts.ai.memory.transactions.maintenance.user-id=<owner> \
  --suts.ai.memory.transactions.maintenance.conversation-id=<server-conversation-id> \
  --suts.ai.memory.transactions.maintenance.epoch=<verified-epoch> \
  --suts.ai.memory.transactions.maintenance.producer-checkpoint=<independently-verified-head> \
  --suts.ai.memory.transactions.maintenance.writers-drained=true \
  --suts.ai.memory.transactions.maintenance.dry-run=true
```

将 action 改为 `RELEASE_TO_CACHE` 执行回退核验，需要私有配置包含真实 generation 和两个保留参数。dry-run 不提交检查点或释放证据；明确核验后设 `dry-run=false`。命令记录 action、checkpoint、dryRun、结果，不输出正文。禁止与向量迁移或 M01 维护进程同时执行。

## 测试与边界

```sh
bash agent/src/test/fixtures/memory-transactions/start.sh
mvn -B -ntp -f agent/pom.xml verify -Pmemory-transactions \
  -Dmemory.transactions.fixture=agent-transactions-integration-only
docker compose -f agent/src/test/fixtures/memory-transactions/compose.yaml down --volumes
```

Windows 可按 compose 文件启动同名私有测试项目，并执行 start.sh 中的 `SETNX` 和预建 Topic；PowerShell 中给带点的 Maven `-D` 参数加单引号。测试只接受固定本机端口、MySQL 标记、Redis generation 和独立 Broker 名；HTTP 故障测试会重启该私有 Redis 并停止/重启该私有 Broker，不操作用户服务。Compose 的保留期、回查 2 秒/5 次仅用于加速测试，不是生产建议。

M02 不提供客户端跨 HTTP 重试的业务工具幂等、不改变 HTTP 超时后的任务取消、不替代既有长期向量尽力写入，也未实现 M03 摘要/Token 水位/厂商 TTL。使用固定模型 HTTP 响应验证协议与记忆传递，不宣称测得真实模型质量或费用收益。证据见 [VALIDATION-M02.md](VALIDATION-M02.md)。

协议依据：[RocketMQ 事务消息](https://rocketmq.apache.org/docs/4.x/producer/06message5/)、[5.4.0 回查耗尽源码](https://github.com/apache/rocketmq/blob/rocketmq-all-5.4.0/broker/src/main/java/org/apache/rocketmq/broker/transaction/queue/DefaultTransactionalMessageCheckListener.java)。
