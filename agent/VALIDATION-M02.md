# M02 本地验证记录

日期：2026-10-18。基线：上游 main `07ceb103`（已合并 M01 / PR #30）。分支：`feat/agent-memory-transactions`。

## 变更范围

接入 RocketMQ 事务消息的半消息 ACK、本地 Redis 原子提交、跨实例回查、MySQL 幂等消费、Redis 回执修复与有界对账重投。增加记忆状态响应头、故障准入、原有写路径/维护命令保护、独立检查点接管与回退释放命令、真实基础设施 CI。

原 Spring AI ChatClient/工具/Advisor/窗口继续工作；消费端复用 M01 的 JdbcChatMemoryRepository 同事务快照与完整轮次归档。本 PR 不新增 Outbox、前台同步 SQL 写入、摘要或模型厂商 TTL 策略。

## 环境与隔离

- Maven 3.9.11，Microsoft Java 17.0.20.1、本机 Java 21；编译目标 Java 17。
- Boot 4.0.8、Spring AI 2.0.1、RocketMQ 客户端/Broker 5.4.0、MySQL 8.0.46、Redis 8.2.1。
- Docker Compose 独立项目 `suts-agent-memory-transactions-it`；私有固定端口、MySQL 测试标记、Redis generation、独立 Topic 和 Broker 名均核验后才写入。Redis 使用 AOF / always / noeviction / 128 MiB，Broker 使用同步刷盘，测试回查间隔与超时均 2 秒、最多 5 次。
- 另起独立 Redis Query Engine/JSON 容器验证向量路径；本地 Redis 可执行文件只启动自己的临时进程。
- 模型为本机 HTTP 脚本响应，工具协议使用真实 Spring AI 循环；志愿接口使用替身。未调用真实模型供应商或实际志愿业务，没有读取私有 API Key。

## 验证命令与结果

以下省略本机 Maven settings/JDK 设置；PowerShell 需给组合 profiles 和带点的 `-D` 参数加引号。

| 环境 | 验证 | 结果 |
| --- | --- | --- |
| Java 17 | `mvn -B -ntp -f agent/pom.xml clean verify -Plocal-redis,mysql-archive,redis-vector,memory-transactions -Dredis.server.executable=<private executable> -Dmysql.archive.port=13316 -Dredis.vector.port=16392 -Dmemory.transactions.fixture=agent-transactions-integration-only` | 231 普通测试＋53 集成测试全部通过（8 私有 Redis＋12 MySQL＋8 向量＋18 事务＋7 HTTP） |
| Java 21 | `mvn -B -ntp -f agent/pom.xml clean verify -Pmysql-archive,redis-vector,memory-transactions -Dmysql.archive.port=13316 -Dredis.vector.port=16392 -Dmemory.transactions.fixture=agent-transactions-integration-only` | 231 普通测试＋44 集成测试通过；当时 HTTP 集成含 6 项 |
| Java 21 补充 | `mvn -B -ntp -f agent/pom.xml test-compile failsafe:integration-test failsafe:verify -Pmemory-transactions -Dit.test=TransactionApplicationIT#mysqlOutageAcceptsWarmTurnsAndArchivesAfterRecovery -Dmemory.transactions.fixture=agent-transactions-integration-only` | 新增的真实 MySQL 停机 1 项通过；Java 21 共验证 45 项不同的集成用例 |
| Picture / Java 17 | `mvn -B -ntp -f picture/pom.xml clean verify` | 8 项通过 |
| 全新容器 / Java 17 | 删除本次私有测试项目的容器/卷后，以 CI 的 `start.sh` 重建，再单独执行 `memory-transactions` Failsafe 验证 | 18 事务＋7 HTTP，共 25 项通过，启动/迁移不依赖先前测试数据 |
| 实际 JAR / Java 17 | 私有基础设施上的 ADOPT dry-run、ADOPT、RELEASE_TO_CACHE dry-run、RELEASE_TO_CACHE | 四个独立进程均 exit 0、checkpoint=1、result=true；最终热点存在、事务账本已受控释放，数据库保留 |

Java 17 的最终全量运行包含实际 Cluster 连接门禁和真实 MySQL 停机用例。Java 21 在全量验证后补跑新增 MySQL 停机用例，统计按不同用例计数，不重复计入先前已通过的测试。

## 关键故障证据

- 半消息发送失败不会执行 Redis 本地回调；发送返回不明但共享提交证据存在时返回 pending。消息超预算在发送前拒绝；Redis-only 默认模式不创建 MQ/SQL 依赖。
- 真实 Lua 验证旧租约中止、重复提交、同 ID 内容冲突、错误 Key 类型无部分提交、连续 head 和回执不回退。单条 MSET 同时提交热点与证据；账本没有普通热点 TTL。
- 真实 Broker UNKNOWN 回查后继续消费；关闭原生产者客户端，由同组另一个客户端实例读取共享证据并完成回查。测试捕获并修复了“新实例还没发送过消息、未注册回查心跳”的问题；这里未强杀整个 Agent JVM 或模拟主机断电。
- 真实 Broker 经过 5 次回查后，把未决半消息移入 `TRANS_CHECK_MAX_TIME_TOPIC`；测试实际拉取该系统 Topic 核对事件。保留的 COMMITTED 证据随后以相同事件 ID 重投，完成归档。
- 热点过期后完整待落库正文仍在账本，后台重新发送事务消息并归档；只按独立 producer head 与 SQL 连续水位匹配恢复，不把最高已见序号当完整性证明。
- SQL 提交后回执丢失，依据 APPLIED 消费日志修复回执而不重发；重复/乱序消费和数据库事务整体回滚继续由 M01 的真实 MySQL/H2 契约验证。
- MySQL 异常注入验证同事件重试；新增 HTTP 用例进一步停止真实 MySQL：已有热点会话前台继续接受新轮次，数据库重启后补齐连续归档。首次会话/冷恢复仍可能需要 SQL 查询，不把数据库完全移出所有前台路径。
- 完整 Boot/Tomcat/auth HTTP 集成覆盖同步与异步响应头、JSON 字段兼容、已完成求助在记忆失败后不丢失/不重放、停止准入、冷缓存恢复、Redis AOF 持久化重启、真实 Broker 停机时模型/工具前拒绝。
- 缺失/损坏/超时证据保持 UNKNOWN；generation 错误拒绝准入。真实 Redis 重启保留证据；没有伪造“Redis 永久丢失后仍能凭 SQL MAX 自动完整恢复”的验证结果。
- 未归档正文不因证据保留期清理；归档回执后才起算保留期。待归档数量/字节达到阈值拒绝新工作；多节点共享重投租约；坏会话不阻断同批其他会话。
- 受控接管要求冻结/排空、连续检查点与现有热点核验。回退释放要求 SQL 已确认全部接受轮次、无 pending、证据超过保留期；dry-run 不改数据。旧 M01 维护及 Redis-only 写入不能越过尚存的事务账本。
- 最终 JAR 中仅有 OkHttp 4.12.0 与 Okio JVM 3.6.0；排除了 RocketMQ 间接引入的另一份 OkHttp 5 / 旧 Okio JVM，实现与原 OpenAI SDK 的实际 HTTP 回归兼容。

## CI 与范围边界

新增 `verify-memory-transactions` 的 Java 17/21 矩阵，自动创建并清理私有 Redis/MySQL/RocketMQ，上传 Surefire/Failsafe 报告；保留原有 Java 17/21、MySQL、向量、Picture job。本地通过不能代替远端 Actions 状态，推送后以 GitHub 结果为准。

未执行生产迁移、真实供应商质量/费用评测、公开网络 ACL/TLS、Redis Cluster、磁盘永久损坏恢复或跨 HTTP 请求的业务幂等验证。M02 的单节点 Redis 前提、保留参数、观测计数含义和回退步骤见 [ARCHIVE-M02.md](ARCHIVE-M02.md)。
