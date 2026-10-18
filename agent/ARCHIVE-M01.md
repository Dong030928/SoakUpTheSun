# M01：JDBC 会话归档与恢复基础

本 PR 提供 M02 消费端需要的归档、快照、幂等日志和离线恢复服务。**当前聊天仍只提交 Redis，不自动异步归档，也不在前台同步写 MySQL。** RocketMQ 半消息、回查、生产端序号/检查点和接受状态在 M02 接入；厂商 TTL、双水位、摘要与摘要向量写入在 M03 实现。本 PR 不更改这些策略。

## Spring AI 如何参与

- `ConversationMemoryStore.Session` 抽象现有会话处理权和框架仓库，Redis 实现继续使用续约租约和 Lua fencing。
- `JdbcChatMemoryRepository` 保存有界、按 `sequence_id` 排序的 USER/ASSISTANT 快照。它与原始轮次、`memory_log` 共用一个 DataSource/事务管理器。
- 完整历史另存 `agent_memory_turn`，不让框架窗口淘汰删除原始轮次。框架默认 `conversation_id VARCHAR(36)` 容纳不了本项目 ID，因此通过 Flyway 管理 160 字符的快照字段及其他表。
- 不引入 JDBC 记忆 Starter，不让自动配置替换当前 ChatMemory，也不引入 Outbox。新增 JDBC/Flyway 组件仅在显式启用时连接数据库。

## 数据语义

`ArchivedTurn` 是给未来受信任生产者/消费者使用的内部事件：事件 ID、认证用户、服务端会话 ID、Epoch、序号、原始输入、最终回复、时间。它不是 HTTP 接口或模型工具；不得从模型参数获取身份。

`JdbcConversationArchive.archive` 在短事务中锁定会话、检查 Epoch/归属、插入完整轮次、推进连续落库水位、更新框架快照并插入 `memory_log(APPLIED)`。相同事件 ID 和摘要返回 duplicate；相同 ID 的不同内容、同位置不同事件或错误 Epoch 被拒绝。

消费者可能先收到第 2 轮：此时 `highestSeen=2`、`archivedThrough=0`，不能恢复为完整对话。第 1 轮落库后才推进至 2；快照只包含连续部分。原始历史不受快照消息/字符窗口裁剪。最大前向缺口默认 10000，可配置，以限制单事务扫描。

数据库事务使用 READ_COMMITTED，避免等待行锁后仍从旧一致性视图读消息。首次并发建会话可能发生数据库瞬态锁冲突，调用方应重试**同一事件**；M02 将由消费者重试实现。事务不跨模型、Redis 或远端业务调用。

进程级 `statistics()` 提供成功事件、重复事件、失败调用及累计归档耗时，不含用户标签。它不代表跨节点总量或端到端 P95；离线导入报告另行统计，不重复计为在线消费。

## 可选配置

默认 `AI_MEMORY_ARCHIVE_ENABLED=false`，无需数据库即可启动现有服务。准备独立 Agent Schema 后，可在外部配置中加入：

```yaml
suts:
  ai:
    memory:
      archive:
        enabled: true
        jdbc-url: jdbc:mysql://127.0.0.1:3306/agent_archive?connectTimeout=5000&socketTimeout=10000
        username: ${ARCHIVE_DB_USERNAME}
        password: ${ARCHIVE_DB_PASSWORD}
        migrate: false
        max-sequence-gap: 10000
```

对应环境变量：`AI_MEMORY_ARCHIVE_ENABLED/JDBC_URL/USERNAME/PASSWORD/MIGRATE`。全名例如 `AI_MEMORY_ARCHIVE_JDBC_URL`。

第一次部署显式设置 `migrate=true`，由 Flyway `agent_memory_schema_history` 执行 `db/archive/V1__conversation_archive.sql`；后续建议设回 false，只校验准备好的 Schema。初始化要求独立空 Schema，生产配置不自动 baseline 已有未知表。Schema 不存在/不匹配、启用却缺少凭证时启动失败，不隐式降级。

MySQL 8.0.46 已在私有容器验证；驱动和 Flyway 版本由 Boot BOM 管理。生产 TLS 与账号权限由部署配置提供，示例未关闭 TLS。不要把数据库密码放到 JDBC URL、源码或 Git。

## 离线导入、恢复和保留清理

所有操作必须停止并排空**全部实例/消费者**，使用非 Web 进程、`memory.writes-enabled=false` 和明确 `writers-drained=true`。这两个本地标记不会替你停止其他实例。默认 dry-run=true；只打印动作和计数，不打印历史、账号或票据。

完整示例参数（使用正常外部服务配置启动 JAR；示例身份需换为操作对象）：

```powershell
$archiveArgs = @(
  '--spring.main.web-application-type=none',
  '--suts.ai.memory.archive.enabled=true',
  '--suts.ai.memory.writes-enabled=false',
  '--suts.ai.memory.archive.maintenance.enabled=true',
  '--suts.ai.memory.archive.maintenance.action=IMPORT_RECENT',
  '--suts.ai.memory.archive.maintenance.user-id=42',
  '--suts.ai.memory.archive.maintenance.conversation-id=user:42:REPLACE_WITH_64_HEX_SESSION_HASH',
  '--suts.ai.memory.archive.maintenance.epoch=1',
  '--suts.ai.memory.archive.maintenance.writers-drained=true',
  '--suts.ai.memory.archive.maintenance.dry-run=true'
)
java -jar agent/target/agent-0.0.1-SNAPSHOT.jar @archiveArgs
```

- **IMPORT_RECENT**：导入 Redis v2 仍保存的最近完整轮次，最多 500 轮，分配独立导入 Epoch 中的连续序号；无法补回被裁剪/过期内容。事件 ID 由归属、位置及文本确定，重复执行跳过相同数据，源窗口变化则冲突停止。整个导入在一个事务中验证，dry-run 模拟后整体回滚，不保留业务记录；会短暂持有数据库锁，并非只读查询。源时间未知记为 0，不伪造为导入时间。
- **RESTORE**：指定 `action=RESTORE`，增加 `producer-checkpoint=<独立核实的完整源序号>`。要求数据库当前最高已见序号和连续落库序号都与检查点相等、Epoch 有效；不允许从数据库 MAX(seq) 自行生成“全量已追平”证据。M01 无生产端事务检查点，故**仅提供离线恢复，不启用聊天自动回源**。
- **RETIRE**：指定 `action=RETIRE` 和 `cutoff=<明确的毫秒时间戳>`，只清理已连续归档且活动时间早于 cutoff 的整个会话原文/快照，保留无正文的消费摘要和 retired tombstone。未知旧时间记录拒绝按时间清理；不设擅自决定的 30 天策略。迟到事件不能复活该 Epoch。必须先确保无未决事务和后续任务；M02/M03 上线后需扩展清理门禁后才能复用此操作。

先审阅 dry-run，再将参数中的 dry-run **改为** false；不要重复传同一属性。在 Web 模式、同时启用旧向量迁移命令、未冻结或未确认排空时拒绝执行。成功报告后进程关闭；异常退出不代表已经执行成功。

恢复持有 Redis 租约，Lua 只在热点 Key 不存在时写入；即使现有记录损坏，也不会以较旧数据库快照覆盖它。返回 false 表示未写入。慢恢复/旧租约不能覆盖新热点。恢复后仍使用已有窗口和 TTL；模型缓存 TTL 策略留在 M03，未在本 PR 更改。

退休清理先持久化 tombstone，再删缓存；若缓存删除失败，保持写入冻结，重跑同一清理以修复，不能直接恢复流量。数据库和 Redis 之间不是原子事务。

## 验证与回退

普通测试：`mvn -B -ntp -f agent/pom.xml clean verify`，H2 只用于隔离测试，不作为生产数据库。

真实 MySQL 测试要求独立容器和标记，拒绝未标记实例；不 drop/flush 用户数据库：

```sh
docker run -d --rm --name suts-archive-it -p 127.0.0.1:13306:3306 -e MYSQL_ROOT_PASSWORD=agent-archive-integration-only -e MYSQL_DATABASE=agent_archive_it mysql:8.0.46
# 等待数据库 TCP 就绪后创建测试标记：
docker exec -e MYSQL_PWD=agent-archive-integration-only suts-archive-it mysql -h127.0.0.1 -uroot agent_archive_it -e "CREATE TABLE suts_agent_test_fixture (marker VARCHAR(64) NOT NULL PRIMARY KEY); INSERT INTO suts_agent_test_fixture VALUES ('agent-archive-integration-only');"
mvn -B -ntp -f agent/pom.xml verify -Pmysql-archive -Dmysql.archive.port=13306
docker stop suts-archive-it
```

测试夹具只有在核验标记后，才允许针对自己的新 Schema baseline 到 0；生产配置不使用此选项。Windows 下组合 profiles 要加引号，例如 `'-Plocal-redis,mysql-archive'`。真实 Redis 恢复测试沿用 `-Plocal-redis`，不依赖现有 Redis。

数据切换：冻结/排空 → 备份 → dry-run → 导入 → 对账 → 按后续 M02 方案启用写入。回退先停维护和新归档，必要时将已确认近期历史恢复成兼容 Redis v2 快照；保留数据库、消费日志和 tombstone。不通过删除 Flyway 历史或清空缓存回退。

固定评测输入见 `evaluation/memory-baseline.json`，全部为合成数据。M01 测试的是归档/恢复契约，未测定真实 LLM 的回答质量、降本比例或生产迁移效果。
