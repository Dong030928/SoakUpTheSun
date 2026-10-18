# M03：双水位异步摘要与长期检索

在 M01 原始轮次归档和 M02 事务消息上增加摘要，默认关闭。不改聊天响应的三个字段或 `X-Agent-Memory-Status` 含义，不新增业务工具、模型供应商、向量数据库或 Outbox。

## Spring AI 在这里做什么

- 前台仍使用 `ChatClient`、`MessageChatMemoryAdvisor` 和请求内的 `MessageWindowChatMemory`。开启摘要后，窗口按本次 Token 预算准备，不再先截成旧的 20 条消息。原始轮次通过 M02 单独提交，不把视觉参考、摘要提示和工具协议当作用户原文保存。
- 新增 `SummaryBudgetAdvisor`，排在 `ToolCallingAdvisor` 内侧，每次真正请求模型前重新计入工具 Schema、参数、工具结果、系统提示、当前输入、视觉/长期参考、摘要和完整历史轮次。超过预算会停止继续调用；已完成业务动作仍沿用现有兜底返回。
- 后台通过另一个**没有工具和工具 Advisor** 的 Spring AI `ChatClient`，生成“旧摘要＋新增旧轮次”的继承式摘要。设置输出上限，拒绝空、超长或带工具调用的结果。
- `JdbcChatMemoryRepository` 仍维护 M01 的兼容快照；完整原文和摘要/任务分开保存。长期向量继续复用 `EmbeddingModel`、`RedisVectorStore`、`VectorStorageService` 和 `ChatMemoryManager`。
- MQ 事务、Redis Lua、任务幂等和恢复是应用自己的代码，不是 Spring AI 提供的跨存储事务。

## 预算与触发

显式配置模型容量 C、输出预留 O、安全余量 S、输入预算 B，以及 `0 < L < H < B`，并满足 `B + O + S <= C`。没有生产水位、模型容量或 TTL 默认值。

| 情况 | 行为 |
| --- | --- |
| 缓存有效，输入低于 B，即使未覆盖原文超过 H | 保留完整可容纳前缀，不因超过 H 摘要 |
| 厂商 TTL 到期且未覆盖原文超过 H | 异步摘要较旧完整轮次，保留约 L 的近期原文 |
| 厂商 TTL 到期，未覆盖原文不超过 H | 不为到期本身调用摘要模型 |
| 请求/写入达到 B，或达到原文数量安全上限 | 提交摘要；前台按完整轮次有界选取，当前输入和固定内容放不下时明确拒绝 |

水位计算读取所有未被有效摘要覆盖的完整轮次，不读取裁剪后的 JDBC/Redis 20 条兼容快照。达到消息正文、摘要模型输入或 MQ 上限时，可分成连续的小范围任务；不能切断一个轮次伪装成完整输入。单个不可分轮次本身过大时保留原文，需要提高经确认的预算或人工处理，不自动丢弃它。

当前 DeepSeek 模型没有配套的精确 tokenizer，因此 `MemoryTokenBudget` 使用明确标记的 UTF-8 字节保守估计、消息/协议开销和安全余量，并用实际 usage 向上校准。它不是厂商 Token 数的数学保证，也没有声称固定节省 62%。工具循环中的后续输入同样要通过校验。

## 厂商 TTL 与动态策略

TTL 策略按 `provider/model/mode` 精确匹配。每条需填写版本、厂商依据、续期规则；使用持续时长时还要显式填写计时起点：

- `CREATION`：按首次确认创建时间计算，不随每条消息滑动。
- `CACHE_HIT`：只在 Spring AI usage 明确报告缓存命中时续期；没有命中数据不会猜测命中。
- `PROVIDER_EXPIRY`：只接受适配器传入的实际到期时间戳，不从时长推断。
- `clock-origin` 可选 `REQUEST_STARTED` 或 `RESPONSE_COMPLETED`，应与厂商公开规则一致；不会擅自把模型耗时追加到厂商从请求开始计时的 TTL。

共享状态保存匹配的供应商/模型/模式、策略版本、依据、时长、规则、计时基准、创建/命中时间及实际计算到期时间。每次准备和扫描重新绑定 Spring `Environment`：同一计时规则只修改 TTL 时，用已保存的创建/命中基准重算；模型、缓存模式或计时规则变化时清除旧时钟，等待新观测。来自旧摘要前缀的迟到响应不能把新摘要标成已缓存。

DeepSeek 的 `prompt_cache_hit_tokens` 在本次锁定的 Spring AI 2.0.1 中没有直接填入统一缓存计数。本 PR 为非流式 HTTP 响应补充兼容映射，再读取框架保留的 native usage；优先保留厂商已有的 `prompt_tokens_details.cached_tokens`，缺失/非法字段不伪造命中。Token 校验和缓存观测位于工具循环内，每次供应商请求分别记录起止时间。策略在半消息 RPC 或模型调用期间更新时，旧触发决定/迟到观测不能沿用旧配置登记新任务或重启新策略时钟。

这不代表直接修改本地 YAML 后应用会监视文件自动刷新：文件修改需正常重载/重启，运行中 `Environment` 的配置源更新会在下一次校验生效。本 PR 没有开放配置管理 HTTP 接口。

当前实际模型适配器为 DeepSeek，没有厂商实际过期时间返回值映射；因此 `PROVIDER_EXPIRY` 在该适配器上不会凭空生成到期时间。未填写厂商策略时，仅关闭 TTL 触发，B 的上限处理仍可工作。不同供应商的缓存保留承诺不能互相套用；[DeepSeek 官方缓存说明](https://api-docs.deepseek.com/guides/kv_cache/)也不能作为任意固定 45 分钟的依据。

## 事务投递、持久任务与合并

```text
达到触发条件
  -> 固定 Epoch/baseVersion/连续范围，生成稳定 jobId 与不可变完整输入
  -> RocketMQ 半消息 ACK
  -> Lua 同时登记 Redis pending 意图和共享 COMMITTED 证明
  -> MQ COMMIT / 其他生产者回查共享证明
  -> 消费端登记 agent_memory_job，等待完整归档输入并核对原文 hash
  -> 获取数据库 worker 代次与租约，模型调用在数据库事务外执行
  -> 持久化 RESULT（摘要正文与 usage）
  -> Lua CAS 合并摘要，只裁剪已归档且在固定范围内的旧热轮次
  -> 幂等更新 MySQL 摘要投影，标记 MERGED，解除该会话摘要槽位
  -> 写入向量、清理旧版，标记 DONE / SKIPPED / OBSOLETE
```

新增 Flyway `V2__conversation_summary.sql`：`agent_memory_summary` 是当前投影；`agent_memory_job` 保存输入、结果、worker/lease、重试时间、阶段、向量状态、usage 和失败类型。V1 不变，原始 `agent_memory_turn` 不因摘要删除。

M03 完整热轮次与 M02 事务账本、旧版兼容快照在同一条 Lua `MSET` 中提交，避免“归档已接受但摘要读取缺少这轮”的双写窗口。摘要任务正文含旧摘要和完整范围，不是临时 Key。未完成归档或 hash 不符时重试，不调用模型生成缺片摘要。

Lua 合并核对 Epoch、baseVersion、连续输入、worker 代次/租约和归档水位；CAS 重读后保留摘要期间追加的新轮次，head 增长不会使固定旧范围失效。结果先入库，再执行合并；合并响应丢失时按 jobId/version 确认，修复投影，不重复调用模型。向量失败保留 `MERGED`，只重试向量阶段。

## 扫描与恢复边界

默认每 5 秒调度，每批最多检查 50 个会话；这些是扫描参数，不是厂商 TTL。到期 ZSet 和增量 SCAN 分配批次，避免低水位的到期项长期挡住其他会话。领取使用可续租的会话锁，不在可靠投递前删除唯一索引；SQL 任务由独立有界线程池恢复。

ZSet 只是索引。扫描共享会话/生产检查点和任务证明可重建索引；MySQL 持久任务可恢复进程退出后的处理租约、结果合并和向量阶段。同一任务的回查耗尽/投递不明通过共享证明重投，沿用 M02 的显式证据保留期限和 generation 校验，不猜测回滚。

完整摘要热状态和未决证明**不使用模型 TTL 或旧 24 小时热点 TTL**。原文/任务/证据保留独立管理；没有新增默认保留天数。达到容量且后台尚未释放空间时，在模型和工具执行前拒绝新轮次。摘要状态缺失仅在独立生产 head 与完整 SQL 水位吻合时恢复；Redis generation/生产证明永久丢失仍需人工对账，不从 SQL MAX(seq) 猜测完整性。

本次沿用 M02 的独立单节点 Redis、AOF `appendfsync always`、有界 `noeviction` 部署要求。脚本只操作同一会话 hash slot；ZSet 分开更新，不使用跨槽 Lua，不宣称已支持 Redis Cluster 扫描。跨节点时钟应同步，worker 租约需覆盖模型超时及合并时间。

## 摘要向量与兼容索引

有效摘要投影确认后才异步 Embedding。文档 ID 由会话、Epoch、摘要版本确定；元数据包含 userId、conversationId、epoch、summaryVersion、coveredFrom/To、`memoryType=conversation_summary`，保留原模型指纹/维度隔离。

检索验证 SQL 当前投影和 Redis 有效摘要版本，迟到旧文档即使还没物理删除也不会生效；摘要更新异步删除旧版本。当前用户可跨会话召回，其他用户被原有服务端过滤隔离。

[Spring AI 2.0.1 的 RedisVectorStore](https://github.com/spring-projects/spring-ai/blob/v2.0.1/vector-stores/spring-ai-redis-store/src/main/java/org/springframework/ai/vectorstore/redis/RedisVectorStore.java)只返回已声明索引字段。本实现使用一次有界 `JSON.MGET` 补读新增来源字段，兼容已有 JSON 索引，不重建或清空索引，也不强制执行 `FT.ALTER`。Redis 账号需允许 `JSON.MGET`。同源完整轮次与有效摘要按覆盖范围去重，旧记录没有来源信息时只做原有文本去重。

原有整轮长期记忆的异步写入仍是尽力而为；本 PR 给新写入补充来源，未把它改造成另一套持久队列。摘要向量由持久 job 跟踪。禁用 Embedding/向量/向量写入时标记 `SKIPPED`，短期摘要仍能使用；跳过后重新启用不会自动补做已完成任务，需要显式安排补齐。

## 配置与发布

先暂停新流量并排空 M02 在途任务，准备 V2 Schema；把消费/回查实例统一部署为 `summary.enabled=true, summary.accepting=false` 并提供预算配置，确认都识别 `conversation-summary-v1` 后，再开启 `summary.accepting` 和业务流量。仅更新二进制但保持 `summary.enabled=false` 不会创建摘要处理器。生产组/消费组遵守 M02 的一致性要求，禁止同一消费组混用不同 TAG 订阅；生产环境可以继续用同一事务 Topic。测试使用独立 `SutsMemorySummaryIT`，避免干扰 M02 的协议故障用例。这不是新旧模式任意混跑的零停机承诺。

通过既有 Flyway 部署流程执行 V2，或在受控迁移进程临时设置 `archive.migrate=true`；确认后恢复 `false`。新二进制开启 JDBC 时会校验 V2，不隐式带病启动。

下面全部模型参数需由部署者确认后提供，示例不替你填写生产水位：

```yaml
suts:
  ai:
    memory:
      archive:
        enabled: true # 连接、凭证与 V2 迁移见 M01/M02
      transactions:
        enabled: true # Broker、generation、证据保留等沿用 M02
      summary:
        enabled: true
        accepting: true
        model-capacity-tokens: ${AI_MEMORY_MODEL_CAPACITY_TOKENS}
        output-reserve-tokens: ${AI_MEMORY_OUTPUT_RESERVE_TOKENS}
        safety-tokens: ${AI_MEMORY_SAFETY_TOKENS}
        input-budget-tokens: ${AI_MEMORY_INPUT_BUDGET_TOKENS}
        low-tokens: ${AI_MEMORY_LOW_TOKENS}
        high-tokens: ${AI_MEMORY_HIGH_TOKENS}
        summary-max-tokens: ${AI_MEMORY_SUMMARY_MAX_TOKENS}
        summary-max-chars: ${AI_MEMORY_SUMMARY_MAX_CHARS}
        worker-lease-ms: 120000 # 默认；需大于模型 timeout + 15000ms
        # 没有厂商 TTL 依据时省略 cache-policies，硬上限照常工作。
        # cache-policies:
        #   - provider: deepseek
        #     model: ${AI_MODEL_NAME}
        #     mode: automatic
        #     version: ${AI_CACHE_POLICY_VERSION}
        #     vendor-basis: ${AI_CACHE_VENDOR_BASIS}
        #     renewal: ${AI_CACHE_RENEWAL_RULE}
        #     clock-origin: ${AI_CACHE_CLOCK_ORIGIN}
        #     ttl-seconds: ${AI_CACHE_VENDOR_TTL_SECONDS}
```

摘要最大字符数还需放得进现有向量文档预算（默认 6000）。其他容量参数：`max-raw-turns=1000`、`max-state-bytes=8388608`、`max-input-chars=262144`、`retry-delay-ms=10000`；它们限制资源使用，不表示数据保留时间。状态需留出完整轮次和待处理消息空间，SQL/MQ/Redis 故障持续期间可能触发准入拒绝。

日志仅写阶段和异常类型；SQL `last_error` 记录异常类型，正文、Key 和认证信息不放进错误标签。进程计数可观察摘要尝试、失败和完成；持久任务可按 state/vector_state/retry_at/lease_until 排查积压。无新增公开管理接口或完整监控平台。

## 停止、回退与保留

先设 `summary.accepting=false` 停止创建新摘要，保留处理/恢复/向量线程，排空半消息、`READY/RESULT/MERGED` 和重试任务，核对 SQL 投影、Redis version/coveredThrough 与生产 head。该开关不取消正在处理的模型调用。

需要先回退上下文效果时，再设 `summary.use-summary-context=false`，按原 `memory.max-messages/max-message-chars/max-context-chars` 使用有界近期窗口，仍校验 B，保留原文、摘要和版本。设 `summary.vector-enabled=false` 可单独停用摘要候选及待写阶段，不影响原有轮次候选；该阶段明确记录 `SKIPPED`。这两个开关不删除存储，也不等于已切换到旧二进制；停摘要后长时间持续写入仍会受原文容量准入限制。

关闭整个摘要模块不等于已经迁移回旧格式。新版本旧写入路径和 M01/M02 维护命令检测到摘要状态/证明会拒绝绕过；归档清理也拒绝未完成任务和有效摘要投影。需要冻结并排空全部写入，备份原文、持久结果与索引，再制定显式的完整近期窗口导出/摘要停用迁移。本 PR 没有提供自动降级旧二进制的一键迁移器，不能通过删除 Redis Key 解除门控。已写入的向量不会因应用回退自动撤销。

验证命令与本次结果见 [VALIDATION-M03.md](VALIDATION-M03.md)。此前规则见 [ARCHIVE-M01.md](ARCHIVE-M01.md)、[ARCHIVE-M02.md](ARCHIVE-M02.md)。
