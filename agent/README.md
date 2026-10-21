# Agent

Agent 使用 Spring AI 调用 DeepSeek 对话模型与 OpenAI 兼容的 Embedding 接口，并使用原生工具调用替代三节点责任链，对外聊天协议保持不变。Picture 仍使用原 Spring Boot 3 技术栈，通过 Redis 向 Agent 提供视觉证据。

## 运行版本

| 组件 | 版本 |
| --- | --- |
| Java | 17 或 21，编译目标为 17 |
| Maven | 3.6.3+；本次本地验证使用 3.9.11 |
| Spring Boot | 4.0.8 |
| Spring AI | 2.0.1 |
| Spring Cloud | 2025.1.3 |
| Spring Cloud Alibaba | 2025.1.0.0 |
| Nacos Client | 3.1.1，由 Alibaba 管理 |
| springdoc OpenAPI | 3.0.3 |
| Jedis | 7.4.1，配套 Spring AI RedisVectorStore |

Redis 登录、视觉与短期会话使用 Spring Data Redis/Lettuce；长期记忆使用 Spring AI RedisVectorStore（RedisClient/Jedis），需要 Redis Query Engine 与 JSON 模块。本次以官方 Redis 8.2.1 容器验证。普通 Redis 可承担短期会话，缺少向量扩展时长期记忆会降级。各服务的登录、视觉缓存须连接同一 Redis 实例和数据库。

已有长期记忆的部署需按本文末尾的“离线迁移与回退”先完成存量数据演练：新版本使用独立 v2 JSON 索引，不自动读取或覆盖旧 HASH 向量。只切应用而不迁移，会暂时无法召回旧长期记忆。

可选 JDBC 归档、RocketMQ 事务归档和双水位异步摘要默认关闭，原 Redis 模式不需要 MySQL/MQ。事务模式包含共享回查证据、MySQL 幂等落库及对账重投；摘要在其上提供厂商 TTL 规则、持久任务恢复和摘要向量检索。启用条件见下文“可选归档与摘要”。

Knife4j 替换为支持 Boot 4 的 springdoc，`/doc.html` 仍可访问，跳转至 `/swagger-ui/index.html`；OpenAPI 为 `/v3/api-docs`。原 AI 线程池参数与 TTL 透传保持不变，底层线程池改由 Spring 初始化和关闭。

## 配置与启动

默认监听 10000 端口、Redis `127.0.0.1:6379`、Nacos `127.0.0.1:8848`。默认 profile 为 local，可通过 `SPRING_PROFILES_ACTIVE` 覆盖。

| 环境变量 | 用途 |
| --- | --- |
| `AGENT_PORT` | HTTP 端口 |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` | 登录、视觉与向量缓存 |
| `NACOS_SERVER_ADDR` | Nacos 地址 |
| `NACOS_USERNAME` / `NACOS_PASSWORD` | Nacos 认证信息，按部署需要设置 |
| `AI_API_HOST` / `AI_API_KEY` / `AI_MODEL_NAME` | 对话模型；默认地址为 DeepSeek |
| `AI_EMBEDDING_HOST` / `AI_EMBEDDING_API_KEY` / `AI_EMBEDDING_MODEL_NAME` | Embedding 提供方与模型 |
| `AI_EMBEDDING_ENABLED` | 默认 true；设为 false 可显式禁用长期记忆的向量化 |
| `AI_TIMEOUT` / `AI_EMBEDDING_TIMEOUT` | 对话/Embedding 超时毫秒，默认 60000/10000 |
| `AI_VECTOR_DIMENSION` | 默认 1024；与模型匹配，参与 v2 命名空间指纹 |
| `AI_MEMORY_WRITES_ENABLED` | 默认 true；冻结短期与长期记忆写入 |
| `AI_MEMORY_MAX_MESSAGES` / `AI_MEMORY_TTL_SECONDS` | 默认 20 条消息、86400 秒；保留完整轮次 |
| `AI_MEMORY_MAX_INPUT_CHARS` / `AI_MEMORY_MAX_MESSAGE_CHARS` | 默认各 4000 字符 |
| `AI_MEMORY_MAX_CONTEXT_CHARS` / `AI_MEMORY_LONG_TERM_CHARS` | 默认文本上下文 12000、长期召回 2000 字符 |
| `AI_MEMORY_LEASE_MS` | 默认 30000 毫秒，后台续约的会话处理锁 |
| `AI_MEMORY_VECTOR_ENABLED` / `AI_MEMORY_VECTOR_WRITES_ENABLED` | 长期记忆读取/写入开关，均默认 true |

原 `suts.ai.*` 配置名均保留，环境变量继续映射到这些属性；模型 Bean 由 `AiModelConfiguration` 显式构建，不混用一套独立的 `spring.ai.*` 凭证配置。对话必须配置 API Key 与模型名；Embedding 启用时同样必须配置其 Key 和模型名，缺少关键配置会在启动时报出属性名。测试使用 `src/test/resources/application.properties` 的本机虚构凭证，不连接真实供应商。

对话使用 `ChatClient` + `DeepSeekChatModel`，Embedding 使用 `OpenAiEmbeddingModel`，由 Spring AI 及其 SDK 处理 HTTP/JSON。两条路径均显式禁用 HTTP 自动重试；工具执行后向模型回传结果属于新的模型请求，不是网络重试。当前非推理聊天保留 temperature 设置。Embedding 失败、输出空/零向量、包含非有限数或维度不匹配时，跳过该轮记忆检索/保存，不再制造零向量。

对话模型必须支持原生 `tools/tool_calls`。本机私有 YAML 可通过 `--spring.config.additional-location=file:/absolute/path/to/agent-secrets.yaml` 显式加载；不要将真实 Key 写入源码或测试资源。

在项目根目录执行：

```sh
mvn -B -ntp -f agent/pom.xml clean verify
java -jar agent/target/agent-0.0.1-SNAPSHOT.jar
```

启动真实服务前配置自己的 Redis、Nacos 和模型凭证。默认模式不要求 MySQL、Elasticsearch 或 RocketMQ；启用事务归档时需要 MySQL 和 RocketMQ。求助工具依赖通过服务发现获得的 volunteer 服务，以及其下游服务。

## 接口兼容

- `POST /api/agent/chat`：同步对话。
- `POST /api/agent/chat/async`：DeferredResult 异步入口，原有 60 秒等待超时。
- 请求体仍为原始字符串，推荐 `text/plain;charset=UTF-8`。现有 `application/json` 请求体仍按字符串整体读取，不会自动解析为 AiChatReqDTO。
- header `token` 或 query 参数 `token` 用于读取现有 User 服务的 Redis 登录记录；Feign 继续向志愿服务透传 `token`。
- 可选 `X-Conversation-Id` 请求头：1–64 位字母、数字、下划线或连字符，省略时使用当前用户默认会话。服务端将认证用户 ID 与会话标识一起构造存储键；相同名称不会跨用户共享短期记忆。
- 成功码仍为字符串 `0`，响应数据为 `text/clientCommand/commandData`。客户端动作仍为 `NONE`、`CONNECT_WS`、`DISCONNECT_WS`、`CAPTURE_UPLOAD`、`START_VIDEO_CALL`。
- 可选响应头 `X-Agent-Memory-Status` 表示记忆接受状态；`accepted/pending` 均不等于 MySQL 已归档，详见 M02 说明。归档依赖或完整恢复不可用时，两种入口均在模型/工具前返回业务码 `503`。

```sh
curl -X POST http://localhost:10000/api/agent/chat \
  -H 'Content-Type: text/plain;charset=UTF-8' \
  -H 'token: YOUR_LOGIN_TOKEN' \
  --data '你好'
```

错误包装保留原行为：未登录/同步空输入为 `A000001`，同步未捕获异常为 `B000001`；异步空输入、失败、拥塞、等待超时分别为 `400/500/503/504`。这些是 Result 的业务码，不能等同于 HTTP 状态码。同一会话有未结束请求时，两种入口均返回业务码 `409`；不同会话可并发。超过输入预算或无效会话头在同步入口返回原客户端错误包装。

## Spring AI 工具编排

PR-03 删除 `AbstractChainNode`、`AiChatContext` 和分析/执行/总结三个节点，改由 `ChatClient` 与 `ToolCallingAdvisor` 驱动工具循环。模型直接回复自然语言，需要操作时返回标准 `tool_calls`；不再要求模型在正文里生成自定义 JSON 决策。

| Spring AI 机制 | 本项目用途 |
| --- | --- |
| `@Tool`、`@ToolParam` | 声明 `create_volunteer_help` 和 `request_client_action`；从方法签名生成模型可见的参数 Schema |
| `ChatClient.tools(agentTools)` | 每次请求显式提供这两个工具，不开放任意 Bean 查找 |
| `ToolCallingAdvisor` | 接收工具调用、回传执行结果，并继续请求模型生成最终自然语言；替代手动总结节点 |
| `ToolCallingManager` | 解析、分发和执行工具；每轮同一工具最多 2 次、总计最多 2 次，达到上限即结束循环 |
| `ToolContext` | 传递本轮可信身份与结果状态，数据不发送给模型；工具核对当前登录上下文 |

`AgentTools` 是无状态单例，`AgentToolState` 每次聊天新建。工具只能产生 `CONNECT_WS/DISCONNECT_WS/CAPTURE_UPLOAD`；`START_VIDEO_CALL` 仅在志愿服务成功返回非空房间链接及票据后由后端设置。票据和房间链接保留在响应 `commandData`，不回传给模型。没有调用工具的闲聊固定为 `NONE`。

每轮最多选择一种操作，同轮重复请求复用已有结果，后续冲突动作被拒绝，不会再次创建求助或覆盖已确定指令。参数无效时返回脱敏错误供模型纠正。工具完成后若模型总结报错、为空或循环触及上限，使用已执行结果作为回复，并保留对应指令与数据。这是单轮去重，不是跨 HTTP 请求的业务幂等保障。

客户端动作表示等待设备执行；创建求助成功只表示取得通话信息，不表示志愿者已经接通。长期记忆与视觉读取继续作为参考文本提供给模型；当前版本已增加下述 Spring AI 会话记忆和 VectorStore 实现。

## 短期会话与长期记忆

`MessageWindowChatMemory` 管理最近消息，`MessageChatMemoryAdvisor` 在工具循环外注入历史。每轮先在内存中暂存，只在得到最终回复或已执行工具的兜底回复后，把原始用户输入和最终助手文本作为完整轮次写入 Redis。模型失败且没有已执行动作时，不保存半轮消息。

Advisor 顺序为消息记忆 +200、当前参考内容 +250、工具循环 +300。视觉/长期召回在记忆捕获后注入；系统消息、原始视觉/召回块、工具协议、身份上下文和 commandData 不会写入短期记录。正常助手回复仍可包含本轮识别或召回的信息。

会话使用带随机 Token 的 Redis 租约串行处理，后台续约；更新与释放由 Lua 原子核对 Token。失去租约的旧请求不能覆盖新记录，工具开始执行前也会核对租约。短期记录只含版本化、完整的 USER/ASSISTANT 文本对，默认滑动 TTL 一天。读到损坏记录可降级；Redis 无法取得处理锁时会在工具执行前终止请求。

文本预算覆盖系统提示、当前输入、视觉参考、长期召回和短期窗口；按完整轮次裁剪短期历史。该字符预算不包含工具 Schema，也不等于模型 Token 预算。原 `suts.ai.chat.*` 的无效旧窗口配置已移除，使用 `suts.ai.memory.*`。

长期记忆由原生 `RedisVectorStore.add/similaritySearch` 完成 Embedding、JSON 保存和 KNN。每次检索强制认证 userId、Schema、模型及维度过滤，并再次核对返回记录；默认 Top K=3、相似度阈值=0.7，并限制文本预算、去重。模型名另存安全编码标签，兼容 `Qwen/...` 等带斜杠的名称。供应商地址、模型和维度共同确定命名空间，禁止静默混用向量。

记忆写入使用独立的 2–4 线程、100 队列执行器，不复用对话线程池，也不携带登录上下文。队列拒绝或向量服务失败只记录降级，不推翻已完成的业务响应；这意味着长期记忆写入是尽力而为，并非可靠消息队列。存量切换需冻结并排空写入，完成备份、离线迁移与增量回退演练。

## 可选归档与摘要

配置通过仓库外 YAML 或环境变量提供，基础映射见 [application.yaml](src/main/resources/application.yaml)，数据库迁移脚本位于 [db/archive](src/main/resources/db/archive)。

| 能力 | 启用要求 |
| --- | --- |
| JDBC 归档 | `suts.ai.memory.archive.enabled=true`，提供独立 MySQL Schema、`jdbc-url/username/password`；首次受控部署可设置 `migrate=true`，后续恢复为 false 以校验 Schema |
| 事务归档 | 同时开启 archive 与 `suts.ai.memory.transactions.enabled`；配置 `name-server/topic/producer-group/consumer-group/generation`，预建事务 Topic，并显式提供 `recovery-window-seconds/evidence-retention-seconds`，证据保留期须覆盖恢复窗口 |
| 双水位摘要 | 开启 `suts.ai.memory.summary.enabled`，先完成 V2 Schema 和事务归档部署；明确提供 `model-capacity-tokens/output-reserve-tokens/safety-tokens/input-budget-tokens/low-tokens/high-tokens/summary-max-tokens/summary-max-chars` |

事务归档要求独立单节点 Redis，启用 AOF、`appendfsync always`、有界内存与 `noeviction`。首次无流量部署时由运维执行 `SETNX suts:agent:memory:transactions:generation <generation>`，所有实例的配置须一致；generation 丢失后需先对账恢复。生产组须跨实例一致，同一消费组的订阅须一致；启用摘要前暂停流量并排空在途任务，统一部署摘要处理器后再开放准入。

摘要预算须满足 `0 < L < H < B` 与 `B + O + S <= C`，分别对应低/高水位、输入预算、输出预留、安全余量及模型容量；worker 租约须大于模型超时加 15 秒。`cache-policies` 按供应商、模型和缓存模式配置厂商提供的 TTL 及续期/计时规则；不提供厂商依据时仅关闭 TTL 触发，输入硬上限仍生效。

完整字段及校验规则见 [MemoryArchiveProperties](src/main/java/org/hgc/suts/gateway/config/MemoryArchiveProperties.java)、[MemoryTransactionProperties](src/main/java/org/hgc/suts/gateway/config/MemoryTransactionProperties.java) 和 [MemorySummaryProperties](src/main/java/org/hgc/suts/gateway/config/MemorySummaryProperties.java)。关闭准入前可分别设置 `transactions.accepting=false` 或 `summary.accepting=false`，后台消费与恢复仍继续；切换二进制前还须排空任务并核对持久状态，不能通过删除状态绕过门控。

## 自动验证

普通测试不需要模型凭证、外部 Redis、Nacos、Docker 或其他微服务：

```sh
mvn -B -ntp -f agent/pom.xml clean verify
```

- `AgentChatContractTest`：接口与模型协议测试，增加连续追问、会话头、重叠拒绝和输入预算，覆盖原生工具循环、非法参数、未知工具、同轮去重与循环上限、失败与空总结兜底，以及模型超时、Embedding 降级和视觉注入。原“非法 JSON 决策”用例改为验证自然语言无需 JSON 解析，正文中的伪造 JSON 不能下发指令。
- `AgentApplicationTest`：5 项完整应用测试，运行嵌入式 Tomcat、Boot 自动配置、Jackson 3、springdoc、真实 Spring AI HTTP 客户端、NacosDiscoveryClient 和 LoadBalancer/Feign；NamingService 传输、Redis 和模型供应商为替身，志愿服务为临时 HTTP 服务。覆盖同步/异步接口、异步工具调用后的 Token 透传与通话信息返回、日期格式、API 文档。
- `AiModelConfigurationTest` 与 `VisionContextManagerTest`：配置缺失、显式禁用、非法向量、旧 Key/嵌套 answer、过期/损坏/跨用户记录及长度限制。
- `AgentToolsTest`：登录上下文或会话租约无效时拒绝执行，并发用户不串票据，下一轮不继承上一轮动作。
- `ConversationMemoryTest`、`RedisConversationMemoryRepositoryTest`、`ChatMemoryManagerTest`：窗口/字符预算、原始文本与完整轮次、冻结/拒绝降级、租约及真实异步写入。
- `CheckedEmbeddingModelTest`、`ManagedMemoryVectorStoreTest`、`RedisVectorStorageServiceTest`：向量验证、模型指纹、索引兼容性、用户过滤和 Qwen 标签。
- `MemoryMigrationServiceTest`、`MemoryMigrationRunnerTest`：离线门控、dry-run、断点续跑、失败恢复、检查点租约和增量回退。

真实 Redis 集成测试需要本机安装 redis-server，显式指定可执行文件：

```sh
mvn -B -ntp -f agent/pom.xml verify -Plocal-redis \
  -Dredis.server.executable=/absolute/path/to/redis-server
```

PowerShell 示例：

```powershell
mvn -B -ntp -f agent/pom.xml verify -Plocal-redis '-Dredis.server.executable=D:/Develop/Redis/Redis-8.2.1/redis-server.exe'
```

`LocalRedisIT` 在临时目录、随机端口启动一个带密码且关闭持久化的独立进程；不访问现有 Redis，也不执行 flushdb。它验证真实登录缓存、会话持久化/重建、TTL、租约续约与过期写入隔离、参考内容不持久化，以及 Picture 实际 Lua 脚本的原子更新和乱序拒绝。脚本作为测试资源从仓库的 `picture/` 目录复制；模型与向量组件仍使用替身，因此不代表真实向量检索验证。

真实向量与迁移集成使用独立、带密码且有测试标记的 Redis 8.2.1 容器。测试拒绝未标记的实例，不执行 flushdb：

```powershell
docker run -d --rm --name suts-memory-it -p 127.0.0.1:16379:6379 redis:8.2.1 redis-server --save "" --appendonly no --requirepass agent-vector-integration-only
docker exec suts-memory-it redis-cli --no-auth-warning -a agent-vector-integration-only SET suts:agent:test:redis-vector-fixture agent-vector-integration-only
mvn -B -ntp -f agent/pom.xml verify -Predis-vector '-Dredis.vector.port=16379'
docker stop suts-memory-it
```

CI 在 Java 17/21 上运行 Agent 普通验证；Java 17 额外运行私有 Redis、Query Engine/JSON 向量及迁移集成，并保留 Picture 契约测试。

M03 与 M02 共用隔离基础设施，在 Java 17/21 CI 运行真实 Redis/MySQL/RocketMQ 和摘要恢复测试：

```sh
bash agent/src/test/fixtures/memory-transactions/start.sh
mvn -B -ntp -f agent/pom.xml verify -Pmemory-transactions,memory-summary,rag-profile \
  -Dmemory.transactions.fixture=agent-transactions-integration-only
docker compose -f agent/src/test/fixtures/memory-transactions/compose.yaml down --volumes
```

这些集成测试的模型和 Embedding 是替身，普通测试与 CI 不读取私有 Key。真实模型检查由 `live-memory-summary` / `live-profile` profile 单独启用，测试入口见 [LiveSummaryIT](src/test/java/org/hgc/suts/gateway/memory/summary/LiveSummaryIT.java) 和 [LiveProfileIT](src/test/java/org/hgc/suts/gateway/memory/profile/LiveProfileIT.java)。

## 长期用户画像

画像默认关闭。启用后，M03 原有的无工具 `ChatClient` 在同一次异步摘要调用中，使用 Spring AI `BeanOutputConverter` 的结构化格式生成 `summaryText` 和 `profileChanges`。候选必须引用本次完整轮次中的用户原话；应用核对属性范围、长度、轮次、引用与归档 hash。助手推测和历史摘要不能单独作为画像来源。模型抽取仍可能判断错误，用户可独立查询与纠正。

摘要正文和画像候选一起持久化，摘要仍以纯正文合并；确认摘要投影时，在同一数据库事务将画像阶段置为 READY。后续画像应用和 Redis 索引由独立有界 worker 恢复，不占用会话摘要槽位。索引故障只重试索引阶段；已有合格持久结果后不重新请求模型。模型返回到持久化前崩溃仍可能重试一次模型调用。

先执行新增的 Flyway `V3__user_profile.sql`（使用既有迁移流程或受控的 `archive.migrate=true`），再统一部署处理实例。沿用已配置的 M01～M03 连接、事务 Topic、generation、预算和厂商 TTL，增加：

```yaml
suts:
  ai:
    memory:
      profile:
        enabled: true
        accepting: true
        vector-enabled: true
```

`profile.enabled` 要求 M03 摘要已启用。结构化输出格式、画像快照也计入摘要输入与输出预算；如原预算容纳不下，任务保留并明确失败，需调整经确认的预算，不会擅自增大水位或跳过校验。默认最多 12 个候选、100 个画像属性；旧画像参考最多 20 条/6000 字符，值最多 500 字符、引用最多 1000 字符。属性前缀默认 `preference/habit/device/accessibility/personal`，可通过 `attribute-prefixes` 配置。属性以独立事实存储，不将整个画像拼成一个会被覆盖的自由文本。

画像 MySQL 状态为 ACTIVE、CONFLICT 或 DELETED，版本与来源单独追加保留。同会话后续原话可纠正旧值；跨会话不同值进入冲突，等待用户明确确认。直接纠正有 `expectedVersion` 和用户绑定的 `requestId`，防止并发覆盖与客户端重试重复执行。迟到任务不能降级用户已经确认的更正。

画像 Redis 使用独立 HNSW 索引和前缀 `suts:agent:profile:*`；模型端点、模型名、维度及索引命名共同绑定投影目标。旧整轮/摘要索引保持兼容。检索在 TopK 前过滤用户和目标，返回前再查 MySQL 当前有效状态与版本；当前实现有界补召回，积累大量旧候选时仍可能少返回，需要排查投影清理积压。

所有接口沿用 `token` 登录认证，不接受由请求指定其他用户：

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/agent/profile?includeInactive=false&limit=20` | 查询当前有效事实；开启 includeInactive 可查看冲突与遗忘状态 |
| `GET /api/agent/profile/{factId}/versions?beforeVersion=...&limit=20` | 分页查询来源和历史版本 |
| `POST /api/agent/profile/search` | Redis 向量查询，正文 `{"query":"饮品偏好","limit":5}` |
| `POST /api/agent/profile/{factId}/correct` | 纠正/解决冲突或显式恢复已遗忘事实 |
| `POST /api/agent/profile/{factId}/forget` | 使画像失效并登记向量清理 |
| `GET /api/agent/profile/extractions?limit=20` | 查询本人抽取阶段和错误类型，不返回任务原文 |
| `GET /api/agent/profile/{factId}/projections` | 查询向量投影进度 |
| `POST /api/agent/profile/reindex` | 为本用户当前事实重建当前目标投影，并重驱动旧版清理 |

纠正正文示例：`{"requestId":"correction-001","expectedVersion":1,"value":"绿茶","statement":"我确认喜欢绿茶"}`。statement 作为这次用户直接确认的来源，必须包含新值。遗忘正文示例：`{"requestId":"forget-001","expectedVersion":2,"statement":"忘掉我的饮品偏好"}`。同一 requestId 重试须复用原正文；过期版本返回业务码 409。列表和接口仍使用项目原有 Result 协议。

“遗忘”在本模块指画像逻辑失效：MySQL 保留失效标记、版本和来源用于阻止旧任务复活，索引异步删除；不删除已归档原文、当前对话摘要、日志或备份。删除标记不会被后续自动抽取恢复，重新记录使用显式 correct 入口。历史版本中的状态表示当时状态，当前有效性始终以 fact 当前记录为准。

抽取与投影状态在 `agent_profile_extraction/agent_profile_projection` 中持久保存，带 worker 代次、租约、重试时间和错误类型；网络调用不持有数据库事务。`profile.vector-enabled=false` 或关闭 Embedding 时，投影保留 PENDING，MySQL 查询/纠正仍可使用。切换模型、维度或索引命名后，重启并显式 reindex；旧目标状态不代表新目标已写入。

停止新抽取先设 `profile.accepting=false`，保留 `enabled=true` 排空 CAPTURED/RESULT/READY 与投影。该配置需正常重载/重启，不提供配置管理接口。新二进制会拒绝在关闭画像支持时处理仍需结构化处理的旧任务；旧二进制没有这项保护，因此启用前应停流量、排空旧 worker、统一部署，不能混跑旧摘要消费者。回退保留 V3、事实版本和删除状态，不能靠删除 SQL 或 Redis Key 绕过。

`rag-profile` 集成使用既有隔离 MySQL/Redis/RocketMQ，覆盖真实登录、HTTP 模型协议、画像持久化、HNSW、纠正/遗忘、跨用户过滤及 Embedding 故障恢复；模型和 Embedding 采用确定性替身。真实模型抽取是显式付费的独立检查，仅调用两次，使用合成对话与 H2 持久化：

```powershell
mvn -B -ntp -f agent/pom.xml verify -Plive-profile '-Dlive.profile=true' '-Dlive.memory.config=C:/path/outside/repository/agent-secrets.yaml'
```

脱敏的真实模型样例记录见 [profile-live-2026-10-19.json](evaluation/profile-live-2026-10-19.json)。本模块提供独立画像 API；按对话意图调用画像、文档混合检索和多模型路由在后续 PR 接入。

## 文档与混合检索

R02 提供独立检索引擎，默认 `suts.ai.rag.enabled=false`。接收原问题、检索问题、范围和可选文档 ID；查询改写、意图判断和聊天模型路由由后续 Agent 接入完成。支持 `PROFILE`、`DOCUMENT`、`BOTH`、`NONE`；`NONE` 不调用检索和模型。画像沿用上面的 R01 开关，文档检索无需开启会话归档、摘要或 RocketMQ。

链路为并行向量/关键词检索 → 每路最多 50 条 → RRF Top20 → 精排 Top5 → 父块扩展去重与来源。`BOTH` 每路默认画像 10 条、文档 40 条，余量互补；`profile-quota` 与 `document-quota` 可配置且相加为 50。实现对每类有界取最多 50 条后分配共享配额，因此内部取数最多 100 条/通道，进入 RRF 前最多 50 条/通道。一次请求只生成一次查询 Embedding，两类 Redis 索引复用该向量。ES 使用内置 CJK 分词与 BM25。精排默认硅基流动 `Qwen/Qwen3-Reranker-0.6B`，地址和模型可替换。

私有配置示例（Key 与数据库口令放在仓库外）：

```yaml
suts:
  ai:
    rag:
      enabled: true
      jdbc-url: jdbc:mysql://127.0.0.1:3306/agent_rag
      username: your_user
      password: your_password
      migrate: false
      elasticsearch:
        endpoint: http://127.0.0.1:9200
        index-name: suts-agent-rag-v1
        username: your_es_user
        password: your_es_password
      reranker:
        endpoint: https://api.siliconflow.cn/v1
        api-key: your_api_key
        model: Qwen/Qwen3-Reranker-0.6B
```

Embedding 沿用 `suts.ai.embedding-*` 和维度配置。先通过受控迁移执行 `db/rag/V1__rag_documents.sql`，或首次部署显式 `rag.migrate=true`；使用独立 `agent_rag_schema_history`，与其他表同库时从版本 0 建立迁移基线后执行 V1。所有已开启会话归档的部署升级时均需先应用 `db/archive/V4__profile_keyword_projection.sql`，因为归档启动会校验整个迁移目录。升级前排空旧 worker，避免不识别 ES target 的旧实例参与。现有有效画像通过持久检查点回填到 ES；纠正、冲突及遗忘与独立 ES 投影任务在同一 SQL 事务入队。

本地管理命令只在 `web-application-type=none` 下运行，使用操作人员的数据库/索引权限。支持 UTF-8 TXT、Markdown、可提取文本的 PDF，不含 OCR。默认文件上限 10 MiB、正文 50 万字符、父块 4000 字符、子块 1000 字符、重叠 120 字符；这是存储预算，不代表模型 Token。保留 Markdown 编号/标题、PDF 物理页码，父子块 ID 稳定。不要把敏感本机路径作为公共来源，默认只返回文件名。

```sh
java -jar agent/target/agent-0.0.1-SNAPSHOT.jar \
  --spring.main.web-application-type=none \
  --spring.cloud.nacos.discovery.enabled=false \
  --spring.config.additional-location=file:/path/outside/repository/agent-secrets.yaml \
  --suts.ai.rag.management.enabled=true \
  --suts.ai.rag.management.action=import \
  --suts.ai.rag.management.file=/path/guide.md \
  --suts.ai.rag.management.document-id=screen-reader-guide \
  --suts.ai.rag.management.owner-id=42 \
  --suts.ai.rag.management.visibility=PUBLIC
```

`visibility` 默认 `OWNER`，显式 `PUBLIC` 才供其他登录用户召回。管理命令不提供 HTTP 上传入口。相同文档 ID 绑定原 owner，重复相同内容不创建版本。`action=status/delete/reindex` 携带同一 `document-id/owner-id`；`reindex` 强制重建当前索引，`retry` 处理一批持久积压，`profile-backfill` 重置画像 ES 回填。命令退出后的未完成任务在正常启用 RAG 的服务进程恢复。索引故障在 SQL 保留状态、租约、重试时间和错误类型。

```http
POST /api/agent/rag/search
token: <登录 token>
Content-Type: application/json

{"originalQuestion":"这个怎么打开？","query":"演示读屏终端如何打开帮助文档？","scope":"BOTH","documentIds":[]}
```

身份从登录上下文读取；请求不能指定别人的 owner。返回 `evidence` 和 `trace`，包含父块正文、来源、版本、页码和命中子块 ID；Top5 指精排的片段数，父块合并后证据可能更少。证据正文总预算默认 12000 字符，超过预算跳过整个父块。ACL 在两引擎 TopK 前过滤，结果及父块再次按 MySQL 当前有效版本复核；SQL 故障不以索引正文代替权威记录。调用方应将这些正文视为引用数据，不能当作系统指令。

Redis 或 ES 单路失败时使用仍可用的一路；精排失败回退 RRF 顺序最多 5 条，并在 `trace.degradedChannels` 明确标记。SQL/查询/精排共享总时间预算，超时返回已确认的证据或空列表及降级标记。取消不保证底层 SQL 或网络立刻结束，连接与读取仍有独立超时，执行池及队列有界。

双索引写入先保持不可检索，分别记录 receipt 后再发布 SQL 版本、开放召回；这个过程可重试，不是 Redis/ES/MySQL 跨库原子事务，切换期间可能短暂少召回。旧版/遗忘/删除记录即使物理清理暂时失败也不能通过 SQL 复核。切换目标保留旧目标描述符与清理任务；更换 ES 地址时，旧地址清理需使用其原连接配置，当前凭据不会自动发送到另一个地址。历史正文与向量缓存保留用于重建，逻辑下架不会物理删除备份。

普通 CI 使用真实隔离 MySQL、Redis HNSW、ES 和本地模型协议替身：

```sh
bash agent/src/test/fixtures/rag-retrieval/start.sh
mvn -B -ntp -f agent/pom.xml verify -Prag-retrieval -Drag.retrieval.fixture=agent-rag-integration-only
bash agent/src/test/fixtures/rag-retrieval/stop.sh
```

真实供应商检查单独显式启用 `-Plive-rag-retrieval -Dlive.rag=true -Drag.retrieval.fixture=agent-rag-integration-only -Dlive.memory.config=/path/outside/repository/agent-secrets.yaml`，仅一次批量 Embedding 和一次精排，使用合成演示资料。结果写到忽略的 `target/live-rag-report.json`；少量样例只证明可调用，不代表正式资料集的检索质量。用户画像和助盲资料的完整意图路由仍需后续 PR。

## 视觉上下文协议

Picture 与 Agent 均使用 `REDIS_HOST/REDIS_PORT/REDIS_PASSWORD`，默认相同的本机 Redis；部署时必须指定为同一 Redis 实例和数据库。Picture 的 `VISION_QA_URL` 默认 `/vision-qa/qa`，与 Python 当前 multipart 路由一致。

Picture 先校验 Python 返回的非空 `answer`，图片结果查询接口仍保存完整响应，Agent 上下文仅存自然语言 answer：

```json
{"schemaVersion":1,"userId":42,"pictureId":7,"text":"前方是一扇门","timestamp":1792224000000}
```

新 Key 为 `suts:agent:vision:context:{userId}`，TTL 为 5 分钟。新事件携带上传分析请求开始时的 requestedAt，Lua 用它原子比较已有记录时间，拒绝后到的旧分析。旧 MQ 事件缺少此字段时按处理时间兼容，不能保证它们的原始顺序。

Agent 优先读取新 Key，兼容旧 `suts:gateway:vision:context:{userId}` 及其中嵌套的 `{question,answer}` 字符串；纯文本旧记录仅在有效的五分钟内 TTL 下使用。过期、异常未来时间、未知 schema、损坏内容和用户不匹配时忽略，默认最多注入 2000 字符。`suts.ai.vision.max-age-ms/max-chars` 可调整读端窗口/预算；写端保持五分钟。上下文作为用户输入中的图片证据，不作为工具或系统指令。

滚动部署先更新 Agent 兼容读端，再更新 Picture 写端。全量更新并排空旧事件、经过原五分钟缓存窗口后，可在后续 PR 删除旧 Key 兼容读取。无需清空共享缓存或重建向量索引。

## 离线迁移与回退

迁移前备份数据，并停止、排空全部实例和后台写入。在不开放 HTTP、不注册 Nacos 的独立进程中运行，配置 `suts.ai.memory.migration.enabled=true`、`writes-frozen=true`、`writers-drained=true`，同时关闭 `memory.writes-enabled` 与 `memory.vector.writes-enabled`。配置项见 [MemoryMigrationProperties](src/main/java/org/hgc/suts/gateway/memory/migration/MemoryMigrationProperties.java)。

默认 `dry-run=true`，只检查记录、冲突及预计处理量；核对后才显式设置 false。首次正向迁移从旧 HASH 文本重新 Embedding，需预算调用费用。`checkpoint-name` 标识冻结批次，失败续跑复用原检查点，新批次使用新检查点；核对数量、归属与实际召回后再开放写入。

回退前同样排空写入，备份 v2，并以 `direction=ROLLBACK` 补齐 v2 新增内容，再切回旧应用。仅在新旧 Embedding 地址、模型和维度一致时支持向旧 HASH 空间补齐；不匹配时需另行设计重建。短期 v2 窗口旧版不可读，迁移不会自动清理源索引或覆盖原数据。启用事务归档或摘要后还须先完成相应检查点与任务格式核验，不应直接切换到 Redis 旧写路径。
