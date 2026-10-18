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

已有长期记忆的部署应先阅读 `MEMORY-MIGRATION.md`：新版本使用独立 v2 JSON 索引，不自动读取或覆盖旧 HASH 向量。只切应用而不迁移，会暂时无法召回旧长期记忆。

M01 增加可选 JDBC 归档与离线恢复基础，见 [ARCHIVE-M01.md](ARCHIVE-M01.md)。默认关闭，不要求 MySQL；启用后提供内部归档服务和受控维护命令。当前聊天仍提交 Redis，正式事务消息异步落库由 M02 接入，TTL 双水位及摘要向量检索由 M03 实现。

PR-01 中，Agent 没有数据库 Mapper、SQL 操作或具体 MQ 生产者，因此移除了 MyBatis-Plus、MySQL、RocketMQ 及未被调用的 MQ 抽象模板；用 Spring Data Redis 替代只为 StringRedisTemplate 引入的 Redisson。该次升级仅涉及 Agent；PR-02 另修复 Picture 的视觉上下文链路。

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

启动真实服务前配置自己的 Redis、Nacos 和模型凭证。Agent 不再需要 MySQL、Elasticsearch 或 RocketMQ。求助工具依赖通过服务发现获得的 volunteer 服务，以及其下游服务。

## 接口兼容

- `POST /api/agent/chat`：同步对话。
- `POST /api/agent/chat/async`：DeferredResult 异步入口，原有 60 秒等待超时。
- 请求体仍为原始字符串，推荐 `text/plain;charset=UTF-8`。现有 `application/json` 请求体仍按字符串整体读取，不会自动解析为 AiChatReqDTO。
- header `token` 或 query 参数 `token` 用于读取现有 User 服务的 Redis 登录记录；Feign 继续向志愿服务透传 `token`。
- 可选 `X-Conversation-Id` 请求头：1–64 位字母、数字、下划线或连字符，省略时使用当前用户默认会话。服务端将认证用户 ID 与会话标识一起构造存储键；相同名称不会跨用户共享短期记忆。
- 成功码仍为字符串 `0`，响应数据为 `text/clientCommand/commandData`。客户端动作仍为 `NONE`、`CONNECT_WS`、`DISCONNECT_WS`、`CAPTURE_UPLOAD`、`START_VIDEO_CALL`。

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

记忆写入使用独立的 2–4 线程、100 队列执行器，不复用对话线程池，也不携带登录上下文。队列拒绝或向量服务失败只记录降级，不推翻已完成的业务响应；这意味着长期记忆写入是尽力而为，并非可靠消息队列。迁移、发布切换和回退步骤见 `MEMORY-MIGRATION.md`。

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

## 视觉上下文协议

Picture 与 Agent 均使用 `REDIS_HOST/REDIS_PORT/REDIS_PASSWORD`，默认相同的本机 Redis；部署时必须指定为同一 Redis 实例和数据库。Picture 的 `VISION_QA_URL` 默认 `/vision-qa/qa`，与 Python 当前 multipart 路由一致。

Picture 先校验 Python 返回的非空 `answer`，图片结果查询接口仍保存完整响应，Agent 上下文仅存自然语言 answer：

```json
{"schemaVersion":1,"userId":42,"pictureId":7,"text":"前方是一扇门","timestamp":1792224000000}
```

新 Key 为 `suts:agent:vision:context:{userId}`，TTL 为 5 分钟。新事件携带上传分析请求开始时的 requestedAt，Lua 用它原子比较已有记录时间，拒绝后到的旧分析。旧 MQ 事件缺少此字段时按处理时间兼容，不能保证它们的原始顺序。

Agent 优先读取新 Key，兼容旧 `suts:gateway:vision:context:{userId}` 及其中嵌套的 `{question,answer}` 字符串；纯文本旧记录仅在有效的五分钟内 TTL 下使用。过期、异常未来时间、未知 schema、损坏内容和用户不匹配时忽略，默认最多注入 2000 字符。`suts.ai.vision.max-age-ms/max-chars` 可调整读端窗口/预算；写端保持五分钟。上下文作为用户输入中的图片证据，不作为工具或系统指令。

滚动部署先更新 Agent 兼容读端，再更新 Picture 写端。全量更新并排空旧事件、经过原五分钟缓存窗口后，可在后续 PR 删除旧 Key 兼容读取。无需清空共享缓存或重建向量索引。

## 本次验证范围与后续检查

Boot 升级的历史记录见 `VALIDATION.md`，模型和视觉改造记录见 `VALIDATION-PR02.md`，工具编排记录见 `VALIDATION-PR03.md`，本次记忆重构见 `VALIDATION-MEMORY.md`。构建使用 Maven 3.9.11，独立 Redis 为 8.2.1。

已通过仓库外私有配置和实际 JAR 验证真实 DeepSeek `deepseek-flash`、SiliconFlow `Qwen/Qwen3-Embedding-0.6B` 及 Redis 8.2.1 向量检索：连续追问、跨会话长期召回、跨用户隔离、应用重启恢复及异步客户端动作均通过。普通自动化测试继续使用模型替身，不读取真实 Key。

尚未验证真实 Nacos 服务端注册/注销、原版志愿服务及完整视频联调、真实 COS/RocketMQ/Python 图片识别、生产存量数据迁移。前端指令落地、统一请求取消与异步治理、志愿通知占位等仍属后续工作。

## 回退

先停止并排空新版本写入，在隔离备份中演练 `MEMORY-MIGRATION.md` 的增量回退补齐，再切回旧应用与原模型配置。保留旧索引不等于完整回退：新版本新增的记忆只在 v2 中，必须先补齐；模型或维度变化时，不能把新向量直接写入旧空间。短期 v2 数据和新向量在回退中均不自动删除。
