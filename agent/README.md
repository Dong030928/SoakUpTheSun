# Agent

Agent 使用 Spring AI 调用 DeepSeek 对话模型与 OpenAI 兼容的 Embedding 接口，保留现有三节点责任链和对外协议。Picture 仍使用原 Spring Boot 3 技术栈，通过 Redis 向 Agent 提供视觉证据。

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
| Jedis | 5.1.0，保留原向量 API |

Redis 登录缓存使用 Boot 管理的 Spring Data Redis/Lettuce；长期记忆仍通过 Jedis 使用 RediSearch 的 `FT.INFO`、`FT.CREATE`、`FT.SEARCH`。生产环境应提供支持这些命令的 Redis，并保持与 User/Picture 服务的缓存配置一致。

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
| `AI_VECTOR_DIMENSION` | 默认 1024，必须匹配既有模型与 Redis 索引 |

原 `suts.ai.*` 配置名均保留，环境变量继续映射到这些属性；模型 Bean 由 `AiModelConfiguration` 显式构建，不混用一套独立的 `spring.ai.*` 凭证配置。对话必须配置 API Key 与模型名；Embedding 启用时同样必须配置其 Key 和模型名，缺少关键配置会在启动时报出属性名。测试使用 `src/test/resources/application.properties` 的本机虚构凭证，不连接真实供应商。

对话使用 `ChatClient` + `DeepSeekChatModel`，Embedding 使用 `OpenAiEmbeddingModel`，由 Spring AI 及其 SDK 处理 HTTP/JSON。两条路径均显式禁用自动重试，保留每次调用仅一次尝试；当前非推理聊天保留 temperature 设置。Embedding 失败、输出空/零向量、包含非有限数或维度不匹配时，跳过该轮记忆检索/保存，不再制造零向量。

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
- 成功码仍为字符串 `0`，响应数据为 `text/clientCommand/commandData`。客户端动作仍为 `NONE`、`CONNECT_WS`、`DISCONNECT_WS`、`CAPTURE_UPLOAD`、`START_VIDEO_CALL`。

```sh
curl -X POST http://localhost:10000/api/agent/chat \
  -H 'Content-Type: text/plain;charset=UTF-8' \
  -H 'token: YOUR_LOGIN_TOKEN' \
  --data '你好'
```

错误包装保留原行为：未登录/同步空输入为 `A000001`，同步未捕获异常为 `B000001`；异步空输入、失败、拥塞、等待超时分别为 `400/500/503/504`。这些是 Result 的业务码，不能等同于 HTTP 状态码。本次没有统一这些既有差异。

## 自动验证

普通测试不需要模型凭证、外部 Redis、Nacos、Docker 或其他微服务：

```sh
mvn -B -ntp -f agent/pom.xml clean verify
```

- `AgentChatContractTest`：保留原 15 项回归，并增加模型失败、超时、异常 Embedding 降级及视觉注入检查；使用实际 Spring AI 客户端与本机 HTTP 供应商替身。
- `AgentApplicationTest`：4 项完整应用测试，运行嵌入式 Tomcat、Boot 自动配置、Jackson 3、springdoc、真实 NacosDiscoveryClient 和 LoadBalancer/Feign；NamingService 传输、Redis 和模型为替身，志愿服务为临时 HTTP 服务。覆盖同步/异步接口、服务发现后的 Token 透传与旧 Result 解析、日期格式、API 文档。
- `AiModelConfigurationTest` 与 `VisionContextManagerTest`：配置缺失、显式禁用、非法向量、旧 Key/嵌套 answer、过期/损坏/跨用户记录及长度限制。

真实 Redis 集成测试需要本机安装 redis-server，显式指定可执行文件：

```sh
mvn -B -ntp -f agent/pom.xml verify -Plocal-redis \
  -Dredis.server.executable=/absolute/path/to/redis-server
```

PowerShell 示例：

```powershell
mvn -B -ntp -f agent/pom.xml verify -Plocal-redis '-Dredis.server.executable=D:/Develop/Redis/Redis-8.2.1/redis-server.exe'
```

`LocalRedisIT` 在临时目录、随机端口启动一个带密码且关闭持久化的独立进程；不访问现有 Redis，也不执行 flushdb。它验证真实登录缓存，以及 Picture 实际 Lua 脚本的原子更新、乱序拒绝和 Agent 读取。脚本作为测试资源从仓库的 `picture/` 目录复制；模型与向量组件仍使用替身，因此不代表真实向量检索验证。

CI 在 Java 17、21 上运行 Agent 普通验证，在 Java 17 上额外运行独立 Redis 集成和 Picture 契约测试。Picture 仅增加测试依赖，未升级其 Boot 版本。

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

Boot 升级的历史记录见 `VALIDATION.md`；本次模型和视觉改造记录见 `VALIDATION-PR02.md`。构建使用 Maven 3.9.11，独立 Redis 为 8.2.1。

已通过仓库外私有配置验证真实 DeepSeek `deepseek-flash` 和 SiliconFlow `Qwen/Qwen3-Embedding-0.6B` 调用，并以独立 Redis、预置视觉文本验证实际 JAR 的同步/异步聊天。该手工联调与使用替身的自动化测试分开执行，详情见 `VALIDATION-PR02.md`。

尚未验证真实 Nacos 服务端注册/注销及 gRPC 传输、原版志愿服务与完整视频服务联调、真实图片识别链路和 Redis 向量扩展。部署前应在隔离环境启动真实服务，检查 Agent 注册、求助路径、登录共享缓存与关闭行为。NamingService 替身验证不等于 Nacos 服务端互操作通过。

前端尚未处理 Agent 指令、记忆异步、通用工具调用与志愿通知占位等问题仍由后续 PR 处理。

## 回退

本次向量索引和存储格式不变，Embedding 模型标识及维度须保持原配置。回退前恢复对应旧模型配置；视觉新记录仍保留旧 Agent 使用的 text 字段，可回退应用版本，无需删缓存。若连同 Picture 一起回退，其旧写端将恢复旧 Key，旧 Agent 原本的视觉链路问题也会恢复，应按部署环境验证。
