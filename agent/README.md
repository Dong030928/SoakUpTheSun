# Agent

Agent 的 Spring Boot 4 迁移阶段保留原模型调用与三节点责任链，尚未引入 Spring AI。其余微服务可保持原版本；实际部署仍需验证服务端和协议兼容。

## 运行版本

| 组件 | 版本 |
| --- | --- |
| Java | 17 或 21，编译目标为 17 |
| Maven | 3.6.3+；本次本地验证使用 3.9.11 |
| Spring Boot | 4.0.8 |
| Spring Cloud | 2025.1.3 |
| Spring Cloud Alibaba | 2025.1.0.0 |
| Nacos Client | 3.1.1，由 Alibaba 管理 |
| springdoc OpenAPI | 3.0.3 |
| Jedis | 5.1.0，保留原向量 API |

Redis 登录缓存使用 Boot 管理的 Spring Data Redis/Lettuce；长期记忆仍通过 Jedis 使用 RediSearch 的 `FT.INFO`、`FT.CREATE`、`FT.SEARCH`。生产环境应提供支持这些命令的 Redis，并保持与 User/Picture 服务的缓存配置一致。

Agent 没有数据库 Mapper、SQL 操作或具体 MQ 生产者，因此移除了 MyBatis-Plus、MySQL、RocketMQ 及未被调用的 MQ 抽象模板；用 Spring Data Redis 替代只为 StringRedisTemplate 引入的 Redisson。未修改其他服务。

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

- `AgentChatContractTest`：15 项回归，使用真实责任链和本机临时 HTTP 模型替身，缓存/志愿服务为 Mockito 替身。同一组测试在原 Boot 3.0.7（临时覆盖 Lombok 1.18.30）与 Boot 4 下验证。
- `AgentApplicationTest`：4 项完整应用测试，运行嵌入式 Tomcat、Boot 自动配置、Jackson 3、springdoc、真实 NacosDiscoveryClient 和 LoadBalancer/Feign；NamingService 传输、Redis 和模型为替身，志愿服务为临时 HTTP 服务。覆盖同步/异步接口、服务发现后的 Token 透传与旧 Result 解析、日期格式、API 文档。

真实 Redis 集成测试需要本机安装 redis-server，显式指定可执行文件：

```sh
mvn -B -ntp -f agent/pom.xml verify -Plocal-redis \
  -Dredis.server.executable=/absolute/path/to/redis-server
```

PowerShell 示例：

```powershell
mvn -B -ntp -f agent/pom.xml verify -Plocal-redis '-Dredis.server.executable=D:/Develop/Redis/Redis-8.2.1/redis-server.exe'
```

`LocalRedisIT` 在临时目录、随机端口启动一个带密码且关闭持久化的独立进程；不访问现有 Redis，也不执行 flushdb。它验证 Lettuce 与 Jedis 认证及真实 Redis 登录缓存下的同步/异步 HTTP；模型、向量索引与向量业务使用替身，因此普通 Redis 即可，不能将此测试描述为真实向量检索验证。

CI 在 Java 17、21 上运行普通验证，在 Java 17 上额外运行独立 Redis 集成测试。报告位于 `agent/target/surefire-reports` 与 `agent/target/failsafe-reports`。

## 本次验证范围与后续检查

本地升级前 15 项基线通过；升级后 JDK 17 验证包含 19 项普通测试和 1 项真实 Redis 集成测试。构建使用 Maven 3.9.11，Redis 为 8.2.1；具体执行记录见同目录 `VALIDATION.md`。

尚未验证真实 Nacos 服务端注册/注销及 gRPC 传输、原版志愿服务与完整视频服务联调、真实付费模型和 Redis 向量扩展。部署前应在隔离环境启动真实服务，检查 Agent 注册、求助路径、登录共享缓存与关闭行为。NamingService 替身验证不等于 Nacos 服务端互操作通过。

视觉缓存 Key 不一致、前端尚未处理 Agent 指令、Embedding 失败零向量、记忆异步与志愿通知占位等既有问题，继续由后续重构处理；本次不更改这些业务逻辑。

## 回退

本次没有更改业务数据格式或 Redis Key，也没有迁移索引。回退此提交并恢复原依赖/配置即可恢复旧版本；移除的 MySQL、RocketMQ 配置不应重新加入当前 Agent 版本。保留其他微服务及缓存数据。
