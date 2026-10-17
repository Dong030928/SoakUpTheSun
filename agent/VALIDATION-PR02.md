# PR-02：Spring AI 与视觉上下文验证记录

日期：2026-10-17。基于已合并 PR-01 的 main（2065c941），开发分支为 `refactor/agent-spring-ai-vision`。

## 实现范围

- Agent 引入 Spring AI 2.0.1 BOM、DeepSeekChatModel/ChatClient 与 OpenAiEmbeddingModel，移除手写模型 HTTP 和响应 JSON 解析。Boot 保持 4.0.8，Picture 保持 3.0.7。
- 保留 `suts.ai.*` 配置与责任链接口，增加启动配置校验、明确的 Embedding 禁用开关及独立超时。对话和 Embedding 每次均仅尝试一次。
- 验证维度与有限非零向量；失败只降级记忆，不写入人造零向量。
- Picture 修复可配置 URL 字段被声明成常量的问题，纠正 Python 路由，验证文本 answer，关闭 HTTP 响应流。
- 统一视觉 Redis Key 和环境变量，保存 v1 文本上下文，沿用完整图片结果查询响应；新事件以请求开始时间作为顺序依据，Lua 原子拒绝较旧结果。
- Agent 兼容旧 Key 与嵌套 answer，忽略过期/损坏/未知格式及跨用户记录，限制视觉证据长度。
- CI 增加 Picture 契约测试及触发路径。

## 本地验证

Maven 3.9.11；Windows 11；Microsoft JDK 17.0.20.1、Oracle JDK 21.0.8；独立 Redis 8.2.1。使用临时 Maven 设置访问 HTTPS Central，未修改全局设置。

| 验证 | 命令/方法 | 结果 |
| --- | --- | --- |
| Agent JDK 17 | `mvn -f agent/pom.xml clean verify -Plocal-redis -Dredis.server.executable=<本机redis-server>` | 41 项普通测试 + 2 项真实 Redis 集成测试 |
| Agent JDK 21 | `mvn -f agent/pom.xml verify` | 41 项普通测试 |
| Picture JDK 17 | `mvn -f picture/pom.xml clean verify` | 8 项契约测试及打包 |
| 实际 Agent 可执行 JAR | 随机端口、独立带密码 Redis、本机 HTTP 模型替身、禁用真实 Nacos | 启动、OpenAPI、同步/异步聊天与视觉文本进入模型请求 |
| 源码与依赖检查 | `git diff --check`、POM/依赖检查 | 仅 Agent、Picture 视觉链路及 CI/文档变化；不改向量索引 |

普通测试使用实际 Spring AI 客户端与本机 HTTP 供应商替身验证协议和鉴权。测试凭证仅为 fixture；异常用例预期会打印失败或降级日志，不代表测试失败。原 15 项接口回归保留，响应仍为原字符串请求及 Result/客户端动作协议。

## 测试覆盖

- 模型名、两套独立鉴权、消息/temperature、一次调用策略、401/429/503、读取超时、空回复。
- Embedding 提供方失败、响应缺失、维度错误、NaN/Infinity、零向量；失败时向量仓库没有检索或保存调用，对话正常降级。
- 新记录、旧 Key、旧嵌套 answer、纯文本 TTL、过期/未来时间、坏 JSON、未知版本、用户隔离、长度预算，以及以花括号开头的有效新版文本。
- Picture 实际 multipart HTTP 请求字段、成功/空/错误类型/非法 JSON answer、请求开始时间保留和上下文 JSON 字段。
- 在真实 Redis 执行 Picture 的同一 Lua 文件，确认晚到旧结果返回 0、保留新文本，损坏旧值可以被替换；Agent 能读取写入的证据。
- 完整 Boot 上下文、HTTP 同步/异步、Jackson、OpenAPI、NacosDiscoveryClient/Feign 的原 PR-01 测试继续运行。

## 发布配置与兼容性

原 AI 配置名无需迁移；现在缺少对话 Key、启用 Embedding 时缺少其 Key/模型名会启动失败。无 Embedding 服务时显式配置 `AI_EMBEDDING_ENABLED=false`。维度必须与原 Embedding 模型及既有 Redis 索引一致，本 PR 不执行向量重建或模型迁移。

Picture 的 Redis 默认值由原硬编码远端改为与 Agent 一致的本机值；部署环境应显式设置两者的 `REDIS_HOST/REDIS_PORT/REDIS_PASSWORD` 并保持数据库一致。Python 地址支持 `VISION_QA_URL`，默认不含多余 `/api` 前缀。

先部署兼容旧 Key 的 Agent，再部署 Picture。新事件有 requestedAt 才具备请求时间顺序保障；旧事件缺少字段时只能按处理时间兼容。原缓存 TTL 为 5 分钟，旧事件排空并过完该窗口后，可在后续 PR 移除旧读协议。新记录保留旧 Agent 可读的 text 字段，不删除旧缓存，不迁移已有向量数据。

## 验证边界

- 未调用真实 DeepSeek、SiliconFlow 或其他付费供应商；HTTP 替身仅验证协议和故障路径，不能替代供应商能力验收。
- 未连接真实 COS、RocketMQ 或执行 Python 模型推理；Picture 消费者使用 COS 对象流替身和本机 HTTP 路由。
- Nacos 服务端传输和原版其他服务全链路仍未验证。已有 NamingService 替身测试继续确认发现适配和 Feign 契约。
- 普通 Redis 不包含 RediSearch；集成测试验证视觉 Lua 和缓存读写，不表示长期向量检索已获真实扩展验收。
- 前端客户端动作、工具框架迁移、短期会话记忆及完整异步治理属于后续 PR。

所有外部验收均应使用隔离环境；不向仓库中已有远程地址或真实凭证发送测试请求。
