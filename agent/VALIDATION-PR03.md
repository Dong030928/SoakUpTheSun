# PR-03：Spring AI 工具编排重构与验证

日期：2026-10-17。基于已合并 PR-02 的 main（88350c40），分支 `codex/refactor-agent-spring-ai-tools`。版本保持 Boot 4.0.8、Spring AI 2.0.1，不新增依赖。

## 重构方式

| 原实现 | 新实现 | 责任边界 |
| --- | --- | --- |
| ToolAnalysisNode 要求模型输出自定义 JSON 并手动解析 tool/args/command/reply | ChatClient 发送 `@Tool`/`@ToolParam` 自动生成的 Schema，模型返回原生 `tool_calls` | 参数转换与方法匹配交给 Spring AI；普通回复为自然语言 |
| ToolExecuteNode 按字符串 switch 分发后台工具与前端动作 | AgentTools 的两个带注解方法，由 ToolCallingManager 执行 | 业务代码仍负责调用 Feign、校验结果和生成客户端指令 |
| ToolSummaryNode 手动构造总结 Prompt 并发起第二次调用 | 显式配置的 ToolCallingAdvisor 回传工具结果并继续调用模型 | Spring AI 负责工具调用、结果消息与最终回复的循环 |
| 共享节点与 AiChatContext 串联 | 每轮新建 AgentToolState，通过 ToolContext 传递 | 状态、可信身份和票据不发送给模型；服务层映射原响应 DTO |

服务层保留视觉/长期记忆读取与保存，删除整个 `chain` 包。没有引入另一个自定义执行循环，也没有把旧决策 JSON 包装成工具协议。`AiModelManager` 的基础对话辅助方法保留供底层协议验证，实际业务聊天通过服务层的 ChatClient 执行。

实现参考 [Spring AI 2.0.1 Tool Calling](https://docs.spring.io/spring-ai/reference/api/tools.html)。工具按请求注册；ToolCallingManager 禁止按名称回退查找未注册工具，每轮同一工具及工具总调用数上限均为 2。每次 HTTP 请求仍禁用自动重试，工具回传后的新一轮模型请求不属于网络重试。

## 接口与行为

- 同步/异步路径、字符串请求体、Token 校验、Result 包装及 text/clientCommand/commandData 保持不变。
- `request_client_action` 参数为三值枚举，不能由模型直接设置 START_VIDEO_CALL、票据或房间数据。工具只下发指令，不声称设备已执行。
- `create_volunteer_help` 使用当前登录身份与 Feign Token 透传；只有有效成功结果才产生 START_VIDEO_CALL。返回给模型的工具结果不含房间链接或认证票据。
- 单轮重复求助复用结果，失败也不重复创建；每轮最多一种操作，冲突动作不覆盖第一种。两个用户并发和不同轮次的状态独立。
- 非法参数向模型返回固定脱敏提示。工具结果已有时，总结失败、为空或循环达上限均保留已执行动作和原通话数据；尚无工具结果的模型失败沿用原接口错误包装。
- 自然语言不再经历旧 JSON 决策解析；模型正文即使包含 command JSON，也不能改变后端生成的客户端指令。

## 本地验证

Windows 11；Maven 3.9.11；Microsoft JDK 17.0.20.1、Oracle JDK 21.0.8；隔离 Redis 8.2.1。Maven 使用临时 HTTPS Central 设置，不改全局镜像。

| 验证 | 方法 | 结果 |
| --- | --- | --- |
| Agent / JDK 17 | `mvn -f agent/pom.xml clean verify -Plocal-redis -Dredis.server.executable=<redis-server>` | 62 项普通测试 + 2 项真实 Redis 集成测试 |
| Agent / JDK 21 | `mvn -f agent/pom.xml clean verify` | 62 项普通测试 |
| Picture / JDK 17 | `mvn -f picture/pom.xml clean verify` | 8 项契约测试及打包 |
| 真实供应商 + 可执行 JAR / JDK 17 | 独立带密码 Redis、真实 DeepSeek/SiliconFlow、本机志愿 HTTP 替身 | 下述 6 个业务请求均通过 |

普通测试明细：AgentChatContractTest 41、AgentApplicationTest 5、AiModelConfigurationTest 6、VisionContextManagerTest 7、AgentToolsTest 3。原普通测试在工具协议迁移后继续运行，并新增参数、循环、同轮去重、隐私与上下文隔离检查。

真实供应商使用用户明确提供的仓库外 YAML，未将 Key 写入源码、参数或输出。对话模型为 `deepseek-flash`，Embedding 为 `Qwen/Qwen3-Embedding-0.6B`，维度 1024。实际运行打包 JAR，经 Token 登录、ChatClient 原生工具循环、Feign 与 HTTP 响应验证：

1. 根据预置视觉参考回复“蓝色雨伞”，动作 NONE。
2. 同步开启避障，动作 CONNECT_WS。
3. 异步关闭避障，动作 DISCONNECT_WS。
4. 同步重新拍照上传，动作 CAPTURE_UPLOAD。
5. 异步志愿求助，动作 START_VIDEO_CALL，并返回本机志愿替身的完整通话信息；替身只收到 1 次请求且收到正确 Token。
6. 后续纯闲聊，动作恢复 NONE，commandData 为空，不继承上一轮票据。

模型在操作回复中区分“已下发指令/等待设备执行”和“取得通话信息/等待客户端发起”。真实模型测试是少量功能验收，不代表所有表达、负载或长期运行质量。

## 验证边界与后续工作

- 真实模型测试没有向生产志愿服务创建任务，也没有通知真人；志愿服务由本机 HTTP 替身承担，实际 Feign 调用与 Token 透传已验证。
- 未验证真实 Nacos 服务端传输；自动化使用 NamingService 替身，打包手工联调使用 SimpleDiscoveryClient 的本机实例配置。
- 未执行真实 COS/RocketMQ/Python 图片识别，视觉证据预置于隔离 Redis。Picture 契约测试和原视觉 Lua 集成继续通过。
- 普通 Redis 不包含 RediSearch，真实模型联调中长期记忆检索/写入会降级；本次不声称向量检索已获真实验收。
- 记忆仍为自定义 ChatMemoryManager/Jedis 存储，不是 Spring AI ChatMemory、记忆 Advisor 或 VectorStore 迁移。
- 同轮去重不是跨请求幂等，也不能消除远程调用超时后的业务结果不确定性；创建求助接口后续可增加业务幂等标识。
- 原异步入口 60 秒等待、线程池与 Token 透传保留，未实现超时取消正在运行的模型/工具。工具次数上限不等于整个请求的耗时上限。
- 不涉及前端实际执行指令、视频建立、志愿者接听或通知链路。

## 配置与回退

无需增加模型凭证配置，但对话模型必须支持原生工具调用。无 Redis Schema/索引迁移，不改 Picture 与其他服务。恢复 PR-02 版本及原配置即可回退。
