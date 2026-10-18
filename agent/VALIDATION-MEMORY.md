# Agent 记忆重构验证记录

日期：2026-10-18。基于已合并工具调用重构的 main（37109a12），分支 `refactor/agent-memory`。这是原五阶段计划中的记忆迁移阶段，避免与已经合并的工具 PR-03 混淆。

## Spring AI 重构方式

| 原实现 | 本次实现 |
| --- | --- |
| 无实际生效的短期会话，只有未使用的列表读取方法 | `ChatMemory`、`MessageWindowChatMemory`、`MessageChatMemoryAdvisor` 与会话绑定的 Redis `ChatMemoryRepository` |
| 在服务中把视觉/召回直接拼入用户输入 | +250 的引用 Advisor 在外层记忆捕获后追加当前参考内容，+300 的工具循环继续使用完整本轮上下文 |
| 手写 Hash、FLOAT32、KNN 和索引创建 | 原生 `RedisVectorStore.add/similaritySearch`，新版本 JSON 索引与模型命名空间 |
| `@Async` 没有启用，记忆写入实际同步 | 明确提交到独立有界记忆执行器，清理继承的登录上下文，队列满时降级 |

Redis 仓库采用续约租约和 Token 校验 Lua，跨实例拒绝同会话重叠，提交完整轮次而不持久化模型失败的半轮。会话 ID 同时包含认证用户与可选会话名的散列。消息数量、单条文本、召回与完整文本上下文均有限额。

框架 Memory Advisor 在本轮暂存窗口中运行，成功或工具兜底后才提交原始用户文本和权威最终回复。系统消息、原始视觉/召回块、工具中间协议和 commandData 不持久化；助手的正常自然语言仍可包含识别结果或历史事实。短期提交失败或失去租约时，不再排队长期写入，已完成工具响应仍保留。读不到原快照时也不使用空快照覆盖旧记录。

长期存储强制 userId、Schema、模型标签与维度过滤，并对返回元数据和分数再次检查。原始模型名保留，使用精确 UTF-8 十六进制标签过滤，修复 Spring AI 2.0.1 Redis TAG 转换对 `/` 的兼容问题。向量验证覆盖维度、空/零向量、非有限值和大数归一化。

## 本地验证

Windows 11，Maven 3.9.11，Microsoft JDK 17.0.20.1、Oracle JDK 21.0.8。普通 Redis 为独立原生 8.2.1 进程；Query Engine/JSON 使用官方 Redis 8.2.1 Docker 镜像、随机 localhost 端口、测试密码和测试专用标记，不挂载现有数据。

| 项目 | 验证方法 | 结果 |
| --- | --- | --- |
| Agent / JDK 17 | `clean verify -Plocal-redis -Dredis.server.executable=<本机redis-server>` | 182 项普通测试 + 6 项真实 Redis 集成通过；向量入口另跑 8 项通过 |
| Agent / JDK 21 | `clean verify -Predis-vector -Dredis.vector.port=<隔离容器端口>` | 182 项普通测试 + 8 项真实向量/迁移集成通过 |
| Picture / JDK 17 | `mvn -f picture/pom.xml clean verify` | 8 项契约测试及打包通过 |
| 实际 JAR / JDK 17 | 真实 DeepSeek、SiliconFlow、隔离 Redis Query Engine/JSON | 下述六个聊天请求、实际向量写入和短期恢复通过 |
| 实际 JAR 离线迁移入口 / JDK 17 | 隔离旧 HASH fixture、仓库外配置、显式冻结/排空、non-web dry-run | 退出码 0；扫描/计划各 1，写入 0，无 Embedding 调用 |

普通回归不读取私有模型配置，不需要外部 Redis 或 Docker。两个 Redis 集成入口显式分开，原生 Redis 入口不会误跑向量测试。新增 CI 作业启动并标记独立 Redis 容器，执行真实向量与数据迁移测试。

覆盖内容：

- 既有同步/异步、Token、客户端动作、志愿求助、Feign、JSON/日期、OpenAPI 和视觉契约。
- 连续追问、同用户多会话、跨用户隔离、窗口与字符预算、租约续约、旧持有者隔离、TTL、存储重建、失败和冻结。
- 真实异步线程和登录上下文清理；队列拒绝与写入失败不改变已完成业务响应。
- 原生 JSON 索引、Owner/Schema/模型过滤、Top K、阈值、空结果、去重、同维度不同模型隔离、索引格式不符及异常向量。
- 旧格式 fixture 重新 Embedding、dry-run、确定性 ID、检查点续跑/去重、损坏归属与 UTF-8、失败停止、租约保护、增量回退和防覆盖。

## 真实模型与可执行 JAR

使用用户明确提供的仓库外 YAML。对话模型 `deepseek-flash`，Embedding `Qwen/Qwen3-Embedding-0.6B`，维度 1024；使用默认 Top K=3、阈值 0.7。未提交或输出真实 Key。

1. 用户 A 的 alpha 会话告知偏好火龙果；真实 Embedding 写入 JSON 向量索引。
2. 同会话通过异步接口追问，正确回答火龙果。
3. 用户 A 的新 beta 会话没有短期历史，仍通过长期召回正确回答。
4. 用户 B 使用同名 alpha 会话，回答不知道，没有读到用户 A 的偏好。
5. 停止并重新启动实际 JAR，用户 A 的 alpha 会话仍能回答偏好。
6. 异步开启避障返回 CONNECT_WS；短期 Redis 记录只含 USER/ASSISTANT 文本，无 Token 或工具调用协议。

## 数据切换与边界

本次未迁移任何生产记录。迁移命令默认关闭、默认 dry-run，必须在离线模式、冻结且排空写入后显式执行。始终从旧文本重新 Embedding；不以维度相同推断模型兼容。回退补齐要求原供应商地址、模型和维度完全匹配，保留旧 HASH，新增记录写到确定性回退键；不自动删除源数据。

完整演练步骤和费用/增量/回退要求见 `MEMORY-MIGRATION.md`。保留旧索引并不能替代新版本增量补齐；短期 v2 窗口旧版不可读。

尚未验证真实 Nacos 服务端、生产志愿服务/通知/视频、COS/RocketMQ/Python 图片识别、生产数据规模和并发压力。字符预算不等于 Token 预算，向量队列不是可靠消息系统，跨请求幂等和统一超时取消仍由后续工作处理。会话存储暂不可达时无法取得租约，系统会在工具开始前拒绝请求；已有动作完成后的记忆提交失败则降级返回动作结果。
