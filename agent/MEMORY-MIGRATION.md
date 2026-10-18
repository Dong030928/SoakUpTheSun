# 记忆存储迁移与回退

本版本替换手写向量访问，并新增 Redis 短期会话。线上存量数据必须先在备份或隔离副本演练；命令默认关闭、默认 dry-run，不会在正常启动时自动扫描或转换数据。

## 存储与前置条件

| 数据 | 旧版 | 新版 |
| --- | --- | --- |
| 短期会话 | 未使用的旧列表读取逻辑 | `suts:agent:memory:v2:{user:<id>:<session-sha256>}:messages`，版本化完整文本轮次与 TTL |
| 长期记忆 | `suts:vector:data:memory:<userId>:<id>` HASH，原索引 `suts:vector:index:memory` | `suts:agent:memory:vector:v2:<fingerprint>:<id>` Redis JSON，原生 Spring AI RedisVectorStore |
| 向量索引 | 手写 HASH HNSW | `suts:agent:memory:index:v2:<fingerprint>` JSON HNSW |

指纹包含 Schema、Embedding 地址、模型名及维度。新记录同时保存 userId、schemaVersion、embeddingModel、embeddingModelTag、embeddingDimension、createdAt，迁移来源另含 legacyKey。编码标签用于兼容模型名中的 `/`；原模型名仍保留并校验。

Redis 需具备 Query Engine/JSON，已验证官方 Redis 8.2.1。向量扩展使用 DB 0；各客户端的 host、port、认证及 database 配置必须一致。普通 Redis 的短期记忆仍可用，长期记忆会降级。原始向量缺少可靠模型来源，正向迁移始终从旧文本重新 Embedding，不复制旧浮点数据；执行前预算调用费用。

旧列表没有生效的写入路径，故不会自动当作可信历史导入。短期 v2 从新会话开始，旧 HASH 文本通过下述长期迁移保留。若实际部署另有旧列表写入，先另行确认其消息协议与归属，不能直接导入系统/工具/凭证字段。

## 正向切换

1. 备份旧索引与 HASH，记录原 Embedding 地址、模型、维度、数量和失败项。在隔离副本运行 dry-run。
2. 停止所有旧版本对话实例和后台记忆写入，等待正在执行的对话/Embedding 排空。仅在新版本设置冻结开关不能阻止旧进程写入。
3. 在独立离线进程中加载模型私有配置与同一 Redis DB，显式声明冻结并排空写入。迁移进程不开放 HTTP、不注册 Nacos。
4. dry-run 只校验、统计待处理记录与待 Embedding 字符数，不调用 Embedding，不改源记录或进度。非法 UTF-8、归属不一致、超预算或冲突记录会停止处理；不静默截断/跳过。
5. 确认报告后显式执行写入。检查点与确定性文档 ID 支持失败后续跑；修复原因后复用同一检查点。已完成检查点再跑为无操作，新的冻结批次或增量扫描需使用新的 checkpoint-name。
6. 核对源记录数、目标记录归属、导入来源、失败报告和实际召回。导入记录以当前时间记录 createdAt，不声称保留旧记录不存在的创建时间。
7. 确认无旧写入后，启动新版本、解除短期与长期写入冻结，再开放流量。保留旧索引和备份，清理另行操作。

项目根目录 PowerShell 示例，地址/凭证通过仓库外 YAML 提供。Redis 配置也应放入该私有文件或环境变量，不能误连生产默认值：

```powershell
$privateConfig = 'file:C:/Users/Administrator/.suts/agent-secrets.yaml'
$migrationArgs = @(
  '--spring.main.web-application-type=none',
  "--spring.config.additional-location=$privateConfig",
  '--spring.cloud.nacos.discovery.enabled=false',
  '--spring.cloud.service-registry.auto-registration.enabled=false',
  '--suts.ai.memory.migration.enabled=true',
  '--suts.ai.memory.migration.writes-frozen=true',
  '--suts.ai.memory.migration.writers-drained=true',
  '--suts.ai.memory.writes-enabled=false',
  '--suts.ai.memory.vector.writes-enabled=false',
  '--suts.ai.memory.migration.checkpoint-name=cutover-001'
)
# 默认 dry-run：先核对报告和调用费用预算
java -jar agent/target/agent-0.0.1-SNAPSHOT.jar @migrationArgs
# 仅在冻结和排空已完成、dry-run 通过后执行
java -jar agent/target/agent-0.0.1-SNAPSHOT.jar @migrationArgs --suts.ai.memory.migration.dry-run=false
```

`writes-frozen/writers-drained` 是操作者的明确声明，不会远程停止其他服务。实际执行会验证两类运行时写入开关均关闭，并用租约保护迁移作业；检查点更新原子核对 Token，旧作业不能覆盖后继进度。进度只保存游标、计数和失败记录散列/异常类型，不保存正文或供应商错误响应。

迁移期间不修改源文本。若发现已有目标 ID 与源归属/正文不符，任务停止；不能通过复用检查点掩盖源数据变化。扫描有重复时依靠确定性 ID 去重，报告的 scannedAttempts 是处理尝试数，不一定等于唯一文档数。

## 回退补齐

保留旧索引不等于完整回退。新版本新增长期记忆仅存在 v2；回退前必须停止并排空新写入，备份 v2，并把这些增量补齐。

本工具仅支持**原模型地址、模型名和维度与当前 v2 完全一致**时补齐旧 HASH：

```powershell
$rollbackArgs = @($migrationArgs | Where-Object { $_ -notlike '--suts.ai.memory.migration.checkpoint-name=*' }) + @(
  '--suts.ai.memory.migration.direction=ROLLBACK',
  '--suts.ai.memory.migration.checkpoint-name=rollback-001',
  '--suts.ai.memory.migration.rollback-embedding-host=https://api.siliconflow.cn/v1',
  '--suts.ai.memory.migration.rollback-embedding-model=Qwen/Qwen3-Embedding-0.6B',
  '--suts.ai.memory.migration.rollback-embedding-dimension=1024'
)
java -jar agent/target/agent-0.0.1-SNAPSHOT.jar @rollbackArgs
java -jar agent/target/agent-0.0.1-SNAPSHOT.jar @rollbackArgs --suts.ai.memory.migration.dry-run=false
```

示例模型值必须替换为有据可查的原部署配置，不能为了通过检查照抄当前模型。模型或维度不同，需要使用旧模型从文本重新生成向量，本命令拒绝该情况，不提供跨模型直接复制。

导入来源的旧 HASH 若仍存在且正文一致则保留；新版本新增记录写入确定性 rollback 键。Lua 只创建不存在的 HASH，完全相同的重复导出为无操作，冲突则停止。旧 HASH 的原向量不会被新向量覆盖。

补齐后验证旧版索引能按用户正确检索，再切回旧应用及其原模型配置。工具不删除 v2/源记录；短期 v2 窗口旧版不可读，回退后短期会话会重新开始。后续再切正向时应使用新的检查点，重复文本由召回层去重，但不同导入来源可能保留多个文档。

## 限制

- 记忆异步队列为尽力而为，排空后仍需核对降级/拒绝日志，不能假定每轮都有长期记录。
- 作业租约为十分钟，每条记录前后续约；Embedding 超时应远小于该租期。不能用超长单次调用替代分批迁移。
- 向量写入本身不是跨进程事务。依赖冻结、排空和源数据不变；异常停止后按确定性 ID 校验并续跑。
- 迁移不能撤销志愿求助或客户端动作；本工具只处理记忆数据。
