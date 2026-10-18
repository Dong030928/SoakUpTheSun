# M03 验证记录

日期：2026-10-18。基线为上游合并 M02 的 `3573de56`，分支 `ffeat/agent-memory-summary`。Spring Boot 4.0.8、Spring AI 2.0.1、RocketMQ 客户端/Broker 5.4.0；没有增加模型/向量供应商或改变依赖基线。

## 最终自动化结果

| 检查 | 结果 |
| --- | --- |
| Agent Java 17：普通测试＋全部隔离集成 | 258 普通＋73 集成，通过 |
| Agent Java 21：普通测试＋隔离 MySQL/向量/MQ/摘要，反向类顺序 | 258 普通＋65 集成，通过 |
| Picture Java 17 | 8 测试，通过 |
| 真实 DeepSeek 模型 | 两次继承摘要＋一次追问，通过 |

普通测试新增 27 项：厂商 TTL/双水位/配置竞态/近期窗口回退、稳定任务身份、数据库任务租约/投影、工具循环内预算、usage 校准、DeepSeek 缓存字段兼容。普通测试不访问真实模型，不读取私有配置。

最终集成测试组成：`LocalRedisIT` 8（仅 Java 17）、`MysqlArchiveIT` 12、`RedisVectorIT` 8、`MemoryTransactionsIT` 18、`TransactionApplicationIT` 7、`MemorySummaryIT` 16、`SummaryApplicationIT` 4。使用独立标记 MySQL 8.0.46、Redis 8.2.1（AOF/JSON/Query Engine）和 RocketMQ 5.4.0；模型、Embedding 和志愿服务由测试替身提供。

M02/M03 的 Boot 测试均在类结束时关闭 Spring 上下文，停止 MQ 客户端、对账和扫描线程；Maven 明确指定类顺序。Java 21 使用反向顺序，避免重现 M02 上次“上下文未关闭导致故障测试被后台对账抢跑”的问题。GitHub CI 在 Java 17/21 合并运行两个 profile，矩阵保留 `fail-fast: false`。

## M03 真实基础设施覆盖

- 半消息 ACK 后本地提交、共享证明回查，原生产者退出后由同组另一生产者回查；消费者尚未登记 SQL job 时的重投恢复。
- 全部原文参与判断与选择，超过旧 20 条窗口仍保留前缀；四种 TTL/水位组合、厂商计时起点、实际过期时间和未配置 TTL。
- 创建时计时、确认命中才续期、同规则修改时长重算；旧模型响应、半消息 RPC 期间的配置更新不能沿用过期决定。
- 摘要期间新消息被完整接受并归档，旧范围合并不会删除新轮次；未完成归档时不调用摘要模型，不裁剪唯一原文。
- worker 代次/租约、重复任务、旧版本迟到、空/超长摘要，保留重试阶段和完整输入。
- 模拟 Lua 已写入但响应丢失：持久结果修复投影，模型调用一次；模拟 Embedding 故障：从 `MERGED` 只重试向量阶段。
- 丢失热摘要状态后按独立生产 head 和完整 SQL 原文恢复；ZSet 丢失后增量补扫；不从 SQL MAX(seq) 推测完整性。
- 两次摘要继承、确定性文档、旧版本失效、同源轮次去重、同用户跨会话召回与跨用户隔离；真实 JSON.MGET/RedisVectorStore，不清空或重建旧索引。
- 禁用 Embedding 时明确 `SKIPPED`；单独停用摘要向量候选和写入；近期窗口回退保留摘要及原文。
- 实际 Boot/Tomcat/auth/Spring AI HTTP 路径下，超过 20 条历史、摘要参考注入、输出 reserve、模型/工具前预算拒绝、原 DTO/响应头兼容；原有工具、视觉、迁移和归档契约继续通过。

## 三次真实模型检查

私有文件通过测试专用参数加载，未修改、复制或提交 Key。只调用当前配置的 DeepSeek `deepseek-flash`，没有调用真实业务工具、Embedding 或用户历史。

| 调用 | 输入 Token | 输出 Token |
| --- | ---: | ---: |
| 第一轮摘要 | 290 | 54 |
| 继承式第二轮摘要 | 268 | 75 |
| 基于最终摘要追问 | 143 | 68 |
| 合计 | 701 | 197 |

合计 898 Token，三次调用用时约 2.94 秒（单次测试总耗时，不是前台延迟指标）。成功保留“绿茶而非红茶、海鲜而非花生、朋友小王/同事小王、复诊时间更新、尚未叫车”。合成输入的实际摘要和追问输出见 [评测记录](evaluation/memory-summary-live-2026-10-18.json)。

该检查是小规模连通性和事实保留验证，不是完整质量评测、性能压测或费用节省对比；没有真实厂商 TTL 验证，没有声称 62% 降本、P95 改善或生产故障不丢失。

## 复现

```sh
bash agent/src/test/fixtures/memory-transactions/start.sh
mvn -B -ntp -f agent/pom.xml clean verify -Pmemory-transactions,memory-summary \
  -Dmemory.transactions.fixture=agent-transactions-integration-only
docker compose -f agent/src/test/fixtures/memory-transactions/compose.yaml down --volumes
```

完整本地矩阵另外启用 `local-redis,mysql-archive,redis-vector`，并显式指定私有 Redis 可执行文件、带标记 MySQL 端口和向量 Redis 端口。Java 21 另传 `-Dfailsafe.runOrder=reversealphabetical`。仅操作测试 compose 项目和测试容器，不执行共享实例 `FLUSHDB`。

可选真实模型检查会付费调用三次，只接受仓库外的 YAML；普通测试/CI 不启用它：

```powershell
mvn -B -ntp -f agent/pom.xml verify -Plive-memory-summary `
  '-Dlive.memory.summary=true' `
  '-Dlive.memory.config=C:/path/outside-repository/agent-secrets.yaml'
```

该 YAML 只需包含已有 `suts.ai.api-host/api-key/model-name`。输出到 `agent/target/live-memory-summary-report.json`，只含合成历史、模型名、usage 和耗时。

## 边界

- 摘要默认关闭，启用前需要经确认的 C/O/S/B/L/H 和 V2 迁移。没有为 DeepSeek 编造固定 TTL，也没有真实验证供应商实际过期时间。匹配 tokenizer 缺失时的计量局限见 [ARCHIVE-M03.md](ARCHIVE-M03.md)。
- 未验证生产存量迁移、Redis Cluster、真实 Nacos/志愿服务/视频完整链路、生产吞吐和冷热缓存费用对比。Picture 仍以 Java 17 验证；其原依赖在本机 Java 21 直接构建遇到旧 Lombok `JCImport` 兼容错误，本 PR 未修改 Picture 的依赖。
- Redis 永久丢失、Broker 回查耗尽、人工更改 generation 或跨节点时钟偏差仍有 M02 所述恢复边界。近期窗口/摘要向量有功能回退开关，旧二进制回退仍需冻结、排空和显式迁移，不能删除证明 Key 强制放行。
