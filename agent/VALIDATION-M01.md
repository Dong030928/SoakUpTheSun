# M01 本地验证记录

日期：2026-10-18。基线：上游 main `da17661d`（已合并 PR #28）。分支：`feat/agent-memory-jdbc`。

## 实现范围

新增 Spring AI JDBC 归档/快照、原始轮次和 memory_log 的同事务服务、连续落库水位、明确检查点恢复、离线导入/退休清理、版本化 Schema 和隔离 MySQL CI。框架快照字段适配现有用户会话 ID，不使用默认 36 字符 Schema。

当前在线聊天仍提交 Redis；本次没有前台同步 MySQL、RocketMQ 生产/消费、模型厂商 TTL、摘要或摘要向量实现。M02/M03 按已有设计继续，不把基础组件验证写成整个记忆演进已完成。运行和迁移说明见 [ARCHIVE-M01.md](ARCHIVE-M01.md)。

## 环境

- Maven 3.9.11；Java 17.0.20.1 和本机 Java 21；编译目标 17。
- Spring AI 2.0.1、Boot 4.0.8；Flyway 11.14.1 与 JDBC 驱动由 Boot BOM 管理。
- H2 2.4.240 用于普通隔离测试；Flyway 对其版本提示未完成兼容认证，因此另以真实 MySQL 验证生产 Schema、方言和事务行为。
- 私有 MySQL 8.0.46 容器，显式端口及测试标记；私有 Redis 8.2.1 容器提供 Query Engine/JSON；本机 Redis 8.2.1 可执行文件另起独立进程测试。
- 容器无用户目录挂载；只访问临时测试数据，不连接业务 MySQL/Redis。

## 执行结果

| 环境 | 命令（省略本机 Maven settings/JDK 环境设置） | 结果 |
| --- | --- | --- |
| Java 17 | `mvn -B -ntp -f agent/pom.xml clean verify -Plocal-redis,mysql-archive,redis-vector -Dredis.server.executable=<private redis executable> -Dmysql.archive.port=<private port> -Dredis.vector.port=<private port>` | 204 普通测试＋8 私有 Redis＋12 MySQL＋8 向量集成，全部通过 |
| Java 21 | `mvn -B -ntp -f agent/pom.xml clean verify -Pmysql-archive,redis-vector -Dmysql.archive.port=<private port> -Dredis.vector.port=<private port>` | 204 普通测试＋12 MySQL＋8 向量集成，全部通过 |
| Picture / Java 17 | `mvn -B -ntp -f picture/pom.xml clean verify` | 8 项通过 |
| 实际 JAR / Java 17 | 启用真实私有 MySQL/Redis 的非 Web 维护命令 | dry-run 规划 1 轮、导入 1 轮、重复导入 0 轮、热点删除后恢复成功，四个进程 exit 0 |

组合 profiles 在 PowerShell 中使用引号。测试统计区分 Surefire 普通测试与 Failsafe 集成测试；CI 新增真实 MySQL job，其他既有验证保留。

## 关键验证

- 同一事件重投无新增；相同 ID 不同正文、同位置不同事件和错误用户/Epoch 被拒绝。
- 先收到第 2 轮时连续落库水位为 0，不形成伪完整历史；收到第 1 轮后补齐，并发与重复消息最终收敛。
- 通过真实框架 JDBC 仓库写快照后故意抛异常，验证原始轮次、会话、框架快照及消费日志整体回滚。
- 窗口/字符裁剪仅影响框架快照，不删除或截断原始历史；长会话 ID、中文与大小写不同的事件 ID 正常保存。
- 导入 dry-run/后续冲突整体回滚；重复导入确定性去重；原时间未知不伪造，不根据未知时间做退休清理。
- 恢复要求冻结写入和排空确认，检查点不一致或存在序号缺口即拒绝。Lua 只回填缺失 Key；新热点、损坏热点和其他持有者的租约均受到保护。
- 退休保留 tombstone，迟到事件不能复活；源/消费者仍有未决任务时需要 M02/M03 的扩展清理门禁，当前仅可离线操作。
- 显式启用才建立数据库服务；默认完整 Boot 应用和原聊天/工具/视觉/向量契约通过。启用缺配置快速失败，真实 MySQL 验证生产配置读取准备好的 Schema。
- 实际 JAR 使用虚构模型配置和禁用 Embedding，验证维护命令无需真实供应商调用；本 PR 没有更改模型调用行为，不重复付费做无关模型测试。

## 评测和限制

`evaluation/memory-baseline.json` 是固定合成场景与报告字段，用于后续 M03 的记忆质量/成本对比；本 PR 没有测定真实模型事实准确率、摘要收益或降本百分比。

M01 不自动获取生产端完整水位，也不启用聊天自动回源。数据库最高已见序号不证明没有尚未到达的消息；恢复的生产检查点必须独立核实，旧窗口无法补回早已裁剪的数据。

没有执行生产迁移、共享基础设施联调或新的远端 PR 创建。远端 Actions 是否通过应查看提交后的 GitHub 状态，不能用本地通过代替。
