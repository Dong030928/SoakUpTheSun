# PR-01 验证记录

日期：2026-10-17。范围仅为 Agent 的 Boot 4 升级及回归基线，未接入 Spring AI。

## 环境与依赖

- Windows 11，Maven 3.9.11。
- Java 17：Microsoft OpenJDK 17.0.20.1；Java 21：Oracle JDK 21.0.8。
- Redis 8.2.1，在临时目录和随机端口启动独立带密码进程，关闭持久化。
- 本机全局 Maven 配置使用旧 HTTP 镜像。本次通过临时 `-s` 设置文件访问 HTTPS Maven Central；未修改全局配置，未将机器路径或该设置文件提交到仓库。

依赖树主要结果：

```text
spring-boot-starter-webmvc              4.0.8
spring-boot-starter-data-redis          4.0.8
spring-cloud-starter-alibaba-nacos-discovery 2025.1.0.0
nacos-client                           3.1.1
spring-cloud-commons                   5.0.3
spring-cloud-context                   5.0.3
spring-cloud-starter-openfeign         5.0.3
spring-cloud-loadbalancer              5.0.3
springdoc-openapi-starter-webmvc-ui     3.0.3
lettuce-core                          6.8.2.RELEASE
jedis                                 5.1.0
lombok                                1.18.46
```

Cloud 由 BOM 2025.1.3 管理，Alibaba 由 BOM 2025.1.0.0 管理。排除测试 Starter 中重复的 android-json，保留 Jedis 使用的 org.json，避免两个 JSONObject 实现并存。

## 验证步骤

| 步骤 | 命令或方法 | 结果与边界 |
| --- | --- | --- |
| 升级前基线 | 原 POM 下 `mvn -f agent/pom.xml -Dlombok.version=1.18.30 test` | JDK 21 上 15 项通过。Lombok 覆盖只用于原 Boot 3 基线，新 POM 无此覆盖 |
| Java 17 干净构建与 Redis 集成 | `mvn -f agent/pom.xml clean verify -Plocal-redis -Dredis.server.executable=<redis-server绝对路径>` | 19 项普通测试、1 项 Redis 集成测试通过；生成可执行 JAR |
| Java 21 回归 | `mvn -f agent/pom.xml verify` | 19 项普通测试通过；编译目标仍为 Java 17 |
| 打包产物启动 | `java -jar agent/target/agent-0.0.1-SNAPSHOT.jar`，注入隔离环境参数 | Java 17 下实际启动 JAR；禁用 Nacos，使用临时真实 Redis 与本机 HTTP 模型替身 |
| 打包产物 HTTP 检查 | 向实际端口请求 `/v3/api-docs`、两个聊天接口 | OpenAPI 包含两个接口；同步/异步均返回 code=0、预期模型替身回复与 NONE 指令 |
| 依赖与补丁检查 | `mvn -f agent/pom.xml dependency:tree`、`git diff --check` | 依赖解析和差异检查通过 |

执行命令均额外使用前述临时 Maven 设置。报告由 Surefire/Failsafe 生成，位于 `target/`，不提交生成产物或完整用户机器日志。独立进程在验证结束后关闭。

## 覆盖内容

- 原始字符串输入、JSON 请求体作为字符串读取、同步/异步响应、空输入、缺失/过期 Token、客户端三个动作、志愿房间与凭证返回、模型失败、非法模型决策、线程池拒绝。
- 完整 Spring Boot 自动配置与嵌入式 Tomcat、Jackson 3 的日期格式和旧 Result 包装、OpenAPI 和 `/doc.html` 重定向。
- NacosDiscoveryClient、LoadBalancer、真实 Feign HTTP 调用及 Token 透传。此处仅 NamingService 传输为 Mockito 替身，不会访问 Nacos 服务端。
- 真实 Redis 中 User 服务同格式的登录 Key/Hash，Lettuce 与 Jedis 的密码认证，以及两种聊天入口。
- 实际可执行 JAR 不依赖 MySQL、Elasticsearch、RocketMQ 即可在隔离配置下启动。

## 尚未执行的外部验收

- 真实 Nacos 服务端注册、注销、gRPC 和认证，以及与原版其他微服务的互操作。
- 真实 DeepSeek/Embedding 供应商调用和视频求助全链路。
- RediSearch 的索引创建与向量检索。Redis 集成测试替换了向量组件；打包启动 smoke 使用普通 Redis，现有索引初始化会记录缺少 FT 命令，测试不将其视为向量能力通过。
- GitHub Actions 的远端实际运行；已提供工作流，本地等效 Maven 检查已执行。

这些边界不影响已验证的构建与接口回归结论，但不能据此声称生产部署或全链路联调完成。部署前按 README 在隔离环境补齐检查。

## 回退

无 Redis Key、数据 Schema 或索引迁移。恢复升级前提交及配置即可回退；不删除或清空共享缓存，不升级其他服务。
