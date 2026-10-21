package org.hgc.suts.gateway.rag;

import com.sun.net.httpserver.HttpServer;
import org.hgc.suts.gateway.AgentApplication;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.profile.JdbcProfileStore;
import org.hgc.suts.gateway.memory.summary.SummaryWorker;
import org.hgc.suts.gateway.memory.transactions.RocketMqTransactionTransport;
import org.hgc.suts.gateway.rag.document.DocumentEtl;
import org.hgc.suts.gateway.rag.document.JdbcDocumentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hgc.suts.gateway.rag.RagData.*;

/** Actual Boot MVC/auth/configuration/worker wiring, private real stores and local model wire protocols. */
@Timeout(180)
class RagApplicationIT {
    private static final String MARKER = "agent-rag-integration-only";
    private static final String ES = "http://127.0.0.1:19200";
    private static final String MYSQL = "jdbc:mysql://127.0.0.1:13326/";
    private static final String JDBC_OPTIONS = "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=2000&socketTimeout=5000&serverTimezone=UTC";
    private final RagIndexHttp infrastructureHttp = new RagIndexHttp(10000, 1024 * 1024, Map.of());
    private final HttpClient browser = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    @Test void documentOnlyBootImportsPublishesAuthenticatesExpandsAndRevokesWithoutMemoryOrMq() throws Exception {
        assertThat(System.getProperty("rag.retrieval.fixture"))
                .as("Explicitly opt in with -Drag.retrieval.fixture=agent-rag-integration-only").isEqualTo(MARKER);
        var fixtureJdbc = new JdbcTemplate(dataSource("agent_rag_it"));
        assertThat(fixtureJdbc.queryForList("SELECT marker FROM suts_agent_test_fixture", String.class)).containsExactly(MARKER);
        assertThat(getEs("/").path("cluster_name").asText()).isEqualTo("suts-agent-rag-retrieval-it");
        assertThat(getEs("/suts-rag-fixture-marker/_doc/fixture").path("_source").path("marker").asText()).isEqualTo(MARKER);
        String namespace = UUID.randomUUID().toString().replace("-", "");
        String database = "agent_rag_app_" + namespace, esIndex = "suts-rag-app-" + namespace;
        String documentId = "app_" + namespace, ownerToken = "rag-app-owner-" + namespace, foreignToken = "rag-app-foreign-" + namespace;
        long owner = 4200000 + Integer.toUnsignedLong(namespace.hashCode()), foreign = owner + 1;
        String ownerAccount = "rag-app-owner-" + namespace, foreignAccount = "rag-app-foreign-" + namespace;
        boolean databaseCreated = false;
        JdbcTemplate applicationJdbc = null;
        RedisRagIndex vectors = null;
        try (var privateRedis = RedisClient.builder().hostAndPort("127.0.0.1", 16393).clientConfig(DefaultJedisClientConfig.builder()
                .password(MARKER).connectionTimeoutMillis(2000).socketTimeoutMillis(3000).build()).build();
             var models = new LocalModels()) {
            assertThat(privateRedis.get("suts:agent:test:rag-retrieval-fixture")).isEqualTo(MARKER);
            try {
                assertThat(database).matches("agent_rag_app_[a-f0-9]{32}");
                fixtureJdbc.execute("CREATE DATABASE " + database + " CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); databaseCreated = true;
                applicationJdbc = new JdbcTemplate(dataSource(database));
                applicationJdbc.execute("CREATE TABLE suts_agent_test_fixture (marker VARCHAR(64) NOT NULL PRIMARY KEY)");
                applicationJdbc.update("INSERT INTO suts_agent_test_fixture (marker) VALUES (?)", MARKER);
                assertThat(applicationJdbc.queryForObject("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=?", Long.class, database)).isEqualTo(1);

                try (ConfigurableApplicationContext context = new SpringApplicationBuilder(AgentApplication.class).run(
                        arguments(database, namespace, esIndex, models.endpoint()))) {
                    assertThat(context.getBeansOfType(RocketMqTransactionTransport.class)).isEmpty();
                    assertThat(context.getBeansOfType(JdbcConversationArchive.class)).isEmpty();
                    assertThat(context.getBeansOfType(JdbcProfileStore.class)).isEmpty();
                    assertThat(context.getBeansOfType(SummaryWorker.class)).isEmpty();
                    assertThat(context.getBeansOfType(ProfileRagAccess.class)).isEmpty();
                    assertThat(context.getBean(RagRetrievalService.class)).isNotNull();
                    assertThat(applicationJdbc.queryForList("SELECT version FROM agent_rag_schema_history WHERE success=TRUE ORDER BY installed_rank", String.class))
                            .containsExactly("0", "1");
                    assertThat(applicationJdbc.queryForObject("SELECT type FROM agent_rag_schema_history WHERE version='0'", String.class)).isEqualTo("BASELINE");
                    assertThat(applicationJdbc.queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class)).isEqualTo(MARKER);

                    int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
                    var redis = context.getBean(StringRedisTemplate.class);
                    login(redis, owner, ownerAccount, ownerToken); login(redis, foreign, foreignAccount, foreignToken);
                    var store = context.getBean(JdbcDocumentStore.class); var etl = context.getBean(DocumentEtl.class);
                    vectors = context.getBean(RedisRagIndex.class); var keywords = context.getBean(ElasticsearchRagIndex.class);
                    var worker = context.getBean(DocumentIndexWorker.class);
                    String source = "fixture:wheelchair-guide.md";
                    String markdown = "# 轮椅 wheelchair 申请\n1. 携带身份证到服务窗口申请轮椅 wheelchair。\n"
                            + "   1. 先复印身份证正反面。\n2. 服务人员确认材料后登记预约。\n3. 收到通知后携带回执领取轮椅。\n"
                            + "补充说明：wheelchair 预约完成后请保留回执，并按窗口通知时间到场。\n".repeat(4);
                    var parsed = etl.parse(markdown.getBytes(StandardCharsets.UTF_8), "wheelchair-guide.md");
                    var imported = store.importDocument(documentId, owner, Visibility.OWNER, "轮椅申请指南", source, parsed, vectors.target());
                    assertThat(store.status(owner, documentId).currentVersion()).isZero();
                    worker.process(imported.jobId());
                    var status = store.status(owner, documentId);
                    assertThat(status.currentVersion()).isEqualTo(1);
                    assertThat(status.jobs()).singleElement().satisfies(job -> {
                        assertThat(job.redisDone()).isTrue(); assertThat(job.esDone()).isTrue(); assertThat(job.state()).isEqualTo("DONE");
                    });
                    assertThat(models.embeddingCalls.get()).isEqualTo(parsed.children().size());

                    String plan = RagIndexHttp.JSON.writeValueAsString(new Plan("原始问题：轮椅如何申请？", "轮椅 wheelchair 申请", Scope.DOCUMENT, List.of(documentId)));
                    assertThat(search(port, null, plan).path("code").asText()).isNotEqualTo("0");
                    int beforeQueryEmbedding = models.embeddingCalls.get();
                    JsonNode result = searchEventually(port, ownerToken, plan);
                    assertThat(models.embeddingCalls.get()).isGreaterThan(beforeQueryEmbedding);
                    JsonNode data = result.path("data"), trace = data.path("trace");
                    assertThat(trace.path("degradedChannels").size()).isZero();
                    assertThat(trace.path("vectorHits").asInt()).isPositive(); assertThat(trace.path("keywordHits").asInt()).isPositive();
                    assertThat(trace.path("rerankedHits").asInt()).isBetween(1, 5);
                    assertThat(data.path("evidence").size()).isEqualTo(1);
                    JsonNode evidence = data.path("evidence").get(0);
                    assertThat(evidence.path("text").asText()).contains("1. 携带身份证", "   1. 先复印身份证", "2. 服务人员", "3. 收到通知");
                    assertThat(evidence.path("text").asText().length()).isGreaterThan(128);
                    assertThat(evidence.path("source").asText()).isEqualTo(source);
                    assertThat(evidence.path("title").asText()).contains("轮椅申请指南", "wheelchair");
                    assertThat(evidence.path("version").asLong()).isEqualTo(1); assertThat(evidence.path("pageStart").asInt()).isZero();
                    assertThat(evidence.path("matchedChildren").size()).isPositive();
                    assertThat(models.lastRerank.get().path("model").asText()).isEqualTo("Qwen/Qwen3-Reranker-0.6B");
                    assertThat(models.lastRerank.get().path("documents").size()).isLessThanOrEqualTo(20);
                    JsonNode foreignResult = search(port, foreignToken, plan);
                    assertThat(foreignResult.path("code").asText()).isEqualTo("0"); assertThat(foreignResult.path("data").path("evidence").size()).isZero();

                    store.delete(owner, documentId);
                    // Leave actual search records in place to prove HTTP retrieval enforces the SQL tombstone immediately.
                    assertThat(keywords.search(owner, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(documentId), 50)).isNotEmpty();
                    JsonNode removed = search(port, ownerToken, plan);
                    assertThat(removed.path("code").asText()).isEqualTo("0"); assertThat(removed.path("data").path("evidence").size()).isZero();
                    worker.process(imported.jobId());
                    assertThat(keywords.search(owner, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(documentId), 50)).isEmpty();
                    assertThat(models.chatCalls.get()).as("DOCUMENT retrieval has no chat or MQ dependency").isZero();
                }
            } finally {
                privateRedis.del(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + ownerToken, RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + foreignToken);
                privateRedis.hdel(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + ownerAccount, ownerToken);
                privateRedis.hdel(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + foreignAccount, foreignToken);
                if (vectors != null) {
                    if (applicationJdbc != null) for (String id : applicationJdbc.queryForList("SELECT child_id FROM agent_rag_child", String.class)) privateRedis.del(vectors.prefix() + id);
                    if (privateRedis.ftList().contains(vectors.indexName())) privateRedis.ftDropIndex(vectors.indexName());
                }
                var removedIndex = infrastructureHttp.exchange(URI.create(ES + "/" + esIndex), "DELETE", null);
                assertThat(removedIndex.successful() || removedIndex.status() == 404).isTrue();
                if (databaseCreated) {
                    assertThat(applicationJdbc.queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class)).isEqualTo(MARKER);
                    assertThat(database).matches("agent_rag_app_[a-f0-9]{32}");
                    fixtureJdbc.execute("DROP DATABASE " + database);
                }
            }
        }
    }

    private static DriverManagerDataSource dataSource(String schema) { return new DriverManagerDataSource(MYSQL + schema + JDBC_OPTIONS, "root", MARKER); }
    private JsonNode getEs(String path) {
        var response = infrastructureHttp.exchange(URI.create(ES + path), "GET", null);
        assertThat(response.successful()).isTrue(); return RagIndexHttp.parse(response);
    }
    private static void login(StringRedisTemplate redis, long user, String account, String token) {
        redis.opsForValue().set(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + token, account, Duration.ofMinutes(5));
        redis.opsForHash().put(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + account, token,
                RagIndexHttp.JSON.writeValueAsString(Map.of("id", user, "userAccount", account)));
    }
    private JsonNode search(int port, String token, String plan) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/agent/rag/search"))
                .timeout(Duration.ofSeconds(20)).header("Content-Type", "application/json;charset=UTF-8");
        if (token != null) request.header("token", token);
        var response = browser.send(request.POST(HttpRequest.BodyPublishers.ofString(plan)).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(response.statusCode()).isEqualTo(200); return RagIndexHttp.JSON.readTree(response.body());
    }
    private JsonNode searchEventually(int port, String token, String plan) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos(); JsonNode response;
        do {
            response = search(port, token, plan);
            if (response.path("code").asText().equals("0") && response.path("data").path("trace").path("vectorHits").asInt() > 0
                    && response.path("data").path("trace").path("keywordHits").asInt() > 0) return response;
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        assertThat(response.path("data").path("trace").path("vectorHits").asInt()).isPositive(); return response;
    }
    private static String[] arguments(String database, String namespace, String index, String models) {
        return new String[]{"--server.port=0", "--spring.main.banner-mode=off", "--spring.main.web-application-type=servlet", "--spring.profiles.active=local",
                "--spring.cloud.nacos.discovery.enabled=false", "--spring.cloud.nacos.discovery.register-enabled=false",
                "--spring.cloud.nacos.discovery.watch.enabled=false", "--spring.cloud.service-registry.auto-registration.enabled=false",
                "--spring.data.redis.host=127.0.0.1", "--spring.data.redis.port=16393", "--spring.data.redis.password=" + MARKER,
                "--spring.data.redis.timeout=3000ms", "--spring.data.redis.connect-timeout=2000ms",
                "--suts.ai.api-host=" + models, "--suts.ai.api-key=" + MARKER, "--suts.ai.model-name=fixture-chat", "--suts.ai.timeout=3000",
                "--suts.ai.embedding-host=" + models + "/v1", "--suts.ai.embedding-api-key=" + MARKER,
                "--suts.ai.embedding-model-name=rag-application-fixture", "--suts.ai.embedding-timeout=3000", "--suts.ai.embedding-enabled=true", "--suts.ai.vector.dimension=3",
                "--suts.ai.memory.writes-enabled=false", "--suts.ai.memory.vector.enabled=false", "--suts.ai.memory.archive.enabled=false",
                "--suts.ai.memory.summary.enabled=false", "--suts.ai.memory.profile.enabled=false", "--suts.ai.memory.transactions.enabled=false",
                "--suts.ai.rag.enabled=true", "--suts.ai.rag.jdbc-url=" + MYSQL + database + JDBC_OPTIONS,
                "--suts.ai.rag.username=root", "--suts.ai.rag.password=" + MARKER, "--suts.ai.rag.migrate=true",
                "--suts.ai.rag.index-name=suts:agent:rag:documents:index:app:" + namespace,
                "--suts.ai.rag.prefix=suts:agent:rag:documents:data:app:" + namespace + ":",
                "--suts.ai.rag.max-parent-chars=1024", "--suts.ai.rag.max-child-chars=128", "--suts.ai.rag.overlap-chars=16",
                "--suts.ai.rag.scan-interval-ms=3600000", "--suts.ai.rag.elasticsearch.endpoint=" + ES,
                "--suts.ai.rag.elasticsearch.index-name=" + index, "--suts.ai.rag.elasticsearch.timeout-ms=10000",
                "--suts.ai.rag.elasticsearch.username=", "--suts.ai.rag.elasticsearch.password=",
                "--suts.ai.rag.reranker.endpoint=" + models + "/v1", "--suts.ai.rag.reranker.api-key=" + MARKER,
                "--suts.ai.rag.reranker.model=Qwen/Qwen3-Reranker-0.6B", "--suts.ai.rag.reranker.timeout-ms=3000"};
    }

    private static final class LocalModels implements AutoCloseable {
        private static final int LIMIT = 128 * 1024;
        final AtomicInteger embeddingCalls = new AtomicInteger(), chatCalls = new AtomicInteger();
        final AtomicReference<JsonNode> lastRerank = new AtomicReference<>();
        private final HttpServer server;
        private final ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(16), new ThreadPoolExecutor.AbortPolicy());
        LocalModels() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 16); server.setExecutor(executor);
            server.createContext("/", exchange -> {
                try {
                    byte[] incoming = exchange.getRequestBody().readNBytes(LIMIT + 1);
                    if (incoming.length > LIMIT || !("Bearer " + MARKER).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                        exchange.sendResponseHeaders(400, -1); return;
                    }
                    JsonNode request = RagIndexHttp.JSON.readTree(incoming); Object response;
                    switch (exchange.getRequestURI().getPath()) {
                        case "/v1/embeddings" -> {
                            embeddingCalls.incrementAndGet(); int size = request.path("input").isArray() ? request.path("input").size() : 1;
                            var data = new ArrayList<Object>();
                            for (int i = 0; i < size; i++) data.add(Map.of("object", "embedding", "index", i, "embedding", List.of(1.0, .1, 0.0)));
                            response = Map.of("object", "list", "model", "rag-application-fixture", "data", data,
                                    "usage", Map.of("prompt_tokens", size, "total_tokens", size));
                        }
                        case "/v1/rerank" -> {
                            lastRerank.set(request); int count = request.path("top_n").asInt();
                            if (count < 1 || count > 5 || request.path("documents").size() > 20) { exchange.sendResponseHeaders(400, -1); return; }
                            response = Map.of("results", IntStream.range(0, count).mapToObj(i -> Map.of("index", i, "relevance_score", 1.0 - i / 10.0)).toList());
                        }
                        case "/chat/completions" -> {
                            chatCalls.incrementAndGet(); response = Map.of("choices", List.of(Map.of("index", 0, "finish_reason", "stop",
                                    "message", Map.of("role", "assistant", "content", "fixture response"))));
                        }
                        default -> { exchange.sendResponseHeaders(404, -1); return; }
                    }
                    byte[] outgoing = RagIndexHttp.JSON.writeValueAsBytes(response);
                    exchange.getResponseHeaders().set("Content-Type", "application/json"); exchange.sendResponseHeaders(200, outgoing.length);
                    exchange.getResponseBody().write(outgoing);
                } catch (Exception ignored) { /* Bounded fixture transport failure; never log request text or headers. */ }
                finally { exchange.close(); }
            }); server.start();
        }
        String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        @Override public void close() { server.stop(0); executor.shutdownNow(); }
    }
}
