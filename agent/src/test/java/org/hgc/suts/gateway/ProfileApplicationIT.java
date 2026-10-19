package org.hgc.suts.gateway;

import cn.hutool.json.JSONUtil;
import cn.hutool.json.JSONObject;
import com.sun.net.httpserver.HttpServer;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.ArchiveDatabaseFixture;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.profile.*;
import org.hgc.suts.gateway.memory.summary.*;
import org.hgc.suts.gateway.memory.transactions.*;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.ai.embedding.*;

import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;
import static org.hgc.suts.gateway.memory.profile.ProfileData.*;

/** Real Boot/auth/native model decoding/MQ/MySQL/Redis HNSW, deterministic local model and Embedding fixtures. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.cloud.nacos.discovery.enabled=false", "spring.cloud.service-registry.auto-registration.enabled=false",
        "suts.ai.memory.vector.enabled=false", "suts.ai.embedding-enabled=true", "suts.ai.embedding-model-name=profile/fixture",
        "suts.ai.embedding-host=https://fixture.invalid/v1", "suts.ai.vector.dimension=3",
        "suts.ai.memory.transactions.enabled=true", "suts.ai.memory.archive.enabled=true", "suts.ai.memory.profile.enabled=true",
        "suts.ai.memory.profile.scan-interval-ms=100", "suts.ai.memory.profile.retry-delay-ms=100",
        "spring.data.redis.host=127.0.0.1", "spring.data.redis.port=16391", "spring.data.redis.timeout=2000ms",
        "spring.data.redis.password=agent-transactions-integration-only",
        "suts.ai.api-key=profile-http-model-fixture-only", "suts.ai.model-name=fixture-model", "suts.ai.timeout=3000",
        "suts.ai.memory.archive.username=root", "suts.ai.memory.archive.password=agent-archive-integration-only",
        "suts.ai.memory.transactions.name-server=127.0.0.1:19876", "suts.ai.memory.transactions.topic=SutsMemorySummaryIT",
        "suts.ai.memory.transactions.generation=agent-transactions-integration-only",
        "suts.ai.memory.transactions.evidence-retention-seconds=120", "suts.ai.memory.transactions.recovery-window-seconds=60",
        "suts.ai.memory.transactions.redrive-after-ms=1000", "suts.ai.memory.transactions.reconcile-interval-ms=1000",
        "suts.ai.memory.summary.enabled=true", "suts.ai.memory.summary.model-capacity-tokens=40000",
        "suts.ai.memory.summary.input-budget-tokens=30000", "suts.ai.memory.summary.output-reserve-tokens=2000",
        "suts.ai.memory.summary.safety-tokens=2000", "suts.ai.memory.summary.low-tokens=800",
        "suts.ai.memory.summary.high-tokens=3000", "suts.ai.memory.summary.summary-max-tokens=1200",
        "suts.ai.memory.summary.summary-max-chars=1200", "suts.ai.memory.summary.worker-lease-ms=30000"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ProfileApplicationIT {
    private static final Fixture MODEL = new Fixture();
    private static final String RUN = UUID.randomUUID().toString();
    private static final AtomicLong USERS = new AtomicLong(20000 + Math.abs(RUN.hashCode() % 1000000));
    @LocalServerPort int port;
    @Autowired StringRedisTemplate redis;
    @Autowired RedisSummaryStateStore states;
    @Autowired RedisConversationMemoryRepository hot;
    @Autowired JdbcSummaryJobs jobs;
    @Autowired JdbcProfileStore profiles;
    @Autowired ProfileVectorIndex vectors;
    @Autowired JdbcConversationArchive archive;
    @Autowired RocketMqTransactionTransport messaging;
    @MockitoBean VolunteerRemoteService volunteer;
    @MockitoBean(name = "agentEmbeddingModel") EmbeddingModel embedding;
    private final AtomicBoolean embeddingUnavailable = new AtomicBoolean();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private long user;
    private String token;

    @DynamicPropertySource static void infrastructure(DynamicPropertyRegistry properties) {
        assertThat(System.getProperty("memory.transactions.fixture")).isEqualTo("agent-transactions-integration-only");
        var source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:13316/agent_archive_it"
                + "?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=2000&socketTimeout=5000", "root", "agent-archive-integration-only");
        assertThat(new JdbcTemplate(source).queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class)).isEqualTo("agent-archive-integration-only");
        new ArchiveDatabaseFixture(source, true);
        properties.add("suts.ai.api-host", MODEL::url);
        properties.add("suts.ai.memory.archive.jdbc-url", () -> "jdbc:mysql://127.0.0.1:13316/agent_archive_it?allowPublicKeyRetrieval=true&useSSL=false");
        properties.add("suts.ai.memory.transactions.producer-group", () -> "profile-http-it-producer-" + RUN);
        properties.add("suts.ai.memory.transactions.consumer-group", () -> "profile-http-it-consumer-" + RUN);
        properties.add("suts.ai.memory.profile.index-name", () -> "suts:agent:profile:it:index:" + RUN);
        properties.add("suts.ai.memory.profile.prefix", () -> "suts:agent:profile:it:data:" + RUN + ":");
    }
    @BeforeEach void loginAndEmbedding() {
        user = USERS.incrementAndGet(); token = "profile-http-it-" + user;
        redis.opsForValue().set(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + token, "profile-it-" + user);
        redis.opsForHash().put(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "profile-it-" + user, token,
                "{\"id\":" + user + ",\"userAccount\":\"profile-it\"}");
        embeddingUnavailable.set(false); MODEL.requests.clear();
        when(embedding.call(any(EmbeddingRequest.class))).thenAnswer(call -> {
            if (embeddingUnavailable.get()) throw new IllegalStateException("fixture embedding unavailable");
            EmbeddingRequest request = call.getArgument(0); var results = new ArrayList<Embedding>();
            for (int i = 0; i < request.getInstructions().size(); i++) results.add(new Embedding(new float[]{1, 0, 0}, i));
            return new EmbeddingResponse(results);
        });
    }
    @AfterEach void logout() {
        redis.delete(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + token);
        redis.opsForHash().delete(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "profile-it-" + user, token);
    }
    @AfterAll static void closeModel() { MODEL.close(); }

    private String summarizeThroughRealMq() throws Exception {
        String session = "profile-http-" + UUID.randomUUID();
        for (String text : List.of("我喜欢绿茶", "普通闲聊，不新增画像")) {
            var result = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/agent/chat"))
                    .timeout(Duration.ofSeconds(20)).header("token", token).header("X-Conversation-Id", session)
                    .header("Content-Type", "text/plain;charset=UTF-8").POST(HttpRequest.BodyPublishers.ofString(text)).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(JSONUtil.parseObj(result.body()).getStr("code")).isEqualTo("0");
        }
        String id = ConversationMemory.conversationId(user, session);
        await(() -> archive.snapshot(user, id, 1).map(s -> s.archivedThrough() == 2).orElse(false));
        var state = states.read(id); var event = SummaryEvent.of(state, state.raw().subList(0, 1));
        try (var lease = hot.open(id)) {
            assertThat(messaging.sendSummary(event, () -> states.register(event, state, lease.fencingToken())))
                    .isEqualTo(TransactionTransport.State.COMMITTED);
        }
        await(() -> jobs.read(event.jobId()).map(j -> j.state().equals("DONE")).orElse(false));
        await(() -> profiles.extraction(event.jobId(), false).map(e -> e.state().equals("APPLIED")).orElse(false));
        return event.jobId();
    }
    @Test void nativeStructuredSummaryBecomesOwnedPersistentFactAndRealHnswProjection() throws Exception {
        summarizeThroughRealMq(); var fact = profiles.list(user, false, 20).get(0);
        await(() -> profiles.projectionStatus(user, fact.id()).stream().anyMatch(p -> p.state().equals("WRITTEN")));
        await(() -> !vectors.search(user, "饮品偏好", 3).isEmpty());
        assertThat(vectors.search(user + 10000000, "饮品偏好", 3)).isEmpty();
        var body = request("GET", "/api/agent/profile", null);
        assertThat(body.getJSONArray("data").getJSONObject(0).getStr("value")).isEqualTo("绿茶");
        assertThat(fact.source().quote()).isEqualTo("我喜欢绿茶");
        assertThat(MODEL.requests.stream().filter(r -> r.toString().contains("profileChanges"))).hasSize(1)
                .allSatisfy(r -> assertThat(r.get("tools")).isNull());
        verifyNoInteractions(volunteer);
    }
    @Test void realApiCorrectionConflictAndForgetInvalidateOldVectorCandidatesImmediately() throws Exception {
        summarizeThroughRealMq(); var fact = profiles.list(user, false, 20).get(0);
        var corrected = request("POST", "/api/agent/profile/" + fact.id() + "/correct",
                json(Map.of("requestId", "correct-" + user, "expectedVersion", 1, "value", "红茶", "statement", "我现在喜欢红茶")));
        assertThat(corrected.getJSONObject("data").getLong("version")).isEqualTo(2);
        assertThat(request("POST", "/api/agent/profile/" + fact.id() + "/correct",
                json(Map.of("requestId", "stale-" + user, "expectedVersion", 1, "value", "白茶", "statement", "我喜欢白茶"))).getStr("code")).isEqualTo("409");
        var forgotten = request("POST", "/api/agent/profile/" + fact.id() + "/forget",
                json(Map.of("requestId", "forget-" + user, "expectedVersion", 2, "statement", "忘掉我的饮品偏好")));
        assertThat(forgotten.getJSONObject("data").getStr("state")).isEqualTo("DELETED");
        assertThat(vectors.search(user, "饮品偏好", 3)).isEmpty();
        assertThat(profiles.versions(user, fact.id(), Long.MAX_VALUE, 20)).hasSize(3);
    }
    @Test void embeddingFailureDoesNotBlockSummaryCompletionAndRecoveryDoesNotCallModelAgain() throws Exception {
        embeddingUnavailable.set(true); String job = summarizeThroughRealMq();
        var fact = profiles.list(user, false, 20).get(0);
        assertThat(jobs.read(job).orElseThrow().state()).isEqualTo("DONE");
        assertThat(profiles.projectionStatus(user, fact.id()).get(0).state()).isEqualTo("PENDING");
        long before = MODEL.requests.stream().filter(r -> r.toString().contains("profileChanges")).count();
        embeddingUnavailable.set(false);
        await(() -> profiles.projectionStatus(user, fact.id()).get(0).state().equals("WRITTEN"));
        assertThat(MODEL.requests.stream().filter(r -> r.toString().contains("profileChanges")).count()).isEqualTo(before);
    }
    private JSONObject request(String method, String path, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(15))
                .header("token", token).header("Content-Type", "application/json;charset=UTF-8")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build();
        return JSONUtil.parseObj(http.send(request, HttpResponse.BodyHandlers.ofString()).body());
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < until) { if (condition.getAsBoolean()) return; Thread.sleep(40); }
        assertThat(condition.getAsBoolean()).isTrue();
    }
    private static final class Fixture implements AutoCloseable {
        final List<JSONObject> requests = new CopyOnWriteArrayList<>();
        private final HttpServer server;
        Fixture() {
            try {
                server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                server.createContext("/chat/completions", exchange -> {
                    var request = JSONUtil.parseObj(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)); requests.add(request);
                    String answer = request.toString().contains("profileChanges")
                            ? json(new Envelope("用户喜欢绿茶", List.of(new Candidate("preference.drink", "绿茶", "ASSERT", 1, "我喜欢绿茶"))))
                            : "已理解，请继续。";
                    byte[] body = json(Map.of("choices", List.of(Map.of("index", 0, "finish_reason", "stop",
                                    "message", Map.of("role", "assistant", "content", answer))),
                            "usage", Map.of("prompt_tokens", 100, "completion_tokens", 20))).getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
                    exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close();
                }); server.start();
            } catch (Exception ex) { throw new ExceptionInInitializerError(ex); }
        }
        String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        @Override public void close() { server.stop(0); }
    }
}
