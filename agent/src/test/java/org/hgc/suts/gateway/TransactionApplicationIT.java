package org.hgc.suts.gateway;

import cn.hutool.json.JSONUtil;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.common.web.Results;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.dto.resp.TargetRoomLinkInfoRespDTO;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.ArchiveDatabaseFixture;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.transactions.*;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Real Boot/Tomcat/auth/Spring AI HTTP/tools/Redis/MQ/MySQL. Only model and volunteer endpoints are fixtures. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.cloud.nacos.discovery.enabled=false", "spring.cloud.service-registry.auto-registration.enabled=false",
        "suts.ai.embedding-enabled=false", "suts.ai.memory.vector.enabled=false",
        "suts.ai.memory.transactions.enabled=true", "suts.ai.memory.archive.enabled=true",
        "spring.data.redis.host=127.0.0.1", "spring.data.redis.port=16391",
        "spring.data.redis.password=agent-transactions-integration-only", "spring.data.redis.timeout=2000ms",
        "suts.ai.api-key=transaction-model-fixture-only", "suts.ai.model-name=fixture-model",
        "suts.ai.memory.archive.username=root", "suts.ai.memory.archive.password=agent-archive-integration-only",
        "suts.ai.memory.transactions.name-server=127.0.0.1:19876",
        "suts.ai.memory.transactions.topic=SutsMemoryArchiveIT",
        "suts.ai.memory.transactions.generation=agent-transactions-integration-only",
        "suts.ai.memory.transactions.evidence-retention-seconds=120",
        "suts.ai.memory.transactions.recovery-window-seconds=60",
        "suts.ai.memory.transactions.redrive-after-ms=1000", "suts.ai.memory.transactions.reconcile-interval-ms=1000"
})
// Closing the cached context also stops its MQ clients and reconciler before protocol fault tests run.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TransactionApplicationIT {
    private static final ModelHttpFixture MODEL = new ModelHttpFixture();
    private static final String RUN = UUID.randomUUID().toString();
    private static final String TOKEN = "agent-transaction-http-it-" + RUN;
    @LocalServerPort int port;
    @Autowired StringRedisTemplate redis;
    @Autowired JdbcConversationArchive archive;
    @Autowired MemoryTransactionProperties properties;
    @Autowired RocketMqTransactionTransport transport;
    @MockitoSpyBean RedisTransactionLedger ledger;
    @MockitoBean VolunteerRemoteService volunteer;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    @DynamicPropertySource static void fixtureProperties(DynamicPropertyRegistry properties) {
        assertThat(System.getProperty("memory.transactions.fixture")).isEqualTo("agent-transactions-integration-only");
        var source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:13316/agent_archive_it"
                + "?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=2000&socketTimeout=5000", "root", "agent-archive-integration-only");
        assertThat(new JdbcTemplate(source).queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class))
                .isEqualTo("agent-archive-integration-only");
        new ArchiveDatabaseFixture(source, true); // Marker has been verified before any migration.
        properties.add("suts.ai.api-host", MODEL::baseUrl);
        properties.add("suts.ai.memory.archive.jdbc-url", () -> "jdbc:mysql://127.0.0.1:13316/agent_archive_it?allowPublicKeyRetrieval=true&useSSL=false");
        properties.add("suts.ai.memory.transactions.producer-group", () -> "agent-http-it-producer-" + RUN);
        properties.add("suts.ai.memory.transactions.consumer-group", () -> "agent-http-it-consumer-" + RUN);
    }
    @BeforeEach void login() {
        redis.opsForValue().set(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + TOKEN, "agent-http-it-" + RUN);
        redis.opsForHash().put(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "agent-http-it-" + RUN, TOKEN,
                "{\"id\":42,\"userAccount\":\"agent-http-it\"}");
        properties.setAccepting(true);
        MODEL.reset("HTTP 对话成功");
    }
    @AfterEach void logout() {
        redis.delete(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + TOKEN);
        redis.opsForHash().delete(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "agent-http-it-" + RUN, TOKEN);
        properties.setAccepting(true);
    }
    @AfterAll static void stopModel() { MODEL.close(); }
    private String session() { return "http-" + UUID.randomUUID(); }
    private HttpResponse<String> chat(String path, String session, String text) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(20))
                .header("token", TOKEN).header("X-Conversation-Id", session).header("Content-Type", "text/plain;charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(text)).build(), HttpResponse.BodyHandlers.ofString());
    }
    @Test void synchronousAndAsyncHttpReturnAcceptanceHeaderAndArchiveCompleteTurns() throws Exception {
        for (String path : new String[]{"/api/agent/chat", "/api/agent/chat/async"}) {
            MODEL.reset("HTTP 对话成功");
            String session = session();
            var response = chat(path, session, "请记住我喜欢乌龙茶");
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("X-Agent-Memory-Status")).contains("accepted");
            var body = JSONUtil.parseObj(response.body());
            assertThat(body.getStr("code")).isEqualTo("0");
            assertThat(body.getJSONObject("data").keySet()).containsExactlyInAnyOrder("text", "clientCommand", "commandData");
            String id = ConversationMemory.conversationId(42L, session);
            await(() -> archive.snapshot(42, id, 1).map(s -> s.matchesCheckpoint(1)).orElse(false));
            assertThat(MODEL.requests.toString()).doesNotContain(TOKEN, "transaction-model-fixture-only");
        }
    }
    @Test void failedMemoryCommitPreservesVolunteerActionAndDoesNotReplayIt() throws Exception {
        doReturn(TransactionTransport.State.UNKNOWN).when(ledger).commit(any(), anyString(), anyLong(), anyString(), anyLong());
        MODEL.messages.clear(); MODEL.tool("create_volunteer_help", "{}"); MODEL.text("已记住，求助已创建");
        when(volunteer.createHelpTask()).thenReturn(Results.success(new TargetRoomLinkInfoRespDTO("https://fixture.invalid/room", "fixture-ticket")));
        var response = chat("/api/agent/chat/async", session(), "请找志愿者");
        assertThat(response.headers().firstValue("X-Agent-Memory-Status")).contains("degraded");
        var result = JSONUtil.parseObj(response.body()).getJSONObject("data");
        assertThat(result.getStr("clientCommand")).isEqualTo("START_VIDEO_CALL");
        assertThat(result.getStr("text")).contains("记忆未确认保存");
        assertThat(result.getJSONObject("commandData").getStr("authTicket")).isEqualTo("fixture-ticket");
        verify(volunteer, times(1)).createHelpTask();
        assertThat(MODEL.requests.toString()).doesNotContain("fixture-ticket", TOKEN);
    }
    @Test void stoppedAdmissionRefusesBeforeModelOrTools() throws Exception {
        properties.setAccepting(false);
        var response = chat("/api/agent/chat", session(), "你好");
        assertThat(JSONUtil.parseObj(response.body()).getStr("code")).isEqualTo("503");
        assertThat(MODEL.requests).isEmpty();
        verifyNoInteractions(volunteer);
    }
    @Test void coldCacheRecoversFromVerifiedProducerAndDatabaseWatermarks() throws Exception {
        String session = session(), id = ConversationMemory.conversationId(42L, session);
        chat("/api/agent/chat", session, "我喜欢乌龙茶");
        await(() -> archive.snapshot(42, id, 1).map(s -> s.matchesCheckpoint(1)).orElse(false));
        redis.delete(RedisConversationMemoryRepository.key(id, "messages"));
        MODEL.reset("您喜欢乌龙茶");
        var response = chat("/api/agent/chat", session, "我喜欢什么茶");
        assertThat(JSONUtil.parseObj(response.body()).getStr("code")).isEqualTo("0");
        assertThat(MODEL.requests.toString()).contains("我喜欢乌龙茶", "我喜欢什么茶");
        await(() -> archive.snapshot(42, id, 1).orElseThrow().matchesCheckpoint(2));
    }
    @Test void redisAofRestartRetainsCheckpointAndConversation() throws Exception {
        String session = session(), id = ConversationMemory.conversationId(42L, session);
        chat("/api/agent/chat", session, "我喜欢普洱茶");
        await(() -> archive.snapshot(42, id, 1).map(s -> s.matchesCheckpoint(1)).orElse(false));
        compose("restart", "redis");
        await(() -> {
            try { return properties.getGeneration().equals(redis.opsForValue().get(RedisTransactionLedger.GENERATION_KEY)); }
            catch (RuntimeException reconnecting) { return false; }
        });
        assertThat(ledger.read(id).head()).isEqualTo(1);
        MODEL.reset("您喜欢普洱茶");
        var response = chat("/api/agent/chat", session, "我喜欢什么茶");
        assertThat(JSONUtil.parseObj(response.body()).getStr("code")).isEqualTo("0");
        assertThat(MODEL.requests.toString()).contains("我喜欢普洱茶");
    }
    @Test void brokerOutageRejectsBeforeModelAndTools() throws Exception {
        try {
            compose("stop", "broker");
            var response = chat("/api/agent/chat", session(), "请找志愿者");
            assertThat(JSONUtil.parseObj(response.body()).getStr("code")).isEqualTo("503");
            assertThat(MODEL.requests).isEmpty();
            verifyNoInteractions(volunteer);
        } finally {
            compose("start", "broker");
            await(() -> {
                try { transport.preflight(); return true; }
                catch (RuntimeException restarting) { return false; }
            });
        }
    }
    @Test void mysqlOutageAcceptsWarmTurnsAndArchivesAfterRecovery() throws Exception {
        String session = session(), id = ConversationMemory.conversationId(42L, session);
        chat("/api/agent/chat", session, "第一轮历史");
        await(() -> archive.snapshot(42, id, 1).map(s -> s.matchesCheckpoint(1)).orElse(false));
        try {
            compose("stop", "mysql");
            MODEL.reset("数据库维护期间的回复");
            var response = chat("/api/agent/chat/async", session, "第二轮仍然继续");
            assertThat(JSONUtil.parseObj(response.body()).getStr("code")).isEqualTo("0");
            assertThat(response.headers().firstValue("X-Agent-Memory-Status")).contains("accepted");
            assertThat(ledger.read(id).head()).isEqualTo(2);
        } finally {
            compose("start", "mysql");
            await(() -> {
                try { return archive.snapshot(42, id, 1).map(s -> s.matchesCheckpoint(2)).orElse(false); }
                catch (RuntimeException restarting) { return false; }
            });
        }
    }
    private static void compose(String operation, String service) throws Exception {
        // Fixed, repository-owned compose project; never accepts a user container name or arbitrary path.
        java.nio.file.Path file = java.nio.file.Path.of("src/test/fixtures/memory-transactions/compose.yaml").toAbsolutePath();
        assertThat(file).isRegularFile();
        Process process = new ProcessBuilder("docker", "compose", "-f", file.toString(), operation, service)
                .redirectErrorStream(true).start();
        assertThat(process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(process.exitValue()).withFailMessage(new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)).isZero();
    }
    private static void await(java.util.function.BooleanSupplier condition) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).until(condition::getAsBoolean);
    }
}
