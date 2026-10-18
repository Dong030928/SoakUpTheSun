package org.hgc.suts.gateway;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.sun.net.httpserver.HttpServer;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.archive.ArchiveDatabaseFixture;
import org.hgc.suts.gateway.memory.summary.*;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Full Boot/auth/ChatClient/tool advisor/MQ/Redis/MySQL path, with a local native model HTTP fixture. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.cloud.nacos.discovery.enabled=false", "spring.cloud.service-registry.auto-registration.enabled=false",
        "suts.ai.embedding-enabled=false", "suts.ai.memory.vector.enabled=false",
        "suts.ai.memory.transactions.enabled=true", "suts.ai.memory.archive.enabled=true",
        "spring.data.redis.host=127.0.0.1", "spring.data.redis.port=16391",
        "spring.data.redis.password=agent-transactions-integration-only", "spring.data.redis.timeout=2000ms",
        "suts.ai.api-key=summary-model-fixture-only", "suts.ai.model-name=fixture-model", "suts.ai.timeout=1000",
        "suts.ai.memory.archive.username=root", "suts.ai.memory.archive.password=agent-archive-integration-only",
        "suts.ai.memory.transactions.name-server=127.0.0.1:19876", "suts.ai.memory.transactions.topic=SutsMemorySummaryIT",
        "suts.ai.memory.transactions.generation=agent-transactions-integration-only",
        "suts.ai.memory.transactions.evidence-retention-seconds=120", "suts.ai.memory.transactions.recovery-window-seconds=60",
        "suts.ai.memory.transactions.redrive-after-ms=1000", "suts.ai.memory.transactions.reconcile-interval-ms=1000",
        "suts.ai.memory.summary.enabled=true", "suts.ai.memory.summary.model-capacity-tokens=40000",
        "suts.ai.memory.summary.input-budget-tokens=30000", "suts.ai.memory.summary.output-reserve-tokens=2000",
        "suts.ai.memory.summary.safety-tokens=2000", "suts.ai.memory.summary.low-tokens=800",
        "suts.ai.memory.summary.high-tokens=3000", "suts.ai.memory.summary.summary-max-tokens=600",
        "suts.ai.memory.summary.summary-max-chars=1200", "suts.ai.memory.summary.worker-lease-ms=30000",
        "suts.ai.memory.summary.cache-policies[0].provider=deepseek", "suts.ai.memory.summary.cache-policies[0].model=fixture-model",
        "suts.ai.memory.summary.cache-policies[0].mode=automatic", "suts.ai.memory.summary.cache-policies[0].version=synthetic-v1",
        "suts.ai.memory.summary.cache-policies[0].vendor-basis=TEST FIXTURE ONLY, not a vendor duration",
        "suts.ai.memory.summary.cache-policies[0].ttl-seconds=3600", "suts.ai.memory.summary.cache-policies[0].renewal=CREATION",
        "suts.ai.memory.summary.cache-policies[0].clock-origin=RESPONSE_COMPLETED"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SummaryApplicationIT {
    private static final Fixture MODEL = new Fixture();
    private static final String RUN = UUID.randomUUID().toString(), TOKEN = "summary-http-it-" + RUN;
    @LocalServerPort int port;
    @Autowired StringRedisTemplate redis;
    @Autowired RedisSummaryStateStore states;
    @Autowired JdbcSummaryJobs jobs;
    @Autowired SummaryPolicy policy;
    @Autowired MemoryTransactionProperties tx;
    @Autowired org.springframework.ai.chat.model.ChatModel agentChatModel;
    @Autowired ConfigurableEnvironment environment;
    @MockitoBean VolunteerRemoteService volunteer;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    @DynamicPropertySource static void fixtureProperties(DynamicPropertyRegistry properties) {
        assertThat(System.getProperty("memory.transactions.fixture")).isEqualTo("agent-transactions-integration-only");
        var source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:13316/agent_archive_it"
                + "?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=2000&socketTimeout=5000", "root", "agent-archive-integration-only");
        assertThat(new JdbcTemplate(source).queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class))
                .isEqualTo("agent-archive-integration-only");
        new ArchiveDatabaseFixture(source, true);
        properties.add("suts.ai.api-host", MODEL::url);
        properties.add("suts.ai.memory.archive.jdbc-url", () -> "jdbc:mysql://127.0.0.1:13316/agent_archive_it?allowPublicKeyRetrieval=true&useSSL=false");
        properties.add("suts.ai.memory.transactions.producer-group", () -> "summary-http-it-producer-" + RUN);
        properties.add("suts.ai.memory.transactions.consumer-group", () -> "summary-http-it-consumer-" + RUN);
    }
    @BeforeEach void login() {
        redis.opsForValue().set(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + TOKEN, "summary-http-it-" + RUN);
        redis.opsForHash().put(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "summary-http-it-" + RUN, TOKEN,
                "{\"id\":52,\"userAccount\":\"summary-http-it\"}"); MODEL.requests.clear();
    }
    @AfterEach void logout() {
        redis.delete(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + TOKEN);
        redis.opsForHash().delete(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "summary-http-it-" + RUN, TOKEN);
        environment.getPropertySources().remove("summary-budget-test");
    }
    @AfterAll static void closeModel() { MODEL.close(); }
    private String session() { return "summary-http-" + UUID.randomUUID(); }
    private HttpResponse<String> chat(String session, String text) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/agent/chat"))
                .timeout(Duration.ofSeconds(20)).header("token", TOKEN).header("X-Conversation-Id", session)
                .header("Content-Type", "text/plain;charset=UTF-8").POST(HttpRequest.BodyPublishers.ofString(text)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
    @Test void nativeModelReceivesFullPrefixBeyondLegacyTwentyMessagesAndExplicitOutputReserve() throws Exception {
        String session = session();
        for (int i = 1; i <= 14; i++) {
            var response = chat(session, "第" + i + "轮：用户喜欢乌龙茶。" + "普通交谈内容。".repeat(8));
            assertThat(JSONUtil.parseObj(response.body()).getStr("code")).isEqualTo("0");
            assertThat(response.headers().firstValue("X-Agent-Memory-Status")).contains("accepted");
        }
        var request = MODEL.requests.get(MODEL.requests.size() - 1);
        assertThat(request.getJSONArray("messages").size()).isGreaterThan(22);
        assertThat(request.toString()).contains("第1轮", "第14轮").doesNotContain(TOKEN, "summary-model-fixture-only");
        assertThat(request.getInt("max_tokens")).isEqualTo(2000);
        assertThat(states.read(ConversationMemory.conversationId(52L, session)).raw()).hasSize(14);
        assertThat(states.read(ConversationMemory.conversationId(52L, session)).expiresAt()).isGreaterThan(System.currentTimeMillis());
        verifyNoInteractions(volunteer);
    }
    @Test void expiredAboveHIsSummarizedAndLaterPromptUsesHistoricalReferenceWithoutBusinessToolsInSummary() throws Exception {
        String session = session(), id = ConversationMemory.conversationId(52L, session);
        for (int i = 0; i < 10; i++) assertThat(JSONUtil.parseObj(chat(session, "原来是红茶，现在明确纠正为绿茶。" + "上下文。".repeat(35)).body())
                .getStr("code")).isEqualTo("0");
        states.update(id, s -> s.cache(policy.selected().fingerprint(), 1, 0, System.currentTimeMillis() - 1));
        var response = chat(session, "你好");
        assertThat(JSONUtil.parseObj(response.body()).getJSONObject("data").keySet())
                .containsExactlyInAnyOrder("text", "clientCommand", "commandData");
        await(() -> states.read(id).version() > 0 && states.read(id).pending() == null);
        await(() -> jobs.read(states.read(id).appliedJob()).map(j -> j.state().equals("DONE")).orElse(false));
        assertThat(jobs.read(states.read(id).appliedJob()).orElseThrow().vectorState()).isEqualTo("SKIPPED");
        chat(session, "请问我喜欢什么茶？");
        assertThat(MODEL.requests.get(MODEL.requests.size() - 1).toString()).contains("历史摘要参考", "喜欢绿茶");
        var summaryRequests = MODEL.requests.stream().filter(Fixture::summary).toList();
        assertThat(summaryRequests).isNotEmpty().allSatisfy(r -> {
            assertThat(r.get("tools")).isNull(); assertThat(r.getInt("max_tokens")).isEqualTo(600);
        });
        verifyNoInteractions(volunteer);
    }
    @Test void dynamicBudgetChangeIsReboundAndRejectsBeforeModelOrBusinessTool() throws Exception {
        environment.getPropertySources().addFirst(new MapPropertySource("summary-budget-test", Map.of(
                "suts.ai.memory.summary.input-budget-tokens", "1800", "suts.ai.memory.summary.low-tokens", "400",
                "suts.ai.memory.summary.high-tokens", "1000")));
        var response = chat(session(), "请找志愿者");
        assertThat(JSONUtil.parseObj(response.body()).getStr("code")).isNotEqualTo("0");
        assertThat(MODEL.requests).isEmpty(); verifyNoInteractions(volunteer);
    }
    @Test void documentedProviderHitUsageSurvivesTheRealSpringAiHttpDecoder() {
        var response = agentChatModel.call(new org.springframework.ai.chat.prompt.Prompt("cache usage fixture"));
        assertThat(org.hgc.suts.gateway.manager.ModelCacheUsage.cacheReadTokens(response.getMetadata().getUsage())).isEqualTo(80L);
        assertThat(response.getMetadata().getUsage().getPromptTokens()).isEqualTo(100);
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < until) { if (condition.getAsBoolean()) return; Thread.sleep(25); }
        assertThat(condition.getAsBoolean()).isTrue();
    }
    private static final class Fixture implements AutoCloseable {
        final List<JSONObject> requests = new CopyOnWriteArrayList<>();
        private final HttpServer server;
        Fixture() {
            try {
                server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                server.createContext("/chat/completions", exchange -> {
                    var request = JSONUtil.parseObj(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                    requests.add(request);
                    String answer = summary(request) ? "用户明确纠正：喜欢绿茶，不是红茶；其他操作仍待用户确认。" : "已理解，请继续。";
                    var message = new JSONObject().set("role", "assistant").set("content", answer);
                    byte[] body = new JSONObject().set("choices", List.of(new JSONObject().set("index", 0).set("finish_reason", "stop")
                            .set("message", message))).set("usage", new JSONObject().set("prompt_tokens", 100)
                            .set("completion_tokens", 20).set("prompt_cache_hit_tokens", 80).set("prompt_cache_miss_tokens", 20))
                            .toString().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
                    exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close();
                }); server.start();
            } catch (Exception ex) { throw new ExceptionInInitializerError(ex); }
        }
        String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        static boolean summary(JSONObject request) { return request.getJSONArray("messages").toString().contains("将用户历史摘要与新增完整轮次"); }
        @Override public void close() { server.stop(0); }
    }
}
