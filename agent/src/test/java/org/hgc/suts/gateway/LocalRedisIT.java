package org.hgc.suts.gateway;

import cn.hutool.json.JSONUtil;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.manager.AiModelManager;
import org.hgc.suts.gateway.service.impl.RedisVectorStorageServiceImpl;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.ConversationBusyException;
import org.hgc.suts.gateway.memory.MemoryLeaseLostException;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import redis.clients.jedis.RedisClient;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/** Opt-in integration test: a private Redis process, never the developer's database. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.cloud.nacos.discovery.enabled=false",
        "spring.cloud.service-registry.auto-registration.enabled=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LocalRedisIT {
    private static final String PASSWORD = "agent-integration-only";
    private static final int REDIS_PORT = freePort();
    private static final Process REDIS = startRedis();
    private static final ModelHttpFixture MODEL = new ModelHttpFixture();
    @LocalServerPort int port;
    @Autowired StringRedisTemplate redis;
    @Autowired RedisClient jedis;
    @Autowired org.hgc.suts.gateway.manager.VisionContextManager vision;
    @Autowired RedisConversationMemoryRepository conversations;
    @Autowired ConversationMemory conversationMemory;
    @MockitoBean AiModelManager model;
    @MockitoBean RedisVectorStorageServiceImpl vectors;

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.data.redis.host", () -> "127.0.0.1");
        properties.add("spring.data.redis.port", () -> REDIS_PORT);
        properties.add("spring.data.redis.password", () -> PASSWORD);
        properties.add("suts.ai.api-host", MODEL::baseUrl);
    }

    static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
        catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    static Process startRedis() {
        String executable = System.getProperty("redis.server.executable");
        if (executable == null || executable.isBlank() || executable.startsWith("${")) {
            throw new IllegalStateException("Set -Dredis.server.executable=/absolute/path/to/redis-server for -Plocal-redis");
        }
        Process process = null;
        try {
            var directory = Files.createTempDirectory("suts-agent-redis-").toFile();
            directory.deleteOnExit();
            process = new ProcessBuilder(executable, "--bind", "127.0.0.1", "--port", "" + REDIS_PORT,
                    "--save", "", "--appendonly", "no", "--requirepass", PASSWORD)
                    .directory(directory).redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD).start();
            Process child = process;
            Runtime.getRuntime().addShutdownHook(new Thread(child::destroy));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline && process.isAlive()) {
                try (var connection = new redis.clients.jedis.Jedis("127.0.0.1", REDIS_PORT, 200)) {
                    connection.auth(PASSWORD);
                    if ("PONG".equals(connection.ping())) return process;
                } catch (Exception ignored) { Thread.sleep(50); }
            }
            throw new IllegalStateException("Private Redis failed to start");
        } catch (Exception e) {
            if (process != null) process.destroy();
            throw new ExceptionInInitializerError(e);
        }
    }

    @AfterAll
    static void stopRedis(@Autowired LettuceConnectionFactory connections) throws Exception {
        connections.destroy();
        REDIS.destroy();
        if (!REDIS.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) REDIS.destroyForcibly();
        MODEL.close();
    }

    @Test
    void lettuceAndJedisAuthenticateAndChatUsesRealLoginCache() throws Exception {
        assertThat(jedis.ping()).isEqualTo("PONG");
        redis.opsForValue().set(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "local-token", "local-user");
        redis.opsForHash().put(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "local-user", "local-token",
                "{\"id\":99,\"userAccount\":\"local-user\"}");
        assertThat(jedis.get(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "local-token")).isEqualTo("local-user");
        MODEL.reset("Redis 登录成功");
        MODEL.text("Redis 登录成功");
        when(model.embed(anyString())).thenReturn(Collections.nCopies(1024, 0.0));
        HttpClient http = HttpClient.newHttpClient();
        for (String suffix : new String[]{"", "/async"}) {
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/agent/chat" + suffix))
                    .header("token", "local-token").header("Content-Type", "text/plain;charset=UTF-8")
                    .POST(HttpRequest.BodyPublishers.ofString("你好")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(JSONUtil.parseObj(response.body()).getJSONObject("data").getStr("text")).isEqualTo("Redis 登录成功");
        }
    }

    @Test
    void pictureLuaPublishesFreshEvidenceAndRejectsOutOfOrderAnalysis() {
        var script = new org.springframework.data.redis.core.script.DefaultRedisScript<Long>();
        script.setLocation(new org.springframework.core.io.ClassPathResource("contracts/picture/lua/update_vision_context.lua"));
        script.setResultType(Long.class);
        String key = String.format(RedisCacheConstant.AGENT_VISION_CONTEXT_KEY, 42L);
        long timestamp = System.currentTimeMillis();
        String current = new cn.hutool.json.JSONObject().set("schemaVersion", 1).set("userId", 42L)
                .set("pictureId", 7L).set("text", "新图像").set("timestamp", timestamp).toString();
        String stale = new cn.hutool.json.JSONObject().set("text", "乱序旧图像").set("timestamp", timestamp - 1000).toString();
        assertThat(redis.execute(script, java.util.List.of(key), current, "" + timestamp, "300000")).isEqualTo(1);
        assertThat(redis.execute(script, java.util.List.of(key), stale, "" + (timestamp - 1000), "300000")).isEqualTo(0);
        assertThat(vision.getRecentVision(42L)).isEqualTo("新图像");
        assertThat(redis.getExpire(key, java.util.concurrent.TimeUnit.MILLISECONDS)).isBetween(1L, 300000L);
        assertThat(vision.getRecentVision(7L)).isEmpty();
        redis.opsForValue().set(key, "{broken");
        assertThat(redis.execute(script, java.util.List.of(key), current, "" + timestamp, "300000")).isEqualTo(1);
        assertThat(vision.getRecentVision(42L)).isEqualTo("新图像");
    }

    @Test
    void conversationSurvivesRepositoryRestartAndSeparatesUsersAndSessions() {
        String id = ConversationMemory.conversationId(701L, "test");
        try (var turn = conversationMemory.open(701L, "test", "我喜欢苹果")) { turn.commit("记住了"); }
        var restarted = new RedisConversationMemoryRepository(redis, 30000, 60);
        try {
            try (var lease = restarted.open(id)) {
                var messages = lease.repository().findByConversationId(id);
                assertThat(messages).hasSize(2);
                assertThat(messages.get(0).getText()).isEqualTo("我喜欢苹果");
                assertThat(messages.get(1).getText()).isEqualTo("记住了");
            }
            for (String other : new String[]{ConversationMemory.conversationId(702L, "test"),
                    ConversationMemory.conversationId(701L, "another")}) {
                try (var lease = restarted.open(other)) { assertThat(lease.repository().findByConversationId(other)).isEmpty(); }
            }
            assertThat(redis.getExpire("suts:agent:memory:v2:{" + id + "}:messages", java.util.concurrent.TimeUnit.SECONDS))
                    .isBetween(1L, 86400L);
        } finally { restarted.destroy(); }
    }

    @Test
    void twoRepositoryInstancesSerializeTurnsAndFenceAStaleHolder() {
        String id = ConversationMemory.conversationId(703L, "lease");
        String lock = "suts:agent:memory:v2:{" + id + "}:lock";
        var secondInstance = new RedisConversationMemoryRepository(redis, 30000, 60);
        try (var owner = conversations.open(id)) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> secondInstance.open(id)).isInstanceOf(ConversationBusyException.class);
            owner.repository().saveAll(id, java.util.List.of(new UserMessage("question"), new AssistantMessage("answer")));
            redis.opsForValue().set(lock, "replacement-lease", Duration.ofSeconds(30));
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> owner.repository().saveAll(id,
                    java.util.List.of(new UserMessage("stale"), new AssistantMessage("must not overwrite"))))
                    .isInstanceOf(MemoryLeaseLostException.class);
        } finally { secondInstance.destroy(); }
        assertThat(redis.opsForValue().get(lock)).isEqualTo("replacement-lease");
        assertThat(redis.opsForValue().get("suts:agent:memory:v2:{" + id + "}:messages")).contains("question").doesNotContain("stale");
        redis.delete(lock);
    }

    @Test
    void renewableLeaseRemainsOwnedAndShortMemoryExpiresByTtl() throws Exception {
        String id = ConversationMemory.conversationId(704L, "ttl");
        var shortTtl = new RedisConversationMemoryRepository(redis, 1000, 1);
        var another = new RedisConversationMemoryRepository(redis, 1000, 1);
        try {
            try (var lease = shortTtl.open(id)) {
                lease.repository().saveAll(id, java.util.List.of(new UserMessage("question"), new AssistantMessage("answer")));
                Thread.sleep(1700);
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> another.open(id)).isInstanceOf(ConversationBusyException.class);
                assertThat(lease.repository().findByConversationId(id)).isEmpty();
                lease.assertValid();
            }
            try (var lease = another.open(id)) { lease.assertValid(); }
        } finally { shortTtl.destroy(); another.destroy(); }
    }

    @Test
    void toolTranscriptAndVisualReferenceAreNotStoredInTheUserMessage() throws Exception {
        redis.opsForValue().set(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "privacy-token", "privacy-user");
        redis.opsForHash().put(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "privacy-user", "privacy-token",
                "{\"id\":705,\"userAccount\":\"privacy-user\"}");
        redis.opsForValue().set(String.format(RedisCacheConstant.AGENT_VISION_CONTEXT_KEY, 705L),
                new cn.hutool.json.JSONObject().set("schemaVersion", 1).set("userId", 705L)
                        .set("text", "visual-reference-fixture").set("timestamp", System.currentTimeMillis()).toString());
        MODEL.messages.clear();
        MODEL.requests.clear();
        MODEL.tool("request_client_action", "{\"action\":\"CAPTURE_UPLOAD\"}");
        MODEL.text("等待客户端上传");
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/agent/chat/async"))
                .header("token", "privacy-token").header("X-Conversation-Id", "private")
                .header("Content-Type", "text/plain;charset=UTF-8").POST(HttpRequest.BodyPublishers.ofString("请重新拍照")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(JSONUtil.parseObj(response.body()).getJSONObject("data").getStr("clientCommand")).isEqualTo("CAPTURE_UPLOAD");
        assertThat(MODEL.requests.peek().toString()).contains("visual-reference-fixture");
        String id = ConversationMemory.conversationId(705L, "private");
        String saved = redis.opsForValue().get("suts:agent:memory:v2:{" + id + "}:messages");
        assertThat(saved).contains("请重新拍照", "等待客户端上传")
                .doesNotContain("visual-reference-fixture", "privacy-token", "tool_calls", "request_client_action", "schemaVersion\":1");
    }
}
