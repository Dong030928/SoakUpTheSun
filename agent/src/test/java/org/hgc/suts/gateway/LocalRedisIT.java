package org.hgc.suts.gateway;

import cn.hutool.json.JSONUtil;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.manager.AiModelManager;
import org.hgc.suts.gateway.manager.VectorIndexManager;
import org.hgc.suts.gateway.service.VectorStorageService;
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
import redis.clients.jedis.JedisPooled;

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
    @LocalServerPort int port;
    @Autowired StringRedisTemplate redis;
    @Autowired JedisPooled jedis;
    @MockitoBean AiModelManager model;
    @MockitoBean VectorIndexManager index;
    @MockitoBean VectorStorageService vectors;

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.data.redis.host", () -> "127.0.0.1");
        properties.add("spring.data.redis.port", () -> REDIS_PORT);
        properties.add("spring.data.redis.password", () -> PASSWORD);
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
    }

    @Test
    void lettuceAndJedisAuthenticateAndChatUsesRealLoginCache() throws Exception {
        assertThat(jedis.ping()).isEqualTo("PONG");
        redis.opsForValue().set(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "local-token", "local-user");
        redis.opsForHash().put(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "local-user", "local-token",
                "{\"id\":99,\"userAccount\":\"local-user\"}");
        assertThat(jedis.get(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "local-token")).isEqualTo("local-user");
        when(model.chat(anyString(), anyString())).thenReturn("{\"reply\":\"Redis 登录成功\"}");
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
}
