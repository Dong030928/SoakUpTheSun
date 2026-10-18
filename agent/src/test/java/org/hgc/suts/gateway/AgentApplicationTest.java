package org.hgc.suts.gateway;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.cloud.nacos.NacosServiceManager;
import com.alibaba.cloud.nacos.discovery.NacosDiscoveryClient;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.sun.net.httpserver.HttpServer;
import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.biz.user.UserInfoDTO;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.common.web.Results;
import org.hgc.suts.gateway.manager.AiModelManager;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import redis.clients.jedis.RedisClient;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.service.impl.RedisVectorStorageServiceImpl;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Full Boot auto-configuration, embedded Tomcat, MVC/Jackson 3 and load-balanced Feign. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.cloud.nacos.discovery.register-enabled=false",
        "spring.cloud.nacos.discovery.watch.enabled=false",
        "spring.cloud.service-registry.auto-registration.enabled=false"
})
class AgentApplicationTest {
    private static final HttpServer VOLUNTEER = volunteerServer();
    private static final ModelHttpFixture MODEL = new ModelHttpFixture();
    private static final AtomicReference<String> FORWARDED_TOKEN = new AtomicReference<>();
    @LocalServerPort int port;
    @Autowired VolunteerRemoteService volunteer;
    @Autowired NacosDiscoveryClient discovery;
    @Autowired JsonMapper mapper;
    @MockitoBean StringRedisTemplate redis;
    @MockitoBean RedisClient memoryRedisClient;
    @MockitoBean RedisConversationMemoryRepository conversations;
    @MockitoBean RedisVectorStorageServiceImpl vectors;
    @MockitoBean AiModelManager model;
    @MockitoBean NacosServiceManager nacos;
    private final HttpClient http = HttpClient.newHttpClient();

    @DynamicPropertySource
    static void modelProperties(DynamicPropertyRegistry properties) {
        properties.add("suts.ai.api-host", MODEL::baseUrl);
    }

    static HttpServer volunteerServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/api/volunteer/help/create", exchange -> {
                FORWARDED_TOKEN.set(exchange.getRequestHeaders().getFirst("token"));
                byte[] body = "{\"code\":\"0\",\"message\":null,\"requestId\":null,\"success\":true,\"data\":{\"targetRoomLink\":\"https://example.invalid/room\",\"authTicket\":\"fixture-ticket\"}}"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @AfterAll
    static void stopServer() { VOLUNTEER.stop(0); MODEL.close(); }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void configureLogin() throws Exception {
        NamingService naming = mock(NamingService.class);
        Instance instance = new Instance();
        instance.setIp("127.0.0.1");
        instance.setPort(VOLUNTEER.getAddress().getPort());
        instance.setHealthy(true);
        instance.setEnabled(true);
        when(nacos.getNamingService()).thenReturn(naming);
        when(naming.selectInstances(eq("volunteer"), anyString(), eq(true))).thenReturn(java.util.List.of(instance));
        var values = mock(org.springframework.data.redis.core.ValueOperations.class);
        var hashes = mock(org.springframework.data.redis.core.HashOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(redis.opsForHash()).thenReturn(hashes);
        when(values.get(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "fixture-token")).thenReturn("fixture");
        when(hashes.get(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "fixture", "fixture-token"))
                .thenReturn("{\"id\":42,\"userAccount\":\"fixture\"}");
        ConversationMemoryFixture.configure(conversations);
        MODEL.reset("应用启动成功");
        MODEL.text("应用启动成功");
        when(model.embed(anyString())).thenReturn(java.util.Collections.nCopies(1024, 0.0));
    }

    @Test
    void realServerHandlesSynchronousAndAsynchronousChat() throws Exception {
        for (String path : new String[]{"/api/agent/chat", "/api/agent/chat/async"}) {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(base() + path))
                    .header("token", "fixture-token").header("Content-Type", "text/plain;charset=UTF-8")
                    .POST(HttpRequest.BodyPublishers.ofString("你好")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            JSONObject body = JSONUtil.parseObj(response.body());
            assertThat(body.getStr("code")).isEqualTo("0");
            assertThat(body.getJSONObject("data").getStr("text")).isEqualTo("应用启动成功");
            assertThat(body.getJSONObject("data").getStr("clientCommand")).isEqualTo("NONE");
        }
    }

    @Test
    void feignUsesDiscoveryAndPreservesLegacyVolunteerEnvelope() {
        UserContext.setToken("forwarded-token");
        try {
            assertThat(discovery.getInstances("volunteer")).hasSize(1);
            var result = volunteer.createHelpTask();
            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getData().getTargetRoomLink()).isEqualTo("https://example.invalid/room");
            assertThat(result.getData().getAuthTicket()).isEqualTo("fixture-ticket");
            assertThat(FORWARDED_TOKEN.get()).isEqualTo("forwarded-token");
        } finally {
            UserContext.remove();
        }
    }

    @Test
    void springAiToolLoopCallsFeignWithTheAsyncUsersToken() throws Exception {
        MODEL.messages.clear();
        MODEL.tool("create_volunteer_help", "{}");
        MODEL.text("志愿求助已创建");
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/api/agent/chat/async"))
                .header("token", "fixture-token").header("Content-Type", "text/plain;charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString("请找志愿者")).build(), HttpResponse.BodyHandlers.ofString());
        JSONObject body = JSONUtil.parseObj(response.body());
        assertThat(body.getStr("code")).isEqualTo("0");
        assertThat(body.getJSONObject("data").getStr("text")).isEqualTo("志愿求助已创建");
        assertThat(body.getJSONObject("data").getStr("clientCommand")).isEqualTo("START_VIDEO_CALL");
        assertThat(body.getJSONObject("data").getJSONObject("commandData").getStr("authTicket")).isEqualTo("fixture-ticket");
        assertThat(FORWARDED_TOKEN.get()).isEqualTo("fixture-token");
        assertThat(MODEL.requests).hasSize(2);
        assertThat(MODEL.requests.toString()).doesNotContain("fixture-token", "fixture-ticket", "https://example.invalid/room");
        assertThat(MODEL.requests.stream().skip(1).findFirst().orElseThrow().getJSONArray("messages").toString())
                .contains("\"role\":\"tool\"", "通话信息已返回客户端");
    }

    @Test
    void jacksonKeepsExistingDateAndResultWireFormat() throws Exception {
        UserInfoDTO user = UserInfoDTO.builder().id(42L).birthday(LocalDateTime.of(2000, 1, 2, 3, 4, 5)).build();
        JSONObject json = JSONUtil.parseObj(mapper.writeValueAsString(Results.success(user)));
        assertThat(json.getStr("code")).isEqualTo("0");
        assertThat(json.getBool("success")).isTrue();
        assertThat(json.getJSONObject("data").getStr("birthday")).isEqualTo("2000-01-02 03:04:05");
    }

    @Test
    void openApiAndLegacyDocumentationPathRemainAvailableWithoutLogin() throws Exception {
        var docs = http.send(HttpRequest.newBuilder(URI.create(base() + "/v3/api-docs")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(docs.statusCode()).isEqualTo(200);
        assertThat(JSONUtil.parseObj(docs.body()).getJSONObject("paths").keySet())
                .contains("/api/agent/chat", "/api/agent/chat/async");
        var redirect = http.send(HttpRequest.newBuilder(URI.create(base() + "/doc.html")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(redirect.statusCode()).isEqualTo(302);
        assertThat(URI.create(redirect.headers().firstValue("Location").orElseThrow()).getPath())
                .isEqualTo("/swagger-ui/index.html");
    }

    private String base() { return "http://127.0.0.1:" + port; }
}
