package org.hgc.suts.gateway;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.ttl.threadpool.TtlExecutors;
import com.sun.net.httpserver.HttpServer;
import org.hgc.suts.gateway.chain.node.ToolAnalysisNode;
import org.hgc.suts.gateway.chain.node.ToolExecuteNode;
import org.hgc.suts.gateway.chain.node.ToolSummaryNode;
import org.hgc.suts.gateway.common.biz.user.LoginInterceptor;
import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.common.web.GlobalExceptionHandler;
import org.hgc.suts.gateway.common.web.Results;
import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.controller.AiChatController;
import org.hgc.suts.gateway.dto.resp.TargetRoomLinkInfoRespDTO;
import org.hgc.suts.gateway.manager.AiModelManager;
import org.hgc.suts.gateway.manager.ChatMemoryManager;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.hgc.suts.gateway.service.VectorStorageService;
import org.hgc.suts.gateway.service.impl.AiChatServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Regression coverage shared by the original Boot 3 implementation and Boot 4. */
class AgentChatContractTest {
    private final Queue<String> replies = new ConcurrentLinkedQueue<>();
    private final Queue<JSONObject> chatRequests = new ConcurrentLinkedQueue<>();
    private final AtomicInteger modelStatus = new AtomicInteger(200);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class, RETURNS_DEEP_STUBS);
    private final VectorStorageService vectors = mock(VectorStorageService.class);
    private final VolunteerRemoteService volunteer = mock(VolunteerRemoteService.class);
    private final ExecutorService workers = Executors.newSingleThreadExecutor();
    private HttpServer model;
    private MockMvc mvc;
    private AiChatServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        model = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        model.createContext("/chat/completions", exchange -> {
            chatRequests.add(JSONUtil.parseObj(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            String reply = replies.poll();
            String body = modelStatus.get() == 200
                    ? JSONUtil.toJsonStr(new JSONObject().set("choices", List.of(
                            new JSONObject().set("message", new JSONObject().set("content", reply == null ? "执行完成" : reply)))))
                    : "{\"error\":{\"message\":\"test failure\"}}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
            exchange.sendResponseHeaders(modelStatus.get(), bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        model.createContext("/embeddings", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] bytes = "{\"data\":[{\"embedding\":[0.1,0.2,0.3]}]}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        model.start();
        AiConfig config = new AiConfig();
        String url = "http://127.0.0.1:" + model.getAddress().getPort();
        config.setApiHost(url);
        config.setApiKey("test-key");
        config.setModelName("test-chat");
        config.setEmbeddingHost(url);
        config.setEmbeddingApiKey("test-embedding-key");
        config.setEmbeddingModelName("test-embedding");
        config.setTimeout(2000);
        AiModelManager manager = new AiModelManager(config);
        ChatMemoryManager memory = new ChatMemoryManager(redis, vectors, manager);
        when(vectors.searchMemory(eq(42L), anyList(), eq(10))).thenReturn(List.of("用户喜欢苹果"));
        service = new AiChatServiceImpl(new ToolAnalysisNode(manager), new ToolExecuteNode(volunteer),
                new ToolSummaryNode(manager), redis, memory);
        service.initChain();
        LoginInterceptor login = new LoginInterceptor();
        ReflectionTestUtils.setField(login, "stringRedisTemplate", redis);
        when(redis.opsForValue().get(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "valid-token"))
                .thenReturn("test-user");
        when(redis.opsForHash().get(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "test-user", "valid-token"))
                .thenReturn("{\"id\":42,\"userAccount\":\"test-user\",\"birthday\":\"2000-01-02 03:04:05\"}");
        mvc = MockMvcBuilders.standaloneSetup(new AiChatController(service, TtlExecutors.getTtlExecutor(workers)))
                .setControllerAdvice(new GlobalExceptionHandler()).addInterceptors(login).build();
    }

    @AfterEach
    void tearDown() {
        workers.shutdownNow();
        model.stop(0);
        UserContext.remove();
    }

    @Test
    void rawTextChatPreservesResponseAndMemoryContract() throws Exception {
        replies.add("{\"tool\":null,\"command\":\"NONE\",\"reply\":\"您好\"}");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).characterEncoding("UTF-8").content("你好"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.code").value("0"))
                .andExpect(jsonPath("$.data.text").value("您好"))
                .andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(jsonPath("$.success").value(true));
        JSONObject request = chatRequests.remove();
        assertThat(request.getStr("model")).isEqualTo("test-chat");
        assertThat(request.getBool("stream")).isFalse();
        assertThat(request.getJSONArray("messages").getJSONObject(0).getStr("content")).contains("用户喜欢苹果");
        assertThat(request.getJSONArray("messages").getJSONObject(1).getStr("content")).isEqualTo("你好");
        verify(vectors).saveMemory(eq(42L), eq("User: 你好\nAI: 您好"), eq(List.of(0.1, 0.2, 0.3)));
        assertThat(UserContext.getUserId()).isNull();
    }

    @Test
    void jsonBodyRemainsAnUnparsedString() throws Exception {
        replies.add("{\"reply\":\"收到\"}");
        String body = "{\"userDescription\":\"你好\"}";
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(jsonPath("$.code").value("0"));
        assertThat(chatRequests.remove().getJSONArray("messages").getJSONObject(1).getStr("content")).isEqualTo(body);
    }

    @Test
    void asyncChatPropagatesAuthenticationToWorker() throws Exception {
        replies.add("{\"reply\":\"异步回复\",\"command\":\"NONE\"}");
        MvcResult pending = mvc.perform(post("/api/agent/chat/async").param("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(request().asyncStarted()).andReturn();
        pending.getAsyncResult(5000);
        mvc.perform(asyncDispatch(pending)).andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("0"))
                .andExpect(jsonPath("$.data.text").value("异步回复"));
        verify(vectors).saveMemory(eq(42L), contains("hello"), anyList());
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/agent/chat", "/api/agent/chat/async"})
    void unauthenticatedRequestsAreRejected(String path) throws Exception {
        mvc.perform(post(path).contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(jsonPath("$.code").value("A000001"))
                .andExpect(jsonPath("$.message").value("未登录，请先登录"));
        assertThat(chatRequests).isEmpty();
    }

    @Test
    void expiredTokenIsRejected() throws Exception {
        mvc.perform(post("/api/agent/chat").header("token", "expired")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(jsonPath("$.message").value("登录已过期，请重新登录"));
        assertThat(chatRequests).isEmpty();
    }

    @Test
    void blankSynchronousInputKeepsClientError() throws Exception {
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("   "))
                .andExpect(jsonPath("$.code").value("A000001"))
                .andExpect(jsonPath("$.message").value("输入不能为空"));
    }

    @Test
    void blankAsyncInputKeepsExistingErrorCode() throws Exception {
        MvcResult pending = mvc.perform(post("/api/agent/chat/async").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("   "))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(pending)).andExpect(jsonPath("$.code").value("400"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"CONNECT_WS", "DISCONNECT_WS", "CAPTURE_UPLOAD"})
    void clientCommandsKeepTheirWireValues(String command) throws Exception {
        replies.add("{\"command\":\"" + command + "\",\"reply\":\"收到\"}");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("action"))
                .andExpect(jsonPath("$.data.clientCommand").value(command));
        verifyNoInteractions(volunteer);
    }

    @Test
    void volunteerHelpKeepsRoomAndTicketInCommandData() throws Exception {
        replies.add("{\"tool\":\"create_volunteer_help\",\"args\":{\"description\":\"help\"}}");
        when(volunteer.createHelpTask()).thenReturn(Results.success(
                new TargetRoomLinkInfoRespDTO("https://example.invalid/room/42", "test-ticket")));
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("help"))
                .andExpect(jsonPath("$.data.clientCommand").value("START_VIDEO_CALL"))
                .andExpect(jsonPath("$.data.commandData.targetRoomLink").value("https://example.invalid/room/42"))
                .andExpect(jsonPath("$.data.commandData.authTicket").value("test-ticket"));
        verify(volunteer).createHelpTask();
    }

    @Test
    void modelFailureKeepsSynchronousErrorEnvelope() throws Exception {
        modelStatus.set(503);
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(jsonPath("$.code").value("B000001"));
    }

    @Test
    void malformedDecisionKeepsFallbackReply() throws Exception {
        replies.add("not JSON");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(jsonPath("$.data.text").value("抱歉，请再说一遍。"));
    }

    @Test
    void saturatedExecutorKeepsAsyncServiceUnavailableCode() {
        AiChatController controller = new AiChatController(service, task -> { throw new RejectedExecutionException(); });
        assertThat(controller.chatAsync("hello").getResult()).extracting("code").isEqualTo("503");
    }
}
