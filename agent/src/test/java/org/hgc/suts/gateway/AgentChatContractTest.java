package org.hgc.suts.gateway;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.ttl.threadpool.TtlExecutors;
import com.sun.net.httpserver.HttpServer;
import org.hgc.suts.gateway.common.biz.user.LoginInterceptor;
import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.hgc.suts.gateway.common.web.GlobalExceptionHandler;
import org.hgc.suts.gateway.common.web.Results;
import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.config.AiModelConfiguration;
import org.hgc.suts.gateway.manager.VisionContextManager;
import org.hgc.suts.gateway.controller.AiChatController;
import org.hgc.suts.gateway.dto.resp.TargetRoomLinkInfoRespDTO;
import org.hgc.suts.gateway.manager.AiModelManager;
import org.hgc.suts.gateway.manager.ChatMemoryManager;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.hgc.suts.gateway.service.VectorStorageService;
import org.hgc.suts.gateway.service.impl.AiChatServiceImpl;
import org.hgc.suts.gateway.tool.AgentTools;
import org.springframework.ai.chat.client.ChatClient;
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
    private final Queue<JSONObject> replies = new ConcurrentLinkedQueue<>();
    private final Queue<JSONObject> chatRequests = new ConcurrentLinkedQueue<>();
    private final AtomicInteger modelStatus = new AtomicInteger(200);
    private final AtomicInteger embeddingStatus = new AtomicInteger(200);
    private final AtomicInteger chatCalls = new AtomicInteger();
    private final AtomicInteger chatDelayMs = new AtomicInteger();
    private final Queue<Integer> chatStatuses = new ConcurrentLinkedQueue<>();
    private String embeddingJson = "{\"object\":\"list\",\"model\":\"test-embedding\",\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1},\"data\":[{\"object\":\"embedding\",\"index\":0,\"embedding\":[0.1,0.2,0.3]}]}";
    private AiModelManager manager;
    private ChatClient chatClient;
    private AiConfig config;
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class, RETURNS_DEEP_STUBS);
    private final RedisConversationMemoryRepository conversations = ConversationMemoryFixture.fake();
    private final MemoryProperties memoryProperties = new MemoryProperties();
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
            chatCalls.incrementAndGet();
            assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isEqualTo("Bearer test-key");
            chatRequests.add(JSONUtil.parseObj(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            Integer scriptedStatus = chatStatuses.poll();
            int status = scriptedStatus == null ? modelStatus.get() : scriptedStatus;
            try { Thread.sleep(chatDelayMs.get()); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            JSONObject reply = replies.poll();
            String body = status == 200
                    ? JSONUtil.toJsonStr(new JSONObject().set("choices", List.of(
                            new JSONObject().set("index", 0).set("finish_reason", reply != null && reply.containsKey("tool_calls") ? "tool_calls" : "stop")
                                    .set("message", reply == null ? new JSONObject().set("role", "assistant").set("content", "执行完成") : reply))))
                    : "{\"error\":{\"message\":\"test failure\"}}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        model.createContext("/embeddings", exchange -> {
            assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isEqualTo("Bearer test-embedding-key");
            exchange.getRequestBody().readAllBytes();
            byte[] bytes = embeddingJson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(embeddingStatus.get(), bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        model.start();
        config = new AiConfig();
        String url = "http://127.0.0.1:" + model.getAddress().getPort();
        config.setApiHost(url);
        config.setApiKey("test-key");
        config.setModelName("test-chat");
        config.setEmbeddingHost(url);
        config.setEmbeddingApiKey("test-embedding-key");
        config.setEmbeddingModelName("test-embedding");
        config.setTimeout(2000);
        config.getVector().setDimension(3);
        AiModelConfiguration models = new AiModelConfiguration();
        chatClient = models.agentChatClient(models.agentChatModel(config));
        manager = new AiModelManager(chatClient,
                models.agentEmbeddingModel(config), config);
        ChatMemoryManager memory = new ChatMemoryManager(vectors, Runnable::run, memoryProperties, new org.hgc.suts.gateway.config.MemoryVectorProperties());
        when(vectors.searchMemory(eq(42L), anyString(), eq(6))).thenReturn(List.of("用户喜欢苹果"));
        service = new AiChatServiceImpl(chatClient, new AgentTools(volunteer), new VisionContextManager(redis), memory, new ConversationMemory(conversations, memoryProperties), memoryProperties);
        LoginInterceptor login = new LoginInterceptor();
        ReflectionTestUtils.setField(login, "stringRedisTemplate", redis);
        when(redis.opsForValue().get(RedisCacheConstant.USER_LOGIN_KEY_TOKEN_TO_USER + "valid-token"))
                .thenReturn("test-user");
        when(redis.opsForHash().get(RedisCacheConstant.USER_LOGIN_KEY_USER_TO_TOKEN + "test-user", "valid-token"))
                .thenReturn("{\"id\":42,\"userAccount\":\"test-user\",\"birthday\":\"2000-01-02 03:04:05\"}");
        mvc = MockMvcBuilders.standaloneSetup(new AiChatController(service, TtlExecutors.getTtlExecutor(workers)))
                .setControllerAdvice(new GlobalExceptionHandler(), new org.hgc.suts.gateway.common.web.MemoryStatusAdvice())
                .addInterceptors(login).build();
    }

    @AfterEach
    void tearDown() {
        workers.shutdownNow();
        model.stop(0);
        UserContext.remove();
    }

    @Test
    void rawTextChatPreservesResponseAndMemoryContract() throws Exception {
        reply("您好");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).characterEncoding("UTF-8").content("你好"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.code").value("0"))
                .andExpect(jsonPath("$.data.text").value("您好"))
                .andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(header().string("X-Agent-Memory-Status", "cached"))
                .andExpect(jsonPath("$.data.memoryStatus").doesNotExist())
                .andExpect(jsonPath("$.success").value(true));
        JSONObject request = chatRequests.remove();
        assertThat(request.getStr("model")).isEqualTo("test-chat");
        assertThat(request.getBool("stream")).isFalse();
        assertThat(request.getDouble("temperature")).isEqualTo(0.7);
        assertThat(request.getJSONArray("messages").getJSONObject(1).getStr("content")).contains("用户喜欢苹果");
        assertThat(request.getJSONArray("messages").getJSONObject(1).getStr("content")).startsWith("你好");
        verify(vectors).saveMemory(eq(42L), eq("User: 你好\nAI: 您好"));
        assertThat(UserContext.getUserId()).isNull();
    }

    @Test
    void jsonBodyRemainsAnUnparsedString() throws Exception {
        reply("收到");
        when(vectors.searchMemory(eq(42L), anyString(), eq(6))).thenReturn(List.of());
        String body = "{\"userDescription\":\"你好\"}";
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(jsonPath("$.code").value("0"));
        assertThat(chatRequests.remove().getJSONArray("messages").getJSONObject(1).getStr("content")).isEqualTo(body);
    }

    @Test
    void asyncChatPropagatesAuthenticationToWorker() throws Exception {
        reply("异步回复");
        MvcResult pending = mvc.perform(post("/api/agent/chat/async").param("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(request().asyncStarted()).andReturn();
        pending.getAsyncResult(5000);
        mvc.perform(asyncDispatch(pending)).andExpect(status().isOk())
                .andExpect(header().string("X-Agent-Memory-Status", "cached"))
                .andExpect(jsonPath("$.code").value("0"))
                .andExpect(jsonPath("$.data.text").value("异步回复"));
        verify(vectors).saveMemory(eq(42L), contains("hello"));
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
        toolCall("request_client_action", "{\"action\":\"" + command + "\"}");
        reply("收到");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("action"))
                .andExpect(jsonPath("$.data.clientCommand").value(command));
        verifyNoInteractions(volunteer);
    }

    @Test
    void volunteerHelpKeepsRoomAndTicketInCommandData() throws Exception {
        toolCall("create_volunteer_help", "{}");
        reply("志愿求助已创建");
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
    void plainTextRepliesNeedNoJsonDecisionParsing() throws Exception {
        reply("not JSON");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(jsonPath("$.data.text").value("not JSON"));
    }

    @Test
    void saturatedExecutorKeepsAsyncServiceUnavailableCode() {
        AiChatController controller = new AiChatController(service, task -> { throw new RejectedExecutionException(); });
        assertThat(controller.chatAsync("hello", null).getResult()).extracting("code").isEqualTo("503");
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {401, 429, 503})
    void providerFailuresAreBoundedToOneAttempt(int status) {
        modelStatus.set(status);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> manager.chat("system", "hello"))
                .isInstanceOf(RuntimeException.class);
        assertThat(chatCalls.get()).isEqualTo(1);
    }

    @Test
    void emptyModelOutputIsAnError() {
        reply("");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> manager.chat("system", "hello"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("empty answer");
    }

    @Test
    void modelReadTimeoutDoesNotWaitForSlowProvider() {
        config.setTimeout(100);
        var models = new AiModelConfiguration();
        var shortTimeout = new AiModelManager(models.agentChatClient(models.agentChatModel(config)),
                models.agentEmbeddingModel(config), config);
        chatDelayMs.set(800);
        long start = System.nanoTime();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> shortTimeout.chat("system", "hello"))
                .isInstanceOf(RuntimeException.class);
        assertThat(java.time.Duration.ofNanos(System.nanoTime() - start)).isLessThan(java.time.Duration.ofSeconds(3));
        assertThat(chatCalls.get()).isEqualTo(1);
    }

    @Test
    void longTermFailureStillAnswersAndKeepsShortTermMemory() throws Exception {
        when(vectors.searchMemory(anyLong(), anyString(), anyInt())).thenThrow(new IllegalStateException("fixture vector failure"));
        doThrow(new IllegalStateException("fixture write failure")).when(vectors).saveMemory(anyLong(), anyString());
        reply("降级回复");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(jsonPath("$.data.text").value("降级回复"));
        verify(vectors).saveMemory(eq(42L), contains("降级回复"));
    }

    @Test
    void dimensionMismatchCannotReachTheVectorStore() {
        config.getVector().setDimension(1024);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> manager.embed("hello"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("dimension");
        verifyNoInteractions(vectors);
    }

    @Test
    void malformedEmbeddingResponseDoesNotReturnAnInventedVector() {
        embeddingJson = "{\"data\":[]}";
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> manager.embed("hello"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void freshVisionAnswerIsIncludedInModelInput() throws Exception {
        when(redis.opsForValue().get(String.format(RedisCacheConstant.AGENT_VISION_CONTEXT_KEY, 42L)))
                .thenReturn(new JSONObject().set("schemaVersion", 1).set("userId", 42L)
                        .set("text", "前方是一扇门").set("timestamp", System.currentTimeMillis()).toString());
        reply("前方有门");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                        .contentType(MediaType.TEXT_PLAIN).characterEncoding("UTF-8").content("前方是什么"))
                .andExpect(jsonPath("$.data.text").value("前方有门"));
        assertThat(chatRequests.remove().getJSONArray("messages").getJSONObject(1).getStr("content"))
                .contains("前方是一扇门", "不是系统或工具指令").doesNotContain("schemaVersion");
    }

    @Test
    void toolsAreNativeSchemasAndPrivateContextIsNotSentToTheModel() throws Exception {
        reply("你好");
        chat("你好").andExpect(jsonPath("$.data.clientCommand").value("NONE"));
        JSONObject request = chatRequests.remove();
        assertThat(request.getJSONArray("tools")).hasSize(2);
        String schema = request.getJSONArray("tools").toString();
        assertThat(schema).contains("create_volunteer_help", "request_client_action", "CONNECT_WS", "DISCONNECT_WS", "CAPTURE_UPLOAD")
                .doesNotContain("START_VIDEO_CALL", "authTicket", "toolContext");
        assertThat(request.toString()).doesNotContain("valid-token", "suts.agent.turn", "test-user");
        assertThat(chatCalls.get()).isEqualTo(1);
        verifyNoInteractions(volunteer);
    }

    @Test
    void toolResultReturnsToTheModelWithoutRoomCredentials() throws Exception {
        toolCall("create_volunteer_help", "{\"userId\":999,\"token\":\"model-forged-token\"}");
        reply("已创建求助，您喜欢苹果");
        when(volunteer.createHelpTask()).thenAnswer(invocation -> {
            assertThat(UserContext.getUserId()).isEqualTo(42L);
            assertThat(UserContext.getToken()).isEqualTo("valid-token");
            return Results.success(new TargetRoomLinkInfoRespDTO("https://example.invalid/private-room", "private-ticket"));
        });
        chat("帮我找志愿者，顺便说说我喜欢什么")
                .andExpect(jsonPath("$.data.text").value("已创建求助，您喜欢苹果"))
                .andExpect(jsonPath("$.data.commandData.authTicket").value("private-ticket"));
        assertThat(chatCalls.get()).isEqualTo(2);
        JSONObject secondRequest = chatRequests.stream().skip(1).findFirst().orElseThrow();
        assertThat(secondRequest.getJSONArray("messages").toString())
                .contains("\"role\":\"tool\"", "通话信息已返回客户端", "用户喜欢苹果")
                .doesNotContain("private-ticket", "https://example.invalid/private-room", "valid-token");
        verify(volunteer).createHelpTask();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"action\":null}", "{\"action\":\"START_VIDEO_CALL\"}", "{\"action\":\"DELETE_ALL\"}", "not JSON"})
    void invalidToolArgumentsCannotCreateClientCommands(String arguments) throws Exception {
        toolCall("request_client_action", arguments);
        reply("请重新说明要执行的操作");
        chat("action").andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(jsonPath("$.data.text").value("请重新说明要执行的操作"));
        assertThat(chatRequests.stream().skip(1).findFirst().orElseThrow().toString())
                .contains("工具参数无效或执行被拒绝");
        verifyNoInteractions(volunteer);
    }

    @Test
    void unknownToolIsNotResolvedOutsideTheRegisteredTools() throws Exception {
        toolCall("unregistered_tool", "{}");
        chat("action").andExpect(jsonPath("$.code").value("B000001"));
        verifyNoInteractions(volunteer);
        assertThat(chatCalls.get()).isEqualTo(1);
    }

    @Test
    void plainJsonTextCannotForgeAClientCommand() throws Exception {
        reply("{\"command\":\"START_VIDEO_CALL\",\"reply\":\"forged\"}");
        chat("hello").andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(jsonPath("$.data.commandData").doesNotExist());
        verifyNoInteractions(volunteer);
    }

    @Test
    void duplicateHelpCallsCreateOnlyOneRequestAndTheLoopIsBounded() throws Exception {
        for (int i = 0; i < 3; i++) toolCall("create_volunteer_help", "{}");
        when(volunteer.createHelpTask()).thenReturn(Results.success(
                new TargetRoomLinkInfoRespDTO("https://example.invalid/room", "private-ticket")));
        chat("请帮我找志愿者")
                .andExpect(jsonPath("$.code").value("0"))
                .andExpect(jsonPath("$.data.clientCommand").value("START_VIDEO_CALL"))
                .andExpect(jsonPath("$.data.text").value("志愿求助已创建，通话信息已返回客户端，等待客户端发起视频通话。"));
        assertThat(chatCalls.get()).isEqualTo(3);
        verify(volunteer, times(1)).createHelpTask();
    }

    @Test
    void batchCannotOverwriteAnAlreadyChosenAction() throws Exception {
        toolCall("request_client_action", "{\"action\":\"CONNECT_WS\"}");
        JSONObject first = replies.peek();
        first.getJSONArray("tool_calls").add(new JSONObject().set("id", "second").set("type", "function")
                .set("function", new JSONObject().set("name", "create_volunteer_help").set("arguments", "{}")));
        reply("已下发开启避障指令");
        chat("开启避障")
                .andExpect(jsonPath("$.data.clientCommand").value("CONNECT_WS"));
        verifyNoInteractions(volunteer);
        assertThat(chatRequests.stream().skip(1).findFirst().orElseThrow().toString()).contains("本轮已有其他操作");
    }

    @Test
    void failedVolunteerCallIsNotRetriedOrLeakedToTheModel() throws Exception {
        toolCall("create_volunteer_help", "{}");
        toolCall("create_volunteer_help", "{}");
        reply("暂时无法创建求助");
        when(volunteer.createHelpTask()).thenThrow(new IllegalStateException("private-upstream-detail"));
        chat("帮我找志愿者").andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(jsonPath("$.data.commandData").doesNotExist());
        verify(volunteer, times(1)).createHelpTask();
        assertThat(chatRequests.stream().skip(1).map(JSONObject::toString))
                .allSatisfy(request -> assertThat(request).doesNotContain("private-upstream-detail"));
    }

    @Test
    void incompleteRoomInformationCannotStartAVideoCall() throws Exception {
        toolCall("create_volunteer_help", "{}");
        reply("未取得有效通话信息");
        when(volunteer.createHelpTask()).thenReturn(Results.success(new TargetRoomLinkInfoRespDTO("", "ticket")));
        chat("找志愿者").andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(jsonPath("$.data.commandData").doesNotExist());
    }

    @Test
    void unsuccessfulVolunteerResultCannotStartAVideoCall() throws Exception {
        toolCall("create_volunteer_help", "{}");
        reply("服务暂时繁忙");
        when(volunteer.createHelpTask()).thenReturn(new org.hgc.suts.gateway.common.result.Result<TargetRoomLinkInfoRespDTO>()
                .setCode("500").setData(new TargetRoomLinkInfoRespDTO("https://example.invalid/room", "ticket")));
        chat("找志愿者").andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(jsonPath("$.data.commandData").doesNotExist());
    }

    @Test
    void emptySummaryUsesTheExecutedClientAction() throws Exception {
        toolCall("request_client_action", "{\"action\":\"CAPTURE_UPLOAD\"}");
        reply("");
        chat("拍照").andExpect(jsonPath("$.data.clientCommand").value("CAPTURE_UPLOAD"))
                .andExpect(jsonPath("$.data.text").value("已生成拍照上传指令，等待设备上传图片后再分析。"));
    }

    @Test
    void repeatedInvalidCallsStopWithoutAnyAction() throws Exception {
        for (int i = 0; i < 3; i++) toolCall("request_client_action", "{}");
        chat("action").andExpect(jsonPath("$.data.clientCommand").value("NONE"))
                .andExpect(jsonPath("$.data.text").value("本轮工具调用已达上限，请重新描述您的需求。"));
        assertThat(chatCalls.get()).isEqualTo(3);
        verifyNoInteractions(volunteer);
    }

    @Test
    void summaryFailureKeepsCompletedActionAndCredentials() throws Exception {
        toolCall("create_volunteer_help", "{}");
        chatStatuses.add(200);
        chatStatuses.add(503);
        when(volunteer.createHelpTask()).thenReturn(Results.success(
                new TargetRoomLinkInfoRespDTO("https://example.invalid/room", "private-ticket")));
        chat("帮我找志愿者").andExpect(jsonPath("$.code").value("0"))
                .andExpect(jsonPath("$.data.clientCommand").value("START_VIDEO_CALL"))
                .andExpect(jsonPath("$.data.commandData.authTicket").value("private-ticket"))
                .andExpect(jsonPath("$.data.text").value("志愿求助已创建，通话信息已返回客户端，等待客户端发起视频通话。"));
        verify(volunteer, times(1)).createHelpTask();
        assertThat(chatCalls.get()).isEqualTo(2);
    }

    private org.springframework.test.web.servlet.ResultActions chat(String input) throws Exception {
        return mvc.perform(post("/api/agent/chat").header("token", "valid-token")
                .contentType(MediaType.TEXT_PLAIN).characterEncoding("UTF-8").content(input));
    }

    @Test
    void consecutiveQuestionsRecallOnlyTheAuthenticatedConversation() throws Exception {
        reply("记住了");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token").header("X-Conversation-Id", "alpha")
                .contentType(MediaType.TEXT_PLAIN).characterEncoding("UTF-8").content("我喜欢火龙果"))
                .andExpect(jsonPath("$.code").value("0"));
        reply("火龙果");
        mvc.perform(post("/api/agent/chat").header("token", "valid-token").header("X-Conversation-Id", "alpha")
                .contentType(MediaType.TEXT_PLAIN).characterEncoding("UTF-8").content("我喜欢什么"))
                .andExpect(jsonPath("$.data.text").value("火龙果"));
        assertThat(chatRequests.stream().skip(1).findFirst().orElseThrow().getJSONArray("messages").getJSONObject(1).getStr("content"))
                .isEqualTo("我喜欢火龙果");
        reply("还不知道");
        mvc.perform(post("/api/agent/chat/async").header("token", "valid-token").header("X-Conversation-Id", "beta")
                .contentType(MediaType.TEXT_PLAIN).characterEncoding("UTF-8").content("我喜欢什么"))
                .andExpect(request().asyncStarted()).andReturn().getAsyncResult(5000);
        assertThat(chatRequests.stream().skip(2).findFirst().orElseThrow().getJSONArray("messages").toString())
                .doesNotContain("火龙果");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/agent/chat", "/api/agent/chat/async"})
    void overlappingConversationRequestsHaveAnExplicitBusyResult(String path) throws Exception {
        when(conversations.open(anyString())).thenThrow(new org.hgc.suts.gateway.memory.ConversationBusyException());
        var result = mvc.perform(post(path).header("token", "valid-token").contentType(MediaType.TEXT_PLAIN).content("hello"));
        if (path.endsWith("/async")) {
            MvcResult pending = result.andExpect(request().asyncStarted()).andReturn();
            pending.getAsyncResult(5000);
            mvc.perform(asyncDispatch(pending)).andExpect(jsonPath("$.code").value("409"));
        } else result.andExpect(jsonPath("$.code").value("409"));
        assertThat(chatRequests).isEmpty();
        verifyNoInteractions(volunteer);
    }

    @ParameterizedTest
    @ValueSource(strings = {"../other-user", "user:42", "invalid session"})
    void unsafeConversationHeadersNeverReachTheModel(String session) throws Exception {
        mvc.perform(post("/api/agent/chat").header("token", "valid-token").header("X-Conversation-Id", session)
                .contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(jsonPath("$.code").value("A000001"));
        assertThat(chatRequests).isEmpty();
        verifyNoInteractions(volunteer);
    }

    @Test
    void excessiveInputIsRejectedBeforeMemoryOrTools() throws Exception {
        memoryProperties.setMaxInputChars(3);
        chat("too-long").andExpect(jsonPath("$.code").value("A000001"));
        assertThat(chatRequests).isEmpty();
        verifyNoInteractions(volunteer, vectors);
    }

    @Test
    void lostCommitKeepsTheExecutedActionButDoesNotEnqueueStaleLongTermMemory() throws Exception {
        var lease = mock(RedisConversationMemoryRepository.Lease.class);
        var repository = mock(org.springframework.ai.chat.memory.ChatMemoryRepository.class);
        when(lease.repository()).thenReturn(repository);
        when(lease.commit(anyString(), anyString(), anyString(), anyList())).thenCallRealMethod();
        when(repository.findByConversationId(anyString())).thenReturn(List.of());
        doThrow(new org.hgc.suts.gateway.memory.MemoryLeaseLostException()).when(repository).saveAll(anyString(), anyList());
        when(conversations.open(anyString())).thenReturn(lease);
        toolCall("request_client_action", "{\"action\":\"CAPTURE_UPLOAD\"}");
        reply("等待上传");
        chat("拍照").andExpect(jsonPath("$.code").value("0"))
                .andExpect(jsonPath("$.data.clientCommand").value("CAPTURE_UPLOAD"))
                .andExpect(header().string("X-Agent-Memory-Status", "degraded"))
                .andExpect(jsonPath("$.data.text").value(org.hamcrest.Matchers.containsString("记忆未确认保存")));
        verify(vectors, never()).saveMemory(anyLong(), anyString());
    }
    @ParameterizedTest
    @ValueSource(strings = {"ACCEPTED", "PENDING"})
    void memoryAcceptanceStatusUsesAHeaderWithoutChangingJsonFields(String name) throws Exception {
        var lease = mock(RedisConversationMemoryRepository.Lease.class);
        when(lease.repository()).thenReturn(new org.springframework.ai.chat.memory.InMemoryChatMemoryRepository());
        when(lease.commit(anyString(), anyString(), anyString(), anyList()))
                .thenReturn(org.hgc.suts.gateway.memory.MemoryCommitStatus.valueOf(name));
        when(conversations.open(anyString())).thenReturn(lease);
        reply("本轮回复");
        chat("你好").andExpect(header().string("X-Agent-Memory-Status", name.toLowerCase(java.util.Locale.ROOT)))
                .andExpect(jsonPath("$.data.text").value("本轮回复"))
                .andExpect(jsonPath("$.data.memoryStatus").doesNotExist());
    }
    @ParameterizedTest
    @ValueSource(strings = {"/api/agent/chat", "/api/agent/chat/async"})
    void unavailableTransactionAdmissionRejectsBeforeModelAndTools(String path) throws Exception {
        when(conversations.open(anyString())).thenThrow(new org.hgc.suts.gateway.memory.transactions.MemoryTransactionsUnavailableException());
        var response = mvc.perform(post(path).header("token", "valid-token").contentType(MediaType.TEXT_PLAIN).content("hello"));
        if (path.endsWith("/async")) {
            var pending = response.andExpect(request().asyncStarted()).andReturn();
            pending.getAsyncResult(5000);
            mvc.perform(asyncDispatch(pending)).andExpect(jsonPath("$.code").value("503"));
        } else response.andExpect(jsonPath("$.code").value("503"));
        assertThat(chatRequests).isEmpty();
        verifyNoInteractions(volunteer, vectors);
    }
    private void reply(String text) {
        replies.add(new JSONObject().set("role", "assistant").set("content", text));
    }

    private void toolCall(String name, String arguments) {
        replies.add(new JSONObject().set("role", "assistant").set("content", "")
                .set("tool_calls", List.of(new JSONObject().set("id", "call-" + replies.size()).set("type", "function")
                        .set("function", new JSONObject().set("name", name).set("arguments", arguments)))));
    }

}
