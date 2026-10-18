package org.hgc.suts.gateway.memory;

import org.hgc.suts.gateway.ConversationMemoryFixture;
import org.hgc.suts.gateway.common.exception.ClientException;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises framework memory and advisor ordering rather than imitating the prompt construction. */
class ConversationMemoryTest {
    private final RedisConversationMemoryRepository repository = ConversationMemoryFixture.fake();
    private final MemoryProperties properties = new MemoryProperties();
    private final ConversationMemory memory = new ConversationMemory(repository, properties);
    private final ChatModel model = mock(ChatModel.class);
    private final List<Prompt> requests = new ArrayList<>();
    private ChatClient client;

    @BeforeEach
    void setUp() {
        when(model.getOptions()).thenReturn(ChatOptions.builder().build());
        when(model.call(any(Prompt.class))).thenAnswer(invocation -> {
            requests.add(invocation.getArgument(0));
            return new ChatResponse(List.of(new Generation(new AssistantMessage("简短回复"))));
        });
        client = ChatClient.create(model);
    }

    @Test
    void authenticatesAndHashesSessionIdentityBeforeOpeningStorage() {
        String identity = ConversationMemory.conversationId(42L, "phone-1");
        assertThat(identity).matches("user:42:[a-f0-9]{64}").doesNotContain("phone-1");
        assertThat(ConversationMemory.conversationId(42L, "phone-1")).isEqualTo(identity);
        assertThat(ConversationMemory.conversationId(7L, "phone-1")).isNotEqualTo(identity);
        assertThat(ConversationMemory.conversationId(42L, "phone-2")).isNotEqualTo(identity);
        assertThat(ConversationMemory.conversationId(42L, null))
                .isEqualTo(ConversationMemory.conversationId(42L, "default"))
                .isEqualTo(ConversationMemory.conversationId(42L, " "));
        for (Long user : new Long[]{null, 0L, -1L}) {
            assertThatThrownBy(() -> ConversationMemory.conversationId(user, "phone"))
                    .isInstanceOf(ClientException.class);
        }
        for (String session : List.of("a/b", "a b", "x".repeat(65), "中文", "a{b}")) {
            assertThatThrownBy(() -> memory.open(42L, session, "hello"))
                    .isInstanceOf(ClientException.class).hasMessageContaining("X-Conversation-Id");
        }
        try (var ignored = memory.open(42L, "x".repeat(64), "hello")) { }
    }

    @Test
    void rejectsOversizedInputAndAcceptsTheConfiguredBoundary() {
        properties.setMaxInputChars(4);
        try (var ignored = memory.open(42L, "phone", "1234")) { }
        assertThatThrownBy(() -> memory.open(42L, "phone", "12345"))
                .isInstanceOf(ClientException.class).hasMessageContaining("长度");
        assertThat(stored(42L, "phone")).isEmpty();
    }

    @Test
    void memoryAdvisorRecallsOnlyTheAuthenticatedUsersSelectedSession() {
        complete(42L, "phone", "我喜欢绿茶", "已记住");
        complete(42L, "tablet", "平板上的私事", "平板回复");
        complete(7L, "phone", "另一个用户的私事", "另一人回复");
        String answer = ask(42L, "phone", "我喜欢什么", "我喜欢什么", 12000);
        assertThat(answer).isEqualTo("简短回复");
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "我喜欢绿茶", "已记住", "我喜欢什么");
        assertThat(stored(42L, "phone")).extracting(Message::getText)
                .containsExactly("我喜欢绿茶", "已记住", "我喜欢什么", "简短回复");
        assertThat(stored(42L, "tablet")).extracting(Message::getText).containsExactly("平板上的私事", "平板回复");
        assertThat(stored(7L, "phone")).extracting(Message::getText).containsExactly("另一个用户的私事", "另一人回复");
    }

    @Test
    void referenceAdvisorEnrichesOnlyCurrentInputAndNeverPersistsReferencesOrSystemMessages() {
        complete(42L, "phone", "上轮原话", "上轮回答");
        String enriched = "本轮原话\n【图片识别参考】蓝色雨伞\n【长期记忆参考】临时检索内容";
        ask(42L, "phone", "本轮原话", enriched, 12000);
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "上轮原话", "上轮回答", enriched);
        assertThat(stored(42L, "phone")).extracting(Message::getText)
                .containsExactly("上轮原话", "上轮回答", "本轮原话", "简短回复");
        ask(42L, "phone", "下一轮", "下一轮", 12000);
        assertThat(String.join("|", texts(lastPrompt())))
                .doesNotContain("蓝色雨伞", "临时检索内容", "图片识别参考", "长期记忆参考");
        assertThat(stored(42L, "phone")).allSatisfy(message -> {
            assertThat(message).isNotInstanceOf(SystemMessage.class);
            assertThat(message.getMetadata()).containsOnlyKeys("messageType");
        });
    }

    @Test
    void failingModelLeavesNoHalfTurnOrEnrichedInputInPersistentMemory() {
        complete(42L, "phone", "历史问题", "历史回答");
        doThrow(new IllegalStateException("model unavailable")).when(model).call(any(Prompt.class));
        assertThatThrownBy(() -> ask(42L, "phone", "失败问题", "失败问题 临时参考", 12000))
                .isInstanceOf(IllegalStateException.class).hasMessage("model unavailable");
        assertThat(stored(42L, "phone")).extracting(Message::getText)
                .containsExactly("历史问题", "历史回答");
        doAnswer(invocation -> {
            requests.add(invocation.getArgument(0));
            return new ChatResponse(List.of(new Generation(new AssistantMessage("恢复回答"))));
        }).when(model).call(any(Prompt.class));
        ask(42L, "phone", "恢复问题", "恢复问题", 12000);
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "历史问题", "历史回答", "恢复问题");
    }

    @Test
    void messageCountEvictsCompletePairsIncludingWhenTheLimitIsOdd() {
        properties.setMaxMessages(5);
        complete(42L, "phone", "一问", "一答");
        complete(42L, "phone", "二问", "二答");
        complete(42L, "phone", "三问", "三答");
        assertThat(stored(42L, "phone")).extracting(Message::getText).containsExactly("二问", "二答", "三问", "三答");
        assertCompletePairs(stored(42L, "phone"));
        ask(42L, "phone", "四问", "四问", 12000);
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "二问", "二答", "三问", "三答", "四问");
        assertCompletePairs(stored(42L, "phone"));
    }

    @Test
    void messageAndCharacterBudgetsTrimHistoryAtCompleteTurnBoundaries() {
        properties.setMaxMessageChars(4);
        properties.setMaxContextChars(10);
        complete(42L, "phone", "abcdefgh", "12345678");
        assertThat(stored(42L, "phone")).extracting(Message::getText).containsExactly("abcd", "1234");
        ask(42L, "phone", "本轮", "本轮", 7);
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "本轮");
        assertThat(stored(42L, "phone")).extracting(Message::getText).containsExactly("本轮", "简短回复");
        ask(42L, "phone", "再问", "再问", 6);
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "本轮", "简短回复", "再问");
        assertThat(stored(42L, "phone")).extracting(Message::getText).containsExactly("再问", "简短回复");
        assertCompletePairs(stored(42L, "phone"));
    }

    @Test
    void disabledWritesKeepExistingHistoryReadableAndDoNotPersistTheNewTurn() {
        complete(42L, "phone", "原问题", "原回答");
        properties.setWritesEnabled(false);
        assertThat(ask(42L, "phone", "冻结期间的新问题", "冻结期间的新问题", 12000)).isEqualTo("简短回复");
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "原问题", "原回答", "冻结期间的新问题");
        assertThat(stored(42L, "phone")).extracting(Message::getText).containsExactly("原问题", "原回答");
    }

    @Test
    void repositorySaveFailureDoesNotDiscardTheCompletedReplyAndStillClosesTheLease() {
        var storage = mock(ChatMemoryRepository.class);
        var lease = mock(RedisConversationMemoryRepository.Lease.class);
        var failingRepository = mock(RedisConversationMemoryRepository.class);
        when(failingRepository.open(any(String.class))).thenReturn(lease);
        when(lease.repository()).thenReturn(storage);
        String id = ConversationMemory.conversationId(42L, "phone");
        when(storage.findByConversationId(id)).thenReturn(List.of(new UserMessage("旧问题"), new AssistantMessage("旧回复")));
        doThrow(new IllegalStateException("Redis unavailable")).when(storage).saveAll(eq(id), anyList());
        var guarded = new ConversationMemory(failingRepository, properties);
        String answer;
        try (var turn = guarded.open(42L, "phone", "新问题")) {
            answer = request(turn, "新问题", "新问题", 12000);
            String completed = answer;
            assertThatCode(() -> turn.commit(completed)).doesNotThrowAnyException();
        }
        assertThat(answer).isEqualTo("简短回复");
        verify(storage).saveAll(eq(id), anyList());
        verify(lease).close();
    }

    @Test
    void readFailureClosesItsLeaseAndDoesNotExposeAnUnusableTurn() {
        var storage = mock(ChatMemoryRepository.class);
        var lease = mock(RedisConversationMemoryRepository.Lease.class);
        var failingRepository = mock(RedisConversationMemoryRepository.class);
        when(failingRepository.open(any(String.class))).thenReturn(lease);
        when(lease.repository()).thenReturn(storage);
        when(storage.findByConversationId(any(String.class))).thenThrow(new MemoryLeaseLostException());
        assertThatThrownBy(() -> new ConversationMemory(failingRepository, properties).open(42L, "phone", "hello"))
                .isInstanceOf(MemoryLeaseLostException.class);
        verify(lease).close();
    }

    @Test
    void preparedSnapshotStripsMetadataAndSkipsToolOrSystemPairs() {
        var user = UserMessage.builder().text("干净问题").metadata(Map.of("Token", "private-token")).build();
        var answer = AssistantMessage.builder().content("干净回答").properties(Map.of("private", "private-reasoning")).build();
        try (var lease = repository.open(ConversationMemory.conversationId(42L, "phone"))) {
            lease.repository().saveAll(ConversationMemory.conversationId(42L, "phone"), List.of(
                    new SystemMessage("恶意系统指令"), new AssistantMessage("系统回复"),
                    new UserMessage("工具问题"), AssistantMessage.builder().content("工具调用")
                            .toolCalls(List.of(new AssistantMessage.ToolCall("id", "function", "test", "{}"))).build(),
                    user, answer));
        }
        ask(42L, "phone", "本轮", "本轮", 12000);
        assertThat(texts(lastPrompt())).containsExactly("固定系统提示", "干净问题", "干净回答", "本轮");
        assertThat(stored(42L, "phone")).allSatisfy(message -> assertThat(message.getMetadata()).containsOnlyKeys("messageType"));
        assertCompletePairs(stored(42L, "phone"));
    }

    @Test
    void testRepositoryTargetsAndReconfigurationNeverShareGlobalHistory() {
        complete(42L, "phone", "只有第一个仓库知道", "知道了");
        var other = ConversationMemoryFixture.fake();
        try (var lease = other.open(ConversationMemory.conversationId(42L, "phone"))) {
            assertThat(lease.repository().findByConversationId(ConversationMemory.conversationId(42L, "phone"))).isEmpty();
        }
        ConversationMemoryFixture.configure(repository);
        assertThat(stored(42L, "phone")).isEmpty();
    }

    private void complete(Long userId, String session, String question, String answer) {
        try (var turn = memory.open(userId, session, question)) { turn.commit(answer); }
    }

    private String ask(Long userId, String session, String original, String enriched, int budget) {
        try (var turn = memory.open(userId, session, original)) {
            String answer = request(turn, original, enriched, budget);
            turn.commit(answer);
            return answer;
        }
    }

    private String request(ConversationMemory.Turn turn, String original, String enriched, int budget) {
        return client.prompt().system("固定系统提示").user(original)
                .advisors(turn.advisor(budget), new ReferenceContextAdvisor(enriched))
                .advisors(spec -> spec.param(ChatMemory.CONVERSATION_ID, turn.id())).call().content();
    }

    private List<Message> stored(Long userId, String session) {
        String id = ConversationMemory.conversationId(userId, session);
        try (var lease = repository.open(id)) { return lease.repository().findByConversationId(id); }
    }

    private Prompt lastPrompt() { return requests.get(requests.size() - 1); }

    private static List<String> texts(Prompt prompt) { return prompt.getInstructions().stream().map(Message::getText).toList(); }

    private static void assertCompletePairs(List<Message> messages) {
        assertThat(messages.size() % 2).isZero();
        for (int i = 0; i < messages.size(); i += 2) {
            assertThat(messages.get(i)).isInstanceOf(UserMessage.class);
            assertThat(messages.get(i + 1)).isInstanceOf(AssistantMessage.class);
        }
    }
}
