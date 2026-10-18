package org.hgc.suts.gateway.memory;

import cn.hutool.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedisConversationMemoryRepositoryTest {
    private static final String CONVERSATION = "user:42:" + "a".repeat(64);
    private static final String OTHER_CONVERSATION = "user:7:" + "b".repeat(64);
    private static final String LOCK = "suts:agent:memory:v2:{" + CONVERSATION + "}:lock";
    private static final String MESSAGES = "suts:agent:memory:v2:{" + CONVERSATION + "}:messages";
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> renewal = mock(ScheduledFuture.class);
    private final Map<String, String> data = new HashMap<>();
    private RedisConversationMemoryRepository memory;
    private Runnable renewTask;
    private boolean failGet;
    private boolean failRenew;
    private boolean loseLeaseOnSave;
    private String savedTtl;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        memory = new RedisConversationMemoryRepository(redis, 30000, 86400, scheduler);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenAnswer(call -> {
            String key = call.getArgument(0);
            if (data.containsKey(key)) return false;
            data.put(key, call.getArgument(1));
            return true;
        });
        when(values.get(anyString())).thenAnswer(call -> {
            if (failGet) throw new IllegalStateException("private provider details");
            return data.get(call.getArgument(0));
        });
        doAnswer(call -> {
            renewTask = call.getArgument(0);
            assertThat((long) call.getArgument(1)).isEqualTo(10000);
            assertThat((long) call.getArgument(2)).isEqualTo(10000);
            return renewal;
        }).when(scheduler).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenAnswer(call -> {
            RedisScript<Long> script = call.getArgument(0);
            List<String> keys = call.getArgument(1);
            Object[] args = (Object[]) call.getRawArguments()[2];
            String source = script.getScriptAsString();
            if (source.contains("PEXPIRE") && failRenew) throw new IllegalStateException("Redis unavailable");
            if (source.contains("PSETEX") && loseLeaseOnSave) data.put(keys.get(0), "new-owner");
            if (!args[0].equals(data.get(keys.get(0)))) return 0L;
            if (source.contains("PSETEX")) {
                data.put(keys.get(1), (String) args[1]);
                savedTtl = (String) args[2];
            } else if (source.contains("KEYS[2]")) {
                data.remove(keys.get(1));
            } else if (!source.contains("PEXPIRE")) {
                data.remove(keys.get(0));
            }
            return 1L;
        });
    }

    @Test
    void unreadableSnapshotCannotBeReplacedAfterTheConnectionRecovers() {
        String existing = "do-not-overwrite-unread-history";
        data.put(MESSAGES, existing);
        try (var lease = memory.open(CONVERSATION)) {
            failGet = true;
            assertThat(lease.repository().findByConversationId(CONVERSATION)).isEmpty();
            failGet = false;
            assertThatThrownBy(() -> lease.repository().saveAll(CONVERSATION,
                    List.of(new UserMessage("new"), new AssistantMessage("answer"))))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("could not be read");
            assertThat(data.get(MESSAGES)).isEqualTo(existing);
        }
    }

    @Test
    void persistsOnlyPlainCompleteTurnsAndStripsMetadata() {
        try (var lease = memory.open(CONVERSATION)) {
            var repository = lease.repository();
            var user = UserMessage.builder().text("记住我喜欢绿茶").metadata(Map.of("Token", "private-token")).build();
            var assistant = AssistantMessage.builder().content("好的").properties(Map.of("reasoning", "private-reasoning")).build();
            repository.saveAll(CONVERSATION, List.of(user, assistant));
            assertThat(data.get(MESSAGES)).contains("\"schemaVersion\":2", CONVERSATION, "绿茶")
                    .doesNotContain("private-token", "Token", "private-reasoning", "reasoning");
            assertThat(savedTtl).isEqualTo("86400000");
            assertThat(repository.findByConversationId(CONVERSATION)).extracting(Message::getText)
                    .containsExactly("记住我喜欢绿茶", "好的");
            assertThat(repository.findConversationIds()).containsExactly(CONVERSATION);
        }
        assertThat(data).doesNotContainKey(LOCK).containsKey(MESSAGES);
        verify(renewal).cancel(false);
    }

    @Test
    void overlappingRequestsAreRejectedUntilTheLeaseCloses() {
        var first = memory.open(CONVERSATION);
        assertThatThrownBy(() -> memory.open(CONVERSATION)).isInstanceOf(ConversationBusyException.class);
        first.close();
        try (var second = memory.open(CONVERSATION)) {
            second.assertValid();
        }
    }

    @Test
    void repositoryIsBoundToOneConversation() {
        try (var lease = memory.open(CONVERSATION)) {
            assertThatThrownBy(() -> lease.repository().findByConversationId(OTHER_CONVERSATION))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> lease.repository().saveAll(OTHER_CONVERSATION, List.of()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> lease.repository().deleteByConversationId(OTHER_CONVERSATION))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void corruptCrossOwnerWrongVersionOrIncompleteRecordsDegradeToEmpty() {
        try (var lease = memory.open(CONVERSATION)) {
            lease.repository().saveAll(CONVERSATION, pair());
            String valid = data.get(MESSAGES);
            for (String malformed : List.of("{broken", valid.replace(CONVERSATION, OTHER_CONVERSATION),
                    valid.replace("\"schemaVersion\":2", "\"schemaVersion\":1"),
                    valid.replace("\"role\":\"USER\"", "\"role\":\"SYSTEM\""),
                    new JSONObject().set("schemaVersion", 2).set("conversationId", CONVERSATION)
                            .set("messages", List.of(Map.of("role", "USER", "text", "孤立消息"))).toString(),
                    "[]")) {
                data.put(MESSAGES, malformed);
                assertThat(lease.repository().findByConversationId(CONVERSATION)).isEmpty();
            }
            failGet = true;
            assertThat(lease.repository().findByConversationId(CONVERSATION)).isEmpty();
        }
    }

    @Test
    void rejectsSystemMessagesToolCallsAndUnpairedOrOversizedText() {
        try (var lease = memory.open(CONVERSATION)) {
            for (List<Message> invalid : List.of(
                    List.<Message>of(new SystemMessage("system"), new AssistantMessage("reply")),
                    List.<Message>of(new UserMessage("input")),
                    List.<Message>of(new UserMessage("input"), new UserMessage("not an answer")),
                    List.<Message>of(new UserMessage(" "), new AssistantMessage("reply")),
                    List.<Message>of(new UserMessage("x".repeat(65537)), new AssistantMessage("reply")),
                    List.<Message>of(new UserMessage("input"), AssistantMessage.builder().content("tool")
                            .toolCalls(List.of(new AssistantMessage.ToolCall("id", "function", "test", "{}"))).build()))) {
                assertThatThrownBy(() -> lease.repository().saveAll(CONVERSATION, invalid))
                        .isInstanceOf(IllegalArgumentException.class);
            }
            assertThat(data).doesNotContainKey(MESSAGES);
        }
    }

    @Test
    void lostLeaseCannotWriteOrDeleteAnotherHoldersData() {
        var lease = memory.open(CONVERSATION);
        lease.repository().saveAll(CONVERSATION, pair());
        String previous = data.get(MESSAGES);
        loseLeaseOnSave = true;
        assertThatThrownBy(() -> lease.repository().saveAll(CONVERSATION,
                List.of(new UserMessage("new"), new AssistantMessage("new"))))
                .isInstanceOf(MemoryLeaseLostException.class);
        assertThat(data.get(MESSAGES)).isEqualTo(previous);
        assertThatThrownBy(() -> lease.repository().deleteByConversationId(CONVERSATION))
                .isInstanceOf(MemoryLeaseLostException.class);
        lease.close();
        assertThat(data.get(LOCK)).isEqualTo("new-owner");
        assertThat(data.get(MESSAGES)).isEqualTo(previous);
    }

    @Test
    void backgroundRenewalPreservesOwnershipAndFailsClosedOnRedisFailure() {
        try (var lease = memory.open(CONVERSATION)) {
            renewTask.run();
            lease.assertValid();
            failRenew = true;
            renewTask.run();
            failRenew = false;
            assertThatThrownBy(lease::assertValid).isInstanceOf(MemoryLeaseLostException.class);
        }
    }

    @Test
    void anExpiredLeaseAndClosedLeaseCannotBeReused() {
        var lease = memory.open(CONVERSATION);
        data.remove(LOCK);
        assertThatThrownBy(lease::assertValid).isInstanceOf(MemoryLeaseLostException.class);
        lease.close();
        lease.close();
        assertThatThrownBy(() -> lease.repository().findByConversationId(CONVERSATION))
                .isInstanceOf(MemoryLeaseLostException.class);
    }

    @Test
    void acquisitionFailuresDoNotProceedWithUnconfirmedOwnership() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenThrow(new IllegalStateException("private connection details"));
        assertThatThrownBy(() -> memory.open(CONVERSATION)).isInstanceOf(MemoryLeaseLostException.class)
                .hasMessageNotContaining("private");
        verifyNoInteractions(scheduler);
    }

    @Test
    void ambiguousAcquisitionAndSchedulerRejectionNeverReturnALease() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(null);
        assertThatThrownBy(() -> memory.open(CONVERSATION)).isInstanceOf(MemoryLeaseLostException.class);
        verifyNoInteractions(scheduler);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenAnswer(call -> {
            data.put(call.getArgument(0), call.getArgument(1));
            return true;
        });
        doThrow(new java.util.concurrent.RejectedExecutionException()).when(scheduler)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        assertThatThrownBy(() -> memory.open(CONVERSATION)).isInstanceOf(MemoryLeaseLostException.class);
        assertThat(data).doesNotContainKey(LOCK);
    }

    @Test
    void deletesOnlyTheBoundConversationWhileHoldingItsLease() {
        try (var lease = memory.open(CONVERSATION)) {
            lease.repository().saveAll(CONVERSATION, pair());
            lease.repository().deleteByConversationId(CONVERSATION);
            assertThat(lease.repository().findByConversationId(CONVERSATION)).isEmpty();
            assertThat(data).containsKey(LOCK).doesNotContainKey(MESSAGES);
        }
    }

    @Test
    void rejectsUnsafeConversationIdsAndInvalidSettings() {
        for (String id : List.of("42", "user:42:token", CONVERSATION + "}", "user:-1:" + "a".repeat(64))) {
            assertThatThrownBy(() -> memory.open(id)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new RedisConversationMemoryRepository(redis, 999, 86400, scheduler))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisConversationMemoryRepository(redis, 30000, 0, scheduler))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RedisConversationMemoryRepository(redis, 30000, Long.MAX_VALUE, scheduler))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(values, scheduler);
    }

    @Test
    void closesManagedRenewalExecutor() {
        memory.destroy();
        verify(scheduler).shutdownNow();
    }

    private static List<Message> pair() {
        return List.of(new UserMessage("你好"), new AssistantMessage("您好"));
    }
}
