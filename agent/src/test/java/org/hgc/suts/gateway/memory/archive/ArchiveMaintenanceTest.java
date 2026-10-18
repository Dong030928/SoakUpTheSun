package org.hgc.suts.gateway.memory.archive;

import org.hgc.suts.gateway.ConversationMemoryFixture;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.UserMessage;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArchiveMaintenanceTest {
    final ArchiveDatabaseFixture db = ArchiveDatabaseFixture.h2();
    final MemoryProperties properties = new MemoryProperties();
    final RedisConversationMemoryRepository redis = ConversationMemoryFixture.fake();
    final ArchiveMaintenance operations = new ArchiveMaintenance(db.archive, redis, properties);
    final String id = ConversationMemory.conversationId(42L, "maintenance");

    void seed() {
        try (var lease = redis.open(id)) {
            lease.repository().saveAll(id, List.of(new UserMessage("第一轮"), new AssistantMessage("回答一"),
                    new UserMessage("第二轮"), new AssistantMessage("回答二")));
        }
    }

    @Test void requiresBothFrozenWritesAndExplicitDrainConfirmation() {
        assertThatThrownBy(() -> operations.importRecent(42, id, 1, true, true)).hasMessageContaining("Freeze");
        properties.setWritesEnabled(false);
        assertThatThrownBy(() -> operations.importRecent(42, id, 1, false, true)).hasMessageContaining("Freeze");
        verify(redis, never()).open(anyString());
    }

    @Test void importIsDeterministicDryRunAndRepeatedInvocationAreSafe() {
        seed();
        properties.setWritesEnabled(false);
        assertThat(operations.importRecent(42, id, 1, true, true)).isEqualTo(2);
        assertThat(db.archive.snapshot(42, id, 1)).isEmpty();
        assertThat(operations.importRecent(42, id, 1, true, false)).isEqualTo(2);
        assertThat(operations.importRecent(42, id, 1, true, false)).isZero();
        assertThat(db.archive.snapshot(42, id, 1).orElseThrow().messages()).hasSize(4);
        assertThatThrownBy(() -> operations.retireBefore(42, id, 1, 1000, true))
                .hasMessageContaining("unknown activity");
    }

    @Test void changedSourceCannotOverwriteAnEarlierImport() {
        seed();
        properties.setWritesEnabled(false);
        operations.importRecent(42, id, 1, true, false);
        try (var lease = redis.open(id)) {
            lease.repository().saveAll(id, List.of(new UserMessage("另一个窗口"), new AssistantMessage("别的答案")));
        }
        assertThatThrownBy(() -> operations.importRecent(42, id, 1, true, false)).isInstanceOf(RuntimeException.class);
        assertThat(db.archive.snapshot(42, id, 1).orElseThrow().messages().get(0).getText()).isEqualTo("第一轮");
    }

    @Test void recoveryRefusesMissingOrUnconfirmedHistory() {
        properties.setWritesEnabled(false);
        assertThatThrownBy(() -> operations.restore(42, id, 1, 2, true)).hasMessageContaining("No confirmed archive");
        db.archive.archive(new ArchivedTurn("second", 42, id, 1, 2, "q", "a", 2));
        assertThatThrownBy(() -> operations.restore(42, id, 1, 2, true)).hasMessageContaining("checkpoint");
        verify(redis, atLeastOnce()).open(id);
    }

    @Test void recoveryDryRunNeverWritesOrOverwritesRedis() {
        properties.setWritesEnabled(false);
        db.archive.archive(new ArchivedTurn("first", 42, id, 1, 1, "q", "a", 1));
        assertThat(operations.restore(42, id, 1, 1, true, true)).isTrue();
        try (var lease = redis.open(id)) { assertThat(lease.repository().findByConversationId(id)).isEmpty(); }
        seed();
        assertThat(operations.restore(42, id, 1, 1, true, true)).isFalse();
    }
}
