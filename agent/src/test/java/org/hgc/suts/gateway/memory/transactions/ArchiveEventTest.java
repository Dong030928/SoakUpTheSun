package org.hgc.suts.gateway.memory.transactions;

import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.archive.ArchivedTurn;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

class ArchiveEventTest {
    static ArchivedTurn turn(String event, long sequence) {
        return new ArchivedTurn(event, 42, ConversationMemory.conversationId(42L, "transactions"), 1, sequence,
                "我改喝红茶", "已更新偏好", 12345);
    }
    @Test void wireRoundTripIsSelfContainedAndStable() {
        var event = ArchiveEvent.of("fixture", turn("event-1", 1));
        byte[] bytes = event.encode(262144);
        assertThat(ArchiveEvent.decode(bytes, 262144, "fixture")).isEqualTo(event);
        assertThat(new String(bytes, StandardCharsets.UTF_8)).doesNotContain("apiKey", "authTicket", "token", "toolContext");
        assertThat(event.encode(262144)).isEqualTo(bytes);
    }
    @Test void rejectsTamperingUnknownFieldsAndWrongGeneration() {
        var event = ArchiveEvent.of("fixture", turn("event-1", 1));
        String json = new String(event.encode(262144), StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ArchiveEvent.decode(json.replace("已更新偏好", "伪造").getBytes(StandardCharsets.UTF_8), 262144, "fixture"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ArchiveEvent.decode((json.substring(0, json.length() - 1) + ",\"token\":\"secret\"}")
                .getBytes(StandardCharsets.UTF_8), 262144, "fixture")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ArchiveEvent.decode(event.encode(262144), 262144, "other")).isInstanceOf(RuntimeException.class);
    }
    @Test void rejectsByteOversizeAndNonExactLuaCounters() {
        var event = ArchiveEvent.of("fixture", turn("event-1", 1));
        assertThatThrownBy(() -> event.encode(10)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ArchiveEvent.of("fixture", turn("event-1", 9007199254740992L)))
                .isInstanceOf(IllegalArgumentException.class);
    }
    @Test void configurationRequiresExplicitRetentionAndDependencies() {
        var properties = TransactionTestConfig.create();
        assertThatCode(() -> properties.validate(true, true)).doesNotThrowAnyException();
        assertThatThrownBy(() -> properties.validate(false, true)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> properties.validate(true, false)).isInstanceOf(IllegalStateException.class);
        properties.setEvidenceRetentionSeconds(59);
        assertThatThrownBy(() -> properties.validate(true, true)).isInstanceOf(IllegalArgumentException.class);
        properties.setEvidenceRetentionSeconds(0);
        assertThatThrownBy(() -> properties.validate(true, true)).isInstanceOf(IllegalArgumentException.class);
    }
}
