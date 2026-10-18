package org.hgc.suts.gateway.memory.archive;

import org.hgc.suts.gateway.memory.ConversationMemory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/** The same transaction/concurrency contract runs against H2 and real private MySQL. */
public abstract class ArchiveContract {
    protected ArchiveDatabaseFixture db;
    protected long owner;
    protected String id;
    protected abstract ArchiveDatabaseFixture database();

    @BeforeEach
    void prepare() {
        db = database();
        db.window.setMaxMessages(20);
        db.window.setMaxMessageChars(4000);
        db.window.setMaxContextChars(12000);
        owner = java.util.concurrent.ThreadLocalRandom.current().nextLong(1000000, 1000000000000L);
        id = ConversationMemory.conversationId(owner, UUID.randomUUID().toString());
    }

    protected ArchivedTurn turn(long seq) {
        return new ArchivedTurn("event_" + owner + "_" + seq, owner, id, 1, seq, "用户问题" + seq, "助手回复" + seq, seq * 1000);
    }

    protected long rows(String table) {
        return db.jdbc.queryForObject("SELECT COUNT(*) FROM " + table + " WHERE conversation_id = ?", Long.class, id);
    }

    @Test
    void outOfOrderEventsDoNotMasqueradeAsContiguousHistory() {
        var second = db.archive.archive(turn(2));
        assertThat(second.highestSeen()).isEqualTo(2);
        assertThat(second.archivedThrough()).isZero();
        var incomplete = db.archive.snapshot(owner, id, 1).orElseThrow();
        assertThat(incomplete.messages()).isEmpty();
        assertThat(incomplete.matchesCheckpoint(2)).isFalse();
        db.archive.archive(turn(1));
        var ready = db.archive.snapshot(owner, id, 1).orElseThrow();
        assertThat(ready.matchesCheckpoint(2)).isTrue();
        assertThat(ready.matchesCheckpoint(1)).isFalse();
        assertThat(ready.matchesCheckpoint(3)).isFalse();
        assertThat(ready.messages()).extracting(Message::getText).containsExactly("用户问题1", "助手回复1", "用户问题2", "助手回复2");
    }

    @Test
    void snapshotWindowEvictionDoesNotDeleteRawTurns() {
        db.window.setMaxMessages(2);
        for (int n = 1; n <= 6; n++) db.archive.archive(turn(n));
        assertThat(rows("agent_memory_turn")).isEqualTo(6);
        assertThat(rows("memory_log")).isEqualTo(6);
        assertThat(db.archive.snapshot(owner, id, 1).orElseThrow().messages()).extracting(Message::getText)
                .containsExactly("用户问题6", "助手回复6");
        assertThat(db.jdbc.queryForObject("SELECT user_text FROM agent_memory_turn WHERE event_id = ?", String.class,
                turn(1).eventId())).isEqualTo("用户问题1");
    }

    @Test
    void redeliveryIsIdempotentButChangedContentCannotReuseAnId() {
        db.archive.archive(turn(1));
        assertThat(db.archive.archive(turn(1)).duplicate()).isTrue();
        ArchivedTurn original = turn(1);
        assertThatThrownBy(() -> db.archive.archive(new ArchivedTurn(original.eventId(), owner, id, 1, 1,
                "被篡改内容", original.assistantText(), original.occurredAt()))).hasMessageContaining("conflicts");
        assertThat(rows("memory_log")).isEqualTo(1);
        assertThat(rows("agent_memory_turn")).isEqualTo(1);
    }

    @Test
    void identityAndEpochAreValidatedAndCaseIsSignificant() {
        db.archive.archive(turn(1));
        assertThatThrownBy(() -> db.archive.snapshot(owner + 1, id, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> db.archive.snapshot(owner, id, 2)).hasMessageContaining("epoch");
        assertThatThrownBy(() -> db.archive.archive(new ArchivedTurn("new_epoch", owner, id, 2, 1, "x", "y", 1)))
                .hasMessageContaining("epoch");
        db.archive.archive(new ArchivedTurn("Case_" + owner, owner, id, 1, 2, "a", "b", 2));
        db.archive.archive(new ArchivedTurn("case_" + owner, owner, id, 1, 3, "c", "d", 3));
        assertThat(rows("memory_log")).isEqualTo(3);
    }

    @Test
    void allThreeEffectsRollBackWhenFrameworkSnapshotFailsAfterItsWrite() {
        ChatMemoryRepository broken = new ChatMemoryRepository() {
            public List<String> findConversationIds() { return db.snapshots.findConversationIds(); }
            public List<Message> findByConversationId(String key) { return db.snapshots.findByConversationId(key); }
            public void deleteByConversationId(String key) { db.snapshots.deleteByConversationId(key); }
            public void saveAll(String key, List<Message> messages) {
                db.snapshots.saveAll(key, messages);
                throw new IllegalStateException("forced failure after framework write");
            }
        };
        var failing = new JdbcConversationArchive(db.jdbc, broken, db.transactions, db.window, 10000);
        assertThatThrownBy(() -> failing.archive(turn(1))).hasMessageContaining("forced failure");
        assertThat(rows("agent_memory_turn")).isZero();
        assertThat(rows("memory_log")).isZero();
        assertThat(rows("agent_memory_session")).isZero();
        assertThat(db.snapshots.findByConversationId(id + "@1")).isEmpty();
    }

    @Test
    void dryRunAndFailedImportLeaveNoPartialArchive() {
        assertThat(db.archive.importTurns(List.of(turn(1), turn(2)), true)).isEqualTo(2);
        assertThat(rows("agent_memory_turn")).isZero();
        assertThat(rows("memory_log")).isZero();
        var bad = new ArchivedTurn("collision_" + owner, owner, id, 1, 1, "collision", "answer", 1);
        assertThatThrownBy(() -> db.archive.importTurns(List.of(turn(1), bad), false)).isInstanceOf(RuntimeException.class);
        assertThat(rows("agent_memory_session")).isZero();
        assertThat(rows("memory_log")).isZero();
        assertThat(db.archive.importTurns(List.of(turn(1), turn(2)), false)).isEqualTo(2);
        assertThat(db.archive.importTurns(List.of(turn(1), turn(2)), false)).isZero();
    }

    @Test
    void boundedTextSnapshotDoesNotTruncateTheRawArchive() {
        db.window.setMaxMessageChars(4);
        var event = new ArchivedTurn("long_" + owner, owner, id, 1, 1, "中文问题长度超过四字", "中文回复长度超过四字", 1);
        db.archive.archive(event);
        assertThat(db.archive.snapshot(owner, id, 1).orElseThrow().messages()).allSatisfy(m -> assertThat(m.getText()).hasSize(4));
        assertThat(db.jdbc.queryForObject("SELECT user_text FROM agent_memory_turn WHERE event_id = ?",
                String.class, event.eventId())).isEqualTo(event.userText());
    }

    @Test
    void retirementRequiresGapFreeKnownHistoryAndPreventsLateResurrection() {
        db.archive.archive(turn(2));
        assertThatThrownBy(() -> db.archive.retireBefore(owner, id, 1, 9000)).hasMessageContaining("missing turns");
        db.archive.archive(turn(1));
        assertThat(db.archive.retireBefore(owner, id, 1, 1000)).isFalse();
        assertThat(db.archive.retireBefore(owner, id, 1, 9000)).isTrue();
        assertThat(rows("agent_memory_turn")).isZero();
        assertThat(db.archive.snapshot(owner, id, 1).orElseThrow().retired()).isTrue();
        assertThatThrownBy(() -> db.archive.archive(turn(1))).hasMessageContaining("retired");
        assertThatThrownBy(() -> db.archive.archive(turn(3))).hasMessageContaining("retired");
    }

    @Test
    void gapAndTextBudgetsRejectInvalidDataWithoutMutatingHistory() {
        assertThatThrownBy(() -> db.archive.archive(turn(10001))).hasMessageContaining("gap budget");
        assertThat(rows("agent_memory_session")).isZero();
        assertThatThrownBy(() -> new ArchivedTurn("bad", owner, id, 1, 0, "q", "a", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ArchivedTurn("bad", owner, id, 1, 1, " ", "a", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ArchivedTurn("bad", owner, id, 1, 1, "x".repeat(65537), "a", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentOutOfOrderDeliveryAndRedeliveryConverge() throws Exception {
        db.archive.archive(turn(1));
        var executor = Executors.newFixedThreadPool(4);
        try {
            var work = new ArrayList<java.util.concurrent.Callable<Void>>();
            for (long seq : new long[]{7, 3, 2, 8, 5, 4, 6, 3, 7, 2}) {
                work.add(() -> { db.archive.archive(turn(seq)); return null; });
            }
            for (var result : executor.invokeAll(work)) result.get(20, TimeUnit.SECONDS);
        } finally { executor.shutdownNow(); }
        assertThat(rows("agent_memory_turn")).isEqualTo(8);
        assertThat(rows("memory_log")).isEqualTo(8);
        assertThat(db.archive.snapshot(owner, id, 1).orElseThrow().matchesCheckpoint(8)).isTrue();
    }

    @Test
    void concurrentFirstDeliveryIsRecoverableByRetryingTheSameEvent() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var start = new java.util.concurrent.CyclicBarrier(2);
        try {
            var calls = new ArrayList<java.util.concurrent.Callable<Void>>();
            for (long seq : new long[]{2, 1}) calls.add(() -> {
                start.await(10, TimeUnit.SECONDS);
                for (int attempt = 0; ; attempt++) {
                    try { db.archive.archive(turn(seq)); break; }
                    catch (org.springframework.dao.TransientDataAccessException ex) { if (attempt >= 3) throw ex; }
                }
                return null;
            });
            for (var result : pool.invokeAll(calls)) result.get(20, TimeUnit.SECONDS);
        } finally { pool.shutdownNow(); }
        assertThat(db.archive.snapshot(owner, id, 1).orElseThrow().matchesCheckpoint(2)).isTrue();
        assertThat(rows("agent_memory_turn")).isEqualTo(2);
    }
}
