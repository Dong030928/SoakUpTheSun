package org.hgc.suts.gateway.memory.archive;

import org.hgc.suts.gateway.config.MemoryProperties;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Serializes each session with a short SQL lock; no model/network call runs inside this transaction. */
public final class JdbcConversationArchive implements ConversationArchive {
    private final JdbcTemplate jdbc;
    private final ChatMemoryRepository snapshots;
    private final TransactionTemplate transactions;
    private final MemoryProperties window;
    private final int maxGap;
    private final java.util.concurrent.atomic.LongAdder applied = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder duplicates = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder failed = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder elapsed = new java.util.concurrent.atomic.LongAdder();

    public JdbcConversationArchive(JdbcTemplate jdbc, ChatMemoryRepository snapshots, TransactionTemplate transactions,
                                   MemoryProperties window, int maxGap) {
        this.jdbc = jdbc;
        this.snapshots = snapshots;
        this.transactions = transactions;
        this.transactions.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transactions.setTimeout(15);
        this.window = window;
        Assert.isTrue(maxGap > 0 && maxGap <= 100000, "Invalid sequence gap budget");
        this.maxGap = maxGap;
    }

    @Override
    public ApplyResult archive(ArchivedTurn turn) {
        Assert.notNull(turn, "Archive event is required");
        long started = System.nanoTime();
        try {
            ApplyResult result = transactions.execute(status -> apply(turn));
            if (result.duplicate()) duplicates.increment();
            else applied.increment();
            return result;
        } catch (RuntimeException ex) {
            failed.increment();
            throw ex;
        } finally { elapsed.add(System.nanoTime() - started); }
    }

    /** Low-cardinality process counters; no user IDs, history text, or claimed end-to-end latency. */
    public Statistics statistics() { return new Statistics(applied.sum(), duplicates.sum(), failed.sum(), elapsed.sum()); }
    public record Statistics(long appliedEvents, long duplicateEvents, long failedCalls, long archiveCallNanos) { }

    /** Internal infrastructure capabilities: summary jobs use the same archive database and transaction manager. */
    public JdbcTemplate jdbcOperations() { return jdbc; }
    public TransactionTemplate transactionOperations() { return transactions; }

    public List<org.hgc.suts.gateway.memory.summary.SummaryState.RawTurn> completeRange(long owner, String id,
                                                                                       long epoch, long from, long to) {
        ArchivedTurn.validateIdentity(owner, id, epoch);
        Assert.isTrue(from > 0 && to >= from && to - from < 10000, "Invalid bounded archive range");
        return transactions.execute(status -> {
            Row row = lock(id);
            requireSession(row, owner, epoch);
            Assert.state(!row.retired && row.through >= to, "Summary input has not been completely archived");
            var turns = jdbc.query("SELECT turn_seq, user_text, assistant_text, payload_hash FROM agent_memory_turn "
                            + "WHERE conversation_id = ? AND epoch = ? AND turn_seq >= ? AND turn_seq <= ? ORDER BY turn_seq",
                    (rs, i) -> new org.hgc.suts.gateway.memory.summary.SummaryState.RawTurn(rs.getLong(1), rs.getString(2),
                            rs.getString(3), rs.getString(4)), id, epoch, from, to);
            Assert.state(turns.size() == to - from + 1, "Summary input has a durable history gap");
            return turns;
        });
    }

    /** A committed consumption log, not a speculative highest-seen sequence, repairs a lost Redis receipt. */
    public boolean hasApplied(ArchivedTurn turn) {
        List<String> hashes = jdbc.query("SELECT payload_hash FROM memory_log WHERE event_id = ? AND state = 'APPLIED'",
                (rs, i) -> rs.getString(1), turn.eventId());
        if (hashes.isEmpty()) return false;
        Assert.state(hashes.get(0).equals(turn.payloadHash()), "Archive event ID conflicts with existing content");
        return true;
    }

    /** Import is all-or-nothing; dry-run simulates validation in the same transaction and rolls it back. */
    public int importTurns(List<ArchivedTurn> turns, boolean dryRun) {
        Assert.isTrue(turns != null && !turns.isEmpty() && turns.size() <= 500, "Import requires 1..500 complete turns");
        return transactions.execute(status -> {
            int inserted = 0;
            for (ArchivedTurn turn : turns) if (!apply(turn).duplicate()) inserted++;
            if (dryRun) status.setRollbackOnly();
            return inserted;
        });
    }

    private ApplyResult apply(ArchivedTurn turn) {
        ensureSession(turn);
        Row row = lock(turn.conversationId());
        requireSession(row, turn.userId(), turn.epoch());
        Assert.state(!row.retired, "Archive session has been retired");
        List<String> hashes = jdbc.query("SELECT payload_hash FROM memory_log WHERE event_id = ?",
                (rs, i) -> rs.getString(1), turn.eventId());
        if (!hashes.isEmpty()) {
            Assert.state(hashes.get(0).equals(turn.payloadHash()), "Archive event ID conflicts with existing content");
            return new ApplyResult(true, row.highest, row.through);
        }
        Assert.isTrue(turn.sequence() > row.through && turn.sequence() - row.through <= maxGap,
                "Archive sequence conflicts or exceeds the gap budget");
        jdbc.update("INSERT INTO agent_memory_turn (event_id, conversation_id, epoch, turn_seq, user_text, "
                        + "assistant_text, occurred_at, payload_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                turn.eventId(), turn.conversationId(), turn.epoch(), turn.sequence(), turn.userText(),
                turn.assistantText(), turn.occurredAt(), turn.payloadHash());
        long through = row.through;
        for (Long sequence : jdbc.query("SELECT turn_seq FROM agent_memory_turn WHERE conversation_id = ? AND epoch = ? "
                        + "AND turn_seq > ? ORDER BY turn_seq LIMIT ?", (rs, i) -> rs.getLong(1),
                turn.conversationId(), turn.epoch(), through, maxGap)) {
            if (sequence != through + 1) break;
            through = sequence;
        }
        long highest = Math.max(row.highest, turn.sequence());
        jdbc.update("UPDATE agent_memory_session SET highest_seen = ?, archived_through = ?, last_activity = ? "
                        + "WHERE conversation_id = ?", highest, through, Math.max(row.activity, turn.occurredAt()), turn.conversationId());
        if (through > row.through) snapshots.saveAll(snapshotId(turn.conversationId(), turn.epoch()),
                selectWindow(turn.conversationId(), turn.epoch(), through));
        // This log and the framework's delete/reinsert must commit or roll back with the raw turn.
        jdbc.update("INSERT INTO memory_log (event_id, conversation_id, epoch, payload_hash, state, applied_at) "
                        + "VALUES (?, ?, ?, ?, 'APPLIED', ?)", turn.eventId(), turn.conversationId(), turn.epoch(),
                turn.payloadHash(), System.currentTimeMillis());
        return new ApplyResult(false, highest, through);
    }

    private void ensureSession(ArchivedTurn turn) {
        if (jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_session WHERE conversation_id = ?",
                Long.class, turn.conversationId()) > 0) return;
        // Duplicate insertion is safe for MySQL/H2; any competing initial-creation deadlock is retried by the caller.
        try {
            jdbc.update("INSERT INTO agent_memory_session (conversation_id, user_id, epoch, last_activity) VALUES (?, ?, ?, ?)",
                    turn.conversationId(), turn.userId(), turn.epoch(), turn.occurredAt());
        } catch (DuplicateKeyException ignored) { }
    }

    @Override
    public Optional<Snapshot> snapshot(long userId, String conversationId, long epoch) {
        ArchivedTurn.validateIdentity(userId, conversationId, epoch);
        return transactions.execute(status -> {
            List<Row> rows = rows(conversationId);
            if (rows.isEmpty()) return Optional.empty();
            Row row = rows.get(0);
            requireSession(row, userId, epoch);
            List<Message> messages = row.retired ? List.of() : snapshots.findByConversationId(snapshotId(conversationId, epoch));
            return Optional.of(new Snapshot(userId, conversationId, epoch, row.highest, row.through,
                    row.activity, row.retired, messages));
        });
    }

    /** Offline retention only, invoked after writers/consumers have been stopped and drained. Keeps a tombstone. */
    public boolean retireBefore(long userId, String conversationId, long epoch, long cutoff) {
        ArchivedTurn.validateIdentity(userId, conversationId, epoch);
        return transactions.execute(status -> {
            List<Row> rows = rows(conversationId);
            if (rows.isEmpty()) return false;
            Row row = rows.get(0);
            requireSession(row, userId, epoch);
            Assert.state(jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_job WHERE conversation_id = ? AND epoch = ? "
                            + "AND state NOT IN ('DONE', 'STALE')", Long.class, conversationId, epoch) == 0,
                    "Drain summary jobs before archive retention");
            Assert.state(jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_summary WHERE conversation_id = ? AND epoch = ?",
                    Long.class, conversationId, epoch) == 0, "Retire summary projections and vectors explicitly before deleting their source history");
            Assert.state(row.highest == row.through, "Cannot retire an archive with missing turns");
            Assert.state(row.activity > 0, "Legacy records have unknown activity time; do not infer their retention deadline");
            if (row.retired || row.activity >= cutoff) return false;
            snapshots.deleteByConversationId(snapshotId(conversationId, epoch));
            jdbc.update("DELETE FROM agent_memory_turn WHERE conversation_id = ? AND epoch = ?", conversationId, epoch);
            jdbc.update("UPDATE agent_memory_session SET retired = TRUE WHERE conversation_id = ?", conversationId);
            return true;
        });
    }

    private List<Message> selectWindow(String id, long epoch, long through) {
        List<String[]> turns = jdbc.query("SELECT user_text, assistant_text FROM agent_memory_turn "
                        + "WHERE conversation_id = ? AND epoch = ? AND turn_seq <= ? ORDER BY turn_seq DESC LIMIT ?",
                (rs, i) -> new String[]{rs.getString(1), rs.getString(2)}, id, epoch, through, window.getMaxMessages() / 2);
        List<List<Message>> pairs = new ArrayList<>();
        int remaining = window.getMaxContextChars();
        for (String[] pair : turns) {
            String question = pair[0].substring(0, Math.min(pair[0].length(), window.getMaxMessageChars()));
            String answer = pair[1].substring(0, Math.min(pair[1].length(), window.getMaxMessageChars()));
            if (question.length() + answer.length() > remaining) break;
            pairs.add(List.of(new UserMessage(question), new AssistantMessage(answer)));
            remaining -= question.length() + answer.length();
        }
        Collections.reverse(pairs);
        var memory = MessageWindowChatMemory.builder().maxMessages(window.getMaxMessages()).build();
        memory.add(id, pairs.stream().flatMap(List::stream).toList());
        return memory.get(id);
    }

    private Row lock(String conversationId) { return rows(conversationId).get(0); }

    private List<Row> rows(String conversationId) {
        return jdbc.query("SELECT user_id, epoch, highest_seen, archived_through, last_activity, retired "
                        + "FROM agent_memory_session WHERE conversation_id = ? FOR UPDATE",
                (rs, i) -> new Row(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4), rs.getLong(5), rs.getBoolean(6)),
                conversationId);
    }

    private static void requireSession(Row row, long owner, long epoch) {
        Assert.state(row.owner == owner && row.epoch == epoch, "Archive owner or epoch does not match");
    }

    static String snapshotId(String id, long epoch) { return id + "@" + epoch; }
    private record Row(long owner, long epoch, long highest, long through, long activity, boolean retired) { }
}
