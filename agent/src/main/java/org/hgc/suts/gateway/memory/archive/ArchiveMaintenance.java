package org.hgc.suts.gateway.memory.archive;

import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.springframework.ai.chat.messages.Message;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/** Explicit frozen-writer maintenance, not automatic hot-path fallback ahead of M02's producer checkpoint. */
public final class ArchiveMaintenance {
    private final JdbcConversationArchive archive;
    private final RedisConversationMemoryRepository redis;
    private final MemoryProperties memory;

    public ArchiveMaintenance(JdbcConversationArchive archive, RedisConversationMemoryRepository redis, MemoryProperties memory) {
        this.archive = archive;
        this.redis = redis;
        this.memory = memory;
    }

    public int importRecent(long owner, String conversationId, long epoch, boolean writersDrained, boolean dryRun) {
        frozen(writersDrained);
        ArchivedTurn.validateIdentity(owner, conversationId, epoch);
        try (var lease = redis.open(conversationId)) {
            List<Message> messages = lease.repository().findByConversationId(conversationId);
            Assert.state(!messages.isEmpty() && messages.size() % 2 == 0, "No valid complete Redis turns to import");
            List<ArchivedTurn> turns = new ArrayList<>();
            for (int i = 0; i < messages.size(); i += 2) {
                // Timestamp 0 denotes unknown legacy time, not the time the operator ran the importer.
                ArchivedTurn provisional = new ArchivedTurn("legacy", owner, conversationId, epoch, i / 2 + 1L,
                        messages.get(i).getText(), messages.get(i + 1).getText(), 0);
                turns.add(new ArchivedTurn("legacy_" + provisional.payloadHash(), owner, conversationId, epoch,
                        provisional.sequence(), provisional.userText(), provisional.assistantText(), 0));
            }
            lease.assertValid();
            return archive.importTurns(turns, dryRun);
        }
    }

    public boolean restore(long owner, String conversationId, long epoch, long producerCheckpoint, boolean writersDrained) {
        return restore(owner, conversationId, epoch, producerCheckpoint, writersDrained, false);
    }

    public boolean restore(long owner, String conversationId, long epoch, long producerCheckpoint,
                           boolean writersDrained, boolean dryRun) {
        frozen(writersDrained);
        ArchivedTurn.validateIdentity(owner, conversationId, epoch);
        try (var lease = redis.open(conversationId)) {
            var snapshot = archive.snapshot(owner, conversationId, epoch).orElseThrow(
                    () -> new IllegalStateException("No confirmed archive for recovery"));
            Assert.state(snapshot.matchesCheckpoint(producerCheckpoint), "Archive does not match the verified producer checkpoint");
            Assert.state(!snapshot.messages().isEmpty(), "Archive has no recoverable snapshot");
            if (dryRun) return !lease.hasStoredSnapshot();
            return lease.restoreIfMissing(snapshot.messages());
        }
    }

    public boolean retireBefore(long owner, String id, long epoch, long cutoff, boolean writersDrained) {
        return retireBefore(owner, id, epoch, cutoff, writersDrained, false);
    }

    public boolean retireBefore(long owner, String id, long epoch, long cutoff, boolean writersDrained, boolean dryRun) {
        frozen(writersDrained);
        ArchivedTurn.validateIdentity(owner, id, epoch);
        Assert.isTrue(cutoff > 0, "An explicit retention cutoff is required");
        try (var lease = redis.open(id)) {
            // Keep a tombstone so late messages cannot recreate this archived epoch.
            lease.assertValid();
            if (dryRun) {
                var snapshot = archive.snapshot(owner, id, epoch).orElseThrow();
                Assert.state(snapshot.highestSeen() == snapshot.archivedThrough() && snapshot.lastActivity() > 0,
                        "Retention requires a gap-free archive with a known activity time");
                return !snapshot.retired() && snapshot.lastActivity() < cutoff;
            }
            boolean retired = archive.retireBefore(owner, id, epoch, cutoff);
            if (retired || archive.snapshot(owner, id, epoch).orElseThrow().retired()) {
                lease.repository().deleteByConversationId(id);
            }
            return retired;
        }
    }

    private void frozen(boolean drained) {
        Assert.state(!memory.isWritesEnabled() && drained, "Freeze all writers/consumers and explicitly confirm they are drained");
    }
}
