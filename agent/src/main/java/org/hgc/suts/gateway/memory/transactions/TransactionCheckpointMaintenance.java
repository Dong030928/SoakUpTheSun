package org.hgc.suts.gateway.memory.transactions;

import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.ArchivedTurn;
import org.hgc.suts.gateway.memory.archive.ConversationArchive;
import org.springframework.util.Assert;

/** Frozen offline cutover only. The supplied checkpoint must come from drained producer/import evidence. */
public final class TransactionCheckpointMaintenance {
    private final ConversationArchive archive;
    private final RedisConversationMemoryRepository redis;
    private final RedisTransactionLedger ledger;
    private final MemoryProperties memory;
    public TransactionCheckpointMaintenance(ConversationArchive archive, RedisConversationMemoryRepository redis,
                                             RedisTransactionLedger ledger, MemoryProperties memory) {
        this.archive = archive; this.redis = redis; this.ledger = ledger; this.memory = memory;
    }
    public boolean adopt(long owner, String id, long epoch, long checkpoint, boolean drained, boolean dryRun) {
        Assert.state(!memory.isWritesEnabled() && drained, "Stop and drain writers, consumers and checkers before cutover");
        ArchivedTurn.validateIdentity(owner, id, epoch);
        Assert.isTrue(checkpoint > 0 && checkpoint <= 9999999999999L, "Explicit verified checkpoint is required");
        try (var lease = redis.open(id)) {
            var snapshot = archive.snapshot(owner, id, epoch).orElseThrow();
            Assert.state(snapshot.matchesCheckpoint(checkpoint), "Archive does not match the independent producer checkpoint");
            Assert.state(ledger.read(id) == null, "Cannot replace existing transaction evidence");
            if (lease.hasStoredSnapshot()) {
                var hot = lease.repository().findByConversationId(id);
                Assert.state(!hot.isEmpty() && texts(hot).equals(texts(snapshot.messages())),
                        "Hot history differs from the verified archive; do not overwrite a newer/corrupt snapshot");
            }
            if (dryRun) return true;
            ledger.initialize(id, lease, epoch, checkpoint);
            return true;
        }
    }
    private static java.util.List<String> texts(java.util.List<org.springframework.ai.chat.messages.Message> messages) {
        return messages.stream().map(org.springframework.ai.chat.messages.Message::getText).toList();
    }

    public boolean releaseToCache(long owner, String id, long epoch, long checkpoint, boolean drained, boolean dryRun) {
        Assert.state(!memory.isWritesEnabled() && drained, "Stop and drain writers, consumers and checkers before rollback");
        ArchivedTurn.validateIdentity(owner, id, epoch);
        try (var lease = redis.open(id)) {
            var state = ledger.read(id);
            Assert.state(state != null && state.epoch() == epoch, "Rollback producer epoch does not match the archive");
            var snapshot = archive.snapshot(owner, id, epoch).orElseThrow();
            Assert.state(snapshot.matchesCheckpoint(checkpoint) && !snapshot.messages().isEmpty(), "Rollback requires a complete verified archive");
            if (lease.hasStoredSnapshot()) {
                Assert.state(texts(lease.repository().findByConversationId(id)).equals(texts(snapshot.messages())),
                        "Hot snapshot differs from the archive; reconcile before rollback");
            } else if (!dryRun) lease.restoreIfMissing(snapshot.messages());
            ledger.releaseToCache(id, lease, checkpoint, dryRun);
            return true;
        }
    }
}
