package org.hgc.suts.gateway.memory.transactions;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.ConversationMemoryStore;
import org.hgc.suts.gateway.memory.MemoryCommitStatus;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.ArchivedTurn;
import org.hgc.suts.gateway.memory.archive.ConversationArchive;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.Message;

import java.util.List;
import java.util.UUID;

/** The request thread never writes MySQL. The only acceptance path starts with a Broker half-message ACK. */
@Slf4j
public final class TransactionalConversationMemoryStore implements ConversationMemoryStore {
    private final RedisConversationMemoryRepository redis;
    private final RedisTransactionLedger ledger;
    private final ConversationArchive archive;
    private final TransactionTransport transport;
    private final MemoryTransactionProperties config;
    private final long hotTtlMs;

    public TransactionalConversationMemoryStore(RedisConversationMemoryRepository redis, RedisTransactionLedger ledger,
            ConversationArchive archive, TransactionTransport transport, MemoryTransactionProperties config, long hotTtlMs) {
        this.redis = redis; this.ledger = ledger; this.archive = archive; this.transport = transport;
        this.config = config; this.hotTtlMs = hotTtlMs;
    }

    @Override public Session open(String id) {
        if (!config.isAccepting()) throw new MemoryTransactionsUnavailableException();
        var lease = redis.open(id);
        try {
            // Explicit fail-closed admission happens before any model/tool execution.
            transport.preflight();
            var state = ledger.read(id);
            long userId = Long.parseLong(id.split(":")[1]);
            if (state == null) {
                if (lease.hasStoredSnapshot() || archive.snapshot(userId, id, 1).isPresent()) {
                    throw new MemoryTransactionsUnavailableException(); // Existing history requires verified offline cutover.
                }
                ledger.initialize(id, lease, 1, 0);
                state = ledger.read(id);
            }
            if (state == null) throw new MemoryTransactionsUnavailableException();
            ledger.admit(state);
            if (!lease.hasStoredSnapshot() && state.head() > 0) {
                // Independent producer head + gap-free DB checkpoint; never infer completeness from SQL MAX(seq).
                var checkpoint = archive.snapshot(userId, id, state.epoch()).orElseThrow(MemoryTransactionsUnavailableException::new);
                if (!checkpoint.matchesCheckpoint(state.head()) || checkpoint.messages().isEmpty()) {
                    throw new MemoryTransactionsUnavailableException();
                }
                lease.restoreIfMissing(checkpoint.messages());
            }
            return new TransactionSession(id, userId, lease, state);
        } catch (org.hgc.suts.gateway.memory.ConversationBusyException ex) { lease.close(); throw ex; }
        catch (RuntimeException ex) {
            lease.close();
            log.warn("事务记忆准入或完整恢复不可用 ({})", ex.getClass().getSimpleName());
            throw new MemoryTransactionsUnavailableException();
        }
    }

    private final class TransactionSession implements Session {
        private final String id;
        private final long userId;
        private final RedisConversationMemoryRepository.Lease lease;
        private final RedisTransactionLedger.Ledger base;
        private MemoryCommitStatus result;
        private final ChatMemoryRepository repository;

        private TransactionSession(String id, long userId, RedisConversationMemoryRepository.Lease lease,
                                   RedisTransactionLedger.Ledger base) {
            this.id = id; this.userId = userId; this.lease = lease; this.base = base;
            this.repository = new ChatMemoryRepository() {
                public List<String> findConversationIds() { return List.of(id); }
                public List<Message> findByConversationId(String candidate) {
                    requireId(candidate);
                    List<Message> messages = lease.repository().findByConversationId(id);
                    if (base.head() > 0 && messages.isEmpty()) throw new MemoryTransactionsUnavailableException();
                    return messages;
                }
                public void saveAll(String candidate, List<Message> messages) {
                    throw new UnsupportedOperationException("Transaction memory must commit a complete turn through MQ");
                }
                public void deleteByConversationId(String candidate) {
                    throw new UnsupportedOperationException("Transaction evidence requires drained offline maintenance");
                }
            };
        }
        public ChatMemoryRepository repository() { return repository; }
        public void assertValid() { lease.assertValid(); }
        public void close() { lease.close(); }
        private void requireId(String candidate) {
            if (!id.equals(candidate)) throw new IllegalArgumentException("Conversation owner mismatch");
        }
        @Override public MemoryCommitStatus commit(String candidate, String input, String answer, List<Message> snapshot) {
            requireId(candidate);
            if (result != null) return result;
            result = MemoryCommitStatus.DEGRADED;
            ArchiveEvent event = ArchiveEvent.of(config.getGeneration(), new ArchivedTurn(UUID.randomUUID().toString(),
                    userId, id, base.epoch(), base.head() + 1, input, answer, System.currentTimeMillis()));
            event.encode(config.getMaxMessageBytes()); // Validate before sending a half message.
            String payload = RedisConversationMemoryRepository.encode(snapshot, id);
            String fencingToken = lease.fencingToken();
            try {
                var state = transport.send(event, () -> ledger.commit(event, fencingToken, base.head(), payload, hotTtlMs));
                if (state == TransactionTransport.State.COMMITTED) result = MemoryCommitStatus.ACCEPTED;
            } catch (RuntimeException ex) {
                log.warn("事务发送结果未确认，保留本轮业务结果 ({})", ex.getClass().getSimpleName());
            }
            if (result != MemoryCommitStatus.ACCEPTED && ledger.check(event) == TransactionTransport.State.COMMITTED) {
                result = MemoryCommitStatus.PENDING;
            }
            return result;
        }
    }
}
