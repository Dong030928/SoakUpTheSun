package org.hgc.suts.gateway.memory.transactions;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;

import java.nio.charset.StandardCharsets;

/** Redis evidence is the source of truth for re-drive, not SQL MAX(seq), an in-memory map or an Outbox. */
@Slf4j
public final class ArchiveReconciler implements AutoCloseable {
    private final RedisTransactionLedger ledger;
    private final JdbcConversationArchive archive;
    private final TransactionTransport transport;
    private final MemoryTransactionProperties config;
    private final TransactionMetrics metrics;
    private final java.util.concurrent.ScheduledExecutorService scheduler;
    private long sweeps;

    public ArchiveReconciler(RedisTransactionLedger ledger, JdbcConversationArchive archive, TransactionTransport transport,
                             MemoryTransactionProperties config, TransactionMetrics metrics) {
        this.ledger = ledger; this.archive = archive; this.transport = transport; this.config = config; this.metrics = metrics;
        scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "agent-archive-reconciliation"); thread.setDaemon(true); return thread;
        });
    }
    public void start() {
        scheduler.scheduleWithFixedDelay(this::tick, config.getReconcileIntervalMs(), config.getReconcileIntervalMs(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }
    public void tick() {
        try {
            for (String id : ledger.nextConversations(config.getReconcileBatch())) {
                // One event per conversation per sweep prevents a broken session starving the rest of a scan batch.
                try { reconcile(id, 1); }
                catch (RuntimeException ex) {
                    metrics.reconciliationFailures.increment();
                    log.warn("单会话归档对账失败，继续处理其他会话 ({})", ex.getClass().getSimpleName());
                }
            }
            if (++sweeps % 12 == 0) log.info("事务归档计数 {}", metrics.snapshot());
        } catch (RuntimeException ex) {
            metrics.reconciliationFailures.increment();
            log.warn("事务归档对账暂不可用 ({})", ex.getClass().getSimpleName());
        }
    }
    public int reconcile(String id, int limit) {
        var state = ledger.read(id);
        if (state == null) return 0;
        int handled = 0;
        var ordered = state.events().keySet().stream().sorted(java.util.Comparator.comparingLong(eventId -> {
            var entry = state.events().getJSONObject(eventId);
            return Math.max(entry.getLong("at", Long.MAX_VALUE), entry.getLong("lastRedrive", 0L));
        })).toList();
        for (String eventId : ordered) {
            var entry = state.events().getJSONObject(eventId);
            if (handled >= limit) break;
            if (!"COMMITTED".equals(entry.getStr("state")) || entry.getBool("archived", false)) continue;
            long age = System.currentTimeMillis() - entry.getLong("at", System.currentTimeMillis());
            metrics.maxObservedPendingAgeMs.accumulateAndGet(age, Math::max);
            if (age >= config.getRecoveryWindowSeconds() * 1000) metrics.overdueObservations.increment();
            if (System.currentTimeMillis() - Math.max(entry.getLong("at", System.currentTimeMillis()), entry.getLong("lastRedrive", 0L))
                    < config.getRedriveAfterMs()) continue;
            String body = entry.getStr("body");
            if (body == null) throw new IllegalStateException("Pending evidence lost its immutable archive body");
            ArchiveEvent event = ArchiveEvent.decode(body.getBytes(StandardCharsets.UTF_8), config.getMaxMessageBytes(), config.getGeneration());
            if (!id.equals(event.turn().conversationId()) || !eventId.equals(event.turn().eventId())
                    || ledger.check(event) != TransactionTransport.State.COMMITTED) throw new IllegalStateException("Unproven re-drive rejected");
            if (!ledger.claimRedrive(event)) continue;
            handled++;
            if (archive.hasApplied(event.turn())) {
                var snapshot = archive.snapshot(event.turn().userId(), id, event.turn().epoch()).orElseThrow();
                ledger.acknowledge(event, snapshot.archivedThrough());
            } else {
                metrics.redriven.increment();
                transport.send(event, () -> ledger.check(event));
            }
        }
        return handled;
    }
    @Override public void close() { scheduler.shutdownNow(); }
}
