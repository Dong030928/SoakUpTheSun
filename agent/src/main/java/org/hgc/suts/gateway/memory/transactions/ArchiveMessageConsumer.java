package org.hgc.suts.gateway.memory.transactions;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.archive.ConversationArchive;

/** A successful return means SQL committed. Receipt failure is repaired without rolling back committed SQL. */
@Slf4j
public final class ArchiveMessageConsumer {
    private final ConversationArchive archive;
    private final RedisTransactionLedger ledger;
    private final MemoryTransactionProperties config;
    private final TransactionMetrics metrics;

    public ArchiveMessageConsumer(ConversationArchive archive, RedisTransactionLedger ledger,
                                 MemoryTransactionProperties config, TransactionMetrics metrics) {
        this.archive = archive; this.ledger = ledger; this.config = config; this.metrics = metrics;
    }
    public void consume(byte[] body, int reconsumeTimes) {
        try {
            ArchiveEvent event = ArchiveEvent.decode(body, config.getMaxMessageBytes(), config.getGeneration());
            // Broker COMMIT is sufficient to consume. Redis failure must not prevent independent full-body SQL archival.
            var result = archive.archive(event.turn());
            metrics.consumed.increment();
            try { ledger.acknowledge(event, result.archivedThrough()); }
            catch (RuntimeException ex) {
                metrics.receiptFailures.increment();
                log.warn("归档已提交，Redis 回执待对账修复 ({})", ex.getClass().getSimpleName());
            }
        } catch (RuntimeException ex) {
            metrics.consumeFailures.increment();
            if (reconsumeTimes >= config.getMaxReconsumeTimes() - 1) {
                metrics.retryExhaustionRisk.increment();
                log.error("归档消费接近死信阈值，需核查 DLQ 与未落库证据 ({})", ex.getClass().getSimpleName());
            } else log.warn("归档消费失败，将以同一事件重试 ({})", ex.getClass().getSimpleName());
            throw ex;
        }
    }
}
