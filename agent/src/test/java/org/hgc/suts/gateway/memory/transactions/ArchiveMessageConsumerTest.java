package org.hgc.suts.gateway.memory.transactions;

import org.hgc.suts.gateway.memory.archive.ArchiveDatabaseFixture;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class ArchiveMessageConsumerTest {
    private final ArchiveDatabaseFixture db = ArchiveDatabaseFixture.h2();
    private final RedisTransactionLedger ledger = mock(RedisTransactionLedger.class);
    private final TransactionMetrics metrics = new TransactionMetrics();
    private final org.hgc.suts.gateway.config.MemoryTransactionProperties config = TransactionTestConfig.create();
    private final ArchiveEvent event = ArchiveEvent.of("agent-transactions-integration-only", ArchiveEventTest.turn("consumer-1", 1));
    private ArchiveMessageConsumer consumer() { return new ArchiveMessageConsumer(db.archive, ledger, config, metrics); }

    @Test void duplicateAfterDatabaseCommitBeforeAckHasOneEffect() {
        byte[] body = event.encode(262144);
        consumer().consume(body, 0);
        consumer().consume(body, 1);
        assertThat(db.jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_turn", Long.class)).isEqualTo(1);
        assertThat(db.jdbc.queryForObject("SELECT COUNT(*) FROM memory_log WHERE state='APPLIED'", Long.class)).isEqualTo(1);
        assertThat(db.archive.hasApplied(event.turn())).isTrue();
        verify(ledger, times(2)).acknowledge(event, 1);
    }
    @Test void receiptFailureDoesNotUndoSqlCommitAndDuplicateRepairsIt() {
        doThrow(new IllegalStateException("redis down")).doNothing().when(ledger).acknowledge(event, 1);
        assertThatCode(() -> consumer().consume(event.encode(262144), 0)).doesNotThrowAnyException();
        consumer().consume(event.encode(262144), 1);
        assertThat(metrics.receiptFailures.sum()).isEqualTo(1);
        assertThat(db.archive.statistics().duplicateEvents()).isEqualTo(1);
    }
    @Test void databaseFailureIsNotAcknowledgedAndExhaustionRiskIsVisible() {
        db.jdbc.execute("DROP TABLE agent_memory_turn");
        assertThatThrownBy(() -> consumer().consume(event.encode(262144), config.getMaxReconsumeTimes() - 1))
                .isInstanceOf(RuntimeException.class);
        verifyNoInteractions(ledger);
        assertThat(metrics.retryExhaustionRisk.sum()).isEqualTo(1);
        assertThat(db.jdbc.queryForObject("SELECT COUNT(*) FROM memory_log", Long.class)).isZero();
    }
    @Test void outOfOrderReceiptUsesContinuousWatermark() {
        var second = ArchiveEvent.of(config.getGeneration(), ArchiveEventTest.turn("consumer-2", 2));
        consumer().consume(second.encode(262144), 0);
        verify(ledger).acknowledge(second, 0);
        consumer().consume(event.encode(262144), 0);
        verify(ledger).acknowledge(event, 2);
    }
    @Test void malformedMessageHasNoDatabaseEffect() {
        assertThatThrownBy(() -> consumer().consume("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8), 0))
                .isInstanceOf(RuntimeException.class);
        assertThat(db.jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_turn", Long.class)).isZero();
    }
}
