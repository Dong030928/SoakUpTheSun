package org.hgc.suts.gateway.memory.transactions;

import cn.hutool.json.JSONObject;
import org.hgc.suts.gateway.memory.archive.ConversationArchive;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class ArchiveReconcilerTest {
    private final org.hgc.suts.gateway.config.MemoryTransactionProperties config = TransactionTestConfig.create();
    private final RedisTransactionLedger ledger = mock(RedisTransactionLedger.class);
    private final JdbcConversationArchive archive = mock(JdbcConversationArchive.class);
    private final TransactionTransport transport = mock(TransactionTransport.class);
    private final TransactionMetrics metrics = new TransactionMetrics();
    private final ArchiveEvent event = ArchiveEvent.of("agent-transactions-integration-only", ArchiveEventTest.turn("redrive-1", 1));
    private void pending() {
        var entry = new JSONObject().set("state", "COMMITTED").set("archived", false).set("at", System.currentTimeMillis() - 5000)
                .set("body", new String(event.encode(262144), StandardCharsets.UTF_8));
        when(ledger.read(event.turn().conversationId())).thenReturn(new RedisTransactionLedger.Ledger(1, 1, 0,
                new JSONObject().set(event.turn().eventId(), entry)));
        when(ledger.check(event)).thenReturn(TransactionTransport.State.COMMITTED);
        when(ledger.claimRedrive(event)).thenReturn(true);
    }
    @Test void committedSqlLogRepairsReceiptWithoutResending() {
        pending();
        when(archive.hasApplied(event.turn())).thenReturn(true);
        when(archive.snapshot(42, event.turn().conversationId(), 1)).thenReturn(Optional.of(
                new ConversationArchive.Snapshot(42, event.turn().conversationId(), 1, 1, 1, 10, false, List.of())));
        try (var reconciler = new ArchiveReconciler(ledger, archive, transport, config, metrics)) {
            assertThat(reconciler.reconcile(event.turn().conversationId(), 1)).isEqualTo(1);
        }
        verify(ledger).acknowledge(event, 1);
        verifyNoInteractions(transport);
    }
    @Test void anotherWorkersRedriveLeasePreventsASecondSend() {
        pending(); when(ledger.claimRedrive(event)).thenReturn(false);
        try (var reconciler = new ArchiveReconciler(ledger, archive, transport, config, metrics)) {
            assertThat(reconciler.reconcile(event.turn().conversationId(), 1)).isZero();
        }
        verifyNoInteractions(archive, transport);
    }
    @Test void corruptConversationDoesNotStarveLaterConversationsInTheScanBatch() {
        pending();
        when(ledger.nextConversations(anyInt())).thenReturn(List.of("corrupt", event.turn().conversationId()));
        when(ledger.read("corrupt")).thenThrow(new IllegalStateException());
        try (var reconciler = new ArchiveReconciler(ledger, archive, transport, config, metrics)) { reconciler.tick(); }
        verify(transport).send(eq(event), any());
        assertThat(metrics.reconciliationFailures.sum()).isEqualTo(1);
    }
}
