package org.hgc.suts.gateway.memory.transactions;

import cn.hutool.json.JSONObject;
import org.hgc.suts.gateway.memory.*;
import org.hgc.suts.gateway.memory.archive.ConversationArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.memory.InMemoryChatMemoryRepository;
import org.springframework.ai.chat.messages.*;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class TransactionalConversationMemoryStoreTest {
    private final RedisConversationMemoryRepository redis = mock(RedisConversationMemoryRepository.class);
    private final RedisConversationMemoryRepository.Lease lease = mock(RedisConversationMemoryRepository.Lease.class);
    private final RedisTransactionLedger ledger = mock(RedisTransactionLedger.class);
    private final TransactionTransport transport = mock(TransactionTransport.class);
    private final ConversationArchive archive = mock(ConversationArchive.class);
    private final org.hgc.suts.gateway.config.MemoryTransactionProperties config = TransactionTestConfig.create();
    private final String id = ConversationMemory.conversationId(42L, "transactions");
    private final InMemoryChatMemoryRepository history = new InMemoryChatMemoryRepository();
    private final List<Message> pair = List.of(new UserMessage("问题"), new AssistantMessage("回复"));
    private final TransactionalConversationMemoryStore store = new TransactionalConversationMemoryStore(redis, ledger, archive, transport, config, 86400000);

    @BeforeEach void setup() {
        when(redis.open(id)).thenReturn(lease);
        when(lease.repository()).thenReturn(history);
        when(lease.fencingToken()).thenReturn("private-fence");
        when(ledger.read(id)).thenReturn(new RedisTransactionLedger.Ledger(1, 0, 0, new JSONObject()));
        when(archive.snapshot(42, id, 1)).thenReturn(Optional.empty());
    }
    @Test void halfMessageCallbackIsTheOnlyLocalWritePathAndCommitIsOncePerSession() {
        when(ledger.commit(any(), anyString(), anyLong(), anyString(), anyLong())).thenReturn(TransactionTransport.State.COMMITTED);
        when(transport.send(any(), any())).thenAnswer(invocation -> {
            verify(ledger, never()).commit(any(), anyString(), anyLong(), anyString(), anyLong());
            return ((Supplier<?>)invocation.getArgument(1)).get();
        });
        try (var session = store.open(id)) {
            assertThat(session.commit(id, "原始问题", "完整回复", pair)).isEqualTo(MemoryCommitStatus.ACCEPTED);
            assertThat(session.commit(id, "原始问题", "完整回复", pair)).isEqualTo(MemoryCommitStatus.ACCEPTED);
        }
        verify(transport, times(1)).send(argThat(event -> event.turn().userText().equals("原始问题")
                && event.turn().assistantText().equals("完整回复") && event.turn().sequence() == 1), any());
        verifyNoMoreInteractions(archive);
        assertThat(history.findByConversationId(id)).isEmpty(); // No bypass through the Redis-only repository.
    }
    @Test void failedHalfMessageNeverExecutesRedisLocalTransaction() {
        when(transport.send(any(), any())).thenThrow(new IllegalStateException());
        when(ledger.check(any())).thenReturn(TransactionTransport.State.UNKNOWN);
        try (var session = store.open(id)) {
            assertThat(session.commit(id, "问题", "回复", pair)).isEqualTo(MemoryCommitStatus.DEGRADED);
        }
        verify(ledger, never()).commit(any(), anyString(), anyLong(), anyString(), anyLong());
    }
    @Test void uncertainSendWithSharedCommitProofReturnsPending() {
        when(transport.send(any(), any())).thenThrow(new IllegalStateException());
        when(ledger.check(any())).thenReturn(TransactionTransport.State.COMMITTED);
        try (var session = store.open(id)) {
            assertThat(session.commit(id, "问题", "回复", pair)).isEqualTo(MemoryCommitStatus.PENDING);
        }
    }
    @Test void unavailableBrokerRejectsBeforeReadingHistoryOrDoingBusiness() {
        doThrow(new MemoryTransactionsUnavailableException()).when(transport).preflight();
        assertThatThrownBy(() -> store.open(id)).isInstanceOf(MemoryTransactionsUnavailableException.class);
        verifyNoInteractions(archive);
        verify(lease).close();
    }
    @Test void coldCacheRequiresIndependentHeadAndCompleteDatabaseCheckpoint() {
        when(ledger.read(id)).thenReturn(new RedisTransactionLedger.Ledger(1, 2, 1, new JSONObject()));
        when(archive.snapshot(42, id, 1)).thenReturn(Optional.of(new ConversationArchive.Snapshot(42, id, 1, 2, 1, 10, false, pair)));
        assertThatThrownBy(() -> store.open(id)).isInstanceOf(MemoryTransactionsUnavailableException.class);
        verify(lease, never()).restoreIfMissing(any());
        when(archive.snapshot(42, id, 1)).thenReturn(Optional.of(new ConversationArchive.Snapshot(42, id, 1, 2, 2, 10, false, pair)));
        try (var ignored = store.open(id)) { }
        verify(lease).restoreIfMissing(pair);
    }
    @Test void existingHotSnapshotIsNotOverwrittenBySlowerArchive() {
        when(ledger.read(id)).thenReturn(new RedisTransactionLedger.Ledger(1, 2, 1, new JSONObject()));
        when(lease.hasStoredSnapshot()).thenReturn(true);
        history.saveAll(id, pair);
        try (var session = store.open(id)) {
            assertThat(session.repository().findByConversationId(id)).hasSize(2);
            assertThatThrownBy(() -> session.repository().saveAll(id, pair)).isInstanceOf(UnsupportedOperationException.class);
        }
        verifyNoInteractions(archive);
        verify(lease, never()).restoreIfMissing(any());
    }
    @Test void existingHistoryWithoutProducerCheckpointRequiresOfflineCutover() {
        when(ledger.read(id)).thenReturn(null);
        when(lease.hasStoredSnapshot()).thenReturn(true);
        assertThatThrownBy(() -> store.open(id)).isInstanceOf(MemoryTransactionsUnavailableException.class);
        verify(ledger, never()).initialize(anyString(), any(), anyLong(), anyLong());
    }
    @Test void admissionAndOversizeAreExplicitFailures() {
        doThrow(new MemoryTransactionsUnavailableException()).when(ledger).admit(any());
        assertThatThrownBy(() -> store.open(id)).isInstanceOf(MemoryTransactionsUnavailableException.class);
        doNothing().when(ledger).admit(any());
        config.setMaxMessageBytes(1024);
        try (var session = store.open(id)) {
            assertThatThrownBy(() -> session.commit(id, "问题", "长".repeat(2000), pair)).isInstanceOf(IllegalArgumentException.class);
        }
        verify(transport, never()).send(any(), any());
    }
}
