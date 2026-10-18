package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.config.MemorySummaryProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.transactions.*;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class SummaryCoordinatorTest {
    private final MemorySummaryProperties config = SummaryTestFixture.config();
    private final org.hgc.suts.gateway.config.AiConfig ai = SummaryTestFixture.ai();
    private final SummaryPolicy policy = new SummaryPolicy(() -> config, ai);
    private final RedisSummaryStateStore states = mock(RedisSummaryStateStore.class);
    private final RedisConversationMemoryRepository.Lease lease = mock(RedisConversationMemoryRepository.Lease.class);
    private final SummaryMessaging messaging = mock(SummaryMessaging.class);
    private final MemoryTokenBudget tokens = new MemoryTokenBudget();
    private final AtomicReference<SummaryState> state = new AtomicReference<>();
    private final SummaryCoordinator coordinator = new SummaryCoordinator(states, mock(RedisConversationMemoryRepository.class),
            mock(RedisTransactionLedger.class), mock(JdbcConversationArchive.class), mock(JdbcSummaryJobs.class),
            TransactionTestConfig.create(), policy, tokens, () -> messaging);
    @BeforeEach @SuppressWarnings("unchecked") void stateMachine() {
        state.set(SummaryTestFixture.state(30, 50));
        when(states.read(anyString())).thenAnswer(i -> state.get());
        when(states.update(anyString(), any())).thenAnswer(i -> state.updateAndGet((UnaryOperator<SummaryState>) i.getArgument(1)));
        when(lease.fencingToken()).thenReturn("synthetic-lease");
    }
    private SummaryContext.Selection select() {
        return coordinator.select(state.get().conversationId(), lease, new SummaryContext.Request("system", "current", "tool schema"));
    }
    private void cache(long expiry) {
        state.updateAndGet(s -> s.cache(policy.selected(), System.currentTimeMillis(), 0, expiry));
    }
    @Test void validVendorCachePreservesTheWholePrefixEvenAboveHAndBeyondTwentyMessages() {
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION)));
        cache(System.currentTimeMillis() + 60000);
        assertThat(select().history()).hasSize(60);
        verifyNoInteractions(messaging);
    }
    @Test void expiredCacheAboveHTriggersAnImmutableContiguousRangeAndKeepsAboutL() {
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION)));
        cache(System.currentTimeMillis() - 1);
        select();
        var event = ArgumentCaptor.forClass(SummaryEvent.class);
        verify(messaging).sendSummary(event.capture(), any());
        assertThat(event.getValue().from()).isEqualTo(1);
        assertThat(event.getValue().to()).isEqualTo(26);
        assertThat(event.getValue().turns()).hasSize(26);
    }
    @Test void expiredCacheBelowHDoesNotSummarizeJustBecauseItExpired() {
        state.set(SummaryTestFixture.state(3, 30));
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION)));
        cache(System.currentTimeMillis() - 1);
        assertThat(select().history()).hasSize(6);
        verifyNoInteractions(messaging);
    }
    @Test void absentVendorTtlNeverManufacturesAnIdleExpiry() {
        assertThat(select().history()).hasSize(60);
        assertThat(state.get().expiresAt()).isZero();
        verifyNoInteractions(messaging);
    }
    @Test void hardBudgetWorksWithoutVendorTtlAndKeepsWholeNewestTurns() {
        state.set(SummaryTestFixture.state(100, 80));
        var selected = select();
        assertThat(selected.estimatedTokens()).isLessThanOrEqualTo(config.getInputBudgetTokens());
        assertThat(selected.history().size()).isEven().isLessThan(200);
        verify(messaging).sendSummary(any(), any());
        assertThat(state.get().raw()).hasSize(100); // Selection cannot delete unarchived/excluded originals.
    }
    @Test void contextRollbackUsesTheBoundedRecentWindowAndRetainsSummaryAndOriginals() {
        var old = state.get(); var event = SummaryEvent.of(old, old.raw().subList(0, 2));
        state.set(old.merged(event, "应当保留但不注入的摘要"));
        config.setAccepting(false); config.setUseSummaryContext(false);
        var selected = select();
        assertThat(selected.history()).hasSize(20).noneMatch(m -> m.getText().contains("不注入的摘要"));
        assertThat(state.get().summary()).isEqualTo("应当保留但不注入的摘要");
        assertThat(state.get().raw()).hasSize(28);
        verifyNoInteractions(messaging);
    }
    @Test void fixedSystemToolsCurrentInputAndReferencesAllConsumeBudget() {
        assertThatThrownBy(() -> coordinator.select(state.get().conversationId(), lease,
                new SummaryContext.Request("s".repeat(6000), "reference+input".repeat(500), "tools".repeat(1000))))
                .isInstanceOf(org.hgc.suts.gateway.common.exception.ClientException.class);
        verifyNoInteractions(messaging);
    }
    @Test void creationExpiryDoesNotSlideWithEachNewMessage() {
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION)));
        long now = System.currentTimeMillis();
        coordinator.observe(state.get().conversationId(), new SummaryContext.CacheObservation(now, false, null));
        coordinator.observe(state.get().conversationId(), new SummaryContext.CacheObservation(now + 5000, true, null));
        assertThat(state.get().expiresAt()).isEqualTo(now + 60000);
    }
    @Test void cacheHitRenewalOnlyRenewsOnConfirmedHit() {
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CACHE_HIT)));
        long now = System.currentTimeMillis(); String id = state.get().conversationId();
        coordinator.observe(id, new SummaryContext.CacheObservation(now, false, null));
        coordinator.observe(id, new SummaryContext.CacheObservation(now + 5000, false, null));
        assertThat(state.get().expiresAt()).isEqualTo(now + 60000);
        coordinator.observe(id, new SummaryContext.CacheObservation(now + 6000, true, null));
        assertThat(state.get().expiresAt()).isEqualTo(now + 66000);
    }
    @Test void vendorRequestStartOriginDoesNotGetExtendedByModelLatency() {
        var c = SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION);
        c.setClockOrigin(MemorySummaryProperties.ClockOrigin.REQUEST_STARTED); config.setCachePolicies(List.of(c));
        long started = System.currentTimeMillis();
        coordinator.observe(state.get().conversationId(), new SummaryContext.CacheObservation(started + 5000, false, null, 0, started));
        assertThat(state.get().expiresAt()).isEqualTo(started + 60000);
    }
    @Test void providerReportedExpiryIsUsedVerbatimAndMissingTimestampIsNotInvented() {
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.PROVIDER_EXPIRY)));
        long now = System.currentTimeMillis(); String id = state.get().conversationId();
        coordinator.observe(id, new SummaryContext.CacheObservation(now, false, null));
        assertThat(state.get().expiresAt()).isZero();
        coordinator.observe(id, new SummaryContext.CacheObservation(now + 1, true, now + 12345));
        assertThat(state.get().expiresAt()).isEqualTo(now + 12345);
    }
    @Test void changedPolicyModelAndModeInvalidateOldIndexObservations() {
        var c = SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION); config.setCachePolicies(List.of(c));
        cache(System.currentTimeMillis() - 1);
        c.setVersion("synthetic-v2"); c.setTtlSeconds(120);
        long created = state.get().cacheCreatedAt();
        select(); assertThat(state.get().expiresAt()).isEqualTo(created + 120000); verifyNoInteractions(messaging);
        ai.setModelName("another-fixture"); select(); assertThat(state.get().expiresAt()).isZero();
        config.setCacheMode("another-mode"); select(); assertThat(state.get().expiresAt()).isZero();
    }
    @Test @SuppressWarnings("unchecked") void ttlDecisionIsRecheckedAfterBrokerHalfMessageAcknowledgement() {
        var c = SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION); config.setCachePolicies(List.of(c));
        cache(System.currentTimeMillis() - 1);
        when(messaging.sendSummary(any(), any())).thenAnswer(invocation -> {
            c.setVersion("changed-during-broker-rpc"); c.setTtlSeconds(600);
            return ((java.util.function.Supplier<TransactionTransport.State>) invocation.getArgument(1)).get();
        });
        select(); verify(states, never()).register(any(), any(), anyString());
        assertThat(state.get().pending()).isNull();
    }
    @Test void delayedUsageFromOldPolicyDoesNotRestartTheNewPolicyClock() {
        var c = SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION); config.setCachePolicies(List.of(c));
        long created = System.currentTimeMillis();
        coordinator.observe(state.get().conversationId(), new SummaryContext.CacheObservation(created, false, null));
        String old = state.get().policy(); c.setVersion("updated"); c.setTtlSeconds(120);
        coordinator.observe(state.get().conversationId(), new SummaryContext.CacheObservation(created + 5000, true, null, 0, created + 4000, old));
        coordinator.refreshPolicy(state.get().conversationId());
        assertThat(state.get().cacheCreatedAt()).isEqualTo(created);
        assertThat(state.get().expiresAt()).isEqualTo(created + 120000);
    }
    @Test void responseFromOldPrefixCannotMarkANewlyMergedSummaryAsProviderCached() {
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION)));
        var old = state.get(); var event = SummaryEvent.of(old, old.raw().subList(0, 2));
        state.set(old.merged(event, "新的摘要前缀"));
        coordinator.observe(state.get().conversationId(), new SummaryContext.CacheObservation(System.currentTimeMillis(), true, null, 0));
        assertThat(state.get().expiresAt()).isZero();
    }
    @Test void missingExplicitBudgetsFailAndNoTtlIsRequiredForHardBudgetMode() {
        assertThatCode(() -> config.validate(true, 1000)).doesNotThrowAnyException();
        var unconfigured = new MemorySummaryProperties(); unconfigured.setEnabled(true);
        assertThatThrownBy(() -> unconfigured.validate(true, 1000)).hasMessageContaining("Explicit");
        config.setLowTokens(config.getHighTokens()); assertThatThrownBy(() -> policy.current()).hasMessageContaining("L < H < B");
    }
    @Test void immutableEventRejectsTamperingAndRoundTripsWithoutTemporaryKeys() {
        var event = SummaryEvent.of(state.get(), state.get().raw().subList(0, 3));
        var wire = event.encode(262144);
        assertThat(SummaryEvent.decode(wire, 262144, event.generation())).isEqualTo(event);
        String tampered = new String(wire, java.nio.charset.StandardCharsets.UTF_8).replace("q".repeat(50), "changed");
        assertThatThrownBy(() -> SummaryEvent.decode(tampered.getBytes(java.nio.charset.StandardCharsets.UTF_8), 262144, event.generation()))
                .isInstanceOf(RuntimeException.class);
        assertThat(event.documentId()).isEqualTo(SummaryEvent.of(state.get(), state.get().raw().subList(0, 4)).documentId());
    }
}
