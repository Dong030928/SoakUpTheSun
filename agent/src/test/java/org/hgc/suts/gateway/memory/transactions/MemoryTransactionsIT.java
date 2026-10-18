package org.hgc.suts.gateway.memory.transactions;

import cn.hutool.json.JSONUtil;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.*;
import org.hgc.suts.gateway.memory.archive.*;
import org.junit.jupiter.api.*;
import org.springframework.ai.chat.messages.*;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Fixed, marked Docker fixtures only. Never touches the user's database, Redis, MQ topic or model API. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MemoryTransactionsIT {
    private final MemoryTransactionProperties config = TransactionTestConfig.create();
    private final TransactionMetrics metrics = new TransactionMetrics();
    private LettuceConnectionFactory connection;
    private StringRedisTemplate redis;
    private RedisTransactionLedger ledger;
    private RedisConversationMemoryRepository hot;
    private ArchiveDatabaseFixture db;
    private JdbcConversationArchive target;
    private RocketMqTransactionTransport transport;
    private TransactionalConversationMemoryStore store;
    private final List<Message> pair = List.of(new UserMessage("请记住我喜欢绿茶"), new AssistantMessage("已记住"));

    @BeforeAll void start() throws Exception {
        assertThat(System.getProperty("memory.transactions.fixture")).as("Explicit isolated fixture opt-in is required")
                .isEqualTo("agent-transactions-integration-only");
        var dataSource = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:13316/agent_archive_it"
                + "?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=2000&socketTimeout=5000", "root", "agent-archive-integration-only");
        assertThat(new JdbcTemplate(dataSource).queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class))
                .isEqualTo("agent-archive-integration-only");
        db = new ArchiveDatabaseFixture(dataSource, true);
        target = spy(db.archive);
        var standalone = new RedisStandaloneConfiguration("127.0.0.1", 16391);
        standalone.setPassword("agent-transactions-integration-only");
        connection = new LettuceConnectionFactory(standalone);
        connection.afterPropertiesSet(); connection.start();
        redis = new StringRedisTemplate(connection);
        assertThat(redis.opsForValue().get(RedisTransactionLedger.GENERATION_KEY)).isEqualTo(config.getGeneration());
        ledger = new RedisTransactionLedger(redis, config, metrics);
        hot = new RedisConversationMemoryRepository(redis, 30000, 86400);
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        config.setProducerGroup(config.getProducerGroup() + "-" + suffix);
        config.setConsumerGroup(config.getConsumerGroup() + "-" + suffix);
        var listener = new ArchiveMessageConsumer(target, ledger, config, metrics);
        config.setConsumerEnabled(false);
        transport = new RocketMqTransactionTransport(config, ledger, listener, metrics);
        transport.start();
        // Verify the intended Broker before sending any test message.
        assertThat(transport.brokerNames()).containsExactly("agent-memory-transactions-it");
        transport.startConsumer();
        store = new TransactionalConversationMemoryStore(hot, ledger, target, transport, config, 86400000);
    }
    @AfterAll void stop() {
        if (transport != null) transport.close();
        if (hot != null) hot.destroy();
        if (ledger != null) ledger.close();
        if (connection != null) connection.destroy();
    }
    @BeforeEach void resetSqlFaults() { clearInvocations(target); }
    private String id() { return ConversationMemory.conversationId(42L, "it-" + UUID.randomUUID()); }
    private ArchiveEvent event(String id, String eventId, long seq) {
        return ArchiveEvent.of(config.getGeneration(), new ArchivedTurn(eventId, 42, id, 1, seq,
                "完整原始问题", "完整最终回复", System.currentTimeMillis()));
    }
    private ArchiveEvent event(String id, long seq) { return event(id, UUID.randomUUID().toString(), seq); }
    private String payload(String id) { return RedisConversationMemoryRepository.encode(pair, id); }
    private void initialize(String id, RedisConversationMemoryRepository.Lease lease) { ledger.initialize(id, lease, 1, 0); }

    @Test void realHalfMessageRedisCommitAndMysqlConsumerComplete() {
        String id = id();
        try (var session = store.open(id)) {
            assertThat(session.commit(id, "完整原始问题", "完整最终回复", pair)).isEqualTo(MemoryCommitStatus.ACCEPTED);
        }
        await(() -> db.archive.snapshot(42, id, 1).map(s -> s.matchesCheckpoint(1)).orElse(false), 30000);
        await(() -> ledger.read(id).archivedThrough() == 1, 5000);
        assertThat(db.jdbc.queryForObject("SELECT user_text FROM agent_memory_turn WHERE conversation_id=?", String.class, id))
                .isEqualTo("完整原始问题");
        assertThat(redis.getExpire(RedisConversationMemoryRepository.key(id, "transactions"))).isEqualTo(-1);
    }
    @Test void anotherProducerResolvesUnknownCommitFromSharedEvidence() {
        String id = id();
        ArchiveEvent event = event(id, 1);
        long checksBefore = metrics.checks.sum();
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            assertThat(transport.send(event, () -> {
                assertThat(ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000)).isEqualTo(TransactionTransport.State.COMMITTED);
                return TransactionTransport.State.UNKNOWN; // Simulates lost COMMIT notification.
            })).isEqualTo(TransactionTransport.State.UNKNOWN);
        }
        RedisTransactionLedger anotherNode = new RedisTransactionLedger(redis, config, new TransactionMetrics());
        assertThat(anotherNode.check(event)).isEqualTo(TransactionTransport.State.COMMITTED);
        await(() -> db.archive.hasApplied(event.turn()), 30000);
        assertThat(metrics.checks.sum()).isGreaterThan(checksBefore);
    }
    @Test void staleLeaseRecordsExplicitAbortAndNeverCreatesHotTurn() {
        String id = id(); ArchiveEvent event = event(id, 1);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            assertThat(ledger.commit(event, "expired-holder", 0, payload(id), 86400000)).isEqualTo(TransactionTransport.State.ABORTED);
            assertThat(ledger.check(event)).isEqualTo(TransactionTransport.State.ABORTED);
            assertThat(redis.hasKey(RedisConversationMemoryRepository.key(id, "messages"))).isFalse();
            assertThat(ledger.read(id).head()).isZero();
        }
    }
    @Test void originalProducerExitIsRecoveredByAnotherInstanceInTheSameGroup() {
        String id = id(); ArchiveEvent event = event(id, 1);
        var peers = TransactionTestConfig.create();
        peers.setProducerGroup("agent-memory-peer-it-" + UUID.randomUUID());
        peers.setConsumerEnabled(false);
        var firstMetrics = new TransactionMetrics();
        var secondMetrics = new TransactionMetrics();
        var listener = new ArchiveMessageConsumer(db.archive, ledger, peers, metrics);
        try (var first = new RocketMqTransactionTransport(peers, ledger, listener, firstMetrics);
             var second = new RocketMqTransactionTransport(peers, new RedisTransactionLedger(redis, peers, secondMetrics), listener, secondMetrics)) {
            first.start(); second.start();
            try (var lease = hot.open(id)) {
                initialize(id, lease);
                first.send(event, () -> {
                    ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000);
                    return TransactionTransport.State.UNKNOWN;
                });
            }
            first.close(); // No in-memory transaction map or original callback survives this point.
            await(() -> db.archive.hasApplied(event.turn()), 30000);
            assertThat(secondMetrics.checks.sum()).isGreaterThan(0);
        }
    }
    @Test void archivedEvidencePrunesOnlyAfterRetentionButPendingBodiesNeverExpire() throws Exception {
        String id = id(); ArchiveEvent first = event(id, 1), second = event(id, 2), third = event(id, 3);
        var shortRetention = TransactionTestConfig.create(); shortRetention.setEvidenceRetentionSeconds(1);
        var bounded = new RedisTransactionLedger(redis, shortRetention, metrics);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            bounded.commit(first, lease.fencingToken(), 0, payload(id), 86400000);
            db.archive.archive(first.turn()); bounded.acknowledge(first, 1);
            bounded.commit(second, lease.fencingToken(), 1, payload(id), 86400000);
            Thread.sleep(1100);
            bounded.commit(third, lease.fencingToken(), 2, payload(id), 86400000);
            assertThat(bounded.check(first)).isEqualTo(TransactionTransport.State.UNKNOWN);
            assertThat(bounded.check(second)).isEqualTo(TransactionTransport.State.COMMITTED);
            assertThat(bounded.read(id).events().getJSONObject(second.turn().eventId()).getStr("body")).isNotBlank();
        }
    }
    @Test void missingCorruptAndUnreachableEvidenceStayUnknown() {
        String id = id(); ArchiveEvent event = event(id, 1);
        assertThat(ledger.check(event)).isEqualTo(TransactionTransport.State.UNKNOWN);
        redis.opsForValue().set(RedisConversationMemoryRepository.key(id, "transactions"), "corrupt");
        assertThat(ledger.check(event)).isEqualTo(TransactionTransport.State.UNKNOWN);
        var down = mock(StringRedisTemplate.class);
        when(down.opsForValue()).thenThrow(new IllegalStateException("timeout"));
        assertThat(new RedisTransactionLedger(down, config, metrics).check(event)).isEqualTo(TransactionTransport.State.UNKNOWN);
    }
    @Test void duplicateCommitAndPayloadConflictCannotAppendAgain() {
        String id = id(); ArchiveEvent event = event(id, "stable-event-" + UUID.randomUUID(), 1);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            for (int attempt = 0; attempt < 2; attempt++) {
                assertThat(ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000)).isEqualTo(TransactionTransport.State.COMMITTED);
            }
            ArchiveEvent conflict = ArchiveEvent.of(config.getGeneration(), new ArchivedTurn(event.turn().eventId(), 42, id, 1, 1,
                    "不同内容", "不同回复", event.turn().occurredAt()));
            assertThat(ledger.commit(conflict, lease.fencingToken(), 0, payload(id), 86400000)).isEqualTo(TransactionTransport.State.UNKNOWN);
            assertThat(ledger.read(id).head()).isEqualTo(1);
        }
    }
    @Test void wrongKeyTypeDoesNotPartiallyChangeCommitEvidence() {
        String id = id(); ArchiveEvent event = event(id, 1);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            redis.opsForHash().put(RedisConversationMemoryRepository.key(id, "messages"), "bad", "type");
            assertThat(ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000)).isEqualTo(TransactionTransport.State.UNKNOWN);
            assertThat(ledger.read(id).head()).isZero();
            assertThat(ledger.check(event)).isEqualTo(TransactionTransport.State.UNKNOWN);
        }
    }
    @Test void pendingBodySurvivesHotExpiryAndIsRedrivenWithoutReplayingBusiness() throws Exception {
        String id = id(); ArchiveEvent event = event(id, 1);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            assertThat(ledger.commit(event, lease.fencingToken(), 0, payload(id), 20)).isEqualTo(TransactionTransport.State.COMMITTED);
        }
        Thread.sleep(1100);
        assertThat(redis.hasKey(RedisConversationMemoryRepository.key(id, "messages"))).isFalse();
        assertThat(ledger.read(id).events().getJSONObject(event.turn().eventId()).getStr("body")).isNotBlank();
        try (var reconciler = new ArchiveReconciler(ledger, db.archive, transport, config, metrics)) {
            assertThat(reconciler.reconcile(id, 1)).isEqualTo(1);
        }
        await(() -> db.archive.hasApplied(event.turn()), 30000);
        try (var session = store.open(id)) {
            assertThat(session.repository().findByConversationId(id)).extracting(Message::getText)
                    .containsExactly("完整原始问题", "完整最终回复");
        }
    }
    @Test void mysqlFailureRetriesSameEventAndReceiptRepairUsesCommittedLog() throws Exception {
        String id = id(); ArchiveEvent event = event(id, 1);
        long failuresBefore = metrics.consumeFailures.sum();
        doThrow(new IllegalStateException("database unavailable")).when(target).archive(argThat(t -> t.eventId().equals(event.turn().eventId())));
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            assertThat(transport.send(event, () -> ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000)))
                    .isEqualTo(TransactionTransport.State.COMMITTED);
        }
        await(() -> metrics.consumeFailures.sum() > failuresBefore, 30000);
        assertThat(db.archive.hasApplied(event.turn())).isFalse();
        doCallRealMethod().when(target).archive(argThat(t -> t.eventId().equals(event.turn().eventId())));
        Thread.sleep(1100);
        try (var reconciler = new ArchiveReconciler(ledger, db.archive, transport, config, metrics)) {
            reconciler.reconcile(id, 1);
        }
        await(() -> db.archive.hasApplied(event.turn()), 30000);
        assertThat(db.jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_turn WHERE event_id=?", Long.class, event.turn().eventId())).isEqualTo(1);
    }
    @Test void sqlCommittedButReceiptLostIsRepairedWithoutSendingAgain() throws Exception {
        String id = id(); ArchiveEvent event = event(id, 1);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000);
        }
        db.archive.archive(event.turn()); // Simulated crash immediately after SQL commit.
        Thread.sleep(1100);
        TransactionTransport unsent = mock(TransactionTransport.class);
        try (var reconciler = new ArchiveReconciler(ledger, db.archive, unsent, config, metrics)) {
            reconciler.reconcile(id, 1);
        }
        verifyNoInteractions(unsent);
        assertThat(ledger.read(id).archivedThrough()).isEqualTo(1);
        assertThat(ledger.read(id).events().getJSONObject(event.turn().eventId()).getStr("body")).isNull();
    }
    @Test void backlogCapacityRejectsNewWorkButKeepsTheOnlyFullTurn() {
        String id = id(); ArchiveEvent event = event(id, 1);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000);
        }
        var boundedConfig = TransactionTestConfig.create(); boundedConfig.setMaxPendingPerConversation(1);
        var bounded = new RedisTransactionLedger(redis, boundedConfig, metrics);
        assertThatThrownBy(() -> bounded.admit(bounded.read(id))).isInstanceOf(MemoryTransactionsUnavailableException.class);
        assertThat(bounded.check(event)).isEqualTo(TransactionTransport.State.COMMITTED);
    }
    @Test void archiveCannotRestoreAcrossProducerGapAndMaintenanceCannotDeleteEvidence() {
        String id = id(); ArchiveEvent event = event(id, 1);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000);
        }
        redis.delete(RedisConversationMemoryRepository.key(id, "messages"));
        assertThatThrownBy(() -> store.open(id)).isInstanceOf(MemoryTransactionsUnavailableException.class);
        var readOnly = new MemoryProperties(); readOnly.setWritesEnabled(false);
        var maintenance = new ArchiveMaintenance(db.archive, hot, readOnly);
        assertThatThrownBy(() -> maintenance.retireBefore(42, id, 1, Long.MAX_VALUE, true, false))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Transaction evidence");
    }
    @Test void verifiedOfflineCheckpointAdoptionEnablesExistingM01Conversation() {
        String id = id(); ArchiveEvent event = event(id, 1);
        db.archive.archive(event.turn());
        var readOnly = new MemoryProperties(); readOnly.setWritesEnabled(false);
        var maintenance = new TransactionCheckpointMaintenance(db.archive, hot, ledger, readOnly);
        assertThat(maintenance.adopt(42, id, 1, 1, true, true)).isTrue();
        assertThat(ledger.read(id)).isNull();
        assertThat(maintenance.adopt(42, id, 1, 1, true, false)).isTrue();
        try (var session = store.open(id)) {
            assertThat(session.repository().findByConversationId(id)).hasSize(2);
            assertThat(session.commit(id, "第二轮问题", "第二轮回复", pair)).isEqualTo(MemoryCommitStatus.ACCEPTED);
        }
        await(() -> db.archive.snapshot(42, id, 1).orElseThrow().matchesCheckpoint(2), 30000);
    }
    @Test void outOfOrderDatabaseReceiptsNeverRegressOrOverstateWatermark() {
        String id = id(); ArchiveEvent first = event(id, 1), second = event(id, 2);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            ledger.commit(first, lease.fencingToken(), 0, payload(id), 86400000);
            ledger.commit(second, lease.fencingToken(), 1, payload(id), 86400000);
        }
        long gapThrough = db.archive.archive(second.turn()).archivedThrough();
        assertThat(gapThrough).isZero();
        ledger.acknowledge(second, gapThrough);
        long fullThrough = db.archive.archive(first.turn()).archivedThrough();
        ledger.acknowledge(first, fullThrough);
        ledger.acknowledge(second, gapThrough); // Late receipt cannot replace the newer continuous checkpoint.
        assertThat(ledger.read(id).archivedThrough()).isEqualTo(2);
    }
    @Test void rollbackReleaseRequiresArchiveDrainRetentionAndAnUnchangedCheckpoint() throws Exception {
        String id = id(); ArchiveEvent event = event(id, 1);
        var shortPolicy = TransactionTestConfig.create(); shortPolicy.setEvidenceRetentionSeconds(1); shortPolicy.setRecoveryWindowSeconds(1);
        var shortLedger = new RedisTransactionLedger(redis, shortPolicy, metrics);
        try (var lease = hot.open(id)) {
            initialize(id, lease);
            shortLedger.commit(event, lease.fencingToken(), 0, RedisConversationMemoryRepository.encode(
                    List.of(new UserMessage(event.turn().userText()), new AssistantMessage(event.turn().assistantText())), id), 86400000);
        }
        var frozen = new MemoryProperties(); frozen.setWritesEnabled(false);
        var operations = new TransactionCheckpointMaintenance(db.archive, hot, shortLedger, frozen);
        assertThatThrownBy(() -> operations.releaseToCache(42, id, 1, 1, true, true)).isInstanceOf(RuntimeException.class);
        var result = db.archive.archive(event.turn()); shortLedger.acknowledge(event, result.archivedThrough());
        assertThatThrownBy(() -> operations.releaseToCache(42, id, 1, 1, true, true)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retention window");
        Thread.sleep(1100);
        assertThat(operations.releaseToCache(42, id, 1, 1, true, true)).isTrue();
        assertThat(shortLedger.read(id)).isNotNull();
        assertThat(operations.releaseToCache(42, id, 1, 1, true, false)).isTrue();
        assertThat(shortLedger.read(id)).isNull();
        try (var lease = hot.open(id)) {
            lease.prepare(); // Redis-only writes can resume only after verified release.
            lease.repository().saveAll(id, pair);
        }
        assertThat(db.archive.hasApplied(event.turn())).isTrue();
    }
    @Test void incrementalScanFindsPersistentEvidenceAndGenerationLossFailsClosed() {
        String id = id();
        try (var lease = hot.open(id)) { initialize(id, lease); }
        var otherConfig = TransactionTestConfig.create(); otherConfig.setGeneration("wrong-generation");
        var lost = new RedisTransactionLedger(redis, otherConfig, metrics);
        assertThatThrownBy(lost::verifyDeployment).isInstanceOf(IllegalStateException.class);
        assertThat(lost.check(event(id, 1))).isEqualTo(TransactionTransport.State.UNKNOWN);
        boolean seen = false;
        for (int i = 0; i < 100 && !seen; i++) seen = ledger.nextConversations(5).contains(id);
        assertThat(seen).isTrue();
    }
    @Test void brokerCheckExhaustionKeepsProofAndSameEventCanBeRedriven() throws Exception {
        String id = id(); ArchiveEvent event = event(id, 1);
        var isolated = TransactionTestConfig.create();
        isolated.setProducerGroup("agent-discard-it-" + UUID.randomUUID());
        isolated.setConsumerEnabled(false);
        var unavailableEvidence = mock(RedisTransactionLedger.class);
        when(unavailableEvidence.check(any())).thenReturn(TransactionTransport.State.UNKNOWN);
        var checks = new TransactionMetrics();
        var unused = new ArchiveMessageConsumer(db.archive, ledger, isolated, checks);
        try (var sender = new RocketMqTransactionTransport(isolated, unavailableEvidence, unused, checks)) {
            sender.start();
            try (var lease = hot.open(id)) {
                initialize(id, lease);
                sender.send(event, () -> {
                    ledger.commit(event, lease.fencingToken(), 0, payload(id), 86400000);
                    return TransactionTransport.State.UNKNOWN;
                });
            }
            var discarded = new org.apache.rocketmq.client.consumer.DefaultMQPullConsumer("agent-discard-observer-" + UUID.randomUUID());
            discarded.setNamesrvAddr(config.getNameServer()); discarded.setVipChannelEnabled(false);
            discarded.setInstanceName("agent-discard-" + UUID.randomUUID());
            discarded.start();
            var lastPull = new java.util.concurrent.atomic.AtomicReference<>("No pull attempted");
            try {
                try { await(() -> {
                    try {
                        for (var queue : discarded.fetchSubscribeMessageQueues(org.apache.rocketmq.common.topic.TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC)) {
                            var result = discarded.pull(queue, "*", 0, 128, 2000);
                            lastPull.set(result.toString());
                            if (result.getMsgFoundList() != null && result.getMsgFoundList().stream().anyMatch(message -> {
                                try { return ArchiveEvent.decode(message.getBody(), 262144, config.getGeneration()).equals(event); }
                                catch (RuntimeException ignored) { return false; }
                            })) return true;
                        }
                    } catch (Exception notYetCreated) { lastPull.set(notYetCreated.getClass().getSimpleName() + ": " + notYetCreated.getMessage()); return false; }
                    return false;
                }, 40000); } catch (RuntimeException ex) { throw new IllegalStateException("Discard inspection: " + lastPull.get(), ex); }
            } finally { discarded.shutdown(); }
            assertThat(checks.checks.sum()).isGreaterThanOrEqualTo(5);
            assertThat(db.archive.hasApplied(event.turn())).isFalse();
            assertThat(ledger.check(event)).isEqualTo(TransactionTransport.State.COMMITTED);
        }
        try (var reconciler = new ArchiveReconciler(ledger, db.archive, transport, config, metrics)) { reconciler.reconcile(id, 1); }
        await(() -> db.archive.hasApplied(event.turn()), 30000);
    }
    private static void await(BooleanSupplier condition, long timeoutMs) {
        org.awaitility.Awaitility.await().atMost(Duration.ofMillis(timeoutMs)).pollInterval(Duration.ofMillis(200))
                .until(condition::getAsBoolean);
    }
}
