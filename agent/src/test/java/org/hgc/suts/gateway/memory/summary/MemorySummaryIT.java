package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.config.*;
import org.hgc.suts.gateway.manager.ManagedMemoryVectorStore;
import org.hgc.suts.gateway.memory.*;
import org.hgc.suts.gateway.memory.archive.*;
import org.hgc.suts.gateway.memory.transactions.*;
import org.hgc.suts.gateway.service.impl.RedisVectorStorageServiceImpl;
import org.junit.jupiter.api.*;
import org.springframework.ai.chat.messages.*;
import org.springframework.ai.chat.model.*;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.*;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Real private Redis/AOF/Query Engine + MySQL + RocketMQ. Only the model/embedding are deterministic fixtures. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MemorySummaryIT {
    private LettuceConnectionFactory connection;
    private StringRedisTemplate redis;
    private RedisClient nativeRedis;
    private ArchiveDatabaseFixture db;
    private RedisConversationMemoryRepository hot;
    private RedisTransactionLedger ledger;
    private RedisSummaryStateStore states;
    private JdbcSummaryJobs jobs;
    private RocketMqTransactionTransport transport;
    private SummaryCoordinator coordinator;
    private SummaryWorker worker;
    private SummaryScanner scanner;
    private TransactionalConversationMemoryStore store;
    private RedisVectorStorageServiceImpl vectors;
    private final MemoryTransactionProperties tx = TransactionTestConfig.create();
    private final MemorySummaryProperties config = SummaryTestFixture.config();
    private final AiConfig ai = SummaryTestFixture.ai();
    private final SummaryPolicy policy = new SummaryPolicy(() -> config, ai);
    private final List<Prompt> modelInputs = new CopyOnWriteArrayList<>();
    private final AtomicReference<Function<Prompt, ChatResponse>> modelAction = new AtomicReference<>();
    private final AtomicInteger embeddings = new AtomicInteger();
    private final AtomicBoolean consumeSummaries = new AtomicBoolean(true);
    private final AtomicInteger pausedDeliveries = new AtomicInteger();

    @BeforeAll void infrastructure() {
        assertThat(System.getProperty("memory.transactions.fixture")).isEqualTo("agent-transactions-integration-only");
        var source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:13316/agent_archive_it"
                + "?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=2000&socketTimeout=5000", "root", "agent-archive-integration-only");
        assertThat(new JdbcTemplate(source).queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class))
                .isEqualTo("agent-archive-integration-only");
        db = new ArchiveDatabaseFixture(source, true);
        var standalone = new RedisStandaloneConfiguration("127.0.0.1", 16391); standalone.setPassword("agent-transactions-integration-only");
        connection = new LettuceConnectionFactory(standalone); connection.afterPropertiesSet(); connection.start();
        redis = new StringRedisTemplate(connection);
        assertThat(redis.opsForValue().get(RedisTransactionLedger.GENERATION_KEY)).isEqualTo(tx.getGeneration());
        var metrics = new TransactionMetrics(); ledger = new RedisTransactionLedger(redis, tx, metrics);
        hot = new RedisConversationMemoryRepository(redis, 30000, 86400);
        states = spy(new RedisSummaryStateStore(redis, ledger, tx, db.window, policy));
        jobs = new JdbcSummaryJobs(db.archive, tx.getMaxMessageBytes(), tx.getGeneration());
        coordinator = new SummaryCoordinator(states, hot, ledger, db.archive, jobs, tx, policy, new MemoryTokenBudget(), () -> transport);
        nativeRedis = RedisClient.builder().hostAndPort("127.0.0.1", 16391).clientConfig(DefaultJedisClientConfig.builder()
                .password("agent-transactions-integration-only").connectionTimeoutMillis(2000).socketTimeoutMillis(3000).build()).build();
        ai.setEmbeddingHost("https://fixture.invalid/v1"); ai.setEmbeddingModelName("summary/fixture"); ai.getVector().setDimension(3);
        var vectorConfig = new MemoryVectorProperties(); String run = UUID.randomUUID().toString();
        vectorConfig.setTopK(20);
        vectorConfig.setIndexName("test:summary:index:" + run); vectorConfig.setPrefix("test:summary:data:" + run + ":");
        var embedding = new EmbeddingModel() {
            @Override public EmbeddingResponse call(EmbeddingRequest request) {
                embeddings.incrementAndGet(); var results = new ArrayList<Embedding>();
                for (int i = 0; i < request.getInstructions().size(); i++) results.add(new Embedding(new float[]{1, 0, 0}, i));
                return new EmbeddingResponse(results);
            }
            @Override public float[] embed(Document document) { return embed(document.getText()); }
        };
        vectors = spy(new RedisVectorStorageServiceImpl(new ManagedMemoryVectorStore(nativeRedis, embedding, ai, vectorConfig), vectorConfig, ai, nativeRedis));
        var beans = new DefaultListableBeanFactory(); beans.registerSingleton("jobs", jobs); beans.registerSingleton("states", states);
        beans.registerSingleton("policy", policy); vectors.summaryPolicy(beans.getBeanProvider(SummaryPolicy.class));
        vectors.summaryValidation(beans.getBeanProvider(JdbcSummaryJobs.class), beans.getBeanProvider(RedisSummaryStateStore.class));
        ChatModel model = mock(ChatModel.class);
        when(model.getOptions()).thenReturn(org.springframework.ai.deepseek.DeepSeekChatOptions.builder().disableThinking().build());
        when(model.call(any(Prompt.class))).thenAnswer(i -> { Prompt prompt = i.getArgument(0); modelInputs.add(prompt); return modelAction.get().apply(prompt); });
        modelAction.set(p -> response("用户喜欢绿茶，纠正了红茶说法；求助是否接通仍待确认。"));
        worker = new SummaryWorker(jobs, db.archive, states, coordinator, policy, new SummaryModel(model, policy), tx, vectors);
        tx.setTopic("SutsMemorySummaryIT"); tx.setProducerGroup("summary-it-producer-" + run); tx.setConsumerGroup("summary-it-consumer-" + run);
        SummaryMessaging.Handler handler = new SummaryMessaging.Handler() {
            public void validate(byte[] body) { worker.validate(body); }
            public TransactionTransport.State check(byte[] body) { return worker.check(body); }
            public void consume(byte[] body) {
                if (!consumeSummaries.get()) {
                    pausedDeliveries.incrementAndGet(); throw new IllegalStateException("fixture consumer paused before SQL registration");
                }
                worker.consume(body);
            }
        };
        transport = new RocketMqTransactionTransport(tx, ledger, new ArchiveMessageConsumer(db.archive, ledger, tx, metrics), metrics, handler);
        transport.start();
        scanner = new SummaryScanner(states, jobs, worker, coordinator, policy, () -> transport); // Explicit sweeps; no background race between fault tests.
        store = new TransactionalConversationMemoryStore(hot, ledger, db.archive, transport, tx, 86400000, coordinator);
    }
    @BeforeEach void resetFixtures() {
        modelInputs.clear(); config.setCachePolicies(List.of()); config.setAccepting(true); ai.setEmbeddingEnabled(true);
        consumeSummaries.set(true); config.setVectorEnabled(true);
        modelAction.set(p -> response("用户喜欢绿茶，纠正了红茶说法；求助是否接通仍待确认。"));
        clearInvocations(vectors, states);
        doCallRealMethod().when(vectors).saveSummary(any());
        doCallRealMethod().when(states).cas(any(), any(), isNull(), eq(true));
    }
    @AfterAll void close() {
        if (scanner != null) scanner.close(); if (transport != null) transport.close();
        if (states != null) states.close(); if (ledger != null) ledger.close(); if (hot != null) hot.destroy();
        if (nativeRedis != null) nativeRedis.close(); if (connection != null) connection.destroy();
    }
    private static ChatResponse response(String text) { return new ChatResponse(List.of(new Generation(new AssistantMessage(text)))); }
    private String seed(int count, int chars) {
        String id = SummaryTestFixture.id();
        for (int i = 1; i <= count; i++) db.archive.archive(SummaryTestFixture.turn(id, i, "问题" + i + "q".repeat(chars), "回复" + i + "a".repeat(chars)));
        try (var lease = hot.open(id)) {
            ledger.initialize(id, lease, 1, count);
            if (count > 0) lease.restoreIfMissing(db.archive.snapshot(42, id, 1).orElseThrow().messages());
            coordinator.prepare(id, 42, lease, ledger.read(id));
        }
        return id;
    }
    private SummaryEvent intent(String id, int count) {
        try (var lease = hot.open(id)) {
            var state = states.read(id); var event = SummaryEvent.of(state, state.raw().subList(0, count));
            assertThat(states.register(event, state, lease.fencingToken())).isEqualTo(TransactionTransport.State.COMMITTED);
            jobs.register(event); return event;
        }
    }
    private void retry(String jobId) throws Exception {
        await(() -> jobs.read(jobId).orElseThrow().retryAt() <= System.currentTimeMillis()); worker.run(jobId);
    }
    private void append(String id, String question, String answer) {
        try (var session = store.open(id)) {
            assertThat(session.commit(id, question, answer, List.of(new UserMessage(question), new AssistantMessage(answer))))
                    .isEqualTo(MemoryCommitStatus.ACCEPTED);
        }
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (System.nanoTime() < deadline) { if (condition.getAsBoolean()) return; Thread.sleep(25); }
        assertThat(condition.getAsBoolean()).as("private asynchronous fixture converged").isTrue();
    }
    @Test void realMqCommitsSummaryIntentThenConsumerMergesProjectsAndEmbeds() throws Exception {
        String id = seed(12, 150);
        config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION)));
        states.update(id, s -> s.cache(policy.selected().fingerprint(), System.currentTimeMillis() - 61000, 0, System.currentTimeMillis() - 1000));
        try (var session = store.open(id)) {
            assertThat(session.selectContext(new SummaryContext.Request("system", "current", "tools")).history()).hasSize(24);
        }
        await(() -> states.read(id).version() == 1 && states.read(id).pending() == null);
        String jobId = states.read(id).appliedJob();
        await(() -> jobs.read(jobId).map(j -> j.state().equals("DONE")).orElse(false));
        assertThat(jobs.read(jobId).orElseThrow().vectorState()).isEqualTo("WRITTEN");
        assertThat(db.jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_turn WHERE conversation_id = ?", Long.class, id)).isEqualTo(12);
        assertThat(redis.getExpire(RedisConversationMemoryRepository.key(id, "summary"))).isEqualTo(-1);
    }
    @Test void anotherProducerChecksSummaryHalfMessageUsingSharedProofAfterOriginalProducerExits() throws Exception {
        String id = seed(3, 60); var state = states.read(id); var event = SummaryEvent.of(state, state.raw().subList(0, 2));
        var peers = TransactionTestConfig.create(); peers.setTopic(tx.getTopic()); peers.setConsumerEnabled(false);
        peers.setProducerGroup("summary-check-peer-" + UUID.randomUUID()); peers.setConsumerGroup("summary-check-unused-" + UUID.randomUUID());
        var peerMetrics = new TransactionMetrics();
        var listener = new ArchiveMessageConsumer(db.archive, ledger, peers, peerMetrics);
        try (var first = new RocketMqTransactionTransport(peers, ledger, listener, new TransactionMetrics(), worker);
             var second = new RocketMqTransactionTransport(peers, ledger, listener, peerMetrics, worker)) {
            first.start(); second.start();
            try (var lease = hot.open(id)) {
                assertThat(first.sendSummary(event, () -> {
                    assertThat(states.register(event, state, lease.fencingToken())).isEqualTo(TransactionTransport.State.COMMITTED);
                    return TransactionTransport.State.UNKNOWN; // Simulate a lost local transaction acknowledgement.
                })).isEqualTo(TransactionTransport.State.UNKNOWN);
            }
            first.close();
            await(() -> peerMetrics.checks.sum() > 0 && jobs.read(event.jobId()).map(j -> j.state().equals("DONE")).orElse(false));
        }
        assertThat(states.read(id).appliedJob()).isEqualTo(event.jobId());
    }
    @Test void acceptedIntentWithoutSqlJobIsRedrivenAfterConsumerOrProducerRestart() throws Exception {
        String id = seed(3, 60); var state = states.read(id); var event = SummaryEvent.of(state, state.raw().subList(0, 2));
        consumeSummaries.set(false);
        int before = pausedDeliveries.get();
        try {
            try (var lease = hot.open(id)) {
                assertThat(transport.sendSummary(event, () -> states.register(event, state, lease.fencingToken())))
                        .isEqualTo(TransactionTransport.State.COMMITTED);
            }
            await(() -> pausedDeliveries.get() > before);
            assertThat(jobs.read(event.jobId())).isEmpty();
        } finally { consumeSummaries.set(true); }
        for (int i = 0; i < 100 && jobs.read(event.jobId()).isEmpty(); i++) { scanner.sweep(); Thread.sleep(30); }
        await(() -> jobs.read(event.jobId()).map(j -> j.state().equals("DONE")).orElse(false));
        assertThat(states.read(id).version()).isEqualTo(1);
    }
    @Test void newAcceptedTurnDuringModelCallIsNeverTrimmedByOldFixedRange() throws Exception {
        String id = seed(3, 60); var event = intent(id, 2);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        modelAction.set(prompt -> { entered.countDown(); try { assertThat(release.await(10, TimeUnit.SECONDS)).isTrue(); }
            catch (InterruptedException ex) { throw new IllegalStateException(ex); } return response("旧轮次摘要"); });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var future = executor.submit(() -> worker.run(event.jobId())); assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            append(id, "新追加消息必须保留", "新追加回复");
            await(() -> ledger.read(id).archivedThrough() == 4);
            release.countDown(); future.get(15, TimeUnit.SECONDS);
        } finally { release.countDown(); executor.shutdownNow(); }
        var state = states.read(id);
        assertThat(state.head()).isEqualTo(4); assertThat(state.coveredThrough()).isEqualTo(2);
        assertThat(state.raw()).extracting(SummaryState.RawTurn::sequence).containsExactly(3L, 4L);
        assertThat(state.raw().get(1).user()).isEqualTo("新追加消息必须保留");
    }
    @Test void lostLuaReplyResumesDurableResultWithoutAnotherModelCall() throws Exception {
        String id = seed(3, 60); var event = intent(id, 2); AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(i -> {
            Object result = i.callRealMethod();
            if (Boolean.TRUE.equals(i.getArgument(3)) && fail.compareAndSet(true, false)) throw new IllegalStateException("fixture lost Lua reply");
            return result;
        }).when(states).cas(any(), any(), isNull(), eq(true));
        try { assertThatThrownBy(() -> worker.run(event.jobId())).hasMessageContaining("lost Lua reply"); }
        finally { doCallRealMethod().when(states).cas(any(), any(), isNull(), eq(true)); }
        assertThat(states.read(id).version()).isEqualTo(1); assertThat(jobs.projection(42, id, 1)).isEmpty();
        assertThat(jobs.read(event.jobId()).orElseThrow().state()).isEqualTo("RESULT");
        retry(event.jobId()); assertThat(modelInputs).hasSize(1);
        assertThat(jobs.read(event.jobId()).orElseThrow().state()).isEqualTo("DONE");
    }
    @Test void vectorFailureRetriesOnlyVectorStageAndDeterministicDocumentSurvivesDuplicateDelivery() throws Exception {
        String id = seed(3, 60); var event = intent(id, 2);
        doThrow(new IllegalStateException("fixture embedding outage")).doCallRealMethod().when(vectors).saveSummary(any());
        assertThatThrownBy(() -> worker.run(event.jobId())).hasMessageContaining("embedding outage");
        assertThat(jobs.read(event.jobId()).orElseThrow().state()).isEqualTo("MERGED");
        assertThat(states.read(id).pending()).isNull(); retry(event.jobId());
        worker.consume(event.encode(tx.getMaxMessageBytes()));
        assertThat(modelInputs).hasSize(1);
        await(() -> vectors.searchMemory(42L, "绿茶", 3).stream().anyMatch(s -> s.contains("会话摘要参考")));
        assertThat(jobs.isCurrent(42, id, 1, 1)).isTrue();
    }
    @Test void emptyOversizedAndFailedModelOutputsLeaveAllRawTurnsAndRetryableJob() throws Exception {
        for (String output : List.of("", "x".repeat(config.getSummaryMaxChars() + 1))) {
            String id = seed(3, 60); var event = intent(id, 2); modelAction.set(p -> response(output));
            assertThatThrownBy(() -> worker.run(event.jobId())).hasMessageContaining("empty or too long");
            assertThat(states.read(id).raw()).hasSize(3); assertThat(states.read(id).version()).isZero();
            assertThat(jobs.read(event.jobId()).orElseThrow().state()).isEqualTo("READY");
            modelAction.set(p -> response("恢复后的有效摘要")); retry(event.jobId());
        }
    }
    @Test void staleWorkerAndInsufficientArchiveWatermarkCannotTrim() throws Exception {
        String id = seed(3, 60); var event = intent(id, 2);
        var old = jobs.claim(event.jobId(), System.currentTimeMillis(), 30000); states.fenceWorker(event, old.worker(), old.leaseUntil());
        db.jdbc.update("UPDATE agent_memory_job SET lease_until = 0 WHERE job_id = ?", event.jobId());
        var next = jobs.claim(event.jobId(), System.currentTimeMillis(), 30000); states.fenceWorker(event, next.worker(), next.leaseUntil());
        assertThatThrownBy(() -> states.merge(event, old.worker(), "stale result")).hasMessageContaining("worker");
        assertThat(states.read(id).raw()).hasSize(3);
        modelAction.set(p -> response("有效结果")); jobs.retry(next, 0, new IllegalStateException("fixture yield")); worker.run(event.jobId());
        assertThat(states.read(id).version()).isEqualTo(1);
    }
    @Test void unarchivedInputIsNeverSummarizedOrTrimmedAndCanResumeAfterArchiveCatchesUp() throws Exception {
        String id = seed(3, 60);
        var turn = SummaryTestFixture.turn(id, 4, "还未归档的唯一原文", "完整原始回复");
        var archiveEvent = ArchiveEvent.of(tx.getGeneration(), turn);
        try (var lease = hot.open(id)) {
            var mutation = coordinator.append(turn);
            assertThat(ledger.commit(archiveEvent, lease.fencingToken(), 3,
                    RedisConversationMemoryRepository.encode(List.of(new UserMessage(turn.userText()), new AssistantMessage(turn.assistantText())), id),
                    86400000, mutation.expected(), mutation.next())).isEqualTo(TransactionTransport.State.COMMITTED);
        }
        var event = intent(id, 4);
        assertThatThrownBy(() -> worker.run(event.jobId())).hasMessageContaining("completely archived");
        assertThat(modelInputs).isEmpty(); assertThat(states.read(id).raw()).hasSize(4);
        var applied = db.archive.archive(turn); ledger.acknowledge(archiveEvent, applied.archivedThrough());
        retry(event.jobId()); assertThat(states.read(id).coveredThrough()).isEqualTo(4);
    }
    @Test void lostHotStateRecoversFromVerifiedFullArchiveAndDurableResult() throws Exception {
        String id = seed(3, 60); var event = intent(id, 2);
        var job = jobs.claim(event.jobId(), System.currentTimeMillis(), 30000);
        jobs.saveResult(job, "持久结果无需再次生成", 10, 10); jobs.retry(job, 0, new IllegalStateException("fixture restart"));
        redis.delete(RedisConversationMemoryRepository.key(id, "summary"));
        worker.run(event.jobId()); assertThat(modelInputs).isEmpty();
        assertThat(states.read(id).summary()).isEqualTo("持久结果无需再次生成");
        assertThat(states.read(id).raw()).extracting(SummaryState.RawTurn::sequence).containsExactly(3L);
    }
    @Test void staleDeliveryCannotRegenerateOrReactivateOldSummaryAndItsProofStopsRedriving() {
        String id = seed(3, 60); var event = intent(id, 2); worker.run(event.jobId());
        // Recreate a delayed pre-result delivery in the private fixture, after the effective version has moved on.
        db.jdbc.update("UPDATE agent_memory_job SET state = 'READY', result_text = NULL, lease_until = 0 WHERE job_id = ?", event.jobId());
        modelInputs.clear(); worker.consume(event.encode(tx.getMaxMessageBytes()));
        assertThat(jobs.read(event.jobId()).orElseThrow().state()).isEqualTo("STALE");
        assertThat(modelInputs).isEmpty(); assertThat(states.pendingEvents(id)).isEmpty();
        assertThat(states.read(id).version()).isEqualTo(1);
    }
    @Test void missingIndexAndAcceptedIntentAreRecoveredByScannerAfterProducerExit() throws Exception {
        String id = seed(12, 150); config.setCachePolicies(List.of(SummaryTestFixture.ttl(MemorySummaryProperties.Renewal.CREATION)));
        states.update(id, s -> s.cache(policy.selected().fingerprint(), 1, 0, System.currentTimeMillis() - 1));
        redis.delete(RedisSummaryStateStore.IDLE_INDEX);
        // Rebuilding does not depend on one process's queue or on deleting the scheduling entry on claim.
        for (int i = 0; i < 25 && states.read(id).version() == 0; i++) { scanner.sweep(); Thread.sleep(30); }
        await(() -> states.read(id).version() == 1);
        await(() -> states.read(id).pending() == null);
        await(() -> jobs.read(states.read(id).appliedJob()).map(j -> j.state().equals("DONE")).orElse(false));
    }
    @Test void disabledEmbeddingRecordsSkippedWhileShortSummaryStillWorks() {
        String id = seed(3, 60); var event = intent(id, 2); ai.setEmbeddingEnabled(false);
        worker.run(event.jobId()); assertThat(states.read(id).summary()).isNotBlank();
        assertThat(jobs.read(event.jobId()).orElseThrow().vectorState()).isEqualTo("SKIPPED");
        ai.setEmbeddingEnabled(true);
    }
    @Test void vectorRollbackDisablesOnlySummaryCandidatesAndSkipsPendingVectorStage() throws Exception {
        String marker = UUID.randomUUID().toString(); modelAction.set(p -> response("向量回退测试 " + marker));
        String id = seed(3, 60); var event = intent(id, 2); worker.run(event.jobId());
        await(() -> vectors.searchMemory(42L, "query", 20).stream().anyMatch(s -> s.contains(marker)));
        config.setVectorEnabled(false);
        assertThat(vectors.searchMemory(42L, "query", 20)).noneMatch(s -> s.contains(marker));
        String another = seed(3, 60); var skipped = intent(another, 2); worker.run(skipped.jobId());
        assertThat(jobs.read(skipped.jobId()).orElseThrow().vectorState()).isEqualTo("SKIPPED");
        assertThat(states.read(id).summary()).contains(marker); config.setVectorEnabled(true);
    }
    @Test void inheritedSummaryReplacesOldVectorVersionAndCoveredTurnDeduplicatesBySource() throws Exception {
        String marker = UUID.randomUUID().toString();
        modelAction.set(p -> response("用户喜欢绿茶，继承测试初版 " + marker));
        String id = seed(4, 60); var first = intent(id, 2); worker.run(first.jobId());
        vectors.saveTurn(42L, "duplicate source of covered first turn", new SummaryContext.Source(id, 1, 1));
        modelInputs.clear(); modelAction.set(p -> response("用户喜欢绿茶，继承测试新版 " + marker));
        var second = intent(id, 1); worker.run(second.jobId());
        assertThat(jobs.read(second.jobId()).orElseThrow().vectorState()).isEqualTo("WRITTEN");
        assertThat(modelInputs.get(0).getUserMessage().getText()).contains("用户喜欢绿茶", "问题3");
        assertThat(vectors.saveSummary(new org.hgc.suts.gateway.service.VectorStorageService.SummaryDocument(42, id, 1, 1, 1, 2,
                "late stale summary", first.documentId()))).isEqualTo("OBSOLETE");
        await(() -> vectors.searchMemory(42L, "绿茶", 20).stream().anyMatch(s -> s.contains("v2")));
        assertThat(vectors.searchMemory(42L, "绿茶", 20)).noneMatch(s -> s.contains("duplicate source") || s.contains("late stale"));
        assertThat(vectors.searchMemory(43L, "绿茶", 20)).isEmpty();
    }
    @Test void olderWriterAndRetentionRefuseToBypassSummaryState() {
        String id = seed(3, 60); var event = intent(id, 2); worker.run(event.jobId());
        var older = new TransactionalConversationMemoryStore(hot, ledger, db.archive, transport, tx, 86400000);
        assertThatThrownBy(() -> older.open(id)).isInstanceOf(MemoryTransactionsUnavailableException.class);
        assertThatThrownBy(() -> db.archive.retireBefore(42, id, 1, System.currentTimeMillis() + 1)).hasMessageContaining("summary");
    }
}
