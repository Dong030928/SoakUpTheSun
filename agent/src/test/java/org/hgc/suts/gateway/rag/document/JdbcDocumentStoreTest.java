package org.hgc.suts.gateway.rag.document;

import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.rag.RagData.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.rag.document.DocumentEtl.hash;

class JdbcDocumentStoreTest {
    final RagProperties config = new RagProperties();
    final String target = hash("embedding:model-a|1024|redis-a|es-a");
    final String otherTarget = hash("embedding:model-b|1024|redis-b|es-b");
    final DriverManagerDataSource source = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
            + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000", "sa", "");
    final JdbcTemplate jdbc = new JdbcTemplate(source);
    final TransactionTemplate transactions = new TransactionTemplate(new DataSourceTransactionManager(source));
    final JdbcDocumentStore store;
    JdbcDocumentStoreTest() {
        Flyway.configure().dataSource(source).table("agent_rag_schema_history").locations("classpath:db/rag")
                .placeholders(Map.of("tableOptions", "")).load().migrate();
        store = new JdbcDocumentStore(jdbc, transactions, config);
        store.registerTarget(target, "redis:a:", "http://127.0.0.1:9200", "shared-index");
        store.registerTarget(otherTarget, "redis:b:", "http://127.0.0.1:9200", "shared-index");
    }
    private DocumentEtl.ParsedDocument parsed(String text) { return new DocumentEtl(config).parse(text.getBytes(StandardCharsets.UTF_8), "guide.md"); }
    private JdbcDocumentStore.ImportResult submit(String text, Visibility visibility) {
        return store.importDocument("guide", 42, visibility, "办事指南", "local:guide.md", parsed(text), target);
    }
    private JdbcDocumentStore.Job claim(JdbcDocumentStore.ImportResult imported) { return store.claim(imported.jobId(), target); }
    private List<String> ids(JdbcDocumentStore.Job job) { return store.chunks(job).stream().map(JdbcDocumentStore.ProjectionChunk::id).toList(); }
    private void both(JdbcDocumentStore.Job job) { store.complete(job, "REDIS"); store.complete(job, "ES"); }

    @Test void exactRetryHasOneVersionStableChunksAndTwoDurableReceiptsGatePublication() {
        var first = submit("# 申请\n1. 身份证\n2. 去窗口", Visibility.PUBLIC);
        var again = submit("# 申请\n1. 身份证\n2. 去窗口", Visibility.PUBLIC);
        assertThat(again.duplicate()).isTrue(); assertThat(again.version()).isEqualTo(1); assertThat(again.jobId()).isEqualTo(first.jobId());
        var job = claim(first); var ids = ids(job);
        assertThat(store.hydrate(43, ids, List.of())).isEmpty();
        store.complete(job, "REDIS"); assertThat(store.status(42, "guide").currentVersion()).isZero();
        assertThat(store.hydrate(43, ids, List.of(), target)).isEmpty();
        store.complete(job, "ES");
        assertThat(store.hydrate(43, ids, List.of(), target)).hasSize(ids.size());
        assertThat(store.status(42, "guide").currentVersion()).isEqualTo(1);
        assertThat(store.cleanup(job)).isEmpty(); store.completeCleanup(job);
        assertThat(store.pending(target)).isEmpty();
        assertThat(store.status(42, "guide").jobs()).singleElement().satisfies(s -> assertThat(s.state()).isEqualTo("DONE"));
    }

    @Test void newlineBoundariesInTitleAndSourceCannotAliasTheImportIdentity() {
        var text = parsed("同一份申请原文");
        var first = store.importDocument("guide", 42, Visibility.PUBLIC, "a\nb", "c", text, target);
        var changed = store.importDocument("guide", 42, Visibility.PUBLIC, "a", "b\nc", text, target);
        assertThat(first.version()).isEqualTo(1); assertThat(changed.version()).isEqualTo(2); assertThat(changed.duplicate()).isFalse();
        var retry = store.importDocument("guide", 42, Visibility.PUBLIC, "a", "b\nc", text, target);
        assertThat(retry.version()).isEqualTo(2); assertThat(retry.duplicate()).isTrue(); assertThat(retry.jobId()).isEqualTo(changed.jobId());
        var job = claim(changed); var children = ids(job); both(job);
        assertThat(store.hydrate(43, children, List.of("guide"))).singleElement().satisfies(candidate -> {
            assertThat(candidate.title()).isEqualTo("a"); assertThat(candidate.source()).isEqualTo("b\nc");
        });
    }

    @Test void canonicalHydrationAndParentExpansionEnforceAclMembershipAndVersion() {
        var imported = submit("# 申请\n1. 身份证\n2. 去窗口", Visibility.OWNER); var job = claim(imported); var ids = ids(job); both(job);
        assertThat(store.hydrate(43, ids, List.of())).isEmpty(); assertThat(store.hydrate(42, ids, List.of("another-document"))).isEmpty();
        var canonical = store.hydrate(42, ids, List.of("guide")).get(0);
        var fabricated = new Candidate(canonical.id(), Kind.KNOWLEDGE_CHILD, "guide", canonical.version(), canonical.parentId(), "fake title", "fake body", "fake source", 0, 0, 0);
        var expanded = store.expand(42, fabricated);
        assertThat(expanded.text()).contains("1. 身份证", "2. 去窗口"); assertThat(expanded.title()).contains("申请").doesNotContain("fake");
        assertThat(store.expand(43, canonical)).isNull();
        assertThat(store.expand(42, new Candidate(canonical.id(), canonical.kind(), "guide", 99, canonical.parentId(), "", "", "", 0, 0, 0))).isNull();
        assertThat(store.expand(42, new Candidate(canonical.id(), canonical.kind(), "guide", 1, "another-parent", "", "", "", 0, 0, 0))).isNull();
        assertThatThrownBy(() -> store.importDocument("guide", 43, Visibility.PUBLIC, "hijack", "x", parsed("text"), target)).hasMessageContaining("another owner");
        assertThatThrownBy(() -> store.delete(43, "guide")).hasMessageContaining("another owner");
    }

    @Test void successfulEngineAndEmbeddingSurviveFailureAndLeaseRecoveryRejectsOldWorker() {
        var imported = submit("1. 材料\n2. 窗口", Visibility.PUBLIC); var old = claim(imported); String child = ids(old).get(0);
        store.cacheEmbedding(old, child, new float[]{1, 2, 3}); store.complete(old, "REDIS");
        store.retry(old, new IllegalStateException("fixture failure"));
        assertThat(store.pending(target)).isEmpty();
        jdbc.update("UPDATE agent_rag_job SET retry_at=0 WHERE job_id=?", old.id());
        var replacement = claim(imported); assertThat(replacement.workerEpoch()).isGreaterThan(old.workerEpoch());
        assertThat(replacement.redisDone()).isTrue(); assertThat(replacement.esDone()).isFalse();
        assertThat(store.embedding(child, target)).containsExactly(1, 2, 3);
        assertThatThrownBy(() -> store.complete(old, "ES")).hasMessageContaining("lost its lease");
        assertThatThrownBy(() -> store.cacheEmbedding(old, child, new float[]{1, 2, 3})).hasMessageContaining("lost its lease");
        assertThatThrownBy(() -> store.cacheEmbedding(replacement, child, new float[]{4, 5, 6})).hasMessageContaining("Conflicting cached embedding");
        store.complete(replacement, "ES"); assertThat(store.status(42, "guide").currentVersion()).isEqualTo(1);
        store.completeCleanup(replacement);
    }

    @Test void expiredLeaseCanBeReclaimedAndCannotBeRenewedByItsPreviousWorker() {
        var imported = submit("办理资料", Visibility.PUBLIC); var old = claim(imported);
        jdbc.update("UPDATE agent_rag_job SET lease_until=0 WHERE job_id=?", old.id());
        assertThatThrownBy(() -> store.renew(old)).hasMessageContaining("lost its lease");
        var recovered = claim(imported); assertThat(store.renew(recovered).workerEpoch()).isEqualTo(recovered.workerEpoch());
        assertThatThrownBy(() -> store.complete(old, "REDIS")).hasMessageContaining("lost its lease");
    }

    @Test void replacementKeepsOldPublishedVersionUntilBothIndexesThenSchedulesOldProjectionCleanup() {
        var first = submit("旧版申请流程", Visibility.PUBLIC); var one = claim(first); var oldIds = ids(one); both(one); store.completeCleanup(one);
        var second = submit("新版申请流程", Visibility.PUBLIC); var two = claim(second); var newIds = ids(two);
        store.complete(two, "ES");
        assertThat(store.hydrate(43, oldIds, List.of())).hasSize(oldIds.size()); assertThat(store.hydrate(43, newIds, List.of())).isEmpty();
        store.complete(two, "REDIS");
        assertThat(store.hydrate(43, oldIds, List.of())).isEmpty(); assertThat(store.hydrate(43, newIds, List.of())).hasSize(newIds.size());
        assertThat(store.cleanup(two)).extracting(JdbcDocumentStore.Cleanup::childId).containsExactlyElementsOf(oldIds);
        store.retry(two, new IllegalStateException()); jdbc.update("UPDATE agent_rag_job SET retry_at=0 WHERE job_id=?", two.id());
        var cleanup = claim(second); assertThat(cleanup.state()).isEqualTo("CLEANUP");
        assertThat(store.cleanup(cleanup)).hasSize(oldIds.size()); store.completeCleanup(cleanup);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_version", Long.class)).isEqualTo(2);
    }

    @Test void supersededImportCannotPublishAndItsProjectionIsDurableCleanupWork() {
        var first = submit("最初版本", Visibility.PUBLIC); var stale = claim(first); var firstIds = ids(stale);
        var second = submit("最新版本", Visibility.PUBLIC);
        both(stale); assertThat(store.status(42, "guide").currentVersion()).isZero();
        assertThat(store.cleanup(stale)).extracting(JdbcDocumentStore.Cleanup::childId).containsExactlyElementsOf(firstIds);
        var current = claim(second); both(current); assertThat(store.status(42, "guide").currentVersion()).isEqualTo(2);
        assertThatThrownBy(() -> store.completeCleanup(stale)).hasMessageContaining("lost its lease");
        assertThat(store.cleanup(current)).extracting(JdbcDocumentStore.Cleanup::childId).containsAll(firstIds);
    }

    @Test void reindexPreservesImmutableVersionAndCachesAreSeparatedByTarget() {
        var imported = submit("申请材料", Visibility.PUBLIC); var original = claim(imported); var originalIds = ids(original); both(original); store.completeCleanup(original);
        var reindex = store.reindex(42, "guide", otherTarget);
        assertThat(reindex.version()).isEqualTo(1);
        var job = store.claim(reindex.jobId(), otherTarget); assertThat(ids(job)).isEqualTo(originalIds);
        assertThat(store.hydrate(43, originalIds, List.of(), otherTarget)).isEmpty();
        assertThat(store.hydrate(43, originalIds, List.of(), target)).hasSize(originalIds.size());
        store.cacheEmbedding(job, originalIds.get(0), new float[]{4, 5});
        assertThat(store.embedding(originalIds.get(0), target)).isNull(); both(job);
        assertThat(store.hydrate(43, originalIds, List.of(), otherTarget)).hasSize(originalIds.size());
        assertThat(store.cleanup(job)).allSatisfy(cleanup -> assertThat(cleanup.targetTag()).isEqualTo(target));
    }

    @Test void returningToFormerTargetWaitsForObsoleteDeletionBeforeRecreatingItsChildren() {
        var imported = submit("申请材料", Visibility.PUBLIC); var original = claim(imported); both(original); store.completeCleanup(original);
        var b = store.reindex(42, "guide", otherTarget); var second = store.claim(b.jobId(), otherTarget); both(second);
        // A stale in-flight cleanup must finish before target A can be reused.
        assertThatThrownBy(() -> store.reindex(42, "guide", target)).hasMessageContaining("Finish pending cleanup");
        store.completeCleanup(second);
        var a = store.reindex(42, "guide", target); var returned = store.claim(a.jobId(), target); both(returned);
        assertThat(store.cleanup(returned)).allSatisfy(cleanup -> assertThat(cleanup.targetTag()).isNotEqualTo(target));
        assertThat(store.status(42, "guide").currentTarget()).isEqualTo(target);
    }

    @Test void explicitSameTargetReindexRebuildsLostIndexesAndRetainsPublishedSqlVersionUntilReady() {
        var imported = submit("申请材料", Visibility.PUBLIC); var first = claim(imported); var childIds = ids(first); both(first); store.completeCleanup(first);
        var repaired = store.reindex(42, "guide", target);
        assertThat(repaired.version()).isEqualTo(1); assertThat(repaired.jobId()).isEqualTo(imported.jobId());
        var work = claim(repaired); assertThat(work.redisDone()).isFalse(); assertThat(work.esDone()).isFalse();
        assertThat(work.workerEpoch()).isGreaterThan(first.workerEpoch());
        assertThat(store.hydrate(43, childIds, List.of(), target)).hasSize(childIds.size());
        store.complete(work, "REDIS"); assertThat(store.status(42, "guide").currentVersion()).isEqualTo(1);
        store.complete(work, "ES"); assertThat(store.cleanup(work)).isEmpty(); store.completeCleanup(work);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_version", Long.class)).isEqualTo(1);
        assertThatThrownBy(() -> store.complete(first, "ES")).hasMessageContaining("lost its lease");
    }

    @Test void immutableDestinationsAndSharedEsIdsProtectThePublishedProjectionDuringCleanup() {
        store.registerTarget(target, "redis:a:", "http://127.0.0.1:9200/", "shared-index");
        assertThatThrownBy(() -> store.registerTarget(target, "another:", "http://127.0.0.1:9200", "shared-index")).hasMessageContaining("already bound");
        var imported = submit("申请材料", Visibility.PUBLIC); var first = claim(imported); both(first); store.completeCleanup(first);
        var reindexed = store.reindex(42, "guide", otherTarget); var second = store.claim(reindexed.jobId(), otherTarget); both(second);
        assertThat(store.cleanup(second)).isNotEmpty().allSatisfy(cleanup -> {
            assertThat(store.getTarget(cleanup.targetTag()).redisPrefix()).isEqualTo("redis:a:");
            assertThat(store.safeToDeleteKeyword(cleanup)).isFalse(); assertThat(store.safeToDeleteVector(cleanup)).isTrue();
        });
        var oldCleanup = store.cleanup(second).get(0);
        store.delete(42, "guide");
        assertThat(store.safeToDeleteKeyword(oldCleanup)).isTrue();
        assertThat(store.safeToDeleteVector(oldCleanup)).isTrue();
    }

    @Test void deleteRevokesVisibilityImmediatelyAndFencesInflightWorkThenAllowsExplicitNewImport() {
        var imported = submit("申请材料", Visibility.PUBLIC); var job = claim(imported); var ids = ids(job); both(job);
        store.delete(42, "guide");
        assertThat(store.hydrate(42, ids, List.of())).isEmpty(); assertThat(store.status(42, "guide").deleted()).isTrue();
        assertThatThrownBy(() -> store.complete(job, "REDIS")).hasMessageContaining("lost its lease");
        var cleanup = store.claim(imported.jobId(), target); assertThat(cleanup.state()).isEqualTo("CLEANUP");
        assertThat(store.cleanup(cleanup)).extracting(JdbcDocumentStore.Cleanup::childId).containsExactlyElementsOf(ids);
        store.completeCleanup(cleanup); store.delete(42, "guide");
        assertThatThrownBy(() -> store.reindex(42, "guide", otherTarget)).hasMessageContaining("deleted");
        var restored = submit("申请材料", Visibility.OWNER); assertThat(restored.version()).isEqualTo(2);
        assertThat(store.status(42, "guide").currentVersion()).isZero();
    }

    @Test void twoConcurrentIdenticalImportsSerializeToOneImmutableVersion() throws Exception {
        var workers = Executors.newFixedThreadPool(2);
        try {
            var start = new CountDownLatch(1);
            Callable<JdbcDocumentStore.ImportResult> importTask = () -> { start.await(); return submit("并发申请材料", Visibility.PUBLIC); };
            Future<JdbcDocumentStore.ImportResult> first = workers.submit(importTask); Future<JdbcDocumentStore.ImportResult> second = workers.submit(importTask); start.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS).jobId()).isEqualTo(second.get(10, TimeUnit.SECONDS).jobId());
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_version", Long.class)).isEqualTo(1);
        } finally { workers.shutdownNow(); }
    }

    @Test void validationAndOuterTransactionRollbackLeaveNoHalfRegisteredContent() {
        assertThatThrownBy(() -> store.importDocument("guide", 42, Visibility.PUBLIC, "", "source", parsed("内容"), target)).hasMessageContaining("title");
        transactions.executeWithoutResult(tx -> { submit("申请资料", Visibility.PUBLIC); tx.setRollbackOnly(); });
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_document", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_child", Long.class)).isZero();
    }

    @Test void orphanCleanupSurvivesOriginalJobCompletionAndStaleAcknowledgementCannotLoseANewerLateWrite() {
        var imported = submit("申请材料", Visibility.PUBLIC); var original = claim(imported); String childId = ids(original).get(0);
        both(original); store.completeCleanup(original); store.delete(42, "guide");
        var deleted = claim(imported); store.completeCleanup(deleted);
        store.recordOrphan(childId, target); var first = store.pendingOrphans().get(0);
        assertThat(store.orphanInFlight(first)).isFalse();
        var cleanup = new JdbcDocumentStore.Cleanup(first.childId(), first.targetTag());
        assertThat(store.safeToDeleteKeyword(cleanup)).isTrue(); assertThat(store.safeToDeleteVector(cleanup)).isTrue();
        store.recordOrphan(childId, target); store.completeOrphan(first);
        var newer = store.pendingOrphans().get(0); assertThat(newer.revision()).isGreaterThan(first.revision());
        store.retryOrphan(first, new IllegalStateException()); assertThat(store.pendingOrphans()).containsExactly(newer);
        store.retryOrphan(newer, new IllegalStateException()); assertThat(store.pendingOrphans()).isEmpty();
        jdbc.update("UPDATE agent_rag_orphan SET retry_at=0");
        assertThat(store.pendingOrphans()).containsExactly(newer); store.completeOrphan(newer); assertThat(store.pendingOrphans()).isEmpty();
        store.recordOrphan(childId, target); var afterAcknowledgement = store.pendingOrphans().get(0);
        assertThat(afterAcknowledgement.revision()).isGreaterThan(newer.revision());
        store.completeOrphan(newer); assertThat(store.pendingOrphans()).containsExactly(afterAcknowledgement);
    }

    @Test void orphanCleanupProtectsTakeoverWritesUntilTheyPublishAndCleanupCanReadPublishedChildren() {
        var imported = submit("申请材料", Visibility.PUBLIC); var abandoned = claim(imported); String childId = ids(abandoned).get(0);
        jdbc.update("UPDATE agent_rag_job SET lease_until=0 WHERE job_id=?", abandoned.id()); var replacement = claim(imported);
        store.recordOrphan(childId, target); var orphan = store.pendingOrphans().get(0);
        assertThat(store.orphanInFlight(orphan)).isTrue();
        var cleanup = new JdbcDocumentStore.Cleanup(orphan.childId(), orphan.targetTag());
        assertThat(store.safeToDeleteKeyword(cleanup)).isFalse(); assertThat(store.safeToDeleteVector(cleanup)).isFalse();
        assertThat(store.publishedChunk(childId)).isEmpty();
        assertThat(store.isCurrent(replacement)).isFalse(); both(replacement);
        assertThat(store.isCurrent(replacement)).isTrue(); assertThat(store.orphanInFlight(orphan)).isFalse();
        assertThat(store.chunks(replacement)).extracting(JdbcDocumentStore.ProjectionChunk::id).contains(childId);
        assertThat(store.publishedChunk(childId)).hasValueSatisfying(chunk -> {
            assertThat(chunk.ownerId()).isEqualTo(42); assertThat(chunk.text()).isEqualTo("申请材料"); assertThat(chunk.visibility()).isEqualTo(Visibility.PUBLIC);
        });
        assertThat(store.safeToDeleteKeyword(cleanup)).isFalse(); assertThat(store.safeToDeleteVector(cleanup)).isFalse();
        // The worker repairs searchable=true for protected current IDs instead of deleting them.
        store.completeOrphan(orphan); store.completeCleanup(replacement); assertThat(store.pendingOrphans()).isEmpty();
        store.delete(42, "guide"); assertThat(store.publishedChunk(childId)).isEmpty();
    }
}
