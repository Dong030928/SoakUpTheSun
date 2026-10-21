package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.rag.ElasticsearchRagIndex;
import org.hgc.suts.gateway.rag.ProfileRagAccess;
import org.hgc.suts.gateway.rag.RagIndexRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.memory.profile.ProfileFixture.tea;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ProfileKeywordWorkerTest {
    private final ProfileFixture fixture = new ProfileFixture();
    private final RagProperties config = new RagProperties();
    private final String target = ProfileKeywordWorker.target(config);
    private final JdbcProfileStore store = new JdbcProfileStore(fixture.db.archive, fixture.config, "fixture", 262144,
            ProfileFixture.GENERATION, Set.of(target));
    private final ElasticsearchRagIndex keyword = mock(ElasticsearchRagIndex.class);

    @Test void atomicallyEnqueuesIndependentTargetsAndRetriesOnlyTheFailedKeywordProjection() {
        var event = fixture.event("我喜欢绿茶"); fixture.ready(event, tea("绿茶", 1, "我喜欢绿茶"));
        store.apply(store.claimExtraction(event.jobId()), List.of(tea("绿茶", 1, "我喜欢绿茶")));
        var fact = store.list(42, false, 20).get(0);
        var vector = store.projectionStatus(42, fact.id()).get(0); var keywordRow = store.projectionStatus(42, fact.id(), target).get(0);
        assertThat(store.claimProjection(keywordRow.id())).isNull();
        store.finishProjection(store.claimProjection(vector.id()), "WRITTEN");
        doThrow(new IllegalStateException("offline")).doNothing().when(keyword).upsert(any());
        try (var worker = new ProfileKeywordWorker(store, keyword, config)) {
            assertThatThrownBy(() -> worker.project(keywordRow.id())).hasMessage("offline");
            assertThat(store.projectionStatus(42, fact.id()).get(0).state()).isEqualTo("WRITTEN");
            fixture.db.jdbc.update("UPDATE agent_profile_projection SET retry_at=0 WHERE projection_id=?", keywordRow.id());
            worker.project(keywordRow.id());
            assertThat(store.projectionStatus(42, fact.id(), target).get(0).state()).isEqualTo("WRITTEN");
            assertThat(store.versions(42, fact.id(), Long.MAX_VALUE, 20)).hasSize(1);
            verify(keyword, times(2)).upsert(any());
        }
        assertThatThrownBy(() -> fixture.db.archive.transactionOperations().executeWithoutResult(status -> {
            store.command(42, fact.id(), 1, "rollback-keyword", "红茶", "我喜欢红茶", false);
            throw new IllegalStateException("rollback");
        })).hasMessage("rollback");
        assertThat(store.fact(42, fact.id()).orElseThrow().version()).isEqualTo(1);
        assertThat(store.projectionStatus(42, fact.id(), target)).hasSize(1);
    }

    @Test void forgetDuringKeywordWriteDeletesTheLateVersionAndFencesItsWorker() {
        fixture.apply(fixture.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        var fact = store.list(42, false, 20).get(0); store.backfillPage(target, 10);
        var row = store.projectionStatus(42, fact.id(), target).get(0);
        doAnswer(call -> {
            store.command(42, fact.id(), 1, "forget-inflight-keyword", null, "忘掉饮品偏好", true); return null;
        }).when(keyword).upsert(any());
        try (var worker = new ProfileKeywordWorker(store, keyword, config)) {
            assertThatThrownBy(() -> worker.project(row.id())).hasMessageContaining("Stale");
            verify(keyword).delete(ProfileRagAccess.candidateId(fact));
            fixture.db.jdbc.update("UPDATE agent_profile_projection SET retry_at=0 WHERE projection_id=?", row.id());
            worker.project(row.id());
            assertThat(store.projectionStatus(42, fact.id(), target).stream().filter(value -> value.version() == 1).findFirst().orElseThrow().state())
                    .isEqualTo("OBSOLETE");
            assertThat(store.list(42, false, 20)).isEmpty();
        }
    }

    @Test void backfillResumesAcrossStoreRestartAndResetWithoutRestoringForgottenHistory() {
        fixture.apply(fixture.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        var forgotten = store.list(42, false, 20).get(0);
        store.command(42, forgotten.id(), 1, "forget-backfill-keyword", null, "忘掉饮品偏好", true);
        fixture.apply(fixture.event(43, null, 1, 0, "我喜欢红茶"), tea("红茶", 1, "我喜欢红茶"));
        assertThat(store.backfillPage(target, 1)).isEqualTo(1);
        String checkpoint = store.backfillStatus(target).lastFactId(); assertThat(checkpoint).isNotEmpty();
        var restarted = new JdbcProfileStore(fixture.db.archive, fixture.config, "fixture", 262144,
                ProfileFixture.GENERATION, Set.of(target));
        assertThat(restarted.backfillStatus(target).lastFactId()).isEqualTo(checkpoint);
        assertThat(restarted.backfillPage(target, 1)).isEqualTo(1);
        assertThat(restarted.backfillPage(target, 1)).isZero(); assertThat(restarted.backfillStatus(target).completed()).isTrue();
        try (var worker = new ProfileKeywordWorker(restarted, keyword, config)) {
            worker.sweep();
            verify(keyword, never()).upsert(argThat(record -> record.ownerId() == 42));
            verify(keyword).upsert(argThat(record -> record.ownerId() == 43 && record.text().contains("红茶")));
            worker.resetBackfill(); assertThat(restarted.backfillStatus(target).lastFactId()).isEmpty();
            worker.sweep();
            verify(keyword, never()).upsert(argThat(record -> record.ownerId() == 42));
        }
    }

    @Test void backfillCheckpointAndProjectionQueueRollBackTogether() {
        fixture.apply(fixture.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        var fact = store.list(42, false, 20).get(0);
        store.backfillStatus(target);
        assertThatThrownBy(() -> fixture.db.archive.transactionOperations().executeWithoutResult(status -> {
            assertThat(store.backfillPage(target, 1)).isEqualTo(1);
            throw new IllegalStateException("backfill rollback");
        })).hasMessage("backfill rollback");
        assertThat(store.backfillStatus(target).lastFactId()).isEmpty();
        assertThat(store.projectionStatus(42, fact.id(), target)).isEmpty();
        assertThat(store.backfillPage(target, 1)).isEqualTo(1);
    }

    @Test void failedLateDeleteReopensCleanupEvenAfterAReplacementWorkerFinishedIt() {
        fixture.apply(fixture.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        var fact = store.list(42, false, 20).get(0); store.backfillPage(target, 10);
        var row = store.projectionStatus(42, fact.id(), target).get(0);
        String candidate = ProfileRagAccess.candidateId(fact);
        doNothing().doThrow(new IllegalStateException("late cleanup unavailable")).doNothing().when(keyword).delete(candidate);
        doAnswer(call -> {
            store.command(42, fact.id(), 1, "forget-after-claim", null, "忘掉饮品偏好", true);
            try (var replacement = new ProfileKeywordWorker(store, keyword, config)) { replacement.project(row.id()); }
            assertThat(store.projectionStatus(42, fact.id(), target).stream().filter(value -> value.version() == 1).findFirst().orElseThrow().state())
                    .isEqualTo("OBSOLETE");
            return null; // The original upsert arrives after the replacement's successful deletion.
        }).when(keyword).upsert(any());
        try (var worker = new ProfileKeywordWorker(store, keyword, config)) {
            assertThatThrownBy(() -> worker.project(row.id())).hasMessage("late cleanup unavailable");
            assertThat(store.projectionStatus(42, fact.id(), target).stream().filter(value -> value.version() == 1).findFirst().orElseThrow().state())
                    .isEqualTo("PENDING");
            fixture.db.jdbc.update("UPDATE agent_profile_projection SET retry_at=0 WHERE projection_id=?", row.id());
            worker.project(row.id());
            assertThat(store.projectionStatus(42, fact.id(), target).stream().filter(value -> value.version() == 1).findFirst().orElseThrow().state())
                    .isEqualTo("OBSOLETE");
            assertThat(store.fact(42, fact.id()).orElseThrow().state()).isEqualTo("DELETED");
            assertThat(store.versions(42, fact.id(), Long.MAX_VALUE, 20)).hasSize(2);
            verify(keyword, times(3)).delete(candidate); verify(keyword, times(1)).upsert(any());
        }
    }
}
