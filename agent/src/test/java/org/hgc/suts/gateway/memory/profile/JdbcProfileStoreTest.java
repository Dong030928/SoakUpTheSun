package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.memory.summary.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.memory.profile.ProfileData.*;
import static org.hgc.suts.gateway.memory.profile.ProfileFixture.*;

class JdbcProfileStoreTest {
    private final ProfileFixture f = new ProfileFixture();

    @Test void capturedSnapshotAndPlainSummaryAreDurableBeforeProfileCanRun() {
        var event = f.event("我喜欢绿茶"); var job = f.result(event, tea("绿茶", 1, "我喜欢绿茶"));
        assertThat(f.profiles.claimExtraction(event.jobId())).isNull();
        assertThat(f.jobs.read(event.jobId()).orElseThrow().result()).isEqualTo("用户画像摘要");
        f.jobs.project(job, event, "用户画像摘要");
        var claimed = f.profiles.claimExtraction(event.jobId()); f.profiles.apply(claimed, List.of(tea("绿茶", 1, "我喜欢绿茶")));
        var fact = f.profiles.list(42, false, 20).get(0);
        assertThat(fact.value()).isEqualTo("绿茶"); assertThat(fact.source().quote()).isEqualTo("我喜欢绿茶");
        assertThat(fact.source().conversationId()).isEqualTo(event.conversationId());
        assertThat(f.profiles.claimExtraction(event.jobId())).isNull();
        assertThat(f.profiles.list(43, true, 20)).isEmpty();
    }
    @Test void summaryAndProfileResultRollBackTogether() {
        var event = f.event("我喜欢绿茶"); f.jobs.register(event);
        var job = f.jobs.claim(event.jobId(), System.currentTimeMillis(), 30000);
        assertThatThrownBy(() -> f.jobs.saveResult(job, new SummaryModel.Result("summary", 1, 1, "[]", false)))
                .hasMessageContaining("frozen input");
        assertThat(f.jobs.read(event.jobId()).orElseThrow().state()).isEqualTo("READY");
    }
    @Test void mergeProjectionAndReadyRegistrationRollBackTogether() {
        var event = f.event("我喜欢绿茶"); var job = f.result(event, tea("绿茶", 1, "我喜欢绿茶"));
        f.db.transactions.executeWithoutResult(s -> { f.jobs.project(job, event, "用户画像摘要"); s.setRollbackOnly(); });
        assertThat(f.jobs.projection(42, event.conversationId(), 1)).isEmpty();
        assertThat(f.profiles.extraction(event.jobId(), false).orElseThrow().state()).isEqualTo("RESULT");
        f.jobs.project(job, event, "用户画像摘要");
        assertThat(f.profiles.extraction(event.jobId(), false).orElseThrow().state()).isEqualTo("READY");
    }
    @Test void rejectsAssistantQuoteMissingSequenceAndEntireInvalidBatchBeforeAnyWrite() {
        var event = f.event("我喜欢绿茶");
        assertThatThrownBy(() -> f.profiles.validate(event, List.of(tea("红茶", 1, "助手未确认的推测")))).hasMessageContaining("user's raw words");
        assertThatThrownBy(() -> f.profiles.validate(event, List.of(tea("绿茶", 2, "我喜欢绿茶")))).isInstanceOf(java.util.NoSuchElementException.class);
        assertThatThrownBy(() -> f.profiles.validate(event, List.of(tea("绿茶", 1, "我喜欢绿茶"), tea("红茶", 1, "我喜欢绿茶"))))
                .hasMessageContaining("one final candidate");
        assertThat(f.profiles.list(42, true, 20)).isEmpty();
    }
    @Test void laterSourceInSameConversationCorrectsWithImmutableVersions() {
        var first = f.event("我喜欢红茶"); f.apply(first, tea("红茶", 1, "我喜欢红茶"));
        var second = f.event(42, first.conversationId(), 2, 1, "纠正一下，我喜欢绿茶");
        f.profiles.capture(second); f.profiles.result(second, json(List.of(tea("绿茶", 2, "我喜欢绿茶"))), false); f.profiles.merged(second);
        f.profiles.apply(f.profiles.claimExtraction(second.jobId()), List.of(tea("绿茶", 2, "我喜欢绿茶")));
        var fact = f.profiles.list(42, false, 20).get(0);
        assertThat(fact.version()).isEqualTo(2); assertThat(fact.value()).isEqualTo("绿茶");
        assertThat(f.profiles.versions(42, fact.id(), Long.MAX_VALUE, 20)).extracting(Fact::value).containsExactly("绿茶", "红茶");
    }
    @Test void crossConversationDisagreementBecomesConflictThenExplicitCorrectionResolvesIt() {
        f.apply(f.event("我喜欢红茶"), tea("红茶", 1, "我喜欢红茶"));
        f.apply(f.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        assertThat(f.profiles.list(42, false, 20)).isEmpty();
        var conflict = f.profiles.list(42, true, 20).get(0); assertThat(conflict.state()).isEqualTo("CONFLICT");
        var resolved = f.profiles.command(42, conflict.id(), conflict.version(), "resolve-request-1", "绿茶", "确认是绿茶", false);
        assertThat(resolved.state()).isEqualTo("ACTIVE"); assertThat(resolved.version()).isEqualTo(3);
    }
    @Test void forgetFencesInFlightJobsAndOldSourcesCannotReviveDeletedFact() {
        f.apply(f.event("我喜欢红茶"), tea("红茶", 1, "我喜欢红茶"));
        var inflight = f.event("我喜欢绿茶"); f.ready(inflight, tea("绿茶", 1, "我喜欢绿茶"));
        Fact old = f.profiles.list(42, false, 20).get(0);
        Fact forgotten = f.profiles.command(42, old.id(), old.version(), "forget-request-1", null, "忘掉我的饮品偏好", true);
        f.profiles.apply(f.profiles.claimExtraction(inflight.jobId()), List.of(tea("绿茶", 1, "我喜欢绿茶")));
        assertThat(f.profiles.extraction(inflight.jobId(), false).orElseThrow().state()).isEqualTo("STALE");
        f.apply(f.event("帮我记住我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        assertThat(f.profiles.fact(42, old.id()).orElseThrow()).isEqualTo(forgotten);
        var restored = f.profiles.command(42, old.id(), forgotten.version(), "restore-request-1", "绿茶", "重新记住绿茶", false);
        assertThat(restored.state()).isEqualTo("ACTIVE");
    }
    @Test void commandsAreOwnedIdempotentAndOptimisticallyVersioned() {
        f.apply(f.event("我喜欢红茶"), tea("红茶", 1, "我喜欢红茶")); Fact fact = f.profiles.list(42, false, 20).get(0);
        var result = f.profiles.command(42, fact.id(), 1, "correction-123", "绿茶", "我喜欢绿茶", false);
        assertThat(f.profiles.command(42, fact.id(), 1, "correction-123", "绿茶", "我喜欢绿茶", false)).isEqualTo(result);
        assertThatThrownBy(() -> f.profiles.command(42, fact.id(), 1, "correction-123", "白茶", "我喜欢白茶", false))
                .hasMessageContaining("reused");
        assertThatThrownBy(() -> f.profiles.command(42, fact.id(), 1, "another-request", "白茶", "我喜欢白茶", false))
                .isInstanceOf(ProfileConflictException.class);
        assertThatThrownBy(() -> f.profiles.command(43, fact.id(), 1, "another-request", "白茶", "我喜欢白茶", false))
                .hasMessageContaining("not found");
        assertThat(f.profiles.versions(43, fact.id(), Long.MAX_VALUE, 20)).isEmpty();
    }
    @Test void delayedExtractionCannotDowngradeAnExplicitHumanCorrection() {
        f.apply(f.event("我喜欢红茶"), tea("红茶", 1, "我喜欢红茶"));
        var delayed = f.event("我喜欢白茶"); f.ready(delayed, tea("白茶", 1, "我喜欢白茶"));
        var fact = f.profiles.list(42, false, 20).get(0);
        var corrected = f.profiles.command(42, fact.id(), fact.version(), "explicit-wins-123", "绿茶", "我确认喜欢绿茶", false);
        f.profiles.apply(f.profiles.claimExtraction(delayed.jobId()), List.of(tea("白茶", 1, "我喜欢白茶")));
        assertThat(f.profiles.fact(42, fact.id()).orElseThrow()).isEqualTo(corrected);
    }
    @Test void forgettingBeforeFirstExtractionCreatesATombstoneAndFencesEarlierWork() {
        var delayed = f.event("我喜欢红茶"); f.ready(delayed, tea("红茶", 1, "我喜欢红茶"));
        var forget = f.event("忘掉我的饮品偏好");
        f.apply(forget, new Candidate("preference.drink", "饮品偏好", "FORGET", 1, "忘掉我的饮品偏好"));
        f.profiles.apply(f.profiles.claimExtraction(delayed.jobId()), List.of(tea("红茶", 1, "我喜欢红茶")));
        assertThat(f.profiles.list(42, false, 20)).isEmpty();
        assertThat(f.profiles.list(42, true, 20).get(0).state()).isEqualTo("DELETED");
        assertThat(f.profiles.extraction(delayed.jobId(), false).orElseThrow().state()).isEqualTo("STALE");
    }
    @Test void oldSummaryJobsRemainPlainAndNewStructuredJobsRequireProfileSupport() {
        var event = f.event("我喜欢绿茶"); f.result(event, tea("绿茶", 1, "我喜欢绿茶"));
        var legacy = new JdbcSummaryJobs(f.db.archive, 262144, GENERATION);
        assertThatThrownBy(() -> legacy.requireProfileCompatibility(f.jobs.read(event.jobId()).orElseThrow()))
                .hasMessageContaining("Enable profile");
        var plain = f.event("普通旧任务"); legacy.register(plain);
        var job = legacy.claim(plain.jobId(), System.currentTimeMillis(), 30000); legacy.requireProfileCompatibility(job);
        legacy.saveResult(job, "旧格式摘要", 10, 2); legacy.project(job, plain, "旧格式摘要");
        assertThat(f.profiles.extraction(plain.jobId(), false)).isEmpty();
    }
    @Test void expiredExtractionLeaseCannotApplyAfterAnotherWorkerTakesOver() {
        var event = f.event("我喜欢绿茶"); f.ready(event, tea("绿茶", 1, "我喜欢绿茶"));
        var old = f.profiles.claimExtraction(event.jobId());
        f.db.jdbc.update("UPDATE agent_profile_extraction SET lease_until=0 WHERE summary_job_id=?", event.jobId());
        var next = f.profiles.claimExtraction(event.jobId());
        assertThatThrownBy(() -> f.profiles.apply(old, List.of(tea("绿茶", 1, "我喜欢绿茶")))).hasMessageContaining("lost its lease");
        f.profiles.apply(next, List.of(tea("绿茶", 1, "我喜欢绿茶")));
        assertThat(f.profiles.list(42, false, 20)).hasSize(1);
    }
    @Test void onlyOneConcurrentCorrectionCanUseTheSameExpectedVersion() throws Exception {
        f.apply(f.event("我喜欢红茶"), tea("红茶", 1, "我喜欢红茶")); Fact fact = f.profiles.list(42, false, 20).get(0);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var gate = new CountDownLatch(1);
            var a = pool.submit(() -> correctAfterGate(gate, fact.id(), "绿茶", "parallel-request-a"));
            var b = pool.submit(() -> correctAfterGate(gate, fact.id(), "白茶", "parallel-request-b"));
            gate.countDown(); assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder("ACTIVE", "CONFLICT");
            assertThat(f.profiles.fact(42, fact.id()).orElseThrow().version()).isEqualTo(2);
        } finally { pool.shutdownNow(); }
    }
    private String correctAfterGate(CountDownLatch gate, String id, String value, String request) throws Exception {
        gate.await();
        try { return f.profiles.command(42, id, 1, request, value, "我喜欢" + value, false).state(); }
        catch (ProfileConflictException ex) { return "CONFLICT"; }
    }
}
