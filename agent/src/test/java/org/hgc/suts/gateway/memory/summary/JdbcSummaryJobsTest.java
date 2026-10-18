package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.memory.archive.ArchiveDatabaseFixture;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class JdbcSummaryJobsTest {
    private final ArchiveDatabaseFixture db = ArchiveDatabaseFixture.h2();
    private final SummaryState state = SummaryTestFixture.state(3, 30);
    private final SummaryEvent event = SummaryEvent.of(state, state.raw().subList(0, 2));
    private final JdbcSummaryJobs jobs = new JdbcSummaryJobs(db.archive, 262144, state.generation());
    @Test void stableJobIsIdempotentAndOneBaseVersionCannotHaveDifferentInput() {
        jobs.register(event); jobs.register(event);
        assertThat(db.jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_job", Long.class)).isEqualTo(1);
        assertThatThrownBy(() -> jobs.register(SummaryEvent.of(state, state.raw().subList(0, 1))))
                .hasMessageContaining("conflict");
    }
    @Test void expiredWorkerCannotPersistOrCompleteAfterAnotherWorkerClaimsTheJob() {
        jobs.register(event); long now = System.currentTimeMillis();
        var old = jobs.claim(event.jobId(), now, 1000);
        assertThat(jobs.claim(event.jobId(), now + 1, 1000)).isNull();
        var next = jobs.claim(event.jobId(), now + 1001, 10000);
        assertThat(next.worker()).isEqualTo(old.worker() + 1);
        assertThatThrownBy(() -> jobs.saveResult(old, "stale", 10, 2)).hasMessageContaining("lease expired");
        assertThatThrownBy(() -> jobs.finish(old, "WRITTEN")).hasMessageContaining("Stale");
        jobs.saveResult(next, "valid inherited summary", 10, 2);
        assertThat(jobs.read(event.jobId()).orElseThrow().result()).isEqualTo("valid inherited summary");
    }
    @Test void durableResultSurvivesRetryAndProjectionCanBeRepairedIdempotently() {
        jobs.register(event); var first = jobs.claim(event.jobId(), System.currentTimeMillis(), 30000);
        jobs.saveResult(first, "不是红茶，而是绿茶；求助仍待确认", 100, 20);
        jobs.retry(first, 0, new IllegalStateException("simulated lost Redis reply"));
        var resumed = jobs.claim(event.jobId(), System.currentTimeMillis(), 30000);
        assertThat(resumed.state()).isEqualTo("RESULT");
        jobs.project(resumed, event, resumed.result()); jobs.project(resumed, event, resumed.result());
        assertThat(jobs.projection(42, state.conversationId(), 1).orElseThrow().version()).isEqualTo(1);
        assertThat(jobs.projection(43, state.conversationId(), 1)).isEmpty();
        jobs.finish(resumed, "SKIPPED");
        assertThat(jobs.read(event.jobId()).orElseThrow()).satisfies(job -> {
            assertThat(job.state()).isEqualTo("DONE"); assertThat(job.vectorState()).isEqualTo("SKIPPED");
        });
    }
    @Test void newProjectionInheritsContinuousCoverageAndRejectsOlderOverwrite() {
        jobs.register(event); var first = jobs.claim(event.jobId(), System.currentTimeMillis(), 30000);
        jobs.saveResult(first, "first summary", 1, 1); jobs.project(first, event, "first summary");
        var merged = state.merged(event, "first summary"); var second = SummaryEvent.of(merged, merged.raw());
        jobs.register(second); var next = jobs.claim(second.jobId(), System.currentTimeMillis(), 30000);
        jobs.saveResult(next, "inherited summary with correction", 2, 2); jobs.project(next, second, "inherited summary with correction");
        assertThatThrownBy(() -> jobs.project(first, event, "old overwrite")).hasMessageContaining("newer summary");
        assertThat(jobs.projection(42, state.conversationId(), 1).orElseThrow().through()).isEqualTo(3);
    }
}
