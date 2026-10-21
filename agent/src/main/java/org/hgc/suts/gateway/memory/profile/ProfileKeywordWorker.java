package org.hgc.suts.gateway.memory.profile;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.memory.summary.SummaryPolicy;
import org.hgc.suts.gateway.rag.ElasticsearchRagIndex;
import org.hgc.suts.gateway.rag.ProfileRagAccess;
import org.hgc.suts.gateway.rag.RagData;
import org.hgc.suts.gateway.rag.RagIndexRecord;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Independent durable keyword projection; backfill never replays historical extraction. */
@Slf4j
public final class ProfileKeywordWorker implements AutoCloseable {
    private final JdbcProfileStore store;
    private final ElasticsearchRagIndex keyword;
    private final RagProperties config;
    private final String target;
    private final ScheduledExecutorService scanner = Executors.newSingleThreadScheduledExecutor(run -> {
        var thread = new Thread(run, "agent-profile-keyword"); thread.setDaemon(true); return thread;
    });

    public ProfileKeywordWorker(JdbcProfileStore store, ElasticsearchRagIndex keyword, RagProperties config) {
        this.store = store; this.keyword = keyword; this.config = config; this.target = target(config);
    }
    public static String target(RagProperties config) {
        return SummaryPolicy.hash("profile-keyword-v1|" + config.getElasticsearch().getEndpoint().replaceAll("/+$", "")
                + "|" + config.getElasticsearch().getIndexName());
    }
    public void start() {
        scanner.scheduleWithFixedDelay(() -> {
            try { sweep(); } catch (RuntimeException failure) { log.warn("画像关键词投影等待恢复 ({})", failure.getClass().getSimpleName()); }
        }, config.getScanIntervalMs(), config.getScanIntervalMs(), TimeUnit.MILLISECONDS);
    }
    public void sweep() {
        store.backfillPage(target, config.getScanBatch());
        for (String id : store.pendingProjections(target, config.getScanBatch())) {
            try { project(id); } catch (RuntimeException failure) { log.warn("画像关键词任务保留重试 ({})", failure.getClass().getSimpleName()); }
        }
    }
    public void resetBackfill() { store.resetBackfill(target); }
    public void project(String id) {
        var row = store.claimProjection(id, target, config.getWorkerLeaseMs()); if (row == null) return;
        try {
            keyword.initialize();
            var current = store.fact(row.userId(), row.factId());
            boolean active = current.isPresent() && current.get().version() == row.version() && "ACTIVE".equals(current.get().state());
            String candidateId = ProfileRagAccess.candidateId(row.factId(), row.version());
            if (active) {
                var candidate = ProfileRagAccess.candidate(current.orElseThrow());
                keyword.upsert(new RagIndexRecord(candidate.id(), candidate.kind(), row.userId(), RagData.Visibility.OWNER,
                        candidate.resourceId(), candidate.version(), candidate.parentId(), candidate.title(), candidate.text(),
                        candidate.source(), 0, 0, 0, true));
            } else keyword.delete(candidateId);
            var after = store.fact(row.userId(), row.factId());
            if (active && (after.isEmpty() || after.get().version() != row.version() || !"ACTIVE".equals(after.get().state()))) {
                keyword.delete(candidateId); active = false;
            }
            store.finishProjection(row, active ? "WRITTEN" : "OBSOLETE");
        } catch (RuntimeException failure) {
            store.retryProjection(row, failure, config.getRetryDelayMs());
            store.recoverObsoleteProjection(row, failure, config.getRetryDelayMs());
            throw failure;
        }
    }
    @Override public void close() { scanner.shutdownNow(); }
}
