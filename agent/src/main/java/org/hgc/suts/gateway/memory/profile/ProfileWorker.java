package org.hgc.suts.gateway.memory.profile;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryProfileProperties;

import java.util.Arrays;
import java.util.concurrent.*;

import static org.hgc.suts.gateway.memory.profile.ProfileData.*;

/** Recoverable stages, independent of the summary slot and without another model call. */
@Slf4j
public final class ProfileWorker implements AutoCloseable {
    private final JdbcProfileStore store;
    private final ProfileVectorIndex vectors;
    private final MemoryProfileProperties config;
    private final ScheduledExecutorService scanner = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "agent-profile-scanner"));
    private final ExecutorService workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(40),
            r -> daemon(r, "agent-profile-worker"), new ThreadPoolExecutor.AbortPolicy());

    public ProfileWorker(JdbcProfileStore store, ProfileVectorIndex vectors, MemoryProfileProperties config) {
        this.store = store; this.vectors = vectors; this.config = config;
    }
    private static Thread daemon(Runnable run, String name) { var thread = new Thread(run, name); thread.setDaemon(true); return thread; }
    public void start() { scanner.scheduleWithFixedDelay(() -> {
        try { sweep(); } catch (RuntimeException ex) { log.warn("画像扫描等待重试 ({})", ex.getClass().getSimpleName()); }
    }, config.getScanIntervalMs(), config.getScanIntervalMs(), TimeUnit.MILLISECONDS); }
    public void sweep() {
        for (String id : store.pendingExtractions()) if (!submit(() -> extract(id))) break;
        if (vectors.enabled()) for (String id : store.pendingProjections()) if (!submit(() -> project(id))) break;
    }
    private boolean submit(Runnable task) {
        try { workers.execute(() -> {
            try { task.run(); }
            catch (RuntimeException failure) { log.warn("画像阶段等待恢复，持久任务保留 ({})", failure.getClass().getSimpleName()); }
        }); return true; }
        catch (RejectedExecutionException full) { return false; }
    }
    public void extract(String id) {
        var row = store.claimExtraction(id); if (row == null) return;
        try {
            var candidates = Arrays.asList(JSON.readValue(row.result(), Candidate[].class));
            try { store.validate(store.event(row), candidates); }
            catch (IllegalArgumentException | java.util.NoSuchElementException invalid) { store.reject(row, invalid); return; }
            store.apply(row, candidates);
        } catch (RuntimeException failure) { store.retryExtraction(row, failure); throw failure; }
    }
    public void project(String id) {
        if (!vectors.enabled()) return; // Keep PENDING work for an explicit later resume/reindex.
        var row = store.claimProjection(id); if (row == null) return;
        try {
            var current = store.fact(row.userId(), row.factId());
            boolean active = current.isPresent() && current.get().version() == row.version() && current.get().state().equals("ACTIVE");
            if (active) vectors.write(current.orElseThrow());
            else vectors.delete(row.factId(), row.version());
            // A correction/forget may have committed while Embedding/Redis was in flight.
            var after = store.fact(row.userId(), row.factId());
            if (active && (after.isEmpty() || after.get().version() != row.version() || !after.get().state().equals("ACTIVE"))) {
                vectors.delete(row.factId(), row.version()); active = false;
            }
            store.finishProjection(row, active ? "WRITTEN" : "OBSOLETE");
        } catch (RuntimeException failure) { store.retryProjection(row, failure); throw failure; }
    }
    @Override public void close() { scanner.shutdownNow(); workers.shutdownNow(); }
}
