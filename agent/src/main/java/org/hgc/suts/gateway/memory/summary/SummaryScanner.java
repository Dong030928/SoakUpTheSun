package org.hgc.suts.gateway.memory.summary;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.memory.transactions.RocketMqTransactionTransport;

import java.util.concurrent.*;
import java.util.function.Supplier;

/** The ZSet is only an index. SCAN repairs missing entries/proof, SQL resumes expired processing leases. */
@Slf4j
public final class SummaryScanner implements AutoCloseable {
    private final RedisSummaryStateStore states;
    private final JdbcSummaryJobs jobs;
    private final SummaryWorker worker;
    private final SummaryCoordinator coordinator;
    private final SummaryPolicy policy;
    private final Supplier<SummaryMessaging> messaging;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        var thread = new Thread(r, "agent-summary-scanner"); thread.setDaemon(true); return thread;
    });
    private final ExecutorService workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(50), r -> {
        var thread = new Thread(r, "agent-summary-recovery"); thread.setDaemon(true); return thread;
    }, new ThreadPoolExecutor.AbortPolicy());
    public SummaryScanner(RedisSummaryStateStore states, JdbcSummaryJobs jobs, SummaryWorker worker,
            SummaryCoordinator coordinator, SummaryPolicy policy, Supplier<SummaryMessaging> messaging) {
        this.states = states; this.jobs = jobs; this.worker = worker; this.coordinator = coordinator; this.policy = policy; this.messaging = messaging;
    }
    public void start() { scheduler.scheduleWithFixedDelay(this::safeSweep, policy.current().getScanIntervalMs(),
            policy.current().getScanIntervalMs(), TimeUnit.MILLISECONDS); }
    private void safeSweep() {
        try { sweep(); }
        catch (RuntimeException ex) { log.warn("摘要扫描暂不可用，保留共享任务 ({})", ex.getClass().getSimpleName()); }
    }
    public void sweep() {
        int limit = policy.current().getScanBatch();
        var ids = new java.util.LinkedHashSet<>(states.due(System.currentTimeMillis(), Math.max(1, limit / 2)));
        for (String id : states.scan(limit)) { if (ids.size() >= limit) break; ids.add(id); }
        for (String id : ids) {
            try {
                coordinator.scanConversation(id);
                for (var event : states.pendingEvents(id)) {
                    // Shared redrive throttling prevents every producer from republishing the same exhausted half-message.
                    if (states.claimRedrive(event)) messaging.get().sendSummary(event, () -> states.check(event));
                }
            } catch (org.hgc.suts.gateway.memory.ConversationBusyException busy) { /* Recovered after its request lease ends. */ }
            catch (RuntimeException ex) { log.warn("单个摘要会话暂不可恢复 ({})", ex.getClass().getSimpleName()); }
        }
        for (String id : jobs.recoverable(System.currentTimeMillis(), limit)) {
            try { workers.execute(() -> { try { worker.run(id); } catch (RuntimeException ignored) { } }); }
            catch (RejectedExecutionException full) { break; } // SQL intent is retained; never report a dropped task as completed.
        }
    }
    @Override public void close() { scheduler.shutdownNow(); workers.shutdownNow(); }
}
