package org.hgc.suts.gateway.memory.summary;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.transactions.TransactionTransport;
import org.hgc.suts.gateway.service.VectorStorageService;
import org.springframework.util.Assert;

import java.util.concurrent.atomic.LongAdder;

/** Persistent stages: READY -> RESULT -> MERGED -> DONE. Retries resume a stage, not a model call. */
@Slf4j
public final class SummaryWorker implements SummaryMessaging.Handler {
    private final JdbcSummaryJobs jobs;
    private final JdbcConversationArchive archive;
    private final RedisSummaryStateStore states;
    private final SummaryCoordinator coordinator;
    private final SummaryPolicy policy;
    private final SummaryModel model;
    private final MemoryTransactionProperties tx;
    private final VectorStorageService vectors;
    public final LongAdder modelCalls = new LongAdder(), failures = new LongAdder(), completed = new LongAdder();
    public SummaryWorker(JdbcSummaryJobs jobs, JdbcConversationArchive archive, RedisSummaryStateStore states,
            SummaryCoordinator coordinator, SummaryPolicy policy, SummaryModel model, MemoryTransactionProperties tx, VectorStorageService vectors) {
        this.jobs = jobs; this.archive = archive; this.states = states; this.coordinator = coordinator;
        this.policy = policy; this.model = model; this.tx = tx; this.vectors = vectors;
    }
    private SummaryEvent event(byte[] body) { return SummaryEvent.decode(body, tx.getMaxMessageBytes(), tx.getGeneration()); }
    @Override public void validate(byte[] body) { event(body); }
    @Override public TransactionTransport.State check(byte[] body) { return states.check(event(body)); }
    @Override public void consume(byte[] body) {
        var event = event(body);
        jobs.register(event);
        run(event.jobId());
        var completed = jobs.read(event.jobId()).orElseThrow();
        if ("DONE".equals(completed.state()) || "STALE".equals(completed.state())) states.acknowledge(event);
    }
    public void run(String jobId) {
        var job = jobs.claim(jobId, System.currentTimeMillis(), policy.current().getWorkerLeaseMs());
        if (job == null) return; // Another worker/explicit retry deadline owns it; the durable scanner will resume it.
        try {
            var event = jobs.event(job);
            if (states.read(event.conversationId()) == null) coordinator.recover(event.conversationId());
            if ("READY".equals(job.state())) {
                var current = states.read(event.conversationId());
                if (current.epoch() != event.epoch() || current.version() != event.baseVersion()) { stale(job, event); return; }
                Assert.state(current.coveredThrough() + 1 == event.from() && current.summary().equals(event.previousSummary()),
                        "Summary inheritance/version conflict");
                Assert.state(archive.completeRange(event.userId(), event.conversationId(), event.epoch(), event.from(), event.to()).equals(event.turns()),
                        "Wait for all immutable input turns to be durably archived");
                states.fenceWorker(event, job.worker(), job.leaseUntil());
                modelCalls.increment();
                var result = model.summarize(event); // NO SQL transaction held here, and NO business tools.
                jobs.saveResult(job, result.text(), result.promptTokens(), result.completionTokens());
                job = jobs.read(job.id()).orElseThrow();
            }
            if ("RESULT".equals(job.state())) {
                var current = states.read(event.conversationId());
                if (current.epoch() != event.epoch() || current.version() > event.baseVersion() + 1
                        || (current.version() == event.baseVersion() + 1 && !current.appliedJob().equals(event.jobId()))) {
                    stale(job, event); return;
                }
                states.fenceWorker(event, job.worker(), job.leaseUntil());
                if (!states.merge(event, job.worker(), job.result())) { stale(job, event); return; }
                // The durable result predates the Redis merge; repeating this projection is idempotent.
                jobs.project(job, event, job.result());
                job = jobs.read(job.id()).orElseThrow();
            }
            Assert.state("MERGED".equals(job.state()), "Unexpected summary job stage");
            states.finishMerge(event);
            String vectorState;
            if (!policy.current().isVectorEnabled()) vectorState = "SKIPPED";
            else if (!jobs.isCurrent(event.userId(), event.conversationId(), event.epoch(), event.baseVersion() + 1)) {
                vectors.deleteSummaryDocuments(java.util.List.of(event.documentId())); vectorState = "OBSOLETE";
            } else {
                vectorState = vectors.saveSummary(new VectorStorageService.SummaryDocument(event.userId(), event.conversationId(), event.epoch(),
                        event.baseVersion() + 1, 1, event.to(), job.result(), event.documentId()));
                if (event.baseVersion() > 0 && !"SKIPPED".equals(vectorState)) {
                    String previous = "summary_" + SummaryPolicy.hash(event.conversationId() + "|" + event.epoch() + "|" + event.baseVersion());
                    vectors.deleteSummaryDocuments(java.util.List.of(previous));
                }
            }
            jobs.finish(job, vectorState);
            completed.increment();
        } catch (RuntimeException ex) {
            failures.increment();
            jobs.retry(job, policy.current().getRetryDelayMs(), ex);
            log.warn("摘要任务保留于持久阶段，等待重试 ({})", ex.getClass().getSimpleName());
            throw ex;
        }
    }
    private void stale(JdbcSummaryJobs.Job job, SummaryEvent event) {
        jobs.stale(job);
        states.acknowledge(event); // Durable STALE status retains the input; stop endlessly redriving its old intent.
    }
}
