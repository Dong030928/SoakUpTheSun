package org.hgc.suts.gateway.rag;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.manager.CheckedEmbeddingModel;
import org.hgc.suts.gateway.rag.document.JdbcDocumentStore;
import org.springframework.ai.embedding.EmbeddingModel;
import redis.clients.jedis.RedisClient;

import java.util.NoSuchElementException;
import java.util.concurrent.*;

/** Durable receipts and immutable embeddings make partial dual-index writes restartable. */
@Slf4j
public final class DocumentIndexWorker implements AutoCloseable {
    private final JdbcDocumentStore store;
    private final RedisRagIndex vectors;
    private final ElasticsearchRagIndex keywords;
    private final RedisClient redis;
    private final RagProperties config;
    private final EmbeddingModel embedding;
    private final ScheduledExecutorService scanner = Executors.newSingleThreadScheduledExecutor(run -> {
        Thread thread = new Thread(run, "agent-rag-documents"); thread.setDaemon(true); return thread;
    });

    public DocumentIndexWorker(JdbcDocumentStore store, RedisRagIndex vectors, ElasticsearchRagIndex keywords,
                               RedisClient redis, EmbeddingModel embedding, int dimension, RagProperties config) {
        this.store = store; this.vectors = vectors; this.keywords = keywords; this.redis = redis;
        this.embedding = new CheckedEmbeddingModel(embedding, dimension); this.config = config;
        store.registerTarget(vectors.target(), vectors.prefix(), config.getElasticsearch().getEndpoint(), config.getElasticsearch().getIndexName());
    }
    public void start() { scanner.scheduleWithFixedDelay(() -> {
        try { sweep(); } catch (RuntimeException failure) { log.warn("文档索引等待恢复 ({})", failure.getClass().getSimpleName()); }
    }, config.getScanIntervalMs(), config.getScanIntervalMs(), TimeUnit.MILLISECONDS); }
    public void sweep() {
        for (var orphan : store.pendingOrphans()) {
            try {
                if (repairOrphan(orphan)) store.completeOrphan(orphan);
                else store.retryOrphan(orphan, new IllegalStateException("Document projection is still being written"));
            } catch (RuntimeException failure) { store.retryOrphan(orphan, failure); }
        }
        for (String id : store.pending(vectors.target())) {
            try { process(id); } catch (RuntimeException failure) { log.warn("文档索引任务保留重试 ({})", failure.getClass().getSimpleName()); }
        }
    }
    public void process(String id) {
        var job = store.claim(id, vectors.target()); if (job == null) return;
        try {
            if (job.state().equals("WRITING")) {
                var chunks = store.chunks(job);
                if (!job.redisDone()) {
                    vectors.initialize();
                    for (var chunk : chunks) {
                        job = store.renew(job);
                        float[] vector = store.embedding(chunk.id(), job.targetTag());
                        if (vector == null) { vector = embedding.embed(chunk.text()); store.cacheEmbedding(job, chunk.id(), vector); }
                        float[] prepared = vector;
                        job = write(job, chunk.id(), () -> vectors.upsert(record(chunk, false), prepared));
                    }
                    store.complete(job, "REDIS");
                    job = store.renew(job);
                }
                if (!job.esDone()) {
                    keywords.initialize();
                    for (var chunk : chunks) {
                        job = store.renew(job);
                        job = write(job, chunk.id(), () -> keywords.upsert(record(chunk, false)));
                    }
                    store.complete(job, "ES");
                    job = store.renew(job);
                }
            }
            // SQL publication requires both receipts. CLEANUP retries resume publication before deleting old data.
            if (store.isCurrent(job)) {
                vectors.initialize(); keywords.initialize();
                for (var chunk : store.chunks(job)) {
                    job = store.renew(job);
                    if (!store.isCurrent(job)) break;
                    String target = job.targetTag();
                    job = write(job, chunk.id(), () -> publishVector(chunk, target));
                    if (!store.isCurrent(job)) { compensate(chunk.id(), target); break; }
                    job = write(job, chunk.id(), () -> publishKeyword(keywords, chunk));
                    if (!store.isCurrent(job)) { compensate(chunk.id(), target); break; }
                }
            }
            for (var cleanup : store.cleanup(job)) {
                job = store.renew(job);
                var destination = store.getTarget(cleanup.targetTag());
                if (store.safeToDeleteVector(cleanup)) { safePrefix(destination); redis.del(destination.redisPrefix() + cleanup.childId()); }
                if (store.safeToDeleteKeyword(cleanup)) keywordTarget(destination).delete(cleanup.childId());
                job = store.renew(job);
            }
            store.completeCleanup(job);
        } catch (RuntimeException failure) { store.retry(job, failure); throw failure; }
    }
    private JdbcDocumentStore.Job write(JdbcDocumentStore.Job job, String childId, Runnable action) {
        RuntimeException requestFailure = null;
        try { action.run(); } catch (RuntimeException failure) { requestFailure = failure; }
        JdbcDocumentStore.Job renewed;
        try { renewed = store.renew(job); }
        catch (RuntimeException stale) {
            // Record compensation independently: a takeover may already have completed the original cleanup job.
            try { compensate(childId, job.targetTag()); } catch (RuntimeException cleanupFailure) { stale.addSuppressed(cleanupFailure); }
            throw stale;
        }
        if (requestFailure != null) throw requestFailure;
        return renewed;
    }
    private void compensate(String childId, String target) {
        store.recordOrphan(childId, target);
        // Keep the durable row until a later sweep acknowledges its exact revision.
        repairOrphan(new JdbcDocumentStore.Orphan(childId, target, 0));
    }
    private boolean repairOrphan(JdbcDocumentStore.Orphan orphan) {
        if (store.orphanInFlight(orphan)) return false;
        var cleanup = new JdbcDocumentStore.Cleanup(orphan.childId(), orphan.targetTag());
        var destination = store.getTarget(orphan.targetTag()); safePrefix(destination);
        boolean deleteVector = store.safeToDeleteVector(cleanup);
        if (deleteVector) redis.del(destination.redisPrefix() + orphan.childId());
        else repairCurrentVector(orphan, destination);
        // A current publication may have committed while a remote delete was in flight.
        if (deleteVector && !store.safeToDeleteVector(cleanup)) repairCurrentVector(orphan, destination);
        boolean deleteKeyword = store.safeToDeleteKeyword(cleanup);
        var keyword = keywordTarget(destination);
        if (deleteKeyword) keyword.delete(orphan.childId());
        else publishKeyword(keyword, published(orphan.childId()));
        if (deleteKeyword && !store.safeToDeleteKeyword(cleanup)) publishKeyword(keyword, published(orphan.childId()));
        return !store.orphanInFlight(orphan);
    }
    private void repairCurrentVector(JdbcDocumentStore.Orphan orphan, JdbcDocumentStore.Target destination) {
        if (!destination.redisPrefix().equals(vectors.prefix()))
            throw new IllegalStateException("Previous Redis target requires its own recovery configuration");
        publishVector(published(orphan.childId()), orphan.targetTag());
    }
    private JdbcDocumentStore.ProjectionChunk published(String childId) {
        return store.publishedChunk(childId).orElseThrow(() -> new NoSuchElementException("Current document changed during projection repair"));
    }
    private void publishVector(JdbcDocumentStore.ProjectionChunk chunk, String target) {
        try { vectors.publish(chunk.id(), true); }
        catch (RuntimeException missingOrUnavailable) {
            float[] cached = store.embedding(chunk.id(), target);
            if (cached == null) throw missingOrUnavailable;
            vectors.upsert(record(chunk, false), cached); vectors.publish(chunk.id(), true);
        }
    }
    private void publishKeyword(ElasticsearchRagIndex keyword, JdbcDocumentStore.ProjectionChunk chunk) {
        try { keyword.publish(chunk.id(), true); }
        catch (RuntimeException missingOrUnavailable) {
            keyword.initialize(); keyword.upsert(record(chunk, false)); keyword.publish(chunk.id(), true);
        }
    }
    private static void safePrefix(JdbcDocumentStore.Target target) {
        if (!target.redisPrefix().startsWith("suts:agent:rag:documents:")) throw new IllegalStateException("Unsafe document cleanup prefix");
    }
    private ElasticsearchRagIndex keywordTarget(JdbcDocumentStore.Target target) {
        if (!target.esEndpoint().equals(config.getElasticsearch().getEndpoint().replaceAll("/+$", "")))
            throw new IllegalStateException("Previous Elasticsearch endpoint requires its own cleanup configuration");
        if (target.esIndex().equals(config.getElasticsearch().getIndexName())) return keywords;
        var previous = new RagProperties();
        previous.getElasticsearch().setEndpoint(target.esEndpoint()); previous.getElasticsearch().setIndexName(target.esIndex());
        previous.getElasticsearch().setUsername(config.getElasticsearch().getUsername());
        previous.getElasticsearch().setPassword(config.getElasticsearch().getPassword());
        previous.getElasticsearch().setTimeoutMs(config.getElasticsearch().getTimeoutMs());
        return new ElasticsearchRagIndex(previous);
    }
    private RagIndexRecord record(JdbcDocumentStore.ProjectionChunk c, boolean searchable) {
        return new RagIndexRecord(c.id(), RagData.Kind.KNOWLEDGE_CHILD, c.ownerId(), c.visibility(), c.documentId(), c.version(),
                c.parentId(), c.title(), c.text(), c.source(), c.pageStart(), c.pageEnd(), c.ordinal(), searchable);
    }
    @Override public void close() { scanner.shutdownNow(); }
}
