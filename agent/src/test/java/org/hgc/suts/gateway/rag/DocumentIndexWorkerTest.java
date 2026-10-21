package org.hgc.suts.gateway.rag;

import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.rag.document.DocumentEtl;
import org.hgc.suts.gateway.rag.document.JdbcDocumentStore;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import redis.clients.jedis.RedisClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DocumentIndexWorkerTest {
    private final RagProperties config = new RagProperties();
    private final String target = "a".repeat(64), prefix = "suts:agent:rag:documents:fixture:";
    private final DriverManagerDataSource source = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
            + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1", "sa", "");
    private final JdbcTemplate jdbc = new JdbcTemplate(source);
    private final JdbcDocumentStore store;
    private final RedisRagIndex vectors = mock(RedisRagIndex.class);
    private final ElasticsearchRagIndex keywords = mock(ElasticsearchRagIndex.class);
    private final RedisClient redis = mock(RedisClient.class);
    private final EmbeddingModel embedding = mock(EmbeddingModel.class);

    DocumentIndexWorkerTest() {
        Flyway.configure().dataSource(source).table("agent_rag_schema_history").locations("classpath:db/rag")
                .placeholders(Map.of("tableOptions", "")).load().migrate();
        store = new JdbcDocumentStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(source)), config);
        when(vectors.target()).thenReturn(target); when(vectors.prefix()).thenReturn(prefix);
        when(embedding.call(any())).thenReturn(new EmbeddingResponse(List.of(new Embedding(new float[]{1, 0, 0}, 0))));
    }
    private DocumentIndexWorker worker() { return new DocumentIndexWorker(store, vectors, keywords, redis, embedding, 3, config); }
    private JdbcDocumentStore.ImportResult submit() {
        var parsed = new DocumentEtl(config).parse("按回车键打开文档。".getBytes(StandardCharsets.UTF_8), "guide.txt");
        return store.importDocument("guide", 42, RagData.Visibility.PUBLIC, "读屏指南", "guide.txt", parsed, target);
    }
    private void retryNow(String id) { jdbc.update("UPDATE agent_rag_job SET retry_at=0 WHERE job_id=?", id); }

    @Test void partialEngineWritesStayUnsearchableAndResumeWithoutEmbeddingAgain() {
        try (var worker = worker()) {
            var imported = submit();
            doThrow(new IllegalStateException("keyword unavailable")).when(keywords).upsert(any());
            assertThatThrownBy(() -> worker.process(imported.jobId())).hasMessage("keyword unavailable");
            var status = store.status(42, "guide");
            assertThat(status.currentVersion()).isZero();
            assertThat(status.jobs()).singleElement().satisfies(job -> {
                assertThat(job.redisDone()).isTrue(); assertThat(job.esDone()).isFalse();
            });
            verify(vectors).upsert(argThat(record -> !record.searchable()), any(float[].class));
            verify(vectors, never()).publish(anyString(), anyBoolean()); verify(keywords, never()).publish(anyString(), anyBoolean());
            doNothing().when(keywords).upsert(any());
            doAnswer(call -> {
                assertThat(store.status(42, "guide").currentVersion()).isEqualTo(1);
                assertThat(store.status(42, "guide").jobs()).singleElement().satisfies(job -> {
                    assertThat(job.redisDone()).isTrue(); assertThat(job.esDone()).isTrue(); assertThat(job.state()).isEqualTo("CLEANUP");
                }); return null;
            }).when(vectors).publish(anyString(), eq(true));
            retryNow(imported.jobId()); worker.process(imported.jobId());
            verify(embedding, times(1)).call(any());
            verify(vectors, times(1)).upsert(any(), any(float[].class));
            verify(keywords, times(2)).upsert(argThat(record -> !record.searchable()));
            assertThat(store.status(42, "guide").jobs()).singleElement().satisfies(job -> assertThat(job.state()).isEqualTo("DONE"));
        }
    }

    @Test void failureAfterSqlPublicationResumesPublicationFromCleanupWithoutReembedding() {
        try (var worker = worker()) {
            var imported = submit();
            doThrow(new IllegalStateException("publish unavailable")).when(vectors).publish(anyString(), eq(true));
            assertThatThrownBy(() -> worker.process(imported.jobId())).hasMessage("publish unavailable");
            var status = store.status(42, "guide"); assertThat(status.currentVersion()).isEqualTo(1);
            assertThat(status.jobs()).singleElement().satisfies(job -> assertThat(job.state()).isEqualTo("CLEANUP"));
            doNothing().when(vectors).publish(anyString(), eq(true));
            clearInvocations(vectors, keywords); retryNow(imported.jobId()); worker.process(imported.jobId());
            verify(vectors).publish(anyString(), eq(true)); verify(keywords).publish(anyString(), eq(true));
            verify(vectors, never()).upsert(any(), any(float[].class)); verify(keywords, never()).upsert(any());
            verify(embedding, times(1)).call(any()); assertThat(store.pending(target)).isEmpty();
        }
    }

    @Test void lateWriteAfterDeleteAndCompletedCleanupRetainsOrphanWhenCompensationFails() {
        try (var worker = worker()) {
            var imported = submit();
            doAnswer(call -> {
                store.delete(42, "guide");
                var takeover = store.claim(imported.jobId(), target);
                store.completeCleanup(takeover); // A different instance already finished durable deletion.
                return null;
            }).when(vectors).upsert(any(), any(float[].class));
            when(redis.del(anyString())).thenThrow(new IllegalStateException("cleanup unavailable"));
            assertThatThrownBy(() -> worker.process(imported.jobId())).hasMessageContaining("lease");
            assertThat(store.status(42, "guide").deleted()).isTrue();
            assertThat(store.status(42, "guide").jobs()).singleElement().satisfies(job -> assertThat(job.state()).isEqualTo("DONE"));
            assertThat(store.pendingOrphans()).hasSize(1);
            String child = store.pendingOrphans().get(0).childId();
            doReturn(1L).when(redis).del(anyString()); worker.sweep();
            verify(redis, atLeastOnce()).del(prefix + child); verify(keywords).delete(child);
            assertThat(store.pendingOrphans()).isEmpty();
            verify(vectors, never()).publish(anyString(), anyBoolean()); verify(keywords, never()).upsert(any());
        }
    }

    @Test void lateUnsearchableWriteCannotHideOrDeleteAnAlreadyPublishedTakeoverVersion() {
        try (var worker = worker()) {
            var imported = submit();
            doAnswer(call -> {
                jdbc.update("UPDATE agent_rag_job SET lease_until=0 WHERE job_id=?", imported.jobId());
                var takeover = store.claim(imported.jobId(), target);
                store.complete(takeover, "REDIS"); store.complete(takeover, "ES"); store.completeCleanup(takeover);
                return null;
            }).when(vectors).upsert(any(), any(float[].class));
            assertThatThrownBy(() -> worker.process(imported.jobId())).hasMessageContaining("lease");
            String child = store.pendingOrphans().get(0).childId();
            assertThat(store.status(42, "guide").currentVersion()).isEqualTo(1);
            verify(vectors).publish(child, true); verify(keywords).publish(child, true);
            verify(redis, never()).del(anyString()); verify(keywords, never()).delete(anyString());
            worker.sweep(); assertThat(store.pendingOrphans()).isEmpty();
        }
    }
}
