package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;
import org.junit.jupiter.api.*;
import org.springframework.ai.embedding.EmbeddingModel;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.rag.RagData.*;
import static org.mockito.Mockito.*;

class RagRetrievalServiceTest {
    ExecutorService pool;
    RagProperties config;
    @BeforeEach void setup() { pool = Executors.newFixedThreadPool(4); config = new RagProperties(); }
    @AfterEach void close() { pool.shutdownNow(); }
    List<Hit> hits(String prefix, int size) { return IntStream.range(0, size).mapToObj(i -> new Hit(prefix + i, 1.0 - i / 100.0)).toList(); }

    @Test void bothEnforcesQuotasAndSharedFiftyCap() {
        var result = RagRetrievalService.allocate(hits("p", 50), hits("d", 50), 10, 40);
        assertThat(result).hasSize(50);
        assertThat(result.stream().filter(hit -> hit.id().startsWith("p"))).hasSize(10);
    }
    @Test void eitherTypeCanUseUnusedQuota() {
        assertThat(RagRetrievalService.allocate(hits("p", 3), hits("d", 50), 10, 40)).hasSize(50)
                .filteredOn(hit -> hit.id().startsWith("d")).hasSize(47);
        assertThat(RagRetrievalService.allocate(hits("p", 50), hits("d", 7), 10, 40)).hasSize(50)
                .filteredOn(hit -> hit.id().startsWith("p")).hasSize(43);
    }
    @Test void duplicateHitsDoNotConsumeQuota() {
        assertThat(RagRetrievalService.allocate(List.of(new Hit("p", 1), new Hit("p", .5)), hits("d", 50), 10, 40))
                .hasSize(50).extracting(Hit::id).doesNotHaveDuplicates();
    }
    Candidate candidate(String id) { return new Candidate(id, Kind.KNOWLEDGE_CHILD, "guide", 1, "parent", "读屏指南", "按回车键", "synthetic.md", 0, 0, 1); }
    Evidence evidence(Candidate c) { return new Evidence(c.parentId(), c.kind(), c.title(), "完整步骤：按回车键。", c.source(), c.version(), 0, 0, List.of(c.id())); }
    RagRetrievalService.Resolver resolver(List<String> invalid) { return new RagRetrievalService.Resolver() {
        public List<Candidate> hydrate(long user, List<String> ids, List<String> documents) { return ids.stream().filter(id -> !invalid.contains(id)).map(RagRetrievalServiceTest.this::candidate).toList(); }
        public Evidence expand(long user, Candidate c) { return evidence(c); }
    }; }
    EmbeddingModel embedding(AtomicInteger calls) {
        // CheckedEmbeddingModel intentionally invokes the provider request/response boundary.
        return new EmbeddingModel() {
            public org.springframework.ai.embedding.EmbeddingResponse call(org.springframework.ai.embedding.EmbeddingRequest request) {
                calls.incrementAndGet(); return new org.springframework.ai.embedding.EmbeddingResponse(List.of(new org.springframework.ai.embedding.Embedding(new float[]{1, 0}, 0)));
            }
            public float[] embed(org.springframework.ai.document.Document document) { throw new UnsupportedOperationException(); }
        };
    }
    Plan plan(Scope scope) { return new Plan("怎么操作？", "读屏软件 NVDA 如何打开文档", scope, List.of()); }

    @Test void channelsRunConcurrentlyUseOneEmbeddingFuseTwentyRerankFiveAndMergeParents() {
        var calls = new AtomicInteger(); var bothEntered = new CountDownLatch(2);
        RagRetrievalService.VectorChannel vectors = (user, query, vector, kind, docs, limit) -> {
            assertThat(user).isEqualTo(42); assertThat(vector).containsExactly(1, 0);
            bothEntered.countDown(); await(bothEntered); return hits(kind == Kind.PROFILE_FACT ? "p" : "d", 50);
        };
        RagRetrievalService.KeywordChannel keywords = (user, query, kind, docs, limit) -> {
            bothEntered.countDown(); await(bothEntered); return hits(kind == Kind.PROFILE_FACT ? "p" : "d", 50);
        };
        var service = new RagRetrievalService(config, pool, embedding(calls), 2, vectors, keywords, resolver(List.of()), (query, candidates, limit) -> {
            assertThat(query).isEqualTo(plan(Scope.BOTH).query()); assertThat(candidates).hasSize(20); assertThat(limit).isEqualTo(5);
            return IntStream.range(0, 5).mapToObj(i -> new Reranked(i, 1 - i / 10.0)).toList();
        });
        var result = service.retrieve(42, plan(Scope.BOTH));
        assertThat(calls.get()).isEqualTo(1); assertThat(result.trace().degradedChannels()).isEmpty();
        assertThat(result.trace().vectorHits()).isEqualTo(50); assertThat(result.trace().keywordHits()).isEqualTo(50);
        assertThat(result.evidence()).singleElement().satisfies(e -> assertThat(e.matchedChildren()).hasSize(5));
    }
    @Test void staleSqlCandidatesAreRemovedBeforeReranking() {
        var service = new RagRetrievalService(config, pool, embedding(new AtomicInteger()), 2,
                (u,q,v,k,d,l) -> List.of(new Hit("stale", 1)), (u,q,k,d,l) -> List.of(), resolver(List.of("stale")),
                (q,c,l) -> { throw new AssertionError("Must not rerank stale candidate"); });
        assertThat(service.retrieve(42, plan(Scope.DOCUMENT)).evidence()).isEmpty();
    }
    @Test void rerankerFailureFallsBackToFusedOrderAndReportsDegradation() {
        var service = new RagRetrievalService(config, pool, embedding(new AtomicInteger()), 2,
                (u,q,v,k,d,l) -> hits("d", 10), (u,q,k,d,l) -> List.of(), resolver(List.of()), (q,c,l) -> { throw new IllegalStateException("private response"); });
        var result = service.retrieve(42, plan(Scope.DOCUMENT));
        assertThat(result.trace().degradedChannels()).contains("RERANKER"); assertThat(result.evidence()).hasSize(1);
        assertThat(result.trace().rerankedHits()).isZero();
    }
    @Test void keywordChannelSurvivesUnavailableVectorProvider() {
        var service = new RagRetrievalService(config, pool, embedding(new AtomicInteger()), 2,
                (u,q,v,k,d,l) -> { throw new IllegalStateException(); }, (u,q,k,d,l) -> hits("d", 1), resolver(List.of()), (q,c,l) -> List.of(new Reranked(0, 1)));
        assertThat(service.retrieve(42, plan(Scope.DOCUMENT)).trace().degradedChannels()).contains("VECTOR_KNOWLEDGE_CHILD");
    }
    @Test void scopeNoneDoesNoExternalWorkAndInvalidInputIsRejected() {
        var model = mock(EmbeddingModel.class); var resolver = mock(RagRetrievalService.Resolver.class);
        var service = new RagRetrievalService(config, pool, model, 2, (u,q,v,k,d,l) -> { throw new AssertionError(); },
                (u,q,k,d,l) -> { throw new AssertionError(); }, resolver, (q,c,l) -> { throw new AssertionError(); });
        assertThat(service.retrieve(42, plan(Scope.NONE)).evidence()).isEmpty(); verifyNoInteractions(model, resolver);
        assertThatThrownBy(() -> service.retrieve(0, plan(Scope.BOTH))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.retrieve(42, new Plan("a", "a".repeat(4001), Scope.BOTH, List.of()))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.retrieve(42, new Plan("a", "a", Scope.DOCUMENT, List.of("*")))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void sqlHydrationIsAlsoBoundedByTheTotalDeadline() {
        config.setTimeoutMs(150); config.setChannelTimeoutMs(100);
        var resolver = new RagRetrievalService.Resolver() {
            public List<Candidate> hydrate(long user, List<String> ids, List<String> docs) {
                try { new CountDownLatch(1).await(5, TimeUnit.SECONDS); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                return List.of();
            }
            public Evidence expand(long user, Candidate c) { throw new AssertionError(); }
        };
        var service = new RagRetrievalService(config, pool, embedding(new AtomicInteger()), 2,
                (u,q,v,k,d,l) -> hits("d", 1), (u,q,k,d,l) -> List.of(), resolver, (q,c,l) -> { throw new AssertionError(); });
        long started = System.nanoTime();
        assertThat(service.retrieve(42, plan(Scope.DOCUMENT)).trace().degradedChannels()).contains("SQL");
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(1500);
    }
    private void await(CountDownLatch latch) { try { assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue(); } catch (InterruptedException e) { throw new RuntimeException(e); } }
}
