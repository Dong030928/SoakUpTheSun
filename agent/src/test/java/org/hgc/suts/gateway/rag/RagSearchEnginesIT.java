package org.hgc.suts.gateway.rag;

import com.sun.net.httpserver.HttpServer;
import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.AiModelConfiguration;
import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.rag.document.DocumentEtl;
import org.hgc.suts.gateway.rag.document.JdbcDocumentStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hgc.suts.gateway.rag.RagData.*;

/** Opt-in, marked private dependencies only. No application or external model-provider configuration is read. */
@Timeout(180)
class RagSearchEnginesIT {
    private static final String MARKER = "agent-rag-integration-only";
    private static final String ES = "http://127.0.0.1:19200";
    private final Set<String> engineIds = new LinkedHashSet<>();
    private final Set<String> documentIds = new LinkedHashSet<>();
    private final String namespace = UUID.randomUUID().toString().replace("-", "");
    private final RagIndexHttp http = new RagIndexHttp(10000, 1024 * 1024, Map.of());
    private RagProperties properties;
    private RedisClient redis;
    private RedisRagIndex vectors;
    private ElasticsearchRagIndex keywords;
    private JdbcTemplate jdbc;
    private JdbcDocumentStore documents;
    private LocalModels models;
    private EmbeddingModel embedding;
    private ExecutorService retrievalExecutor;
    private boolean keywordCreated;

    @BeforeEach void connectOnlyToMarkedLoopbackFixtures() throws Exception {
        assertThat(System.getProperty("rag.retrieval.fixture"))
                .as("Explicitly opt in with -Drag.retrieval.fixture=agent-rag-integration-only")
                .isEqualTo(MARKER);
        redis = RedisClient.builder().hostAndPort("127.0.0.1", 16393)
                .clientConfig(DefaultJedisClientConfig.builder().password(MARKER)
                        .connectionTimeoutMillis(2000).socketTimeoutMillis(3000).build()).build();
        assertThat(redis.get("suts:agent:test:rag-retrieval-fixture")).as("Redis private-fixture marker").isEqualTo(MARKER);
        var dataSource = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:13326/agent_rag_it"
                + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=2000&socketTimeout=5000&serverTimezone=UTC", "root", MARKER);
        jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForList("SELECT marker FROM suts_agent_test_fixture", String.class))
                .as("MySQL private-fixture marker").containsExactly(MARKER);
        assertThat(get("/").path("cluster_name").asText()).isEqualTo("suts-agent-rag-retrieval-it");
        assertThat(get("/suts-rag-fixture-marker/_doc/fixture").path("_source").path("marker").asText()).isEqualTo(MARKER);

        properties = new RagProperties();
        properties.setIndexName("suts:agent:rag:documents:index:it:" + namespace);
        properties.setPrefix("suts:agent:rag:documents:data:it:" + namespace + ":");
        properties.setMaxParentChars(1024); properties.setMaxChildChars(128); properties.setOverlapChars(16);
        properties.getElasticsearch().setEndpoint(ES);
        properties.getElasticsearch().setIndexName("suts-rag-it-" + namespace);
        properties.getElasticsearch().setTimeoutMs(10000);
        models = new LocalModels();
        properties.getReranker().setEndpoint(models.endpoint() + "/v1");
        properties.getReranker().setApiKey(MARKER);
        var ai = new AiConfig();
        ai.setEmbeddingHost(models.endpoint() + "/v1"); ai.setEmbeddingApiKey(MARKER);
        ai.setEmbeddingModelName("rag-fixture-embedding"); ai.setEmbeddingTimeout(3000); ai.getVector().setDimension(3);
        embedding = new AiModelConfiguration().agentEmbeddingModel(ai);
        vectors = new RedisRagIndex(redis, embedding, properties, 3, DocumentEtl.hash("fixture-embedding-v1"));
        keywords = new ElasticsearchRagIndex(properties);
        keywords.initialize(); keywordCreated = true;
        requireSuccess(http.exchange(URI.create(ES + "/" + properties.getElasticsearch().getIndexName() + "/_settings"),
                "PUT", Map.of("index", Map.of("refresh_interval", "100ms"))));
        Flyway.configure().dataSource(dataSource).table("agent_rag_schema_history").locations("classpath:db/rag")
                .baselineOnMigrate(true).baselineVersion("0")
                .placeholders(Map.of("tableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")).load().migrate();
        documents = new JdbcDocumentStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(dataSource)), properties);
        documents.registerTarget(vectors.target(), vectors.prefix(), ES, properties.getElasticsearch().getIndexName());
        retrievalExecutor = Executors.newFixedThreadPool(4);
    }

    @AfterEach void removeOnlyThisTestsProjectionNamespace() {
        try {
            if (vectors != null && redis != null) {
                for (String id : engineIds) vectors.delete(id);
                if (redis.ftList().contains(vectors.indexName())) redis.ftDropIndex(vectors.indexName());
            }
            if (keywordCreated) requireSuccess(http.exchange(URI.create(ES + "/" + properties.getElasticsearch().getIndexName()), "DELETE", null));
            if (documents != null) {
                for (String id : documentIds) {
                    jdbc.update("DELETE FROM agent_rag_embedding WHERE child_id IN (SELECT child_id FROM agent_rag_child WHERE document_id=?)", id);
                    jdbc.update("DELETE FROM agent_rag_cleanup WHERE job_id IN (SELECT job_id FROM agent_rag_job WHERE document_id=?)", id);
                    for (String table : List.of("agent_rag_job", "agent_rag_child", "agent_rag_parent", "agent_rag_version", "agent_rag_document"))
                        jdbc.update("DELETE FROM " + table + " WHERE document_id=?", id);
                }
                jdbc.update("DELETE FROM agent_rag_target WHERE target_tag=?", vectors.target());
            }
        } finally {
            if (retrievalExecutor != null) retrievalExecutor.shutdownNow();
            if (models != null) models.close();
            if (redis != null) redis.close();
        }
    }

    @Test void realCjkBm25FiltersKindPublicationOwnerAndDocumentBeforeTopK() {
        JsonNode analyzed = RagIndexHttp.parse(requireSuccess(http.exchange(
                URI.create(ES + "/" + properties.getElasticsearch().getIndexName() + "/_analyze"), "POST",
                Map.of("field", "text", "text", "轮椅通行 Wheelchair ACCESS"))));
        List<String> tokens = new ArrayList<>(); analyzed.path("tokens").forEach(token -> tokens.add(token.path("token").asText()));
        assertThat(tokens).contains("轮椅", "通行", "wheelchair", "access");
        var owned = record("owner42", 42, Visibility.OWNER, true, "Accessible wheelchair application");
        var shared = record("public", 99, Visibility.PUBLIC, true, "Wheelchair access 轮椅通行");
        for (int i = 0; i < 12; i++) keywords.upsert(record("owner99_" + i, 99, Visibility.OWNER, true, "wheelchair ".repeat(20)));
        keywords.upsert(record("unpublished", 99, Visibility.PUBLIC, false, "wheelchair ".repeat(20)));
        keywords.upsert(owned); keywords.upsert(shared);
        assertThat(keywords.search(42, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(), 50)).extracting(Hit::id)
                .containsExactlyInAnyOrder(owned.id(), shared.id());
        assertThat(keywords.search(42, "轮椅", Kind.KNOWLEDGE_CHILD, List.of(), 1)).extracting(Hit::id).containsExactly(shared.id());
        assertThat(keywords.search(42, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(owned.resourceId()), 1))
                .singleElement().satisfies(hit -> { assertThat(hit.id()).isEqualTo(owned.id()); assertThat(hit.score()).isPositive(); });
        assertThat(keywords.search(99, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(owned.resourceId()), 1)).isEmpty();
        for (long owner : List.of(42L, 99L)) keywords.upsert(new RagIndexRecord("profile_" + owner, Kind.PROFILE_FACT, owner,
                Visibility.OWNER, "fact_" + owner, 1, "", "profile", "wheelchair preference", "user", 0, 0, 0, true));
        assertThat(keywords.search(42, "wheelchair", Kind.PROFILE_FACT, List.of(), 1)).extracting(Hit::id).containsExactly("profile_42");
        JsonNode mapping = get("/" + properties.getElasticsearch().getIndexName() + "/_mapping")
                .path(properties.getElasticsearch().getIndexName()).path("mappings").path("properties");
        assertThat(mapping.path("ownerId").path("type").asText()).isEqualTo("long");
        assertThat(mapping.path("text").path("similarity").asText()).isEqualTo("BM25");
        keywords.initialize();
    }

    @Test void realRedisHnswFiltersForeignNearestNeighborsAndUnpublishedRecordsBeforeKnn() throws Exception {
        var owned = record("owner42", 42, Visibility.OWNER, true, "wheelchair owner");
        var shared = record("public", 99, Visibility.PUBLIC, true, "wheelchair public");
        for (int i = 0; i < 12; i++) vector(record("foreign_" + i, 99, Visibility.OWNER, true, "wheelchair foreign"), new float[]{1, 0, 0});
        vector(record("hidden", 42, Visibility.OWNER, false, "wheelchair hidden"), new float[]{1, 0, 0});
        vector(record("hidden_public", 99, Visibility.PUBLIC, false, "wheelchair hidden public"), new float[]{1, 0, 0});
        vector(owned, new float[]{.9f, .1f, 0}); vector(shared, new float[]{0, 1, 0});
        await(() -> vectors.search(42, "wheelchair", new float[]{1, 0, 0}, List.of(), 50).size() == 2);
        assertThat(vectors.search(42, "wheelchair", new float[]{1, 0, 0}, List.of(), 1)).extracting(Hit::id).containsExactly(owned.id());
        assertThat(vectors.search(99, "wheelchair", new float[]{1, 0, 0}, List.of(), 50)).hasSize(13);
        assertThat(vectors.search(42, "wheelchair", new float[]{1, 0, 0}, List.of(shared.resourceId()), 1))
                .extracting(Hit::id).containsExactly(shared.id());
        assertThat(vectors.search(99, "wheelchair", new float[]{1, 0, 0}, List.of(owned.resourceId()), 1)).isEmpty();
        assertThat(RagIndexHttp.JSON.writeValueAsString(redis.ftInfo(vectors.indexName()))).contains("HNSW", "COSINE");
        assertThat(models.embeddingCalls.get()).isZero();
    }

    @Test void mysqlReceiptsGatePublicationAndCanonicalSqlRejectsOldDeletedAndForeignEvidence() throws Exception {
        String id = documentId("versioned");
        var first = importDocument(id, Visibility.OWNER, "旧版轮椅 wheelchair 材料。".repeat(20));
        var firstJob = documents.claim(first.jobId(), vectors.target());
        List<String> firstIds = project(firstJob);
        assertThat(documents.hydrate(42, firstIds, List.of(), vectors.target())).isEmpty();
        documents.complete(firstJob, "REDIS");
        assertThat(documents.status(42, id).currentVersion()).isZero();
        documents.complete(firstJob, "ES"); publish(firstIds); documents.completeCleanup(firstJob);
        var old = documents.hydrate(42, firstIds, List.of(id), vectors.target()).get(0);
        var parent = documents.expand(42, old);
        assertThat(parent.text()).contains(old.text()); assertThat(parent.text().length()).isGreaterThan(old.text().length());
        assertThat(documents.hydrate(99, firstIds, List.of())).isEmpty(); assertThat(documents.expand(99, old)).isNull();
        assertThat(documents.hydrate(42, firstIds, List.of("another-document"))).isEmpty();

        var second = importDocument(id, Visibility.OWNER, "新版轮椅 wheelchair 在线申请。".repeat(20));
        var secondJob = documents.claim(second.jobId(), vectors.target()); List<String> secondIds = project(secondJob);
        documents.complete(secondJob, "REDIS");
        assertThat(documents.hydrate(42, firstIds, List.of())).isNotEmpty();
        assertThat(documents.hydrate(42, secondIds, List.of())).isEmpty();
        documents.complete(secondJob, "ES"); publish(secondIds);
        // Keep obsolete engine records searchable deliberately; SQL must still reject their exact old version.
        assertThat(keywords.search(42, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(id), 50)).extracting(Hit::id).contains(firstIds.get(0));
        assertThat(documents.hydrate(42, firstIds, List.of())).isEmpty(); assertThat(documents.expand(42, old)).isNull();
        var current = documents.hydrate(42, secondIds, List.of(), vectors.target());
        assertThat(current).isNotEmpty().allSatisfy(candidate -> assertThat(candidate.version()).isEqualTo(2));
        documents.delete(42, id);
        assertThat(keywords.search(42, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(id), 50)).isNotEmpty();
        assertThat(documents.hydrate(42, secondIds, List.of())).isEmpty(); assertThat(documents.expand(42, current.get(0))).isNull();
        for (String child : engineIds) { vectors.delete(child); keywords.delete(child); }
        await(() -> vectors.search(42, "wheelchair", new float[]{1, 0, 0}, List.of(id), 50).isEmpty());
        assertThat(keywords.search(42, "wheelchair", Kind.KNOWLEDGE_CHILD, List.of(id), 50)).isEmpty();
    }

    @Test void realChannelsAllocateComplementaryQuotasThenRrf20Rerank5AndExpandCurrentParents() throws Exception {
        String text = "共享轮椅 wheelchair support 办理窗口材料。".repeat(12);
        for (int i = 0; i < 15; i++) {
            var imported = importDocument(documentId("guide" + i), Visibility.PUBLIC, text);
            var job = documents.claim(imported.jobId(), vectors.target()); var ids = project(job);
            documents.complete(job, "REDIS"); documents.complete(job, "ES"); publish(ids); documents.completeCleanup(job);
        }
        String query = "轮椅 wheelchair";
        await(() -> vectors.search(42, query, new float[]{1, .1f, 0}, List.of(), 50).size() == 50);
        var vectorHits = vectors.search(42, query, new float[]{1, .1f, 0}, List.of(), 50);
        var keywordHits = keywords.search(42, query, Kind.KNOWLEDGE_CHILD, List.of(), 50);
        assertThat(keywordHits).hasSize(50);
        assertThat(documents.hydrate(99, List.copyOf(engineIds), List.of(), vectors.target())).hasSize(engineIds.size());
        List<Hit> profiles = IntStream.range(0, 50).mapToObj(i -> new Hit("profile-it-" + i, 1.0 - i / 100.0)).toList();
        var allocated = RagRetrievalService.allocate(profiles, vectorHits, 10, 40);
        assertThat(allocated).hasSize(50); assertThat(allocated.stream().filter(hit -> hit.id().startsWith("profile-it-")).count()).isEqualTo(10);
        var fewerProfiles = RagRetrievalService.allocate(profiles.subList(0, 2), vectorHits, 10, 40);
        assertThat(fewerProfiles).hasSize(50);
        assertThat(fewerProfiles.stream().filter(hit -> hit.id().startsWith("profile-it-")).count()).isEqualTo(2);
        var fewerDocuments = RagRetrievalService.allocate(profiles, vectorHits.subList(0, 3), 10, 40);
        assertThat(fewerDocuments).hasSize(50);
        assertThat(fewerDocuments.stream().filter(hit -> !hit.id().startsWith("profile-it-")).count()).isEqualTo(3);
        assertThat(ReciprocalRankFusion.fuse(vectorHits.stream().map(Hit::id).toList(), keywordHits.stream().map(Hit::id).toList(), 60)).hasSize(20);
        int beforeEmbedding = models.embeddingCalls.get();
        var resolver = new RagRetrievalService.Resolver() {
            @Override public List<Candidate> hydrate(long user, List<String> ids, List<String> selected) {
                return documents.hydrate(user, ids, selected, vectors.target());
            }
            @Override public Evidence expand(long user, Candidate candidate) { return documents.expand(user, candidate); }
        };
        var reranker = new SiliconFlowReranker(properties);
        var retrieval = new RagRetrievalService(properties, retrievalExecutor, embedding, 3,
                (user, rewritten, vector, kind, selected, limit) -> kind == Kind.PROFILE_FACT ? List.of()
                        : vectors.search(user, rewritten, vector, selected, limit), keywords::search, resolver, reranker::rerank);
        var result = retrieval.retrieve(42, new Plan("原始问题保持不变", query, Scope.DOCUMENT, List.of()));
        assertThat(result.trace().vectorHits()).isEqualTo(50); assertThat(result.trace().keywordHits()).isEqualTo(50);
        assertThat(result.trace().fusedHits()).isEqualTo(20); assertThat(result.trace().rerankedHits()).isEqualTo(5);
        assertThat(result.trace().degradedChannels()).isEmpty();
        assertThat(result.evidence()).hasSizeBetween(1, 5).allSatisfy(evidence -> {
            assertThat(evidence.version()).isEqualTo(1); assertThat(evidence.text().length()).isGreaterThan(properties.getMaxChildChars());
            assertThat(evidence.matchedChildren()).isNotEmpty();
        });
        assertThat(result.evidence().stream().mapToInt(evidence -> evidence.matchedChildren().size()).sum()).isEqualTo(5);
        assertThat(models.embeddingCalls.get() - beforeEmbedding).isEqualTo(1); assertThat(models.rerankCalls.get()).isEqualTo(1);
        assertThat(models.lastRerank.get().path("documents").size()).isEqualTo(20);
        assertThat(models.lastRerank.get().path("top_n").asInt()).isEqualTo(5);
        assertThat(models.lastRerank.get().path("query").asText()).isEqualTo(query);
        assertThat(models.lastRerank.get().path("model").asText()).isEqualTo("Qwen/Qwen3-Reranker-0.6B");
        assertThat(models.lastRerank.get().path("instruction").asText()).isEqualTo(properties.getReranker().getInstruction());
        assertThat(models.lastRerank.get().path("return_documents").asBoolean()).isFalse();
        assertThat(models.lastRerank.get().has("max_chunks_per_doc")).isFalse();
    }

    private RagIndexRecord record(String suffix, long owner, Visibility visibility, boolean searchable, String text) {
        String id = "child_" + namespace + "_" + suffix;
        return new RagIndexRecord(id, Kind.KNOWLEDGE_CHILD, owner, visibility, "resource_" + namespace + "_" + suffix,
                1, "parent_" + suffix, "wheelchair guide", text, "fixture.md", 0, 0, 0, searchable);
    }
    private void vector(RagIndexRecord record, float[] vector) { engineIds.add(record.id()); vectors.upsert(record, vector); }
    private String documentId(String suffix) { String id = "it_" + namespace + "_" + suffix; documentIds.add(id); return id; }
    private JdbcDocumentStore.ImportResult importDocument(String id, Visibility visibility, String text) {
        var parsed = new DocumentEtl(properties).parse(("# 轮椅 wheelchair\n" + text).getBytes(StandardCharsets.UTF_8), "guide.md");
        return documents.importDocument(id, 42, visibility, "轮椅 wheelchair guide", "local:guide.md", parsed, vectors.target());
    }
    private List<String> project(JdbcDocumentStore.Job job) {
        assertThat(job).isNotNull(); var result = new ArrayList<String>();
        for (var child : documents.chunks(job)) {
            var record = new RagIndexRecord(child.id(), Kind.KNOWLEDGE_CHILD, child.ownerId(), child.visibility(), child.documentId(),
                    child.version(), child.parentId(), child.title(), child.text(), child.source(), child.pageStart(), child.pageEnd(), child.ordinal(), false);
            float[] vector = embedding.embed(child.text()); documents.cacheEmbedding(job, child.id(), vector);
            vector(record, vector); keywords.upsert(record); result.add(record.id());
        }
        return List.copyOf(result);
    }
    private void publish(List<String> ids) { for (String id : ids) { vectors.publish(id, true); keywords.publish(id, true); } }
    private JsonNode get(String path) { return RagIndexHttp.parse(requireSuccess(http.exchange(URI.create(ES + path), "GET", null))); }
    private static RagIndexHttp.Response requireSuccess(RagIndexHttp.Response response) {
        assertThat(response.successful()).as("Private fixture HTTP operation").isTrue(); return response;
    }
    private static void await(BooleanSupplier ready) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) { if (ready.getAsBoolean()) return; Thread.sleep(50); }
        assertThat(ready.getAsBoolean()).as("Real Redis search index becomes visible within ten seconds").isTrue();
    }

    /** Bounded local wire-protocol fixture: never proxies requests or loads account configuration. */
    private static final class LocalModels implements AutoCloseable {
        private static final int BODY_LIMIT = 128 * 1024;
        final AtomicInteger embeddingCalls = new AtomicInteger();
        final AtomicInteger rerankCalls = new AtomicInteger();
        final AtomicReference<JsonNode> lastRerank = new AtomicReference<>();
        final HttpServer server;
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(16), new ThreadPoolExecutor.AbortPolicy());
        LocalModels() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 16); server.setExecutor(executor);
            server.createContext("/v1/", exchange -> {
                try {
                    byte[] bytes = exchange.getRequestBody().readNBytes(BODY_LIMIT + 1);
                    if (bytes.length > BODY_LIMIT || !"Bearer ".concat(MARKER).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                        exchange.sendResponseHeaders(400, -1); return;
                    }
                    JsonNode body = RagIndexHttp.JSON.readTree(bytes); Object response;
                    if (exchange.getRequestURI().getPath().equals("/v1/embeddings")) {
                        embeddingCalls.incrementAndGet(); JsonNode input = body.path("input");
                        int size = input.isArray() ? input.size() : 1; var data = new ArrayList<Object>();
                        for (int i = 0; i < size; i++) data.add(Map.of("object", "embedding", "index", i, "embedding", List.of(1.0, .1, 0.0)));
                        response = Map.of("object", "list", "model", "rag-fixture-embedding", "data", data,
                                "usage", Map.of("prompt_tokens", size, "total_tokens", size));
                    } else if (exchange.getRequestURI().getPath().equals("/v1/rerank")) {
                        rerankCalls.incrementAndGet(); lastRerank.set(body);
                        int count = body.path("top_n").asInt();
                        if (body.path("documents").size() > 20 || count < 1 || count > 5) { exchange.sendResponseHeaders(400, -1); return; }
                        response = Map.of("results", IntStream.range(0, count).mapToObj(i -> Map.of("index", i, "relevance_score", 1.0 - i / 10.0)).toList());
                    } else { exchange.sendResponseHeaders(404, -1); return; }
                    byte[] outgoing = RagIndexHttp.JSON.writeValueAsBytes(response);
                    if (outgoing.length > BODY_LIMIT) { exchange.sendResponseHeaders(500, -1); return; }
                    exchange.getResponseHeaders().set("Content-Type", "application/json"); exchange.sendResponseHeaders(200, outgoing.length);
                    exchange.getResponseBody().write(outgoing);
                } catch (Exception ignored) {
                    // The client receives a bounded failure; payloads and account-like text are never logged.
                } finally { exchange.close(); }
            });
            server.start();
        }
        String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        @Override public void close() { server.stop(0); executor.shutdownNow(); }
    }
}
