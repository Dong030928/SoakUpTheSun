package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.*;
import org.hgc.suts.gateway.manager.CheckedEmbeddingModel;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import redis.clients.jedis.*;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.rag.RagData.*;

/** Explicit opt-in. One real batch embedding and one rerank call using synthetic public passages only. */
class LiveRagRetrievalIT {
    @Test void realSiliconFlowEmbeddingAndRerankerOrderChineseEvidenceFromBothEngines() throws Exception {
        assertThat(System.getProperty("live.rag")).as("Explicit opt-in to two small paid calls").isEqualTo("true");
        assertThat(System.getProperty("rag.retrieval.fixture")).isEqualTo("agent-rag-integration-only");
        Path secret = Path.of(Objects.requireNonNull(System.getProperty("live.memory.config"))).toRealPath();
        assertThat(secret.startsWith(Path.of("").toAbsolutePath())).isFalse();
        var sources = new YamlPropertySourceLoader().load("private-rag", new FileSystemResource(secret));
        var ai = new AiConfig(); ai.setEmbeddingHost(property(sources, "suts.ai.embedding-host"));
        ai.setEmbeddingApiKey(property(sources, "suts.ai.embedding-api-key"));
        ai.setEmbeddingModelName(property(sources, "suts.ai.embedding-model-name"));
        ai.getVector().setDimension(Integer.parseInt(property(sources, "suts.ai.vector.dimension")));
        ai.setEmbeddingTimeout(15000);
        assertThat(java.net.URI.create(ai.getEmbeddingHost()).getHost()).isEqualTo("api.siliconflow.cn");
        var config = new RagProperties();
        config.getElasticsearch().setEndpoint("http://127.0.0.1:19200");
        String namespace = UUID.randomUUID().toString().replace("-", "");
        config.getElasticsearch().setIndexName("suts-rag-live-" + namespace);
        config.setIndexName("suts:agent:rag:documents:index:live:" + namespace);
        config.setPrefix("suts:agent:rag:documents:data:live:" + namespace + ":");
        String configuredRerankKey = optional(sources, "suts.ai.rag.reranker.api-key");
        config.getReranker().setApiKey(configuredRerankKey.isBlank() ? ai.getEmbeddingApiKey() : configuredRerankKey);
        config.getReranker().setTimeoutMs(15000);
        var embedding = new CheckedEmbeddingModel(new AiModelConfiguration().agentEmbeddingModel(ai), ai.getVector().getDimension());
        String query = "演示读屏终端应该怎样打开帮助文档？";
        List<String> passages = List.of(
                "演示读屏终端操作：第一步按下帮助键，第二步选择帮助文档，第三步按确认键打开并朗读文档。",
                "演示设备电量查询：按下电量键后会播报剩余百分比。",
                "演示资料介绍了茶叶的种类、采摘时间和冲泡温度。",
                "演示室内导航：前往会议室需先到前台，再沿指示标志到达。",
                "演示读屏终端音量设置：进入声音选项后调整朗读音量。",
                "演示资料介绍云层和降雨量的记录方式。");
        long started = System.nanoTime();
        List<String> inputs = new ArrayList<>(passages); inputs.add(query);
        List<float[]> prepared;
        try { prepared = embedding.embed(inputs); }
        catch (RuntimeException failure) { throw new IllegalStateException("Live RAG embedding failed (" + failure.getClass().getSimpleName() + ")"); }
        assertThat(prepared).hasSize(7);
        try (var redis = RedisClient.builder().hostAndPort("127.0.0.1", 16393).clientConfig(DefaultJedisClientConfig.builder()
                .password("agent-rag-integration-only").connectionTimeoutMillis(2000).socketTimeoutMillis(5000).build()).build()) {
            assertThat(redis.get("suts:agent:test:rag-retrieval-fixture")).isEqualTo("agent-rag-integration-only");
            var http = new RagIndexHttp(10000, 1024 * 1024, Map.of());
            assertThat(RagIndexHttp.parse(http.exchange(java.net.URI.create("http://127.0.0.1:19200/"), "GET", null))
                    .path("cluster_name").asText()).isEqualTo("suts-agent-rag-retrieval-it");
            var vector = new RedisRagIndex(redis, embedding, config, ai.getVector().getDimension(), new MemoryVectorProperties().fingerprint(ai));
            var keyword = new ElasticsearchRagIndex(config); List<Candidate> all = new ArrayList<>();
            try {
                keyword.initialize();
                for (int i = 0; i < passages.size(); i++) {
                    String id = "live_" + i;
                    var record = new RagIndexRecord(id, Kind.KNOWLEDGE_CHILD, 42, Visibility.PUBLIC, "demo-guide", 1,
                            "parent_" + i, "合成演示说明", passages.get(i), "synthetic-demo.md", 0, 0, i, true);
                    vector.upsert(record, prepared.get(i)); keyword.upsert(record);
                    all.add(new Candidate(id, record.kind(), record.resourceId(), record.version(), record.parentId(), record.title(),
                            record.text(), record.source(), 0, 0, i));
                }
                var vectorHits = vector.search(42, query, prepared.get(6), List.of(), 50);
                var keywordHits = keyword.search(42, query, Kind.KNOWLEDGE_CHILD, List.of(), 50);
                assertThat(vectorHits).isNotEmpty(); assertThat(keywordHits).isNotEmpty();
                var fused = ReciprocalRankFusion.fuse(vectorHits.stream().map(Hit::id).toList(), keywordHits.stream().map(Hit::id).toList(), 60);
                List<Candidate> candidates = fused.stream().map(hit -> all.stream().filter(c -> c.id().equals(hit.id())).findFirst().orElseThrow()).toList();
                List<Reranked> ranked;
                long rerankStarted = System.nanoTime();
                try { ranked = new SiliconFlowReranker(config).rerank(query, candidates, 5); }
                catch (RuntimeException failure) { throw new IllegalStateException("Live RAG reranker failed (" + failure.getClass().getSimpleName() + ")"); }
                assertThat(ranked).hasSize(5); assertThat(candidates.get(ranked.get(0).index()).id()).isEqualTo("live_0");
                var report = Map.of("testedAt", Instant.now().toString(), "embeddingModel", ai.getEmbeddingModelName(),
                        "rerankerModel", config.getReranker().getModel(), "embeddingRequests", 1, "rerankerRequests", 1,
                        "vectorHits", vectorHits.size(), "keywordHits", keywordHits.size(), "ranking", ranked.stream()
                                .map(r -> Map.of("id", candidates.get(r.index()).id(), "score", r.score())).toList(),
                        "elapsedMs", (System.nanoTime() - started) / 1000000, "rerankMs", (System.nanoTime() - rerankStarted) / 1000000);
                Path output = Path.of("target", "live-rag-report.json"); Files.createDirectories(output.getParent());
                Files.writeString(output, RagIndexHttp.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
            } finally {
                for (int i = 0; i < passages.size(); i++) vector.delete("live_" + i);
                if (redis.ftList().contains(vector.indexName())) redis.ftDropIndex(vector.indexName());
                http.exchange(java.net.URI.create("http://127.0.0.1:19200/" + config.getElasticsearch().getIndexName()), "DELETE", null);
            }
        }
    }
    private String property(List<PropertySource<?>> sources, String key) {
        String value = optional(sources, key); if (value.isBlank()) throw new IllegalStateException("Missing private live-test property: " + key); return value;
    }
    private String optional(List<PropertySource<?>> sources, String key) {
        return sources.stream().map(source -> source.getProperty(key)).filter(Objects::nonNull).map(Object::toString).findFirst().orElse("");
    }
}
