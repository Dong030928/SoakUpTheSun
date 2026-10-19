package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryProfileProperties;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.manager.CheckedEmbeddingModel;
import org.hgc.suts.gateway.manager.ManagedMemoryVectorStore;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.ai.vectorstore.redis.RedisVectorStore;
import org.springframework.util.Assert;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.SetParams;

import java.util.*;

import static org.hgc.suts.gateway.memory.profile.ProfileData.*;

/** Separate HNSW namespace; SQL revalidation prevents stale or deleted vectors being returned. */
public final class ProfileVectorIndex {
    private final RedisClient redis;
    private final EmbeddingModel embedding;
    private final AiConfig ai;
    private final MemoryProfileProperties config;
    private final MemoryVectorProperties identity = new MemoryVectorProperties();
    private final JdbcProfileStore facts;
    private final String frozenTarget;
    private volatile RedisVectorStore store;
    private volatile long retryAt;

    public ProfileVectorIndex(RedisClient redis, EmbeddingModel embedding, AiConfig ai, MemoryProfileProperties config, JdbcProfileStore facts) {
        this.redis = redis; this.embedding = new CheckedEmbeddingModel(embedding, ai.getVector().getDimension());
        this.ai = ai; this.config = config; this.facts = facts; this.frozenTarget = target(ai, config);
    }
    public static String target(AiConfig ai, MemoryProfileProperties config) {
        String model = ai.isEmbeddingEnabled() ? new MemoryVectorProperties().fingerprint(ai) : "disabled";
        return org.hgc.suts.gateway.memory.summary.SummaryPolicy.hash("profile1|" + model + "|" + config.getIndexName() + "|" + config.getPrefix());
    }
    public boolean enabled() { return config.isVectorEnabled() && ai.isEmbeddingEnabled(); }
    public String indexName() { return config.getIndexName() + ":" + frozenTarget; }
    public String prefix() { return config.getPrefix() + frozenTarget + ":"; }

    private synchronized RedisVectorStore store() {
        Assert.state(enabled(), "Profile vectors are disabled");
        Assert.state(frozenTarget.equals(target(ai, config)), "Restart and reindex profiles after changing the index/model identity");
        if (store != null) return store;
        Assert.state(System.currentTimeMillis() >= retryAt, "Profile index retry is pending");
        try {
            String manifest = config.getPrefix() + "manifest:" + frozenTarget;
            String signature = "profile-v1\n" + identity.identity(ai);
            redis.set(manifest, signature, SetParams.setParams().nx());
            Assert.state(signature.equals(redis.get(manifest)), "Profile vector model/schema identity mismatch");
            var candidate = RedisVectorStore.builder(redis, embedding).indexName(indexName()).prefix(prefix())
                    .vectorAlgorithm(RedisVectorStore.Algorithm.HNSW).initializeSchema(true)
                    .metadataFields(RedisVectorStore.MetadataField.tag("userId"), RedisVectorStore.MetadataField.tag("schemaVersion"),
                            RedisVectorStore.MetadataField.tag("embeddingModel"), RedisVectorStore.MetadataField.tag("embeddingModelTag"),
                            RedisVectorStore.MetadataField.numeric("embeddingDimension"), RedisVectorStore.MetadataField.numeric("createdAt"),
                            RedisVectorStore.MetadataField.tag("factId"), RedisVectorStore.MetadataField.numeric("factVersion"),
                            RedisVectorStore.MetadataField.tag("recordType"), RedisVectorStore.MetadataField.tag("targetTag"))
                    .build();
            candidate.afterPropertiesSet();
            Map<String, Object> info = redis.ftInfo(indexName());
            ManagedMemoryVectorStore.validateSchema(info, prefix(), ai.getVector().getDimension());
            validateProfileFields(info);
            store = candidate; return candidate;
        } catch (Exception failure) {
            retryAt = System.currentTimeMillis() + 1000;
            throw new IllegalStateException("Profile vector index is unavailable", failure);
        }
    }
    public void write(Fact fact) {
        Assert.state("ACTIVE".equals(fact.state()), "Only active facts may be embedded");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("userId", String.valueOf(fact.userId())); metadata.put("schemaVersion", "profile1");
        metadata.put("embeddingModel", ai.getEmbeddingModelName()); metadata.put("embeddingModelTag", identity.embeddingModelTag(ai));
        metadata.put("embeddingDimension", ai.getVector().getDimension()); metadata.put("createdAt", fact.updatedAt());
        metadata.put("factId", fact.id()); metadata.put("factVersion", fact.version());
        metadata.put("recordType", "profile"); metadata.put("targetTag", frozenTarget);
        store().add(List.of(Document.builder().id(documentId(fact.id(), fact.version()))
                .text(fact.attribute() + ": " + fact.value()).metadata(metadata).build()));
    }
    public void delete(String fact, long version) {
        store();
        // DEL is idempotent for absent JSON documents; framework delete logs an error for a normal retry of a missing key.
        redis.del(prefix() + documentId(fact, version));
    }
    static void validateProfileFields(Map<String, Object> info) {
        Assert.state(info.get("attributes") instanceof List<?>, "Profile index attributes are missing");
        Map<String, Map<String, String>> attributes = new HashMap<>();
        for (Object row : (List<?>) info.get("attributes")) {
            Map<String, String> fields = new HashMap<>();
            if (row instanceof Map<?, ?> map) map.forEach((key, value) -> fields.put(text(key), text(value)));
            else if (row instanceof List<?> list) for (int i = 0; i + 1 < list.size(); i += 2) fields.put(text(list.get(i)), text(list.get(i + 1)));
            attributes.put(fields.get("attribute"), fields);
        }
        for (String key : List.of("factId", "factVersion", "targetTag", "recordType")) {
            var field = attributes.get(key); String type = key.equals("factVersion") ? "NUMERIC" : "TAG";
            Assert.state(field != null && type.equalsIgnoreCase(field.get("type")) && ("$." + key).equals(field.get("identifier")),
                    "Existing index has invalid profile field: " + key);
        }
        Assert.state(attributes.containsKey("embedding") && "HNSW".equalsIgnoreCase(attributes.get("embedding").get("algorithm")),
                "Profile vectors require an HNSW index");
    }
    private static String text(Object value) {
        return value instanceof byte[] bytes ? new String(bytes, java.nio.charset.StandardCharsets.UTF_8) : String.valueOf(value);
    }
    public List<Fact> search(long user, String query, int limit) {
        Assert.isTrue(user > 0 && query != null && !query.isBlank() && query.length() <= 4000 && limit > 0 && limit <= 50,
                "Invalid profile search");
        var f = new FilterExpressionBuilder();
        var filter = f.and(f.eq("userId", String.valueOf(user)), f.and(f.eq("schemaVersion", "profile1"),
                f.and(f.eq("targetTag", frozenTarget), f.eq("recordType", "profile")))).build();
        var docs = store().similaritySearch(SearchRequest.builder().query(query).topK(Math.min(200, limit * 4))
                .similarityThreshold(config.getSimilarityThreshold()).filterExpression(filter).build());
        Map<String, Fact> valid = new LinkedHashMap<>();
        for (Document document : docs) {
            Object id = document.getMetadata().get("factId"), version = document.getMetadata().get("factVersion");
            if (!(id instanceof String key) || !key.matches("[a-f0-9]{64}") || version == null) continue;
            long number;
            try { number = new java.math.BigDecimal(version.toString()).longValueExact(); } catch (RuntimeException invalid) { continue; }
            var fact = facts.fact(user, key);
            if (fact.isPresent() && "ACTIVE".equals(fact.get().state()) && fact.get().version() == number
                    && documentId(key, number).equals(document.getId())) valid.putIfAbsent(key, fact.get());
            if (valid.size() >= limit) break;
        }
        return List.copyOf(valid.values());
    }
}
