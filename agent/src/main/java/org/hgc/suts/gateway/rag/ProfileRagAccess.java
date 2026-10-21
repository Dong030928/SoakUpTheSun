package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.memory.profile.JdbcProfileStore;
import org.hgc.suts.gateway.memory.profile.ProfileData;
import org.hgc.suts.gateway.memory.profile.ProfileVectorIndex;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.ai.vectorstore.redis.RedisVectorStore;
import org.springframework.util.Assert;
import redis.clients.jedis.RedisClient;

import java.math.BigDecimal;
import java.util.*;

/** RAG access to R01's existing profile vectors, always hydrated from current SQL facts. */
public final class ProfileRagAccess {
    private static final RedisVectorStore.MetadataField[] FIELDS = {
            RedisVectorStore.MetadataField.tag("userId"), RedisVectorStore.MetadataField.tag("schemaVersion"),
            RedisVectorStore.MetadataField.tag("targetTag"), RedisVectorStore.MetadataField.tag("recordType"),
            RedisVectorStore.MetadataField.tag("factId"), RedisVectorStore.MetadataField.numeric("factVersion")
    };
    private final RedisClient redis;
    private final ProfileVectorIndex vectors;
    private final JdbcProfileStore facts;
    private final int dimension;
    private final double threshold;

    public ProfileRagAccess(RedisClient redis, ProfileVectorIndex vectors, JdbcProfileStore facts, int dimension, double similarityThreshold) {
        Assert.notNull(redis, "Profile Redis client is required"); Assert.notNull(vectors, "Profile vector index is required");
        Assert.notNull(facts, "Profile facts are required"); Assert.isTrue(dimension > 0, "Invalid profile vector dimension");
        Assert.isTrue(Double.isFinite(similarityThreshold) && similarityThreshold >= 0 && similarityThreshold <= 1, "Invalid profile similarity threshold");
        this.redis = redis; this.vectors = vectors; this.facts = facts; this.dimension = dimension; this.threshold = similarityThreshold;
    }

    public List<RagData.Hit> search(long user, String query, float[] vector, int limit) {
        Assert.isTrue(user > 0 && query != null && !query.isBlank() && query.length() <= 4000 && limit > 0 && limit <= 50, "Invalid profile RAG search");
        var fixed = new FixedQueryEmbeddingModel(query, vector, dimension);
        Assert.state(vectors.enabled(), "Profile vectors are disabled");
        RedisRagIndex.validateSchema(redis.ftInfo(vectors.indexName()), vectors.prefix(), dimension, FIELDS);
        var f = new FilterExpressionBuilder();
        var filter = f.and(f.eq("userId", Long.toString(user)), f.and(f.eq("schemaVersion", "profile1"),
                f.and(f.eq("targetTag", facts.target()), f.eq("recordType", "profile")))).build();
        var documents = RedisVectorStore.builder(redis, fixed).indexName(vectors.indexName()).prefix(vectors.prefix())
                .metadataFields(FIELDS).initializeSchema(false).build().similaritySearch(SearchRequest.builder().query(query)
                        .topK(Math.min(200, limit * 4)).similarityThreshold(threshold).filterExpression(filter).build());
        var valid = new LinkedHashMap<String, RagData.Hit>();
        documents.stream().filter(document -> document.getScore() != null && Double.isFinite(document.getScore()))
                .sorted(Comparator.comparingDouble((Document document) -> document.getScore()).reversed().thenComparing(Document::getId))
                .forEach(document -> {
                    Object id = document.getMetadata().get("factId"), version = document.getMetadata().get("factVersion");
                    if (!(id instanceof String key) || !key.matches("[a-f0-9]{64}") || version == null) return;
                    long number;
                    try { number = new BigDecimal(version.toString()).longValueExact(); } catch (RuntimeException invalid) { return; }
                    if (number < 1 || !ProfileData.documentId(key, number).equals(document.getId())) return;
                    var fact = facts.fact(user, key);
                    if (fact.isPresent() && "ACTIVE".equals(fact.get().state()) && fact.get().version() == number)
                        valid.putIfAbsent(candidateId(key, number), new RagData.Hit(candidateId(key, number), document.getScore()));
                });
        return valid.values().stream().limit(limit).toList();
    }

    public List<RagData.Candidate> hydrate(long user, List<String> ids) {
        Assert.isTrue(user > 0 && ids != null && ids.size() <= 50, "Invalid profile candidate hydration");
        var candidates = new ArrayList<RagData.Candidate>();
        for (String id : new LinkedHashSet<>(ids)) {
            if (id == null || !id.matches("profile:[a-f0-9]{64}:[1-9][0-9]{0,18}")) continue;
            String[] parts = id.split(":"); long version;
            try { version = Long.parseLong(parts[2]); } catch (NumberFormatException invalid) { continue; }
            var fact = facts.fact(user, parts[1]);
            if (fact.isPresent() && "ACTIVE".equals(fact.get().state()) && fact.get().version() == version) candidates.add(candidate(fact.get()));
        }
        return List.copyOf(candidates);
    }

    public static String candidateId(ProfileData.Fact fact) { return candidateId(fact.id(), fact.version()); }
    public static String candidateId(String factId, long version) {
        Assert.isTrue(factId != null && factId.matches("[a-f0-9]{64}") && version > 0, "Invalid profile candidate identity");
        return "profile:" + factId + ":" + version;
    }
    public static RagData.Candidate candidate(ProfileData.Fact fact) {
        return new RagData.Candidate(candidateId(fact), RagData.Kind.PROFILE_FACT, fact.id(), fact.version(), "",
                fact.attribute(), fact.attribute() + ": " + fact.value(), "profile:" + fact.id(), 0, 0, 0);
    }
}
