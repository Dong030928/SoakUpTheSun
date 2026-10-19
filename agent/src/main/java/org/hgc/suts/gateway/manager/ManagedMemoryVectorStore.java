package org.hgc.suts.gateway.manager;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.redis.RedisVectorStore;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.SetParams;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Initializes lazily: missing Query Engine/JSON affects memory, never chat startup. */
@Slf4j
@Component
public class ManagedMemoryVectorStore {
    private final RedisClient redis;
    private final EmbeddingModel embedding;
    private final AiConfig ai;
    private final MemoryVectorProperties properties;
    private volatile RedisVectorStore store;
    private volatile long retryAt;

    public ManagedMemoryVectorStore(RedisClient redis, EmbeddingModel agentEmbeddingModel,
                                    AiConfig ai, MemoryVectorProperties properties) {
        this.redis = redis;
        this.embedding = new CheckedEmbeddingModel(agentEmbeddingModel, ai.getVector().getDimension());
        this.ai = ai;
        this.properties = properties;
        properties.validate();
    }

    public synchronized VectorStore getStore() {
        Assert.state(properties.isEnabled() && ai.isEmbeddingEnabled(), "Long-term memory is disabled");
        if (store != null) return store;
        if (System.currentTimeMillis() < retryAt) throw new IllegalStateException("Long-term memory is temporarily unavailable");
        try {
            var candidate = RedisVectorStore.builder(redis, embedding)
                    .indexName(properties.effectiveIndexName(ai)).prefix(properties.effectivePrefix(ai))
                    .metadataFields(RedisVectorStore.MetadataField.tag("userId"),
                            RedisVectorStore.MetadataField.tag("schemaVersion"),
                            RedisVectorStore.MetadataField.tag("embeddingModel"),
                            RedisVectorStore.MetadataField.tag("embeddingModelTag"),
                            RedisVectorStore.MetadataField.numeric("embeddingDimension"),
                            RedisVectorStore.MetadataField.numeric("createdAt"),
                            RedisVectorStore.MetadataField.tag("legacyKey"))
                    .initializeSchema(properties.isInitializeSchema()).build();
            candidate.afterPropertiesSet();
            validateSchema(redis.ftInfo(properties.effectiveIndexName(ai)), properties.effectivePrefix(ai),
                    ai.getVector().getDimension());
            redis.set(properties.manifestKey(ai), properties.identity(ai), SetParams.setParams().nx());
            Assert.state(properties.identity(ai).equals(redis.get(properties.manifestKey(ai))),
                    "Long-term memory namespace manifest does not match its embedding configuration");
            store = candidate;
            return store;
        } catch (RuntimeException ex) {
            retryAt = System.currentTimeMillis() + properties.getRetryDelayMillis();
            log.warn("长期记忆索引不可用，聊天将降级 ({})", ex.getClass().getSimpleName());
            throw new IllegalStateException("Long-term memory index is unavailable", ex);
        }
    }

    /** Verify RedisVectorStore's existing-index shortcut cannot hide a schema mismatch. */
    public static void validateSchema(Map<String, Object> info, String prefix, int dimension) {
        Map<String, Object> definition = asMap(info.get("index_definition"));
        Assert.state("JSON".equalsIgnoreCase(string(definition.get("key_type"))), "Memory index must index JSON");
        Object prefixes = definition.get("prefixes");
        Assert.state(prefixes instanceof List<?> list && list.size() == 1 && prefix.equals(string(list.get(0))),
                "Memory index prefix does not match configuration");
        Assert.state(info.get("attributes") instanceof List<?>, "Memory index attributes are missing");
        Map<String, Map<String, Object>> fields = new HashMap<>();
        for (Object value : (List<?>) info.get("attributes")) {
            var field = asMap(value);
            fields.put(string(field.get("attribute")), field);
        }
        requireField(fields, "content", "TEXT");
        requireField(fields, "userId", "TAG");
        requireField(fields, "schemaVersion", "TAG");
        requireField(fields, "embeddingModel", "TAG");
        requireField(fields, "embeddingModelTag", "TAG");
        requireField(fields, "embeddingDimension", "NUMERIC");
        requireField(fields, "createdAt", "NUMERIC");
        var vector = requireField(fields, "embedding", "VECTOR");
        Assert.state(String.valueOf(dimension).equals(string(vector.get("dim"))), "Memory index dimension does not match configuration");
        Assert.state("FLOAT32".equalsIgnoreCase(string(vector.get("data_type")))
                        && "COSINE".equalsIgnoreCase(string(vector.get("distance_metric"))),
                "Memory index vector format does not match configuration");
    }

    private static Map<String, Object> requireField(Map<String, Map<String, Object>> fields, String name, String type) {
        var field = fields.get(name);
        Assert.state(field != null && type.equalsIgnoreCase(string(field.get("type")))
                && ("$." + name).equals(string(field.get("identifier"))), "Memory index field does not match: " + name);
        return field;
    }

    private static Map<String, Object> asMap(Object value) {
        Map<String, Object> result = new HashMap<>();
        if (value instanceof Map<?, ?> map) map.forEach((key, item) -> result.put(string(key), item));
        else if (value instanceof List<?> list) {
            for (int i = 0; i + 1 < list.size(); i += 2) result.put(string(list.get(i)), list.get(i + 1));
        }
        return result;
    }

    private static String string(Object value) {
        return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value);
    }
}
