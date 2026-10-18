package org.hgc.suts.gateway.service.impl;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.manager.ManagedMemoryVectorStore;
import org.hgc.suts.gateway.service.VectorStorageService;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import redis.clients.jedis.RedisClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Ownership is a mandatory server-side filter, never a caller-supplied expression. */
@Service
public class RedisVectorStorageServiceImpl implements VectorStorageService {
    private final ManagedMemoryVectorStore managedStore;
    private final MemoryVectorProperties properties;
    private final AiConfig ai;
    private final RedisClient redis;

    public RedisVectorStorageServiceImpl(ManagedMemoryVectorStore managedStore, MemoryVectorProperties properties,
                                        AiConfig ai, RedisClient redis) {
        this.managedStore = managedStore;
        this.properties = properties;
        this.ai = ai;
        this.redis = redis;
    }

    @Override
    public void saveMemory(Long userId, String content) {
        if (!properties.isEnabled() || !ai.isEmbeddingEnabled() || !properties.isWritesEnabled()) return;
        write(userId, content, UUID.randomUUID().toString(), null);
    }

    /** Only the opt-in migration command can import while normal application writes are frozen. */
    public void importMemory(Long userId, String content, String deterministicId, String legacyKey) {
        Assert.state(!properties.isWritesEnabled(), "Freeze vector writes before importing legacy memories");
        Assert.hasText(legacyKey, "Legacy source key is required");
        write(userId, content, deterministicId, legacyKey);
    }

    public boolean containsDocument(String id) {
        validateId(id);
        return redis.exists(properties.effectivePrefix(ai) + id);
    }

    private void write(Long userId, String content, String id, String legacyKey) {
        validateOwner(userId);
        validateId(id);
        Assert.hasText(content, "Memory content must not be empty");
        Assert.isTrue(content.length() <= properties.getMaxDocumentChars(), "Memory content exceeds its character budget");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("userId", userId.toString());
        metadata.put("schemaVersion", MemoryVectorProperties.SCHEMA_VERSION);
        metadata.put("embeddingModel", ai.getEmbeddingModelName());
        metadata.put("embeddingModelTag", properties.embeddingModelTag(ai));
        metadata.put("embeddingDimension", ai.getVector().getDimension());
        metadata.put("createdAt", System.currentTimeMillis());
        if (legacyKey != null) metadata.put("legacyKey", legacyKey);
        managedStore.getStore().add(List.of(Document.builder().id(id).text(content).metadata(metadata).build()));
    }

    @Override
    public List<String> searchMemory(Long userId, String query, int limit) {
        validateOwner(userId);
        Assert.hasText(query, "Memory query must not be empty");
        Assert.isTrue(query.length() <= properties.getMaxDocumentChars(), "Memory query exceeds its character budget");
        Assert.isTrue(limit > 0, "Memory result limit must be positive");
        if (!properties.isEnabled() || !ai.isEmbeddingEnabled()) return List.of();
        var b = new FilterExpressionBuilder();
        var filter = b.and(b.eq("userId", userId.toString()),
                b.and(b.eq("schemaVersion", MemoryVectorProperties.SCHEMA_VERSION),
                        b.and(b.eq("embeddingModelTag", properties.embeddingModelTag(ai)),
                                b.eq("embeddingDimension", ai.getVector().getDimension())))).build();
        var request = SearchRequest.builder().query(query).topK(Math.min(limit, properties.getTopK()))
                .similarityThreshold(properties.getSimilarityThreshold()).filterExpression(filter).build();
        List<Document> documents = managedStore.getStore().similaritySearch(request);
        var result = new ArrayList<String>();
        var seen = new HashSet<String>();
        int remaining = properties.getMaxRecallChars();
        if (documents == null) return List.of();
        for (Document document : documents) {
            if (document == null || !matchesOwnerAndSchema(document, userId)
                    || document.getScore() == null || !Double.isFinite(document.getScore())
                    || document.getScore() < properties.getSimilarityThreshold()
                    || !StringUtils.hasText(document.getText()) || !seen.add(document.getText())) continue;
            String text = document.getText();
            if (text.length() > remaining) text = text.substring(0, remaining);
            result.add(text);
            remaining -= text.length();
            if (remaining == 0 || result.size() >= request.getTopK()) break;
        }
        return List.copyOf(result);
    }

    private boolean matchesOwnerAndSchema(Document document, Long userId) {
        Map<String, Object> metadata = document.getMetadata();
        return userId.toString().equals(String.valueOf(metadata.get("userId")))
                && MemoryVectorProperties.SCHEMA_VERSION.equals(String.valueOf(metadata.get("schemaVersion")))
                && ai.getEmbeddingModelName().equals(String.valueOf(metadata.get("embeddingModel")))
                && properties.embeddingModelTag(ai).equals(String.valueOf(metadata.get("embeddingModelTag")))
                && String.valueOf(ai.getVector().getDimension()).equals(String.valueOf(metadata.get("embeddingDimension")));
    }

    private static void validateOwner(Long userId) {
        Assert.isTrue(userId != null && userId > 0, "Authenticated memory owner is required");
    }

    private static void validateId(String id) {
        Assert.isTrue(id != null && id.matches("[A-Za-z0-9_-]{1,128}"), "Memory document ID is invalid");
    }
}
