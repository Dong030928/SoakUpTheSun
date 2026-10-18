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
import redis.clients.jedis.json.Path;

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
    private java.util.function.Supplier<org.hgc.suts.gateway.memory.summary.JdbcSummaryJobs> summaryJobs = () -> null;
    private java.util.function.Supplier<org.hgc.suts.gateway.memory.summary.RedisSummaryStateStore> summaryStates = () -> null;
    private java.util.function.Supplier<org.hgc.suts.gateway.memory.summary.SummaryPolicy> summaryPolicy = () -> null;

    @org.springframework.beans.factory.annotation.Autowired
    public void summaryPolicy(org.springframework.beans.factory.ObjectProvider<org.hgc.suts.gateway.memory.summary.SummaryPolicy> policy) {
        summaryPolicy = policy::getIfAvailable;
    }

    @org.springframework.beans.factory.annotation.Autowired
    public void summaryValidation(org.springframework.beans.factory.ObjectProvider<org.hgc.suts.gateway.memory.summary.JdbcSummaryJobs> jobs,
            org.springframework.beans.factory.ObjectProvider<org.hgc.suts.gateway.memory.summary.RedisSummaryStateStore> states) {
        summaryJobs = jobs::getIfAvailable; summaryStates = states::getIfAvailable;
    }

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
    @Override public void saveTurn(Long userId, String content, org.hgc.suts.gateway.memory.summary.SummaryContext.Source source) {
        if (!properties.isEnabled() || !ai.isEmbeddingEnabled() || !properties.isWritesEnabled()) return;
        validateOwner(userId);
        org.hgc.suts.gateway.memory.archive.ArchivedTurn.validateIdentity(userId, source.conversationId(), source.epoch());
        Assert.isTrue(source.sequence() > 0 && source.sequence() <= 9999999999999L, "Invalid memory source sequence");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("memoryType", "conversation_turn"); metadata.put("conversationId", source.conversationId());
        metadata.put("epoch", source.epoch()); metadata.put("turnSequence", source.sequence());
        write(userId, content, UUID.randomUUID().toString(), null, metadata);
    }
    @Override public String saveSummary(SummaryDocument document) {
        if (!properties.isEnabled() || !ai.isEmbeddingEnabled() || !properties.isWritesEnabled()) return "SKIPPED";
        if (!currentSummary(document.userId(), document.conversationId(), document.epoch(), document.version())) return "OBSOLETE";
        var metadata = new HashMap<String, Object>();
        metadata.put("memoryType", "conversation_summary"); metadata.put("conversationId", document.conversationId());
        metadata.put("epoch", document.epoch()); metadata.put("summaryVersion", document.version());
        metadata.put("coveredFrom", document.from()); metadata.put("coveredTo", document.to());
        write(document.userId(), document.text(), document.documentId(), null, metadata);
        if (!currentSummary(document.userId(), document.conversationId(), document.epoch(), document.version())) {
            deleteSummaryDocuments(List.of(document.documentId())); return "OBSOLETE";
        }
        return "WRITTEN";
    }
    @Override public void deleteSummaryDocuments(List<String> ids) {
        if (!properties.isEnabled() || !ai.isEmbeddingEnabled() || !properties.isWritesEnabled() || ids.isEmpty()) return;
        ids.forEach(RedisVectorStorageServiceImpl::validateId);
        managedStore.getStore().delete(ids);
    }
    private boolean currentSummary(long user, String id, long epoch, long version) {
        var policy = summaryPolicy.get();
        if (policy != null && !policy.current().isVectorEnabled()) return false;
        var jobs = summaryJobs.get();
        if (jobs == null || !jobs.isCurrent(user, id, epoch, version)) return false;
        var states = summaryStates.get();
        var state = states == null ? null : states.read(id);
        return state != null && state.epoch() == epoch && state.version() == version;
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
        write(userId, content, id, legacyKey, Map.of());
    }
    private void write(Long userId, String content, String id, String legacyKey, Map<String, Object> source) {
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
        metadata.putAll(source);
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
        int requested = Math.min(limit, properties.getTopK());
        var request = SearchRequest.builder().query(query).topK(summaryJobs.get() == null ? requested : Math.min(100, requested * 4))
                .similarityThreshold(properties.getSimilarityThreshold()).filterExpression(filter).build();
        List<Document> documents = managedStore.getStore().similaritySearch(request);
        var result = new ArrayList<String>();
        var seen = new HashSet<String>();
        int remaining = properties.getMaxRecallChars();
        if (documents == null) return List.of();
        // Validate latest version before ranking/dedup. A delayed old worker can never reactivate its document.
        documents = attachSources(documents.stream().filter(d -> d != null && matchesOwnerAndSchema(d, userId)
                        && d.getScore() != null && Double.isFinite(d.getScore()) && d.getScore() >= properties.getSimilarityThreshold()
                        && StringUtils.hasText(d.getText())).toList())
                .stream().filter(d -> validSource(d, userId)).toList();
        var summaries = documents.stream().filter(d -> "conversation_summary".equals(d.getMetadata().get("memoryType"))).toList();
        for (Document document : documents) {
            if (document == null || !matchesOwnerAndSchema(document, userId)
                    || document.getScore() == null || !Double.isFinite(document.getScore())
                    || document.getScore() < properties.getSimilarityThreshold()
                    || !StringUtils.hasText(document.getText()) || !seen.add(document.getText()) || coveredTurn(document, summaries)) continue;
            String text = sourceLabel(document) + document.getText();
            if (text.length() > remaining) text = text.substring(0, remaining);
            result.add(text);
            remaining -= text.length();
            if (remaining == 0 || result.size() >= requested) break;
        }
        return List.copyOf(result);
    }
    private boolean validSource(Document document, long user) {
        if (document.getId().startsWith("summary_") && !"conversation_summary".equals(document.getMetadata().get("memoryType"))) return false;
        if (!"conversation_summary".equals(document.getMetadata().get("memoryType"))) return true;
        var m = document.getMetadata();
        try {
            String id = (String) m.get("conversationId"); long epoch = number(m, "epoch"), version = number(m, "summaryVersion");
            String expected = "summary_" + org.hgc.suts.gateway.memory.summary.SummaryPolicy.hash(id + "|" + epoch + "|" + version);
            return expected.equals(document.getId()) && currentSummary(user, id, epoch, version);
        }
        catch (IllegalArgumentException | NullPointerException | ArithmeticException invalid) { return false; }
    }
    private static long number(Map<String, Object> metadata, String key) {
        return new java.math.BigDecimal(String.valueOf(metadata.get(key))).longValueExact();
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    private List<Document> attachSources(List<Document> documents) {
        if (documents.isEmpty()) return documents;
        // Spring AI 2.0.1 returns only metadataFields declared on the index. Read the new provenance in one
        // bounded JSON.MGET, so existing indexes/documents need no destructive recreation or FT.ALTER.
        String[] keys = documents.stream().map(d -> properties.effectivePrefix(ai) + d.getId()).toArray(String[]::new);
        List<Map> rows = redis.jsonMGet(Path.ROOT_PATH, Map.class, keys);
        if (rows == null || rows.size() != documents.size()) {
            Assert.state(summaryJobs.get() == null, "Summary provenance could not be read completely");
            return documents;
        }
        var result = new ArrayList<Document>();
        for (int i = 0; i < documents.size(); i++) {
            var document = documents.get(i); Map row = rows.get(i);
            if (row == null) continue; // Deleted between the indexed search and provenance read.
            if (!String.valueOf(row.get("userId")).equals(String.valueOf(document.getMetadata().get("userId")))) continue;
            var metadata = new HashMap<>(document.getMetadata());
            for (String field : List.of("memoryType", "conversationId", "epoch", "turnSequence", "summaryVersion", "coveredFrom", "coveredTo")) {
                if (row.containsKey(field)) metadata.put(field, row.get(field));
            }
            result.add(Document.builder().id(document.getId()).text(document.getText()).metadata(metadata).score(document.getScore()).build());
        }
        return result;
    }
    private static boolean coveredTurn(Document turn, List<Document> summaries) {
        var source = turn.getMetadata();
        if (!"conversation_turn".equals(source.get("memoryType"))) return false; // Legacy records have no provable source coverage.
        try {
            return summaries.stream().anyMatch(summary -> {
                var m = summary.getMetadata();
                return java.util.Objects.equals(source.get("conversationId"), m.get("conversationId"))
                        && number(source, "epoch") == number(m, "epoch") && number(source, "turnSequence") >= number(m, "coveredFrom")
                        && number(source, "turnSequence") <= number(m, "coveredTo");
            });
        } catch (IllegalArgumentException | ArithmeticException invalid) { return false; }
    }
    private static String sourceLabel(Document document) {
        var m = document.getMetadata();
        if ("conversation_summary".equals(m.get("memoryType"))) return "【会话摘要参考 v" + number(m, "summaryVersion")
                + "，覆盖轮次 " + number(m, "coveredFrom") + "-" + number(m, "coveredTo") + "】\n";
        if ("conversation_turn".equals(m.get("memoryType"))) return "【会话轮次参考】\n";
        return "";
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
