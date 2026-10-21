package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.manager.CheckedEmbeddingModel;
import org.hgc.suts.gateway.memory.summary.SummaryPolicy;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.BatchingStrategy;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingOptions;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.ai.vectorstore.redis.RedisVectorStore;
import org.springframework.util.Assert;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.json.Path2;

import java.nio.charset.StandardCharsets;
import java.util.*;

/** Independent document HNSW projection. Prepared vectors never invoke a provider again. */
public final class RedisRagIndex {
    private static final RedisVectorStore.MetadataField[] FIELDS = {
            tag("schemaVersion"), tag("embeddingModelTag"), tag("targetTag"), tag("recordType"),
            tag("ownerId"), tag("visibility"), tag("resourceId"), tag("searchable"),
            tag("parentId"), tag("title"), tag("source"), numeric("resourceVersion"),
            numeric("pageStart"), numeric("pageEnd"), numeric("ordinal")
    };
    private final RedisClient redis;
    private final EmbeddingModel checked;
    private final RagProperties properties;
    private final int dimension;
    private final String modelTag;
    private final String target;
    private final String indexName;
    private final String prefix;

    public RedisRagIndex(RedisClient redis, EmbeddingModel checked, RagProperties properties,
                         int dimension, String embeddingModelTag) {
        Assert.notNull(redis, "RAG Redis client is required");
        Assert.notNull(properties, "RAG settings are required");
        Assert.hasText(embeddingModelTag, "RAG embedding identity is required");
        Assert.isTrue(properties.getIndexName().startsWith("suts:agent:rag:documents:")
                && properties.getPrefix().startsWith("suts:agent:rag:documents:") && properties.getPrefix().endsWith(":"),
                "RAG documents require their own vector namespace");
        this.redis = redis; this.checked = new CheckedEmbeddingModel(checked, dimension);
        this.properties = properties; this.dimension = dimension; this.modelTag = embeddingModelTag;
        this.target = SummaryPolicy.hash("rag-doc-v1|" + embeddingModelTag + "|" + dimension + "|"
                + properties.getIndexName() + "|" + properties.getPrefix() + "|"
                + properties.getElasticsearch().getEndpoint().replaceAll("/+$", "") + "|"
                + properties.getElasticsearch().getIndexName());
        this.indexName = properties.getIndexName() + ":" + target;
        this.prefix = properties.getPrefix() + target + ":";
    }

    public String target() { return target; }
    public String indexName() { return indexName; }
    public String prefix() { return prefix; }

    public synchronized void initialize() {
        var store = view(checked, true);
        store.afterPropertiesSet();
        validateSchema(redis.ftInfo(indexName), prefix, dimension, FIELDS);
    }

    public void upsert(RagIndexRecord record, float[] vector) {
        Assert.notNull(record, "RAG vector record is required");
        validId(record.id()); validId(record.resourceId());
        Assert.isTrue(record.kind() == RagData.Kind.KNOWLEDGE_CHILD && record.visibility() != null
                && record.ownerId() >= 0 && (record.visibility() != RagData.Visibility.OWNER || record.ownerId() > 0)
                && record.version() > 0 && record.text() != null && !record.text().isBlank()
                && record.text().length() <= properties.getMaxChildChars(), "Invalid RAG document vector record");
        var metadata = new HashMap<String, Object>();
        metadata.put("schemaVersion", "rag1"); metadata.put("embeddingModelTag", modelTag);
        metadata.put("targetTag", target); metadata.put("recordType", record.kind().name());
        metadata.put("ownerId", Long.toString(record.ownerId())); metadata.put("visibility", record.visibility().name());
        metadata.put("resourceId", record.resourceId()); metadata.put("resourceVersion", record.version());
        metadata.put("searchable", Boolean.toString(record.searchable()));
        metadata.put("parentId", Objects.requireNonNullElse(record.parentId(), ""));
        metadata.put("title", Objects.requireNonNullElse(record.title(), ""));
        metadata.put("source", Objects.requireNonNullElse(record.source(), ""));
        metadata.put("pageStart", record.pageStart()); metadata.put("pageEnd", record.pageEnd()); metadata.put("ordinal", record.ordinal());
        var document = Document.builder().id(record.id()).text(record.text()).metadata(metadata).build();
        var model = new PreparedDocumentModel(document, vector, dimension);
        initialize();
        view(model, false).add(List.of(document));
    }

    public void delete(String id) {
        validId(id); redis.del(prefix + id);
    }

    public void publish(String id, boolean searchable) {
        validId(id);
        Assert.state("OK".equals(redis.jsonSetWithEscape(prefix + id, Path2.of("$.searchable"), Boolean.toString(searchable))),
                "RAG vector publication did not update an existing document");
    }

    public List<RagData.Hit> search(long user, String query, float[] queryVector, List<String> documentIds, int limit) {
        Assert.isTrue(user > 0 && query != null && !query.isBlank() && query.length() <= properties.getMaxQueryChars()
                && limit > 0 && limit <= 50, "Invalid RAG vector search");
        Assert.notNull(documentIds, "RAG document filters are required");
        Assert.isTrue(documentIds.size() <= 50, "Too many RAG document filters"); documentIds.forEach(RedisRagIndex::validId);
        var fixed = new FixedQueryEmbeddingModel(query, queryVector, dimension);
        initialize();
        var f = new FilterExpressionBuilder();
        // Spring AI's Redis converter does not add precedence parentheses for AND/OR nodes.
        var access = f.group(f.or(f.eq("visibility", "PUBLIC"),
                f.group(f.and(f.eq("visibility", "OWNER"), f.eq("ownerId", Long.toString(user))))));
        var filters = f.and(access, f.and(f.eq("searchable", "true"), f.and(f.eq("schemaVersion", "rag1"),
                f.and(f.eq("targetTag", target), f.and(f.eq("embeddingModelTag", modelTag), f.eq("recordType", "KNOWLEDGE_CHILD"))))));
        if (!documentIds.isEmpty()) filters = f.and(filters, f.in("resourceId", documentIds.toArray()));
        return view(fixed, false).similaritySearch(SearchRequest.builder().query(query).topK(limit)
                        .filterExpression(filters.build()).build()).stream()
                .filter(document -> document.getScore() != null && Double.isFinite(document.getScore()))
                .sorted(Comparator.comparingDouble((Document document) -> document.getScore()).reversed().thenComparing(Document::getId))
                .map(document -> new RagData.Hit(document.getId(), document.getScore())).toList();
    }

    private RedisVectorStore view(EmbeddingModel model, boolean initialize) {
        return RedisVectorStore.builder(redis, model).indexName(indexName).prefix(prefix)
                .metadataFields(FIELDS).vectorAlgorithm(RedisVectorStore.Algorithm.HNSW).initializeSchema(initialize).build();
    }

    static void validateSchema(Map<String, Object> info, String prefix, int dimension, RedisVectorStore.MetadataField[] fields) {
        var definition = asMap(info.get("index_definition"));
        Assert.state("JSON".equalsIgnoreCase(text(definition.get("key_type"))) && definition.get("prefixes") instanceof List<?> prefixes
                && prefixes.size() == 1 && prefix.equals(text(prefixes.get(0))), "RAG vector index prefix/type mismatch");
        Assert.state(info.get("attributes") instanceof List<?>, "RAG vector index fields are missing");
        Map<String, Map<String, Object>> attributes = new HashMap<>();
        for (Object row : (List<?>) info.get("attributes")) {
            var field = asMap(row); attributes.put(text(field.get("attribute")), field);
        }
        requireField(attributes, "content", "TEXT");
        for (var field : fields) requireField(attributes, field.name(), field.fieldType().name());
        var vector = requireField(attributes, "embedding", "VECTOR");
        Assert.state("HNSW".equalsIgnoreCase(text(vector.get("algorithm"))) && "FLOAT32".equalsIgnoreCase(text(vector.get("data_type")))
                && "COSINE".equalsIgnoreCase(text(vector.get("distance_metric"))) && Integer.toString(dimension).equals(text(vector.get("dim"))),
                "RAG vector index geometry mismatch");
    }
    private static Map<String, Object> requireField(Map<String, Map<String, Object>> fields, String name, String type) {
        var field = fields.get(name);
        Assert.state(field != null && type.equalsIgnoreCase(text(field.get("type"))) && ("$." + name).equals(text(field.get("identifier"))),
                "RAG vector index field mismatch: " + name);
        return field;
    }
    private static Map<String, Object> asMap(Object value) {
        Map<String, Object> result = new HashMap<>();
        if (value instanceof Map<?, ?> map) map.forEach((key, item) -> result.put(text(key), item));
        else if (value instanceof List<?> list) for (int i = 0; i + 1 < list.size(); i += 2) result.put(text(list.get(i)), list.get(i + 1));
        return result;
    }
    private static String text(Object value) { return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value); }
    private static RedisVectorStore.MetadataField tag(String name) { return RedisVectorStore.MetadataField.tag(name); }
    private static RedisVectorStore.MetadataField numeric(String name) { return RedisVectorStore.MetadataField.numeric(name); }
    private static void validId(String id) { Assert.isTrue(id != null && id.matches("[a-zA-Z0-9:._-]{1,160}"), "Invalid RAG vector resource ID"); }

    /** Accepts only the exact document object prepared by this upsert, never arbitrary text. */
    private static final class PreparedDocumentModel implements EmbeddingModel {
        private final Document document;
        private final float[] vector;
        private final int dimension;
        PreparedDocumentModel(Document document, float[] vector, int dimension) {
            this.document = document; this.dimension = dimension;
            this.vector = new FixedQueryEmbeddingModel(document.getText(), vector, dimension).embed(document.getText());
        }
        @Override public int dimensions() { return dimension; }
        @Override public float[] embed(Document value) { Assert.isTrue(value == document, "Unexpected RAG embedding document"); return vector.clone(); }
        @Override public List<float[]> embed(List<Document> values, EmbeddingOptions options, BatchingStrategy batching) {
            Assert.isTrue(values != null && values.size() == 1, "Only one prepared RAG document is accepted");
            return List.of(embed(values.get(0)));
        }
        @Override public float[] embed(String text) { throw new UnsupportedOperationException("Prepared document embedding only"); }
        @Override public EmbeddingResponse call(EmbeddingRequest request) { throw new UnsupportedOperationException("Prepared document embedding only"); }
        @Override public String getEmbeddingContent(Document value) { throw new UnsupportedOperationException("Prepared document embedding only"); }
    }
}
