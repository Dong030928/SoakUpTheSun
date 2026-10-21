package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;

import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Elasticsearch BM25 projection; access restrictions are applied before result ranking. */
public class ElasticsearchRagIndex {
    private static final int SCHEMA_VERSION = 1;
    private static final Map<String, Object> FIELDS = Map.ofEntries(
            Map.entry("id", Map.of("type", "keyword")), Map.entry("kind", Map.of("type", "keyword")),
            Map.entry("ownerId", Map.of("type", "long")), Map.entry("visibility", Map.of("type", "keyword")),
            Map.entry("resourceId", Map.of("type", "keyword")), Map.entry("version", Map.of("type", "long")),
            Map.entry("parentId", Map.of("type", "keyword")), Map.entry("title", textField()),
            Map.entry("text", textField()), Map.entry("source", Map.of("type", "keyword", "index", false)),
            Map.entry("pageStart", Map.of("type", "integer")), Map.entry("pageEnd", Map.of("type", "integer")),
            Map.entry("ordinal", Map.of("type", "integer")), Map.entry("searchable", Map.of("type", "boolean")));
    private final URI endpoint;
    private final String indexName;
    private final int maxQueryChars;
    private final int maxTextChars;
    private final RagIndexHttp http;

    public ElasticsearchRagIndex(RagProperties properties) {
        Objects.requireNonNull(properties, "RAG properties are required");
        var settings = Objects.requireNonNull(properties.getElasticsearch(), "RAG keyword settings are required");
        endpoint = RagProperties.validateEndpoint(settings.getEndpoint());
        indexName = settings.getIndexName();
        if (indexName == null || !indexName.matches("[a-z0-9][a-z0-9_-]{0,100}")) {
            throw new IllegalArgumentException("Invalid RAG keyword index name");
        }
        maxQueryChars = properties.getMaxQueryChars();
        maxTextChars = properties.getMaxParentChars();
        if (maxQueryChars < 1 || maxQueryChars > 4000 || maxTextChars < 1 || maxTextChars > 16000) {
            throw new IllegalArgumentException("Invalid RAG keyword text budget");
        }
        String username = Objects.requireNonNullElse(settings.getUsername(), "");
        String password = Objects.requireNonNullElse(settings.getPassword(), "");
        Map<String, String> headers = username.isEmpty() && password.isEmpty() ? Map.of() : Map.of("Authorization", "Basic "
                + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8)));
        http = new RagIndexHttp(settings.getTimeoutMs(), 1024 * 1024, headers);
    }

    public void initialize() {
        var existing = http.exchange(uri("/_mapping"), "GET", null);
        if (existing.status() == 404) {
            var created = http.exchange(uri(""), "PUT", Map.of("settings", Map.of("number_of_shards", 1,
                    "number_of_replicas", 0), "mappings", Map.of("dynamic", "strict", "_meta",
                    Map.of("suts_rag_schema", SCHEMA_VERSION), "properties", FIELDS)));
            if (!created.successful()) throw RagIndexHttp.failure();
            existing = http.exchange(uri("/_mapping"), "GET", null);
        }
        if (!existing.successful()) throw RagIndexHttp.failure();
        JsonNode mappings = RagIndexHttp.parse(existing).path(indexName).path("mappings");
        if (!mappings.path("_meta").path("suts_rag_schema").isIntegralNumber()
                || mappings.path("_meta").path("suts_rag_schema").asInt() != SCHEMA_VERSION
                || !"strict".equals(mappings.path("dynamic").asText())) {
            throw new IllegalStateException("RAG keyword index schema is incompatible");
        }
        JsonNode actualFields = mappings.path("properties");
        for (var entry : FIELDS.entrySet()) {
            JsonNode expected = RagIndexHttp.JSON.valueToTree(entry.getValue());
            JsonNode actual = actualFields.path(entry.getKey());
            for (var property : expected.properties()) {
                // ES omits search_analyzer when it equals analyzer; its effective value still inherits analyzer.
                JsonNode effective = "search_analyzer".equals(property.getKey()) && !actual.has("search_analyzer")
                        ? actual.path("analyzer") : actual.path(property.getKey());
                if (!property.getValue().equals(effective)) {
                    throw new IllegalStateException("RAG keyword index schema is incompatible");
                }
            }
        }
    }

    public void upsert(RagIndexRecord record) {
        Objects.requireNonNull(record, "RAG keyword record is required");
        validateId(record.id());
        validateId(record.resourceId());
        if (record.kind() == null || record.visibility() == null || record.ownerId() < 0 || record.version() < 1
                || (record.visibility() == RagData.Visibility.OWNER && record.ownerId() < 1)
                || (record.kind() == RagData.Kind.PROFILE_FACT && record.visibility() != RagData.Visibility.OWNER)
                || record.text() == null || record.text().isBlank() || record.text().length() > maxTextChars
                || (record.title() != null && record.title().length() > 4096)
                || (record.source() != null && record.source().length() > 4096)
                || (record.parentId() != null && record.parentId().length() > 512)
                || record.pageStart() < 0 || record.pageEnd() < record.pageStart() || record.ordinal() < 0) {
            throw new IllegalArgumentException("Invalid RAG keyword record");
        }
        requireSuccess(http.exchange(uri("/_doc/" + segment(record.id()) + "?refresh=wait_for"), "PUT", record));
    }

    public void delete(String id) {
        validateId(id);
        var response = http.exchange(uri("/_doc/" + segment(id) + "?refresh=wait_for"), "DELETE", null);
        if (response.status() != 404) requireSuccess(response);
    }

    public void publish(String id, boolean searchable) {
        validateId(id);
        requireSuccess(http.exchange(uri("/_update/" + segment(id) + "?refresh=wait_for"), "POST",
                Map.of("doc", Map.of("searchable", searchable))));
    }

    public List<RagData.Hit> search(long user, String query, RagData.Kind kind, List<String> documentIds, int limit) {
        if (user < 1 || query == null || query.isBlank() || query.length() > maxQueryChars || kind == null
                || limit < 1 || limit > 50 || (documentIds != null && documentIds.size() > 100)) {
            throw new IllegalArgumentException("Invalid RAG keyword search");
        }
        var filters = new ArrayList<Object>();
        filters.add(Map.of("term", Map.of("kind", kind.name())));
        filters.add(Map.of("term", Map.of("searchable", true)));
        Object ownerAcl = Map.of("bool", Map.of("filter", List.of(Map.of("term", Map.of("ownerId", user)),
                Map.of("term", Map.of("visibility", RagData.Visibility.OWNER.name())))));
        filters.add(kind == RagData.Kind.PROFILE_FACT ? ownerAcl : Map.of("bool", Map.of("should", List.of(
                Map.of("term", Map.of("visibility", RagData.Visibility.PUBLIC.name())), ownerAcl), "minimum_should_match", 1)));
        if (kind == RagData.Kind.KNOWLEDGE_CHILD && documentIds != null && !documentIds.isEmpty()) {
            documentIds.forEach(ElasticsearchRagIndex::validateId);
            filters.add(Map.of("terms", Map.of("resourceId", List.copyOf(documentIds))));
        }
        var response = http.exchange(uri("/_search"), "POST", Map.of("size", limit, "track_total_hits", false,
                "_source", false, "sort", List.of(Map.of("_score", "desc"), Map.of("id", "asc")),
                "query", Map.of("bool", Map.of("filter", filters, "must", List.of(Map.of("multi_match", Map.of(
                        "query", query, "fields", List.of("title^2", "text"), "type", "best_fields")))))));
        requireSuccess(response);
        JsonNode root = RagIndexHttp.parse(response);
        JsonNode hits = root.path("hits").path("hits");
        if (root.path("timed_out").asBoolean(false) || root.path("_shards").path("failed").asInt(0) > 0
                || !hits.isArray() || hits.size() > limit) throw RagIndexHttp.failure();
        var result = new ArrayList<RagData.Hit>(hits.size());
        var seen = new HashSet<String>();
        for (JsonNode hit : hits) {
            JsonNode id = hit.path("_id"), score = hit.path("_score");
            if (!id.isString() || id.asText().isBlank() || id.asText().length() > 512 || !score.isNumber()
                    || !Double.isFinite(score.asDouble()) || !seen.add(id.asText())) throw RagIndexHttp.failure();
            result.add(new RagData.Hit(id.asText(), score.asDouble()));
        }
        return List.copyOf(result);
    }

    private URI uri(String suffix) {
        return URI.create(endpoint.toString().replaceAll("/+$", "") + "/" + indexName + suffix);
    }
    private static Map<String, Object> textField() {
        return Map.of("type", "text", "analyzer", "cjk", "search_analyzer", "cjk", "similarity", "BM25");
    }
    private static void requireSuccess(RagIndexHttp.Response response) {
        if (!response.successful()) throw RagIndexHttp.failure();
    }
    private static void validateId(String id) {
        if (id == null || id.isBlank() || id.getBytes(StandardCharsets.UTF_8).length > 512 || id.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Invalid RAG resource identifier");
        }
    }
    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20").replace(".", "%2E");
    }
}
