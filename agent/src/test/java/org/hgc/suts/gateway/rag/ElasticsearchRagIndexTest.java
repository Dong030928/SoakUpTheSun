package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ElasticsearchRagIndexTest {
    private RagIndexHttpTestServer server;
    private RagProperties properties;

    @BeforeEach void setup() throws Exception {
        server = new RagIndexHttpTestServer();
        properties = new RagProperties();
        properties.getElasticsearch().setEndpoint(server.endpoint());
        properties.getElasticsearch().setIndexName("rag-contract");
    }
    @AfterEach void cleanup() { server.close(); }

    @Test void initializesExplicitCjkBm25SchemaAndChecksAnExistingIndex() {
        var mapping = new AtomicReference<JsonNode>();
        mockMappings(mapping);
        var index = new ElasticsearchRagIndex(properties);
        index.initialize();
        assertThat(server.requests).extracting(RagIndexHttpTestServer.Request::method).containsExactly("GET", "PUT", "GET");
        var fields = mapping.get().path("properties");
        assertThat(mapping.get().path("dynamic").asText()).isEqualTo("strict");
        assertThat(fields.path("ownerId").path("type").asText()).isEqualTo("long");
        assertThat(fields.path("kind").path("type").asText()).isEqualTo("keyword");
        assertThat(fields.path("searchable").path("type").asText()).isEqualTo("boolean");
        assertThat(fields.path("text").path("analyzer").asText()).isEqualTo("cjk");
        assertThat(fields.path("title").path("search_analyzer").asText()).isEqualTo("cjk");
        assertThat(fields.path("text").path("similarity").asText()).isEqualTo("BM25");
        index.initialize();
        assertThat(server.requests).hasSize(4);
        assertThat(server.requests.get(3).method()).isEqualTo("GET");
        ((ObjectNode) fields.path("ownerId")).put("type", "integer");
        assertThatThrownBy(index::initialize).hasMessage("RAG keyword index schema is incompatible");
    }

    @Test void rejectsIndexesWithoutTheVersionedSchemaOrWithDifferentAnalyzers() {
        var mapping = new AtomicReference<JsonNode>();
        mockMappings(mapping);
        var index = new ElasticsearchRagIndex(properties);
        index.initialize();
        ((ObjectNode) mapping.get().path("properties").path("text")).put("analyzer", "standard");
        assertThatThrownBy(index::initialize).hasMessage("RAG keyword index schema is incompatible");
        ((ObjectNode) mapping.get()).remove("_meta");
        assertThatThrownBy(index::initialize).hasMessage("RAG keyword index schema is incompatible");
    }

    @Test void acceptsInheritedSearchAnalyzerButRejectsAnExplicitlyDifferentSearchAnalyzer() {
        var mapping = new AtomicReference<JsonNode>();
        mockMappings(mapping);
        var index = new ElasticsearchRagIndex(properties);
        index.initialize();
        for (String field : List.of("title", "text")) {
            ((ObjectNode) mapping.get().path("properties").path(field)).remove("search_analyzer");
        }
        index.initialize();
        ((ObjectNode) mapping.get().path("properties").path("text")).put("search_analyzer", "standard");
        assertThatThrownBy(index::initialize).hasMessage("RAG keyword index schema is incompatible");
    }

    @Test void documentAclAndResourceRestrictionsPrecedeTopKAndQueryRemainsAJsonValue() {
        server.responder = request -> hits("doc-child", 1.2);
        String untrustedQuery = "\"}],\"filter\":[],\"match_all\":{}} 轮椅";
        var result = new ElasticsearchRagIndex(properties).search(9007199254740993L, untrustedQuery,
                RagData.Kind.KNOWLEDGE_CHILD, List.of("document-a", "document-b"), 40);
        assertThat(result).containsExactly(new RagData.Hit("doc-child", 1.2));
        var request = server.requests.get(0);
        var body = request.body();
        var bool = body.path("query").path("bool");
        var filters = bool.path("filter");
        assertThat(request.path()).isEqualTo("/rag-contract/_search");
        assertThat(body.path("size").asInt()).isEqualTo(40);
        assertThat(body.path("_source").asBoolean()).isFalse();
        assertThat(bool.path("must").get(0).path("multi_match").path("query").asText()).isEqualTo(untrustedQuery);
        assertThat(bool.path("must").get(0).path("multi_match").path("fields").get(0).asText()).isEqualTo("title^2");
        assertThat(filters.get(0).path("term").path("kind").asText()).isEqualTo("KNOWLEDGE_CHILD");
        assertThat(filters.get(1).path("term").path("searchable").asBoolean()).isTrue();
        var acl = filters.get(2).path("bool");
        assertThat(acl.path("minimum_should_match").asInt()).isEqualTo(1);
        assertThat(acl.path("should").get(0).path("term").path("visibility").asText()).isEqualTo("PUBLIC");
        var owner = acl.path("should").get(1).path("bool").path("filter");
        assertThat(owner.get(0).path("term").path("ownerId").asLong()).isEqualTo(9007199254740993L);
        assertThat(owner.get(1).path("term").path("visibility").asText()).isEqualTo("OWNER");
        assertThat(filters.get(3).path("terms").path("resourceId").size()).isEqualTo(2);
    }

    @Test void profilesAlwaysRequireTheCurrentOwnerAndOwnerVisibility() {
        server.responder = request -> hits("profile:fact:1", .5);
        new ElasticsearchRagIndex(properties).search(17, "profile query", RagData.Kind.PROFILE_FACT, List.of(), 10);
        var filters = server.requests.get(0).body().path("query").path("bool").path("filter");
        assertThat(filters.size()).isEqualTo(3);
        var acl = filters.get(2).path("bool");
        assertThat(acl.has("should")).isFalse();
        assertThat(acl.path("filter").get(0).path("term").path("ownerId").asLong()).isEqualTo(17);
        assertThat(acl.path("filter").get(1).path("term").path("visibility").asText()).isEqualTo("OWNER");
    }

    @Test void upsertsHaveStableEscapedIdsAndPublishAndDeleteAreExplicit() {
        var index = new ElasticsearchRagIndex(properties);
        String id = "profile:fact:1";
        var record = record(id, RagData.Visibility.OWNER);
        index.upsert(record);
        index.upsert(record);
        index.publish(id, true);
        server.responder = request -> new RagIndexHttpTestServer.Reply(404, "{}");
        index.delete(id);
        assertThat(server.requests).extracting(RagIndexHttpTestServer.Request::path).containsExactly(
                "/rag-contract/_doc/profile%3Afact%3A1?refresh=wait_for",
                "/rag-contract/_doc/profile%3Afact%3A1?refresh=wait_for",
                "/rag-contract/_update/profile%3Afact%3A1?refresh=wait_for",
                "/rag-contract/_doc/profile%3Afact%3A1?refresh=wait_for");
        assertThat(server.requests.get(0).body().path("id").asText()).isEqualTo(id);
        assertThat(server.requests.get(0).body().path("ownerId").asLong()).isEqualTo(17);
        assertThat(server.requests.get(0).body().path("searchable").asBoolean()).isFalse();
        assertThat(server.requests.get(2).body().path("doc").path("searchable").asBoolean()).isTrue();
        assertThatThrownBy(() -> index.upsert(record(id, RagData.Visibility.PUBLIC)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void acceptsCanonicalDocumentTitlesWithAFullHeadingPathAndBoundsOversizedTitles() {
        var index = new ElasticsearchRagIndex(properties);
        String title = "t".repeat(512) + " / " + "章".repeat(2048);
        index.upsert(new RagIndexRecord("long-title", RagData.Kind.KNOWLEDGE_CHILD, 42, RagData.Visibility.PUBLIC,
                "document", 1, "parent", title, "searchable passage", "local.md", 0, 0, 0, false));
        assertThat(server.requests).hasSize(1);
        assertThat(server.requests.get(0).body().path("title").asText()).isEqualTo(title);
        assertThatThrownBy(() -> index.upsert(new RagIndexRecord("oversized-title", RagData.Kind.KNOWLEDGE_CHILD,
                42, RagData.Visibility.PUBLIC, "document", 1, "parent", "x".repeat(4097), "passage",
                "local.md", 0, 0, 0, false))).isInstanceOf(IllegalArgumentException.class);
        assertThat(server.requests).hasSize(1);
    }

    @Test void rejectsPartialMalformedOrDuplicateSearchResultsAndDoesNotExposeProviderText() {
        var index = new ElasticsearchRagIndex(properties);
        for (String body : List.of(
                "{\"timed_out\":true,\"hits\":{\"hits\":[]}}",
                "{\"_shards\":{\"failed\":1},\"hits\":{\"hits\":[]}}",
                "{\"hits\":{\"hits\":[{\"_id\":\"a\",\"_score\":1},{\"_id\":\"a\",\"_score\":1}]}}",
                "{\"hits\":{\"hits\":[{\"_id\":\"a\",\"_score\":\"NaN\"}]}}",
                "{\"hits\":{\"hits\":[{\"_id\":\"a\",\"_score\":null}]}}",
                "sensitive-provider-body")) {
            server.responder = request -> new RagIndexHttpTestServer.Reply(200, body);
            assertThatThrownBy(() -> index.search(17, "query", RagData.Kind.PROFILE_FACT, List.of(), 10))
                    .hasMessage("RAG remote service request failed").hasNoCause();
        }
        server.responder = request -> new RagIndexHttpTestServer.Reply(500, "sensitive-provider-body");
        assertThatThrownBy(() -> index.search(17, "query", RagData.Kind.PROFILE_FACT, List.of(), 10))
                .hasMessage("RAG remote service request failed").hasNoCause();
        assertThat(server.requests).hasSize(7);
    }

    private void mockMappings(AtomicReference<JsonNode> mapping) {
        server.responder = request -> {
            if (request.method().equals("PUT")) {
                mapping.set(request.body().path("mappings"));
                return new RagIndexHttpTestServer.Reply(200, "{\"acknowledged\":true}");
            }
            if (mapping.get() == null) return new RagIndexHttpTestServer.Reply(404, "{}");
            JsonNode normalized = mapping.get().deepCopy();
            for (String field : List.of("title", "text")) {
                JsonNode definition = normalized.path("properties").path(field);
                if (definition.path("analyzer").equals(definition.path("search_analyzer"))) {
                    ((ObjectNode) definition).remove("search_analyzer");
                }
            }
            return new RagIndexHttpTestServer.Reply(200, RagIndexHttp.JSON.writeValueAsString(
                    Map.of("rag-contract", Map.of("mappings", normalized))));
        };
    }
    private static RagIndexHttpTestServer.Reply hits(String id, double score) {
        return new RagIndexHttpTestServer.Reply(200, RagIndexHttp.JSON.writeValueAsString(Map.of("timed_out", false,
                "_shards", Map.of("failed", 0), "hits", Map.of("hits", List.of(Map.of("_id", id, "_score", score))))));
    }
    private static RagIndexRecord record(String id, RagData.Visibility visibility) {
        return new RagIndexRecord(id, RagData.Kind.PROFILE_FACT, 17, visibility, "fact", 1,
                "", "profile", "user needs a wheelchair", "user", 0, 0, 0, false);
    }
}
