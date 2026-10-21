package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.ai.embedding.EmbeddingModel;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedisRagIndexTest {
    private final RedisClient redis = mock(RedisClient.class);
    private final EmbeddingModel provider = mock(EmbeddingModel.class);
    private final RagProperties properties = new RagProperties();
    private final RedisRagIndex index = new RedisRagIndex(redis, provider, properties, 3, "6d6f64656c");

    @Test void createsAndValidatesIndependentHnswWithoutAProviderDimensionsProbe() {
        when(redis.ftList()).thenReturn(Set.of());
        when(redis.ftCreate(eq(index.indexName()), any(), any(Iterable.class))).thenReturn("OK");
        when(redis.ftInfo(index.indexName())).thenReturn(schema(index.prefix(), 3, false));
        index.initialize(); index.initialize();
        verify(redis, times(2)).ftInfo(index.indexName()); verifyNoInteractions(provider);
        assertThat(index.prefix()).startsWith("suts:agent:rag:documents:");
        var another = new RedisRagIndex(redis, provider, properties, 4, "6d6f64656c");
        assertThat(another.indexName()).isNotEqualTo(index.indexName());
        var movedKeyword = new RagProperties(); movedKeyword.getElasticsearch().setEndpoint("http://other.invalid:9200");
        assertThat(new RedisRagIndex(redis, provider, movedKeyword, 3, "6d6f64656c").target()).isNotEqualTo(index.target());
    }

    @Test void rejectsExistingIndexWithWrongDimensionOrMissingOwnerFilter() {
        when(redis.ftList()).thenReturn(Set.of(index.indexName()));
        when(redis.ftInfo(index.indexName())).thenReturn(schema(index.prefix(), 4, false));
        assertThatThrownBy(index::initialize).hasMessageContaining("geometry");
        var missing = schema(index.prefix(), 3, false);
        ((List<Map<String, Object>>) missing.get("attributes")).removeIf(field -> "ownerId".equals(field.get("attribute")));
        when(redis.ftInfo(index.indexName())).thenReturn(missing);
        assertThatThrownBy(index::initialize).hasMessageContaining("ownerId");
        verifyNoInteractions(provider);
    }

    @Test void springAiWritesOnlyThePreparedVectorAndPreservesPublicationMetadata() {
        ready(); var pipeline = mock(Pipeline.class); when(redis.pipelined()).thenReturn(pipeline);
        when(pipeline.syncAndReturnAll()).thenReturn(List.of("OK"));
        var record = new RagIndexRecord("child-1", RagData.Kind.KNOWLEDGE_CHILD, 42, RagData.Visibility.OWNER,
                "document-1", 2, "parent-1", "标题", "原始段落", "fixture.txt", 0, 0, 1, false);
        index.upsert(record, new float[]{0, 1, 0});
        var payload = ArgumentCaptor.forClass(Object.class);
        verify(pipeline).jsonSetWithEscape(eq(index.prefix() + "child-1"), any(Path2.class), payload.capture());
        var fields = (Map<String, Object>) payload.getValue();
        assertThat((float[]) fields.get("embedding")).containsExactly(0, 1, 0);
        assertThat(fields).containsEntry("content", "原始段落").containsEntry("ownerId", "42")
                .containsEntry("searchable", "false").containsEntry("resourceId", "document-1").doesNotContainKey("metadata");
        verifyNoInteractions(provider);
        when(redis.jsonSetWithEscape(eq(index.prefix() + "child-1"), any(Path2.class), eq("true"))).thenReturn("OK");
        index.publish("child-1", true); index.delete("child-1"); verify(redis).del(index.prefix() + "child-1");
        verify(redis).jsonSetWithEscape(index.prefix() + "child-1", Path2.of("$.searchable"), "true");
    }

    @Test void accessPublicationAndSelectedDocumentsAreFilteredBeforeKnnUsingThePreparedQueryVector() {
        ready(); var result = mock(SearchResult.class);
        when(result.getDocuments()).thenReturn(List.of(nativeDocument(index.prefix() + "child-1", Map.of("vector_score", "0.2"))));
        when(redis.ftSearch(eq(index.indexName()), any(Query.class))).thenReturn(result);
        assertThat(index.search(42, "饮品", new float[]{1, 0, 0}, List.of("document-1"), 7))
                .containsExactly(new RagData.Hit("child-1", (double) 0.9f));
        var query = ArgumentCaptor.forClass(Query.class); verify(redis).ftSearch(eq(index.indexName()), query.capture());
        var arguments = arguments(query.getValue());
        String filter = arguments.stream().map(value -> new String(value, StandardCharsets.UTF_8))
                .filter(value -> value.contains("=>[KNN ")).findFirst().orElseThrow();
        assertThat(filter).isEqualTo("((@visibility:{PUBLIC} | (@visibility:{OWNER} @ownerId:{42})) "
                + "@searchable:{true} @schemaVersion:{rag1} @targetTag:{" + index.target() + "} "
                + "@embeddingModelTag:{6d6f64656c} @recordType:{KNOWLEDGE_CHILD} @resourceId:{document\\-1})"
                + "=>[KNN 7 @embedding $BLOB AS vector_score]");
        int parameter = -1;
        for (int i = 0; i < arguments.size(); i++) if ("BLOB".equals(new String(arguments.get(i), StandardCharsets.UTF_8))) parameter = i;
        var vector = ByteBuffer.wrap(arguments.get(parameter + 1)).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
        assertThat(vector.get()).isEqualTo(1); assertThat(vector.get()).isZero(); assertThat(vector.get()).isZero();
        verifyNoInteractions(provider);
    }

    private void ready() {
        when(redis.ftList()).thenReturn(Set.of(index.indexName()));
        when(redis.ftInfo(index.indexName())).thenReturn(schema(index.prefix(), 3, false));
    }
    static List<byte[]> arguments(Query query) {
        var arguments = new CommandArguments(() -> "FT.SEARCH".getBytes(StandardCharsets.UTF_8)); query.addParams(arguments);
        var values = new ArrayList<byte[]>(); arguments.forEach(value -> values.add(value.getRaw())); return values;
    }
    static redis.clients.jedis.search.Document nativeDocument(String id, Map<String, Object> metadata) {
        return new redis.clients.jedis.search.Document(id, metadata);
    }
    static Map<String, Object> schema(String prefix, int dimension, boolean profile) {
        var attributes = new ArrayList<Map<String, Object>>();
        attributes.add(field("content", "TEXT"));
        attributes.add(new HashMap<>(Map.of("attribute", "embedding", "identifier", "$.embedding", "type", "VECTOR",
                "algorithm", "HNSW", "data_type", "FLOAT32", "distance_metric", "COSINE", "dim", dimension)));
        var tags = profile ? List.of("userId", "schemaVersion", "targetTag", "recordType", "factId")
                : List.of("schemaVersion", "embeddingModelTag", "targetTag", "recordType", "ownerId", "visibility", "resourceId", "searchable", "parentId", "title", "source");
        tags.forEach(name -> attributes.add(field(name, "TAG")));
        var numerics = profile ? List.of("factVersion") : List.of("resourceVersion", "pageStart", "pageEnd", "ordinal");
        numerics.forEach(name -> attributes.add(field(name, "NUMERIC")));
        return new HashMap<>(Map.of("index_definition", Map.of("key_type", "JSON", "prefixes", List.of(prefix)), "attributes", attributes));
    }
    private static Map<String, Object> field(String name, String type) {
        return new HashMap<>(Map.of("attribute", name, "identifier", "$." + name, "type", type));
    }
}
