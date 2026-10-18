package org.hgc.suts.gateway.service;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.manager.ManagedMemoryVectorStore;
import org.hgc.suts.gateway.service.impl.RedisVectorStorageServiceImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.redis.RedisFilterExpressionConverter;
import org.springframework.ai.vectorstore.redis.RedisVectorStore;
import redis.clients.jedis.RedisClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RedisVectorStorageServiceTest {
    private final ManagedMemoryVectorStore managed = mock(ManagedMemoryVectorStore.class);
    private final VectorStore nativeStore = mock(VectorStore.class);
    private final RedisClient redis = mock(RedisClient.class);
    private final MemoryVectorProperties properties = new MemoryVectorProperties();
    private final AiConfig ai = config();
    private final RedisVectorStorageServiceImpl service = new RedisVectorStorageServiceImpl(managed, properties, ai, redis);

    @Test
    @SuppressWarnings("unchecked")
    void saveUsesFrameworkDocumentAndOnlyNewSchemaMetadata() {
        when(managed.getStore()).thenReturn(nativeStore);
        service.saveMemory(42L, "User: 喜欢苹果\nAI: 记住了");
        var documents = ArgumentCaptor.forClass(List.class);
        verify(nativeStore).add(documents.capture());
        Document document = (Document) documents.getValue().get(0);
        assertThat(document.getText()).isEqualTo("User: 喜欢苹果\nAI: 记住了");
        assertThat(document.getMetadata()).containsEntry("userId", "42").containsEntry("schemaVersion", "2")
                .containsEntry("embeddingModel", "test-model").containsEntry("embeddingDimension", 3)
                .containsEntry("embeddingModelTag", properties.embeddingModelTag(ai))
                .containsKey("createdAt").doesNotContainKeys("token", "authTicket", "commandData");
        assertThat(document.getId()).matches("[A-Za-z0-9_-]{1,128}");
        verifyNoInteractions(redis);
    }

    @Test
    void disabledAndFrozenWritesDoNotInitializeOrEmbed() {
        properties.setWritesEnabled(false);
        service.saveMemory(42L, "text");
        properties.setWritesEnabled(true);
        properties.setEnabled(false);
        service.saveMemory(42L, "text");
        assertThat(service.searchMemory(42L, "query", 10)).isEmpty();
        verifyNoInteractions(managed, nativeStore, redis);
    }

    @Test
    void searchAlwaysAddsOwnerSchemaModelAndDimensionFilterWithBoundedRecall() {
        properties.setTopK(2);
        properties.setMaxRecallChars(7);
        when(managed.getStore()).thenReturn(nativeStore);
        when(nativeStore.similaritySearch(any(SearchRequest.class))).thenReturn(List.of(
                document("one", "abcdef", 42L, "test-model", "2", 3, 0.9),
                document("two", "abcdef", 42L, "test-model", "2", 3, 0.9),
                document("three", "ghijkl", 42L, "test-model", "2", 3, 0.9)));
        assertThat(service.searchMemory(42L, "@userId:{43}", 100)).containsExactly("abcdef", "g");
        var search = ArgumentCaptor.forClass(SearchRequest.class);
        verify(nativeStore).similaritySearch(search.capture());
        assertThat(search.getValue().getQuery()).isEqualTo("@userId:{43}");
        assertThat(search.getValue().getTopK()).isEqualTo(2);
        assertThat(search.getValue().getSimilarityThreshold()).isEqualTo(0.7);
        var converter = new RedisFilterExpressionConverter(List.of(
                RedisVectorStore.MetadataField.tag("userId"), RedisVectorStore.MetadataField.tag("schemaVersion"),
                RedisVectorStore.MetadataField.tag("embeddingModelTag"), RedisVectorStore.MetadataField.numeric("embeddingDimension")));
        assertThat(converter.convertExpression(search.getValue().getFilterExpression()))
                .contains("@userId:{42}", "@schemaVersion:{2}", "@embeddingModelTag:{" + properties.embeddingModelTag(ai) + "}", "@embeddingDimension:[3 3]")
                .doesNotContain("43");
    }

    @Test
    void foreignMalformedLowScoreAndWrongModelResultsAreRejectedDefensively() {
        when(managed.getStore()).thenReturn(nativeStore);
        when(nativeStore.similaritySearch(any(SearchRequest.class))).thenReturn(List.of(
                document("other", "foreign user", 43L, "test-model", "2", 3, 1),
                document("legacy", "legacy schema", 42L, "test-model", "1", 3, 1),
                document("wrongmodel", "wrong model", 42L, "other-model", "2", 3, 1),
                document("wrongdim", "wrong dimension", 42L, "test-model", "2", 4, 1),
                document("lowscore", "irrelevant", 42L, "test-model", "2", 3, 0.5),
                document("nan", "invalid score", 42L, "test-model", "2", 3, Double.NaN),
                Document.builder().id("noowner").text("missing ownership").score(1.0).build(),
                document("good", "same-user memory", 42L, "test-model", "2", 3, 0.9)));
        assertThat(service.searchMemory(42L, "hello", 10)).containsExactly("same-user memory");
    }

    @Test
    @SuppressWarnings("unchecked")
    void modelNamesWithSlashesUseAnExactSafeTagWhilePreservingReadableProvenance() {
        String model = "Qwen/Qwen3-Embedding-0.6B";
        ai.setEmbeddingModelName(model);
        when(managed.getStore()).thenReturn(nativeStore);
        when(nativeStore.similaritySearch(any(SearchRequest.class)))
                .thenReturn(List.of(document("qwen", "owned memory", 42L, model, "2", 3, 0.9)));
        assertThat(service.searchMemory(42L, "hello", 3)).containsExactly("owned memory");
        var search = ArgumentCaptor.forClass(SearchRequest.class);
        verify(nativeStore).similaritySearch(search.capture());
        var converter = new RedisFilterExpressionConverter(List.of(
                RedisVectorStore.MetadataField.tag("userId"), RedisVectorStore.MetadataField.tag("schemaVersion"),
                RedisVectorStore.MetadataField.tag("embeddingModelTag"), RedisVectorStore.MetadataField.numeric("embeddingDimension")));
        String nativeFilter = converter.convertExpression(search.getValue().getFilterExpression());
        assertThat(properties.embeddingModelTag(ai)).matches("[0-9a-f]+");
        assertThat(nativeFilter).contains("@embeddingModelTag:{" + properties.embeddingModelTag(ai) + "}")
                .doesNotContain(model, "@embeddingModel:", "/");
        service.saveMemory(42L, "new memory");
        var saved = ArgumentCaptor.forClass(List.class);
        verify(nativeStore).add(saved.capture());
        Document document = (Document) saved.getValue().get(0);
        assertThat(document.getMetadata()).containsEntry("embeddingModel", model)
                .containsEntry("embeddingModelTag", properties.embeddingModelTag(ai));
    }

    @Test
    void invalidOwnerInputAndUnsafeMigrationIdNeverReachTheStore() {
        assertThatThrownBy(() -> service.searchMemory(null, "text", 1)).hasMessageContaining("owner");
        assertThatThrownBy(() -> service.saveMemory(-1L, "text")).hasMessageContaining("owner");
        properties.setMaxDocumentChars(3);
        assertThatThrownBy(() -> service.saveMemory(42L, "long text")).hasMessageContaining("budget");
        assertThatThrownBy(() -> service.searchMemory(42L, "long query", 1)).hasMessageContaining("budget");
        assertThatThrownBy(() -> service.importMemory(42L, "ok", "valid", "old-key")).hasMessageContaining("Freeze");
        properties.setWritesEnabled(false);
        assertThatThrownBy(() -> service.importMemory(42L, "ok", "../escape", "old-key")).hasMessageContaining("ID");
        verifyNoInteractions(managed, nativeStore, redis);
    }

    @Test
    @SuppressWarnings("unchecked")
    void migrationImportsIntoCurrentModelWithDeterministicIdWhileLiveWritesAreFrozen() {
        properties.setWritesEnabled(false);
        when(managed.getStore()).thenReturn(nativeStore);
        service.importMemory(42L, "original text", "legacy_abc", "suts:vector:data:memory:42:abc");
        var documents = ArgumentCaptor.forClass(List.class);
        verify(nativeStore).add(documents.capture());
        Document document = (Document) documents.getValue().get(0);
        assertThat(document.getId()).isEqualTo("legacy_abc");
        assertThat(document.getMetadata()).containsEntry("legacyKey", "suts:vector:data:memory:42:abc")
                .containsEntry("embeddingModel", "test-model");
        when(redis.exists(properties.effectivePrefix(ai) + "legacy_abc")).thenReturn(true);
        assertThat(service.containsDocument("legacy_abc")).isTrue();
        verify(redis).exists(properties.effectivePrefix(ai) + "legacy_abc");
    }

    private static AiConfig config() {
        var config = new AiConfig();
        config.setEmbeddingModelName("test-model");
        config.setEmbeddingHost("https://embedding.invalid/v1");
        config.getVector().setDimension(3);
        return config;
    }

    private static Document document(String id, String text, long owner, String model, String schema, int dimension, double score) {
        Map<String, Object> metadata = new HashMap<>(Map.of("userId", String.valueOf(owner), "schemaVersion", schema,
                "embeddingModel", model, "embeddingModelTag", java.util.HexFormat.of().formatHex(model.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "embeddingDimension", dimension));
        return Document.builder().id(id).text(text).metadata(metadata).score(score).build();
    }
}
