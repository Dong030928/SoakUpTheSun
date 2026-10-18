package org.hgc.suts.gateway.manager;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import redis.clients.jedis.RedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class ManagedMemoryVectorStoreTest {
    @Test
    void namespacesChangeWithProviderModelOrDimensionAndNeverContainCredentials() {
        var properties = new MemoryVectorProperties();
        var config = ai();
        String prefix = properties.effectivePrefix(config);
        config.setEmbeddingApiKey("secret-api-key");
        assertThat(properties.effectivePrefix(config)).isEqualTo(prefix).doesNotContain("secret-api-key");
        config.setEmbeddingModelName("other-model");
        assertThat(properties.effectivePrefix(config)).isNotEqualTo(prefix);
        config = ai();
        config.getVector().setDimension(4);
        assertThat(properties.effectivePrefix(config)).isNotEqualTo(prefix);
        config = ai();
        config.setEmbeddingHost("https://other.invalid/v1");
        assertThat(properties.effectivePrefix(config)).isNotEqualTo(prefix);
        properties.setPrefix("suts:vector:data:memory:");
        assertThatThrownBy(properties::validate).hasMessageContaining("legacy");
    }

    @Test
    void nativeStoreInitializationIsLazyAndDoesNotProbeEmbeddingDimensions() {
        var redis = mock(RedisClient.class);
        var model = mock(EmbeddingModel.class);
        var ai = ai();
        var properties = new MemoryVectorProperties();
        var managed = new ManagedMemoryVectorStore(redis, model, ai, properties);
        verifyNoInteractions(redis, model);
        when(redis.ftList()).thenReturn(Set.of(properties.effectiveIndexName(ai)));
        when(redis.ftInfo(anyString())).thenReturn(schema(properties.effectivePrefix(ai), 3));
        when(redis.get(properties.manifestKey(ai))).thenReturn(properties.identity(ai));
        assertThat(managed.getStore()).isSameAs(managed.getStore());
        verify(redis).ftInfo(properties.effectiveIndexName(ai));
        verify(redis).set(eq(properties.manifestKey(ai)), eq(properties.identity(ai)), any());
        verifyNoInteractions(model);
    }

    @Test
    void missingRedisModulesAreBoundedAndManifestMismatchIsRejected() {
        var redis = mock(RedisClient.class);
        var ai = ai();
        var properties = new MemoryVectorProperties();
        var model = mock(EmbeddingModel.class);
        when(redis.ftList()).thenThrow(new IllegalStateException("FT._LIST unavailable"));
        var managed = new ManagedMemoryVectorStore(redis, model, ai, properties);
        assertThatThrownBy(managed::getStore).hasMessageContaining("unavailable");
        assertThatThrownBy(managed::getStore).hasMessageContaining("temporarily");
        verify(redis, times(1)).ftList();
        reset(redis);
        when(redis.ftList()).thenReturn(Set.of(properties.effectiveIndexName(ai)));
        when(redis.ftInfo(anyString())).thenReturn(schema(properties.effectivePrefix(ai), 3));
        when(redis.get(properties.manifestKey(ai))).thenReturn("different model identity");
        assertThatThrownBy(new ManagedMemoryVectorStore(redis, model, ai, properties)::getStore)
                .hasMessageContaining("unavailable").hasRootCauseMessage("Long-term memory namespace manifest does not match its embedding configuration");
    }

    @Test
    void validatesExistingNativeIndexSchemaInsteadOfTrustingItsName() {
        String prefix = "test:v2:";
        ManagedMemoryVectorStore.validateSchema(schema(prefix, 3), prefix, 3);
        assertThatThrownBy(() -> ManagedMemoryVectorStore.validateSchema(schema(prefix, 4), prefix, 3))
                .hasMessageContaining("dimension");
        assertThatThrownBy(() -> ManagedMemoryVectorStore.validateSchema(schema("wrong:", 3), prefix, 3))
                .hasMessageContaining("prefix");
        var wrongType = schema(prefix, 3);
        wrongType.put("index_definition", List.of("key_type", "HASH", "prefixes", List.of(prefix)));
        assertThatThrownBy(() -> ManagedMemoryVectorStore.validateSchema(wrongType, prefix, 3)).hasMessageContaining("JSON");
        var malformed = schema(prefix, 3);
        ((List<?>) malformed.get("attributes")).remove(2);
        var missingOwner = malformed;
        assertThatThrownBy(() -> ManagedMemoryVectorStore.validateSchema(missingOwner, prefix, 3)).hasMessageContaining("userId");
    }

    static AiConfig ai() {
        var ai = new AiConfig();
        ai.setEmbeddingHost("https://embedding.invalid/v1");
        ai.setEmbeddingModelName("model-with/slash");
        ai.getVector().setDimension(3);
        return ai;
    }

    static Map<String, Object> schema(String prefix, int dimension) {
        List<Object> attributes = new ArrayList<>();
        attributes.add(field("content", "TEXT"));
        attributes.add(List.of("identifier", "$.embedding", "attribute", "embedding", "type", "VECTOR",
                "data_type", "FLOAT32", "dim", (long) dimension, "distance_metric", "COSINE"));
        attributes.add(field("userId", "TAG"));
        attributes.add(field("schemaVersion", "TAG"));
        attributes.add(field("embeddingModel", "TAG"));
        attributes.add(field("embeddingModelTag", "TAG"));
        attributes.add(field("embeddingDimension", "NUMERIC"));
        attributes.add(field("createdAt", "NUMERIC"));
        return new java.util.HashMap<>(Map.of("index_definition", List.of("key_type", "JSON", "prefixes", List.of(prefix)),
                "attributes", attributes));
    }

    private static List<String> field(String name, String type) {
        return List.of("identifier", "$." + name, "attribute", name, "type", type);
    }
}
