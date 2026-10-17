package org.hgc.suts.gateway.manager;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.AiModelConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.embedding.EmbeddingModel;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AiModelConfigurationTest {
    @Test
    void missingCredentialsFailWithPropertyNames() {
        AiConfig config = new AiConfig();
        config.setApiHost("https://example.invalid");
        assertThatThrownBy(() -> new AiModelConfiguration().agentChatModel(config))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("suts.ai.api-key");
        config.setEmbeddingHost("https://example.invalid/v1");
        assertThatThrownBy(() -> new AiModelConfiguration().agentEmbeddingModel(config))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("suts.ai.embedding-api-key");
    }

    @Test
    void disablingEmbeddingNeedsNoProviderCredentials() {
        AiConfig config = new AiConfig();
        config.setEmbeddingEnabled(false);
        EmbeddingModel model = new AiModelConfiguration().agentEmbeddingModel(config);
        assertThatThrownBy(() -> model.embed("hello")).isInstanceOf(IllegalStateException.class).hasMessageContaining("disabled");
    }

    static Stream<float[]> invalidVectors() {
        return Stream.of(new float[0], new float[]{0, 0}, new float[]{1, Float.NaN}, new float[]{1, Float.POSITIVE_INFINITY});
    }

    @ParameterizedTest
    @MethodSource("invalidVectors")
    void invalidEmbeddingCannotBeUsedAsMemory(float[] vector) {
        AiConfig config = new AiConfig();
        config.getVector().setDimension(2);
        EmbeddingModel model = mock(EmbeddingModel.class);
        when(model.embed("hello")).thenReturn(vector);
        AiModelManager manager = new AiModelManager(mock(ChatClient.class), model, config);
        assertThatThrownBy(() -> manager.embed("hello")).isInstanceOf(IllegalStateException.class);
    }
}
