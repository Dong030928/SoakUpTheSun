package org.hgc.suts.gateway.manager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CheckedEmbeddingModelTest {
    @Test
    void dimensionsNeverProbesProviderAndDocumentMetadataIsNotEmbedded() {
        var delegate = mock(EmbeddingModel.class);
        var checked = new CheckedEmbeddingModel(delegate, 3);
        assertThat(checked.dimensions()).isEqualTo(3);
        verifyNoInteractions(delegate);
        when(delegate.call(any())).thenAnswer(invocation -> {
            EmbeddingRequest request = invocation.getArgument(0);
            assertThat(request.getInstructions()).containsExactly("原始对话");
            return new EmbeddingResponse(List.of(new Embedding(new float[]{1, 0, 0}, 0)));
        });
        checked.embed(Document.builder().text("原始对话").metadata(Map.of("userId", "42", "token", "private")).build());
        verify(delegate).call(any());
    }

    @ParameterizedTest
    @MethodSource("invalidVectors")
    void rejectsInvalidVectorsBeforeRedis(float[] vector) {
        var delegate = mock(EmbeddingModel.class);
        when(delegate.call(any())).thenReturn(new EmbeddingResponse(List.of(new Embedding(vector, 0))));
        assertThatThrownBy(() -> new CheckedEmbeddingModel(delegate, 3).embed("hello"))
                .isInstanceOf(IllegalStateException.class);
    }

    static Stream<float[]> invalidVectors() {
        return Stream.of(new float[]{}, new float[]{1}, new float[]{0, 0, 0},
                new float[]{Float.NaN, 0, 1}, new float[]{Float.POSITIVE_INFINITY, 0, 1});
    }

    @Test
    void rejectsMissingAndReorderedBatchResponses() {
        var delegate = mock(EmbeddingModel.class);
        var checked = new CheckedEmbeddingModel(delegate, 3);
        when(delegate.call(any())).thenReturn(new EmbeddingResponse(List.of()));
        assertThatThrownBy(() -> checked.embed("hello")).hasMessageContaining("count");
        when(delegate.call(any())).thenReturn(new EmbeddingResponse(List.of(
                new Embedding(new float[]{1, 0, 0}, 1), new Embedding(new float[]{0, 1, 0}, 0))));
        assertThatThrownBy(() -> checked.embed(List.of("first", "second"))).hasMessageContaining("order");
    }

    @Test
    void largeFiniteVectorCannotBecomeZeroDuringCosineNormalization() {
        var delegate = mock(EmbeddingModel.class);
        when(delegate.call(any())).thenReturn(new EmbeddingResponse(List.of(
                new Embedding(new float[]{Float.MAX_VALUE, Float.MAX_VALUE, 0}, 0))));
        float[] vector = new CheckedEmbeddingModel(delegate, 3).embed("hello");
        assertThat(vector[0]).isCloseTo((float) (1 / Math.sqrt(2)), org.assertj.core.data.Offset.offset(0.00001f));
        assertThat(vector[1]).isEqualTo(vector[0]);
        assertThat(vector[2]).isZero();
    }
}
