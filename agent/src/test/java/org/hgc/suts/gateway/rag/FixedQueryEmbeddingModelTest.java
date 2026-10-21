package org.hgc.suts.gateway.rag;

import org.junit.jupiter.api.Test;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingOptions;
import org.springframework.ai.embedding.EmbeddingRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixedQueryEmbeddingModelTest {
    @Test
    void preservesTheValidatedVectorAndCopiesEveryArrayBoundary() {
        float[] original = {0.6f, 0.8f, 0};
        EmbeddingModel model = new FixedQueryEmbeddingModel("饮品偏好", original, 3);
        original[0] = 99;

        float[] first = model.embed("饮品偏好");
        assertThat(first).containsExactly(0.6f, 0.8f, 0);
        assertThat(first).isNotSameAs(original);
        first[1] = 99;

        float[] second = model.embed(new String("饮品偏好"));
        assertThat(second).containsExactly(0.6f, 0.8f, 0);
        assertThat(second).isNotSameAs(first);
        assertThat(model.dimensions()).isEqualTo(3);
        assertThat(model.dimensions()).isEqualTo(3);
    }

    @Test
    void bindsTheExactQueryWithoutTrimmingOrChangingCase() {
        var model = new FixedQueryEmbeddingModel(" Tea ", new float[]{1, 0}, 2);
        assertThat(model.embed(" Tea ")).containsExactly(1, 0);
        for (String other : List.of("Tea", " tea ", " Tea", "Tea ", "", "another query")) {
            assertThatThrownBy(() -> model.embed(other)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> model.embed((String) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEveryBatchDocumentAndResponseEntryPointEvenForMatchingText() {
        EmbeddingModel model = new FixedQueryEmbeddingModel("饮品偏好", new float[]{1, 0}, 2);
        var document = Document.builder().text("饮品偏好").metadata(Map.of("userId", "42")).build();
        var options = EmbeddingOptions.builder().build();

        assertThatThrownBy(() -> model.embed(document)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> model.getEmbeddingContent(document)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> model.embed(List.of("饮品偏好"))).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> model.embed(List.of("饮品偏好", "其他文本"))).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> model.embed(List.of(document), options, documents -> {
            throw new AssertionError("Query-only models must not invoke a document batching strategy");
        })).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> model.embedForResponse(List.of("饮品偏好"))).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> model.call(new EmbeddingRequest(List.of("饮品偏好"), options)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> model.call(new EmbeddingRequest(List.of("其他文本"), options)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThat(model.dimensions()).isEqualTo(2);
        assertThat(model.embed("饮品偏好")).containsExactly(1, 0);
    }

    @Test
    void separateQueryInstancesRemainIndependentWhenUsedInSequence() {
        var first = new FixedQueryEmbeddingModel("first", new float[]{1, 0}, 2);
        var second = new FixedQueryEmbeddingModel("second", new float[]{0, 1, 0}, 3);

        assertThat(first.embed("first")).containsExactly(1, 0);
        assertThat(second.embed("second")).containsExactly(0, 1, 0);
        assertThat(first.dimensions()).isEqualTo(2);
        assertThat(second.dimensions()).isEqualTo(3);
        assertThatThrownBy(() -> first.embed("second")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> second.embed("first")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentQueryInstancesCannotShareOrMutateEachOthersVectors() throws Exception {
        var first = new FixedQueryEmbeddingModel("first", new float[]{1, 0}, 2);
        var second = new FixedQueryEmbeddingModel("second", new float[]{0, 1, 0}, 3);
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var firstTask = executor.submit(() -> assertConcurrentReads(first, "first", "second", new float[]{1, 0}, ready, start));
            var secondTask = executor.submit(() -> assertConcurrentReads(second, "second", "first", new float[]{0, 1, 0}, ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            firstTask.get(5, TimeUnit.SECONDS);
            secondTask.get(5, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsInvalidConstructorInputsBeforeTheyCanReachRedis() {
        assertThatThrownBy(() -> new FixedQueryEmbeddingModel(null, new float[]{1}, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FixedQueryEmbeddingModel(" ", new float[]{1}, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FixedQueryEmbeddingModel("query", null, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FixedQueryEmbeddingModel("query", new float[]{1}, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FixedQueryEmbeddingModel("query", new float[]{1}, 2)).isInstanceOf(IllegalArgumentException.class);
        for (float[] vector : List.of(new float[]{}, new float[]{0, 0}, new float[]{Float.NaN, 1},
                new float[]{Float.POSITIVE_INFINITY, 1}, new float[]{Float.NEGATIVE_INFINITY, 1})) {
            assertThatThrownBy(() -> new FixedQueryEmbeddingModel("query", vector, vector.length))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static void assertConcurrentReads(FixedQueryEmbeddingModel model, String query, String other,
            float[] expected, CountDownLatch ready, CountDownLatch start) {
        ready.countDown();
        try {
            assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
        for (int i = 0; i < 100; i++) {
            assertThat(model.dimensions()).isEqualTo(expected.length);
            float[] result = model.embed(query);
            assertThat(result).containsExactly(expected);
            result[0] = 99;
            assertThatThrownBy(() -> model.embed(other)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
