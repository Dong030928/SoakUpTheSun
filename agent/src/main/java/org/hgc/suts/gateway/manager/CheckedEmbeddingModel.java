package org.hgc.suts.gateway.manager;

import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.util.Assert;

import java.util.ArrayList;

/** Validates every framework embedding path and avoids a remote dimensions probe. */
public final class CheckedEmbeddingModel implements EmbeddingModel {
    private final EmbeddingModel delegate;
    private final int dimension;

    public CheckedEmbeddingModel(EmbeddingModel delegate, int dimension) {
        Assert.notNull(delegate, "Embedding model is required");
        Assert.isTrue(dimension > 0, "Embedding dimension must be positive");
        this.delegate = delegate;
        this.dimension = dimension;
    }

    @Override
    public EmbeddingResponse call(EmbeddingRequest request) {
        Assert.notNull(request, "Embedding request is required");
        request.getInstructions().forEach(text -> Assert.hasText(text, "Embedding input must not be empty"));
        var response = delegate.call(request);
        if (response == null || response.getResults() == null
                || response.getResults().size() != request.getInstructions().size()) {
            throw new IllegalStateException("Embedding response count does not match the request");
        }
        var checked = new ArrayList<Embedding>();
        for (int i = 0; i < response.getResults().size(); i++) {
            var embedding = response.getResults().get(i);
            if (embedding == null || embedding.getIndex() == null || embedding.getIndex() != i) {
                throw new IllegalStateException("Embedding response order does not match the request");
            }
            checked.add(new Embedding(validate(embedding.getOutput()), i, embedding.getMetadata()));
        }
        return new EmbeddingResponse(checked, response.getMetadata());
    }

    @Override
    public float[] embed(Document document) {
        Assert.notNull(document, "Embedding document is required");
        // Document metadata contains ownership; embed only the human-readable text.
        return embed(document.getText());
    }

    @Override
    public int dimensions() { return dimension; }

    private float[] validate(float[] vector) {
        if (vector == null || vector.length != dimension) {
            throw new IllegalStateException("Embedding dimension does not match configuration");
        }
        double normSquared = 0;
        for (float value : vector) {
            if (!Float.isFinite(value)) throw new IllegalStateException("Embedding contains a non-finite value");
            normSquared += (double) value * value;
        }
        if (normSquared == 0 || !Double.isFinite(normSquared)) {
            throw new IllegalStateException("Embedding returned an invalid zero vector");
        }
        // RedisVectorStore uses float accumulation for COSINE normalization. Normalize
        // with double precision first, so large finite values cannot overflow to zeros.
        double norm = Math.sqrt(normSquared);
        float[] normalized = new float[vector.length];
        for (int i = 0; i < vector.length; i++) normalized[i] = (float) (vector[i] / norm);
        return normalized;
    }
}
