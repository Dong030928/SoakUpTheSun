package org.hgc.suts.gateway.rag;

import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.BatchingStrategy;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingOptions;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.util.Assert;

import java.util.List;

/** Supplies one request's precomputed query vector to read-only vector store views. */
public final class FixedQueryEmbeddingModel implements EmbeddingModel {
    private final String query;
    private final float[] vector;
    private final int dimension;

    public FixedQueryEmbeddingModel(String query, float[] vector, int dimension) {
        Assert.hasText(query, "Fixed query must not be blank");
        Assert.notNull(vector, "Fixed query vector is required");
        Assert.isTrue(dimension > 0 && vector.length == dimension, "Fixed query vector dimension does not match");
        float[] copy = vector.clone();
        double normSquared = 0;
        for (float value : copy) {
            Assert.isTrue(Float.isFinite(value), "Fixed query vector must contain only finite values");
            normSquared += (double) value * value;
        }
        Assert.isTrue(normSquared > 0 && Double.isFinite(normSquared), "Fixed query vector must be nonzero");
        this.query = query;
        this.vector = copy;
        this.dimension = dimension;
    }

    @Override
    public float[] embed(String text) {
        Assert.isTrue(query.equals(text), "Fixed query vector cannot be used for a different query");
        return vector.clone();
    }

    @Override
    public int dimensions() { return dimension; }

    @Override
    public float[] embed(Document document) { throw queryOnly(); }

    @Override
    public String getEmbeddingContent(Document document) { throw queryOnly(); }

    @Override
    public List<float[]> embed(List<String> texts) { throw queryOnly(); }

    @Override
    public List<float[]> embed(List<Document> documents, EmbeddingOptions options, BatchingStrategy batchingStrategy) {
        throw queryOnly();
    }

    @Override
    public EmbeddingResponse embedForResponse(List<String> texts) { throw queryOnly(); }

    @Override
    public EmbeddingResponse call(EmbeddingRequest request) { throw queryOnly(); }

    private static UnsupportedOperationException queryOnly() {
        return new UnsupportedOperationException("Only the exact bound query can use this precomputed vector");
    }
}
