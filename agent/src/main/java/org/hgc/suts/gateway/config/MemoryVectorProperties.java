package org.hgc.suts.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** A new namespace is used for each embedding provider/model/dimension combination. */
@Data
@Component
@ConfigurationProperties(prefix = "suts.ai.memory.vector")
public class MemoryVectorProperties {
    public static final String SCHEMA_VERSION = "2";
    private boolean enabled = true;
    private boolean writesEnabled = true;
    private boolean initializeSchema = true;
    private String indexName = "suts:agent:memory:index:v2";
    private String prefix = "suts:agent:memory:vector:v2:";
    private int topK = 3;
    private double similarityThreshold = 0.7;
    private int maxRecallChars = 3000;
    private int maxDocumentChars = 6000;
    private long retryDelayMillis = 30000;

    public void validate() {
        Assert.hasText(indexName, "Memory vector index name must be configured");
        Assert.hasText(prefix, "Memory vector prefix must be configured");
        Assert.isTrue(!indexName.equals("suts:vector:index:memory"), "The legacy vector index must not be reused");
        Assert.isTrue(!prefix.startsWith("suts:vector:data:memory:"), "The legacy vector prefix must not be reused");
        Assert.isTrue(topK > 0 && topK <= 20, "Memory top-k must be between 1 and 20");
        Assert.isTrue(Double.isFinite(similarityThreshold) && similarityThreshold >= 0 && similarityThreshold <= 1,
                "Memory similarity threshold must be between 0 and 1");
        Assert.isTrue(maxRecallChars > 0 && maxDocumentChars > 0, "Memory character limits must be positive");
        Assert.isTrue(retryDelayMillis > 0, "Memory retry delay must be positive");
    }

    public String identity(AiConfig config) {
        Assert.hasText(config.getEmbeddingHost(), "Embedding host is required for a memory namespace");
        Assert.hasText(config.getEmbeddingModelName(), "Embedding model is required for a memory namespace");
        Assert.isTrue(config.getVector() != null && config.getVector().getDimension() > 0,
                "Embedding dimension must be positive");
        String endpoint = config.getEmbeddingHost().replaceAll("/+$", "");
        return SCHEMA_VERSION + "\n" + endpoint + "\n" + config.getEmbeddingModelName() + "\n" + config.getVector().getDimension();
    }

    public String fingerprint(AiConfig config) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(identity(config).getBytes(StandardCharsets.UTF_8))).substring(0, 16);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }
    }

    public String effectiveIndexName(AiConfig config) { return indexName + ":" + fingerprint(config); }
    public String effectivePrefix(AiConfig config) { return prefix + fingerprint(config) + ":"; }
    public String manifestKey(AiConfig config) { return "suts:agent:memory:manifest:v2:" + fingerprint(config); }

    /** Spring AI 2.0.1's Redis TAG converter does not escape slashes in provider model names. */
    public String embeddingModelTag(AiConfig config) {
        Assert.hasText(config.getEmbeddingModelName(), "Embedding model is required for a memory tag");
        // Hex is reversible and exact, while its alphabet cannot contain Redis TAG query syntax.
        return HexFormat.of().formatHex(config.getEmbeddingModelName().getBytes(StandardCharsets.UTF_8));
    }
}
