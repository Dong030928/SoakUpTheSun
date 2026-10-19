package org.hgc.suts.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "suts.ai.memory.profile")
public class MemoryProfileProperties {
    private boolean enabled;
    private boolean accepting = true;
    private boolean vectorEnabled = true;
    private int maxCandidates = 12;
    private int maxFacts = 100;
    private int maxSnapshotFacts = 20;
    private int maxSnapshotChars = 6000;
    private int maxValueChars = 500;
    private int maxQuoteChars = 1000;
    private int maxResultChars = 24000;
    private int scanBatch = 20;
    private long scanIntervalMs = 5000;
    private long workerLeaseMs = 30000;
    private long retryDelayMs = 10000;
    private double similarityThreshold = 0.7;
    private String indexName = "suts:agent:profile:index:v1";
    private String prefix = "suts:agent:profile:vector:v1:";
    private List<String> attributePrefixes = List.of("preference", "habit", "device", "accessibility", "personal");

    public void validate() {
        Assert.isTrue(maxCandidates > 0 && maxCandidates <= 50 && maxFacts > 0 && maxFacts <= 500,
                "Invalid bounded profile capacity");
        Assert.isTrue(maxSnapshotFacts > 0 && maxSnapshotFacts <= maxFacts && maxSnapshotChars > 0
                && maxSnapshotChars <= 32000 && maxValueChars > 0 && maxValueChars <= 2000
                && maxQuoteChars > 0 && maxQuoteChars <= 4000 && maxResultChars > 0 && maxResultChars <= 131072,
                "Invalid profile text limits");
        Assert.isTrue(scanBatch > 0 && scanBatch <= 100 && scanIntervalMs >= 100 && workerLeaseMs >= 1000
                && retryDelayMs > 0, "Invalid bounded profile worker settings");
        Assert.isTrue(Double.isFinite(similarityThreshold) && similarityThreshold >= 0 && similarityThreshold <= 1,
                "Invalid profile similarity threshold");
        Assert.isTrue(indexName != null && indexName.startsWith("suts:agent:profile:")
                && prefix != null && prefix.startsWith("suts:agent:profile:") && prefix.endsWith(":"),
                "Profile vectors require their own profile namespace");
        Assert.isTrue(attributePrefixes != null && !attributePrefixes.isEmpty() && attributePrefixes.size() <= 20
                && attributePrefixes.stream().allMatch(s -> s != null && s.matches("[a-z][a-z0-9_]{0,31}")),
                "Invalid profile attribute allowlist");
    }

    public void attribute(String key) {
        Assert.isTrue(key != null && key.matches("[a-z][a-z0-9_]{0,31}\\.[a-z][a-z0-9_.-]{0,62}")
                && attributePrefixes.contains(key.substring(0, key.indexOf('.'))), "Profile attribute is not allowed");
    }
}
