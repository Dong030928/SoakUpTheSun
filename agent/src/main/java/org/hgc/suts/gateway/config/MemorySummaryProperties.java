package org.hgc.suts.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/** No invented model capacity, watermarks, provider TTL or history retention defaults. */
@Data
@ConfigurationProperties(prefix = "suts.ai.memory.summary")
public class MemorySummaryProperties {
    private boolean enabled;
    private boolean accepting = true;
    private boolean useSummaryContext = true;
    private boolean vectorEnabled = true;
    private int modelCapacityTokens;
    private int outputReserveTokens;
    private int safetyTokens;
    private int inputBudgetTokens;
    private int lowTokens;
    private int highTokens;
    private int summaryMaxTokens;
    private int summaryMaxChars;
    private String provider = "deepseek";
    private String cacheMode = "automatic";
    private List<CachePolicy> cachePolicies = new ArrayList<>();
    private long scanIntervalMs = 5000;
    private int scanBatch = 50;
    private long workerLeaseMs = 120000;
    private long retryDelayMs = 10000;
    private int maxRawTurns = 1000;
    private int maxStateBytes = 8 * 1024 * 1024;
    private int maxInputChars = 262144;

    public void validate(boolean transactionsEnabled, int modelTimeoutMs) {
        Assert.state(enabled && transactionsEnabled, "Summary memory requires M02 transaction archival");
        Assert.isTrue(inputBudgetTokens > 0 && outputReserveTokens > 0 && safetyTokens > 0
                        && (long) inputBudgetTokens + outputReserveTokens + safetyTokens <= modelCapacityTokens,
                "Explicit B, C, O, S with B <= C - O - S are required");
        Assert.isTrue(lowTokens > 0 && lowTokens < highTokens && highTokens < inputBudgetTokens,
                "Explicit watermarks must satisfy 0 < L < H < B");
        Assert.isTrue(summaryMaxTokens > 0 && summaryMaxTokens <= outputReserveTokens
                        && summaryMaxChars > 0 && summaryMaxChars <= 65536,
                "Explicit summary output limits fitting the output reserve are required");
        Assert.isTrue("deepseek".equals(provider) && cacheMode != null && cacheMode.matches("[A-Za-z0-9_-]{1,64}"),
                "The current ChatModel adapter is DeepSeek; configure its actual provider and cache mode");
        Assert.isTrue(scanIntervalMs >= 100 && scanBatch > 0 && scanBatch <= 500
                        && workerLeaseMs > modelTimeoutMs + 15000L && workerLeaseMs <= 3600000
                        && retryDelayMs >= 100 && maxRawTurns >= 2 && maxRawTurns <= 10000
                        && maxStateBytes >= 262144 && maxStateBytes <= 32 * 1024 * 1024
                        && maxInputChars >= 1024 && maxInputChars <= 1048576,
                "Invalid summary worker or storage limits");
        var identities = new java.util.HashSet<String>();
        for (CachePolicy policy : cachePolicies) {
            policy.validate();
            Assert.isTrue(identities.add(policy.provider + "/" + policy.model + "/" + policy.mode),
                    "Duplicate provider/model/cache-mode policy");
        }
    }

    public enum Renewal { CREATION, CACHE_HIT, PROVIDER_EXPIRY }
    public enum ClockOrigin { REQUEST_STARTED, RESPONSE_COMPLETED }

    @Data
    public static class CachePolicy {
        private String provider;
        private String model;
        private String mode;
        private String version;
        private String vendorBasis;
        private long ttlSeconds;
        private Renewal renewal;
        private ClockOrigin clockOrigin;
        public void validate() {
            Assert.hasText(provider, "Cache provider is required");
            Assert.hasText(model, "Cache model is required");
            Assert.hasText(mode, "Cache mode is required");
            Assert.hasText(version, "Cache policy version is required");
            Assert.hasText(vendorBasis, "Document the vendor's TTL and renewal rule; do not infer a TTL");
            Assert.notNull(renewal, "Vendor renewal rule is required");
            Assert.isTrue(renewal == Renewal.PROVIDER_EXPIRY || clockOrigin != null,
                    "Configure the vendor's timing origin explicitly for a duration-based TTL");
            Assert.isTrue(ttlSeconds >= 0 && ttlSeconds <= 31536000
                            && (renewal == Renewal.PROVIDER_EXPIRY || ttlSeconds > 0),
                    "A vendor duration or provider-reported expiration is required");
        }
    }
}
