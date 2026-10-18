package org.hgc.suts.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

@Data
@ConfigurationProperties("suts.ai.memory.transactions")
public class MemoryTransactionProperties {
    private boolean enabled;
    private boolean accepting = true;
    private boolean consumerEnabled = true;
    private String nameServer = "";
    private String topic = "";
    private String producerGroup = "";
    private String consumerGroup = "";
    private String generation = "";
    // No guessed data-retention window: operators must cover their Broker/retry/audit policy explicitly.
    private long evidenceRetentionSeconds;
    private long recoveryWindowSeconds;
    private int maxPendingPerConversation = 100;
    private int maxEvidencePerConversation = 1000;
    private int maxLedgerBytes = 4 * 1024 * 1024;
    private int maxMessageBytes = 256 * 1024;
    private int sendTimeoutMs = 5000;
    private int maxReconsumeTimes = 16;
    private long redriveAfterMs = 60000;
    private long reconcileIntervalMs = 5000;
    private int reconcileBatch = 50;

    public void validate(boolean archiveEnabled, boolean writesEnabled) {
        Assert.state(archiveEnabled && (!accepting || writesEnabled), "Transaction mode requires JDBC archive and enabled producer writes");
        Assert.hasText(nameServer, "Transaction name-server is required");
        for (String name : new String[]{topic, producerGroup, consumerGroup}) {
            Assert.isTrue(name != null && name.matches("[A-Za-z0-9_-]{1,127}") && !name.startsWith("rmq_sys"),
                    "Dedicated transaction topic/groups are required");
        }
        Assert.isTrue(generation != null && generation.matches("[A-Za-z0-9_-]{1,64}"), "Explicit Redis generation is required");
        Assert.isTrue(recoveryWindowSeconds > 0 && evidenceRetentionSeconds >= recoveryWindowSeconds
                        && evidenceRetentionSeconds <= 315360000L,
                "Explicit evidence retention must cover the Broker/retry/audit recovery window");
        Assert.isTrue(maxPendingPerConversation > 0 && maxPendingPerConversation <= 1000
                        && maxEvidencePerConversation > maxPendingPerConversation && maxEvidencePerConversation <= 10000,
                "Invalid bounded transaction evidence capacity");
        Assert.isTrue(maxMessageBytes >= 1024 && maxMessageBytes <= 1048576
                        && maxLedgerBytes >= maxMessageBytes * 2 && maxLedgerBytes <= 16777216,
                "Invalid transaction byte budgets");
        Assert.isTrue(sendTimeoutMs >= 100 && sendTimeoutMs <= 30000 && maxReconsumeTimes >= 1 && maxReconsumeTimes <= 100,
                "Invalid transaction network/retry limits");
        Assert.isTrue(reconcileIntervalMs >= 1000 && redriveAfterMs >= reconcileIntervalMs
                        && reconcileBatch > 0 && reconcileBatch <= 500,
                "Invalid reconciliation limits");
    }
}
