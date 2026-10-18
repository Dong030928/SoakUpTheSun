package org.hgc.suts.gateway.memory.transactions;

import org.hgc.suts.gateway.config.MemoryTransactionProperties;

public final class TransactionTestConfig {
    private TransactionTestConfig() { }
    public static MemoryTransactionProperties create() {
        var config = new MemoryTransactionProperties();
        config.setEnabled(true);
        config.setNameServer("127.0.0.1:19876");
        config.setTopic("SutsMemoryArchiveIT");
        config.setProducerGroup("agent-memory-transactions-it-producer");
        config.setConsumerGroup("agent-memory-transactions-it-consumer");
        config.setGeneration("agent-transactions-integration-only");
        config.setRecoveryWindowSeconds(60);
        config.setEvidenceRetentionSeconds(120);
        config.setRedriveAfterMs(1000);
        config.setReconcileIntervalMs(1000);
        return config;
    }
}
