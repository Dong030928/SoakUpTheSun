package org.hgc.suts.gateway.memory.transactions;

import java.util.concurrent.atomic.LongAdder;

/** Process-level, low-cardinality operational counters. No history, credentials or user labels. */
public final class TransactionMetrics {
    public final LongAdder committed = new LongAdder(), aborted = new LongAdder(), unknown = new LongAdder();
    public final LongAdder checks = new LongAdder(), consumed = new LongAdder(), consumeFailures = new LongAdder();
    public final LongAdder receiptFailures = new LongAdder(), redriven = new LongAdder(), reconciliationFailures = new LongAdder();
    public final LongAdder capacityRejected = new LongAdder(), retryExhaustionRisk = new LongAdder();
    public final LongAdder overdueObservations = new LongAdder();
    public final java.util.concurrent.atomic.AtomicLong maxObservedPendingAgeMs = new java.util.concurrent.atomic.AtomicLong();
    public String snapshot() {
        return "committed=" + committed.sum() + ", aborted=" + aborted.sum() + ", unknown=" + unknown.sum()
                + ", checks=" + checks.sum() + ", consumed=" + consumed.sum() + ", consumeFailures=" + consumeFailures.sum()
                + ", receiptFailures=" + receiptFailures.sum() + ", redriven=" + redriven.sum()
                + ", reconciliationFailures=" + reconciliationFailures.sum() + ", capacityRejected=" + capacityRejected.sum()
                + ", retryExhaustionRisk=" + retryExhaustionRisk.sum() + ", overdueObservations=" + overdueObservations.sum()
                + ", maxObservedPendingAgeMs=" + maxObservedPendingAgeMs.get();
    }
}
