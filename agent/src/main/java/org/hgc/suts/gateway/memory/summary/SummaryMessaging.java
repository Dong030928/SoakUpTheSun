package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.memory.transactions.TransactionTransport;
import java.util.function.Supplier;

/** Shares the existing transaction producer/checker; no Outbox publisher is introduced. */
public interface SummaryMessaging {
    String TAG = "conversation-summary-v1";
    TransactionTransport.State sendSummary(SummaryEvent event, Supplier<TransactionTransport.State> localTransaction);
    interface Handler {
        void validate(byte[] body);
        TransactionTransport.State check(byte[] body);
        void consume(byte[] body);
    }
}
