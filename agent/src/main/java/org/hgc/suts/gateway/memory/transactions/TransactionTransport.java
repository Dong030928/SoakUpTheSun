package org.hgc.suts.gateway.memory.transactions;

import java.util.function.Supplier;

public interface TransactionTransport extends AutoCloseable {
    enum State { COMMITTED, ABORTED, UNKNOWN }
    void preflight();
    /** The local callback runs only after the half-message ACK; an exception has an uncertain outcome. */
    State send(ArchiveEvent event, Supplier<State> localTransaction);
    @Override default void close() { }
}
