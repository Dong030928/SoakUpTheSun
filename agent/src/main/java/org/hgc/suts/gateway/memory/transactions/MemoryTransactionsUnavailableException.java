package org.hgc.suts.gateway.memory.transactions;

/** Raised before model/tools if reliable acceptance or complete recovery is unavailable. */
public final class MemoryTransactionsUnavailableException extends RuntimeException {
    public MemoryTransactionsUnavailableException() { super("对话记忆暂不可用，请稍后重试"); }
}
