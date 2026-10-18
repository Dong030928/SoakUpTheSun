package org.hgc.suts.gateway.memory;

/** Fail closed when ownership of a conversation can no longer be confirmed. */
public class MemoryLeaseLostException extends RuntimeException {
    public MemoryLeaseLostException() {
        super("会话处理锁已失效，请稍后重试。");
    }
}
