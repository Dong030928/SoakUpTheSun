package org.hgc.suts.gateway.memory;

/** A different request currently owns this authenticated conversation. */
public class ConversationBusyException extends RuntimeException {
    public ConversationBusyException() {
        super("当前会话正在处理上一条消息，请稍后重试。");
    }
}
