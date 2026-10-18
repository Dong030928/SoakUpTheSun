package org.hgc.suts.gateway.memory;

import org.springframework.ai.chat.memory.ChatMemoryRepository;

/** A request-scoped, fenced view of conversation storage; persistence is not automatic archival. */
public interface ConversationMemoryStore {
    Session open(String conversationId);

    interface Session extends AutoCloseable {
        ChatMemoryRepository repository();
        void assertValid();
        @Override void close();
    }
}
