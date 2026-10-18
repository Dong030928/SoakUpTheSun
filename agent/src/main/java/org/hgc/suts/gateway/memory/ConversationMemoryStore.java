package org.hgc.suts.gateway.memory;

import org.springframework.ai.chat.memory.ChatMemoryRepository;

/** A request-scoped, fenced view of conversation storage; persistence is not automatic archival. */
public interface ConversationMemoryStore {
    Session open(String conversationId);

    interface Session extends AutoCloseable {
        ChatMemoryRepository repository();
        void assertValid();
        default void prepare() { assertValid(); }
        default MemoryCommitStatus commit(String conversationId, String input, String answer,
                                          java.util.List<org.springframework.ai.chat.messages.Message> snapshot) {
            repository().saveAll(conversationId, snapshot);
            return MemoryCommitStatus.CACHED;
        }
        @Override void close();
    }
}
