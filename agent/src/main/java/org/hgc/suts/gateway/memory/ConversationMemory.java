package org.hgc.suts.gateway.memory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.common.exception.ClientException;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.InMemoryChatMemoryRepository;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/** Uses a request-local framework memory window, then commits a complete sanitized turn under a Redis lease. */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConversationMemory {
    private final ConversationMemoryStore repository;
    private final MemoryProperties properties;

    public Turn open(Long userId, String session, String input) {
        if (input.length() > properties.getMaxInputChars()) throw new ClientException("输入超过允许的长度");
        String id = conversationId(userId, session);
        return new Turn(id, input, repository.open(id));
    }

    public static String conversationId(Long userId, String session) {
        if (userId == null || userId < 1) throw new ClientException("用户未登录");
        String value = session == null || session.isBlank() ? "default" : session;
        if (!value.matches("[A-Za-z0-9_-]{1,64}")) throw new ClientException("X-Conversation-Id 格式无效");
        try {
            String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
            return "user:" + userId + ":" + hash;
        } catch (java.security.NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }

    public final class Turn implements AutoCloseable {
        private final String id;
        private final String input;
        private final ConversationMemoryStore.Session lease;
        private final List<Message> previous;
        private MemoryCommitStatus status = MemoryCommitStatus.DISABLED;

        private Turn(String id, String input, ConversationMemoryStore.Session lease) {
            this.id = id;
            this.input = input;
            this.lease = lease;
            try { lease.prepare(); previous = boundedPairs(lease.repository().findByConversationId(id), properties.getMaxContextChars()); }
            catch (RuntimeException ex) { lease.close(); throw ex; }
        }

        public String id() { return id; }
        public MemoryCommitStatus status() { return status; }

        public void assertValid() { lease.assertValid(); }

        public MessageChatMemoryAdvisor advisor(int remainingChars) {
            ChatMemory memory = MessageWindowChatMemory.builder().maxMessages(properties.getMaxMessages())
                    .chatMemoryRepository(new InMemoryChatMemoryRepository()).build();
            List<Message> selected = boundedPairs(previous, Math.max(0, remainingChars));
            if (!selected.isEmpty()) memory.add(id, selected);
            // Keep the default +200 order, outside enrichment(+250) and tools(+300).
            return MessageChatMemoryAdvisor.builder(memory).build();
        }

        public boolean commit(String answer) {
            if (!properties.isWritesEnabled()) return false;
            try {
                List<Message> complete = new ArrayList<>(previous);
                complete.add(new UserMessage(bounded(input)));
                complete.add(new AssistantMessage(bounded(answer)));
                List<Message> clean = boundedPairs(complete, properties.getMaxContextChars());
                // The framework applies turn-boundary eviction; the Redis repository fences the atomic write.
                // This is replacement of the prepared snapshot, not a second append of the same turn.
                ChatMemory window = MessageWindowChatMemory.builder().maxMessages(properties.getMaxMessages()).build();
                window.add(id, clean);
                status = lease.commit(id, input, answer, window.get(id));
                return status.saved();
            } catch (RuntimeException ex) {
                status = MemoryCommitStatus.DEGRADED;
                log.warn("短期会话保存不可用，本轮回复保持有效 ({})", ex.getClass().getSimpleName());
                return false;
            }
        }

        @Override
        public void close() { lease.close(); }
    }

    private String bounded(String text) { return text.substring(0, Math.min(text.length(), properties.getMaxMessageChars())); }

    private List<Message> boundedPairs(List<Message> messages, int budget) {
        List<Message> kept = new ArrayList<>();
        int chars = 0;
        for (int i = messages.size() - 2; i >= 0; i -= 2) {
            if (!(messages.get(i) instanceof UserMessage user) || !(messages.get(i + 1) instanceof AssistantMessage assistant)
                    || assistant.hasToolCalls() || user.getText() == null || assistant.getText() == null) continue;
            String question = bounded(user.getText());
            String answer = bounded(assistant.getText());
            if (chars + question.length() + answer.length() > budget || kept.size() + 2 > properties.getMaxMessages()) break;
            chars += question.length() + answer.length();
            kept.add(0, new AssistantMessage(answer));
            kept.add(0, new UserMessage(question));
        }
        return kept;
    }
}
