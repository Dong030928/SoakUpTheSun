package org.hgc.suts.gateway.memory.archive;

import org.springframework.ai.chat.messages.Message;

import java.util.List;
import java.util.Optional;

/** Internal server-side API, not a public controller or a model tool. */
public interface ConversationArchive {
    ApplyResult archive(ArchivedTurn turn);
    Optional<Snapshot> snapshot(long userId, String conversationId, long epoch);

    record ApplyResult(boolean duplicate, long highestSeen, long archivedThrough) { }
    record Snapshot(long userId, String conversationId, long epoch, long highestSeen, long archivedThrough,
                    long lastActivity, boolean retired, List<Message> messages) {
        public Snapshot { messages = List.copyOf(messages); }

        /** The caller supplies an independently verified producer checkpoint; highestSeen alone is not proof. */
        public boolean matchesCheckpoint(long expectedThrough) {
            return !retired && expectedThrough > 0 && highestSeen == expectedThrough && archivedThrough == expectedThrough;
        }
    }
}
