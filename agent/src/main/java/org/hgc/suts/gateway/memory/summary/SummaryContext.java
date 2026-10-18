package org.hgc.suts.gateway.memory.summary;

import org.springframework.ai.chat.messages.Message;
import java.util.List;

public final class SummaryContext {
    private SummaryContext() { }
    public record Request(String system, String input, String toolSchema, String rawInput) {
        public Request(String system, String input, String toolSchema) { this(system, input, toolSchema, input); }
    }
    public record Selection(List<Message> history, int outputReserve, int estimatedTokens, long summaryVersion, String policy) { }
    /** Only a provider adapter may supply expiresAt; a null value never manufactures an expiry. */
    public record CacheObservation(long completedAt, boolean cacheHit, Long expiresAt, long summaryVersion, long requestStartedAt, String policy) {
        public CacheObservation(long completedAt, boolean cacheHit, Long expiresAt) { this(completedAt, cacheHit, expiresAt, -1, completedAt, null); }
        public CacheObservation(long completedAt, boolean cacheHit, Long expiresAt, long summaryVersion) {
            this(completedAt, cacheHit, expiresAt, summaryVersion, completedAt, null);
        }
        public CacheObservation(long completedAt, boolean cacheHit, Long expiresAt, long summaryVersion, long requestStartedAt) {
            this(completedAt, cacheHit, expiresAt, summaryVersion, requestStartedAt, null);
        }
    }
    public record Source(String conversationId, long epoch, long sequence) { }
}
