package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.memory.archive.ArchivedTurn;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

/** Persistent hot state. No model-cache TTL is applied to this record or to transaction proof. */
public record SummaryState(int schemaVersion, String generation, String conversationId, long userId, long epoch,
                           long head, long version, long coveredThrough, String summary, String appliedJob,
                           List<RawTurn> raw, String policy, SummaryPolicy.Description cachePolicy,
                           long cacheCreatedAt, long cacheHitAt, long expiresAt,
                           long activity, int overheadTokens, Pending pending, boolean compressionRequested) {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    public SummaryState {
        org.springframework.util.Assert.state(schemaVersion == 1 && epoch > 0 && head >= coveredThrough
                        && coveredThrough >= 0 && version >= 0 && summary != null && raw != null
                        && raw.size() == head - coveredThrough, "Invalid summary coverage");
        ArchivedTurn.validateIdentity(userId, conversationId, epoch);
        long sequence = coveredThrough;
        for (RawTurn turn : raw) org.springframework.util.Assert.state(turn.sequence == ++sequence, "Summary input has a gap");
        raw = List.copyOf(raw);
    }
    public String encode() { return JSON.writeValueAsString(this); }
    public static SummaryState decode(String json) { return JSON.readValue(json, SummaryState.class); }
    public SummaryState append(ArchivedTurn turn) {
        org.springframework.util.Assert.state(turn.sequence() == head + 1 && turn.epoch() == epoch, "Summary head changed");
        var turns = new java.util.ArrayList<>(raw); turns.add(RawTurn.of(turn));
        return copy(turn.sequence(), version, coveredThrough, summary, appliedJob, turns, policy, cacheCreatedAt,
                cacheHitAt, expiresAt, activity + 1, pending, compressionRequested);
    }
    public SummaryState withPending(Pending job) {
        return copy(head, version, coveredThrough, summary, appliedJob, raw, policy, cacheCreatedAt, cacheHitAt,
                expiresAt, activity, job, compressionRequested);
    }
    public SummaryState requestCompression() {
        return copy(head, version, coveredThrough, summary, appliedJob, raw, policy, cacheCreatedAt, cacheHitAt,
                expiresAt, activity, pending, true);
    }
    public SummaryState cache(String fingerprint, long created, long hit, long expiry) {
        return copy(head, version, coveredThrough, summary, appliedJob, raw, fingerprint, created, hit, expiry,
                activity, pending, compressionRequested);
    }
    public SummaryState cache(SummaryPolicy.Selected selected, long created, long hit, long expiry) {
        return new SummaryState(1, generation, conversationId, userId, epoch, head, version, coveredThrough, summary,
                appliedJob, raw, selected.fingerprint(), selected.description(), created, hit, expiry, activity,
                overheadTokens, pending, compressionRequested);
    }
    public SummaryState overhead(int tokens) {
        return new SummaryState(1, generation, conversationId, userId, epoch, head, version, coveredThrough, summary,
                appliedJob, raw, policy, cachePolicy, cacheCreatedAt, cacheHitAt, expiresAt, activity,
                tokens, pending, compressionRequested);
    }
    public SummaryState merged(SummaryEvent event, String result) {
        return copy(head, version + 1, event.to(), result, event.jobId(), raw.stream().filter(t -> t.sequence > event.to()).toList(),
                policy, 0, 0, 0, activity, pending, false);
    }
    private SummaryState copy(long h, long v, long c, String s, String job, List<RawTurn> turns, String p,
                              long created, long hit, long expiry, long a, Pending pending, boolean requested) {
        return new SummaryState(1, generation, conversationId, userId, epoch, h, v, c, s, job, turns, p, cachePolicy,
                created, hit, expiry, a, overheadTokens, pending, requested);
    }
    public record RawTurn(long sequence, String user, String assistant, String payloadHash) {
        public RawTurn {
            org.springframework.util.Assert.isTrue(sequence > 0 && user != null && !user.isBlank() && user.length() <= 65536
                    && assistant != null && !assistant.isBlank() && assistant.length() <= 65536
                    && payloadHash != null && payloadHash.matches("[a-f0-9]{64}"), "Invalid complete summary turn");
        }
        public static RawTurn of(ArchivedTurn turn) { return new RawTurn(turn.sequence(), turn.userText(), turn.assistantText(), turn.payloadHash()); }
        public List<Message> messages() { return List.of(new UserMessage(user), new AssistantMessage(assistant)); }
    }
    public record Pending(String jobId, String body, long worker, long leaseUntil, long sentAt) { }
}
