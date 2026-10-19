package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.memory.summary.SummaryPolicy;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

public final class ProfileData {
    public static final JsonMapper JSON = JsonMapper.builder().build();
    private ProfileData() { }
    public record Candidate(String attribute, String value, String operation, long sourceSequence, String quote) { }
    public record Envelope(String summaryText, List<Candidate> profileChanges) { }
    public record Source(String kind, String conversationId, long epoch, long sequence, String quote,
                         String payloadHash, long occurredAt, String jobId) { }
    public record Fact(String id, long userId, String attribute, long version, String state, String value,
                       Source source, long updatedAt) { }
    public record Snapshot(long userId, long revision, long memoryEpoch, Map<String, Long> versions,
                           List<Fact> facts) { }
    public record Extraction(String id, long userId, String snapshot, String eventBody, String result,
                             String state, long worker, long leaseUntil) { }
    public record Projection(String id, String factId, long userId, long version, String target,
                             String state, long worker, long leaseUntil) { }
    public record ExtractionStatus(String jobId, String state, long attempts, long retryAt, String errorType, long updatedAt) { }
    public static String factId(long user, String attribute) { return SummaryPolicy.hash(user + "|" + attribute); }
    public static String documentId(String fact, long version) { return "profile_" + SummaryPolicy.hash(fact + "|" + version); }
    public static String json(Object value) { return JSON.writeValueAsString(value); }
}
