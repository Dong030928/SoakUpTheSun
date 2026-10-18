package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.memory.archive.ArchivedTurn;
import org.springframework.util.Assert;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Immutable, self-contained complete turns, never a pointer to an expiring Redis key. */
public record SummaryEvent(int schemaVersion, String generation, String jobId, long userId, String conversationId,
                           long epoch, long baseVersion, long from, long to, String previousSummary,
                           List<SummaryState.RawTurn> turns) {
    private static final JsonMapper JSON = JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    public SummaryEvent {
        Assert.isTrue(schemaVersion == 1 && generation != null && generation.matches("[A-Za-z0-9_-]{1,64}")
                        && epoch > 0 && epoch <= 9999999999999L && baseVersion >= 0 && to <= 9999999999999L
                        && from > 0 && to >= from && turns != null && turns.size() == to - from + 1
                        && previousSummary != null && previousSummary.length() <= 65536,
                "Invalid summary event");
        ArchivedTurn.validateIdentity(userId, conversationId, epoch);
        long sequence = from;
        for (var turn : turns) Assert.isTrue(turn.sequence() == sequence++, "Summary event input is not contiguous");
        turns = List.copyOf(turns);
        String expected = identity(generation, conversationId, epoch, baseVersion, from, to, previousSummary, turns);
        if (jobId == null) jobId = expected;
        Assert.isTrue(expected.equals(jobId), "Summary job identity conflicts with immutable input");
    }
    public static SummaryEvent of(SummaryState state, List<SummaryState.RawTurn> turns) {
        return new SummaryEvent(1, state.generation(), null, state.userId(), state.conversationId(), state.epoch(),
                state.version(), state.coveredThrough() + 1, turns.get(turns.size() - 1).sequence(), state.summary(), turns);
    }
    public byte[] encode(int maxBytes) {
        byte[] bytes = JSON.writeValueAsBytes(this);
        Assert.isTrue(bytes.length <= maxBytes, "Summary message exceeds MQ capacity"); return bytes;
    }
    public String body(int maxBytes) { return new String(encode(maxBytes), StandardCharsets.UTF_8); }
    public static SummaryEvent decode(byte[] bytes, int maxBytes, String generation) {
        Assert.isTrue(bytes != null && bytes.length > 0 && bytes.length <= maxBytes, "Invalid summary message size");
        var event = JSON.readValue(bytes, SummaryEvent.class);
        Assert.isTrue(generation.equals(event.generation), "Summary generation changed"); return event;
    }
    private static String identity(String generation, String id, long epoch, long base, long from, long to,
                                   String previous, List<SummaryState.RawTurn> turns) {
        return SummaryPolicy.hash(JSON.writeValueAsString(List.of(generation, id, epoch, base, from, to, previous, turns)));
    }
    public String documentId() { return "summary_" + SummaryPolicy.hash(conversationId + "|" + epoch + "|" + (baseVersion + 1)); }
}
