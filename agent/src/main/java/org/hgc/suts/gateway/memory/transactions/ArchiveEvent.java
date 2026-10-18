package org.hgc.suts.gateway.memory.transactions;

import org.hgc.suts.gateway.memory.archive.ArchivedTurn;
import org.springframework.util.Assert;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/** Whitelisted, self-contained wire contract; no token, tool context or model configuration. */
public record ArchiveEvent(int schemaVersion, String generation, ArchivedTurn turn, String payloadHash) {
    private static final JsonMapper JSON = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    public ArchiveEvent {
        Assert.isTrue(schemaVersion == 1 && generation != null && generation.matches("[A-Za-z0-9_-]{1,64}"),
                "Invalid transaction envelope version/generation");
        Assert.notNull(turn, "A complete archived turn is required");
        Assert.isTrue(turn.sequence() <= 9999999999999L && turn.epoch() <= 9999999999999L,
                "Transaction counters exceed the exact Lua/cjson serialization range");
        Assert.isTrue(turn.payloadHash().equals(payloadHash), "Transaction payload checksum mismatch");
    }
    public static ArchiveEvent of(String generation, ArchivedTurn turn) {
        return new ArchiveEvent(1, generation, turn, turn.payloadHash());
    }
    public byte[] encode(int limit) {
        byte[] value = JSON.writeValueAsBytes(this);
        Assert.isTrue(value.length <= limit, "Archive message exceeds the configured Broker budget");
        return value;
    }
    public static ArchiveEvent decode(byte[] value, int limit, String generation) {
        Assert.isTrue(value != null && value.length > 0 && value.length <= limit, "Invalid archive message size");
        ArchiveEvent event = JSON.readValue(value, ArchiveEvent.class);
        Assert.isTrue(event.generation.equals(generation), "Archive generation does not match this deployment");
        return event;
    }
}
