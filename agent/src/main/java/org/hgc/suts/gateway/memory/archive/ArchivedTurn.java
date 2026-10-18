package org.hgc.suts.gateway.memory.archive;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Immutable future MQ envelope. Identity comes from trusted server authentication, never model output. */
public record ArchivedTurn(String eventId, long userId, String conversationId, long epoch, long sequence,
                           String userText, String assistantText, long occurredAt) {
    public ArchivedTurn {
        Assert.isTrue(eventId != null && eventId.matches("[A-Za-z0-9_-]{1,128}"), "Invalid archive event ID");
        validateIdentity(userId, conversationId, epoch);
        Assert.isTrue(sequence > 0 && sequence < Long.MAX_VALUE, "Invalid archive sequence");
        Assert.isTrue(occurredAt >= 0, "Invalid archive timestamp");
        for (String value : new String[]{userText, assistantText}) {
            Assert.isTrue(StringUtils.hasText(value) && value.length() <= 65536, "Invalid archive text budget");
        }
    }

    public static void validateIdentity(long userId, String conversationId, long epoch) {
        Assert.isTrue(userId > 0 && epoch > 0 && conversationId != null
                && conversationId.matches("user:" + userId + ":[a-f0-9]{64}"), "Archive identity does not match owner");
    }

    public String payloadHash() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : new String[]{eventId, Long.toString(userId), conversationId, Long.toString(epoch),
                    Long.toString(sequence), userText, assistantText, Long.toString(occurredAt)}) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
                digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }
}
