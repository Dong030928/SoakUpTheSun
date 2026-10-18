package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemorySummaryProperties;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.archive.ArchivedTurn;

import java.util.ArrayList;
import java.util.UUID;

/** These watermarks and TTL are synthetic test values, never production defaults/vendor claims. */
public final class SummaryTestFixture {
    private SummaryTestFixture() { }
    public static MemorySummaryProperties config() {
        var p = new MemorySummaryProperties(); p.setEnabled(true);
        p.setModelCapacityTokens(16000); p.setInputBudgetTokens(12000); p.setOutputReserveTokens(2000); p.setSafetyTokens(2000);
        p.setLowTokens(600); p.setHighTokens(1800); p.setSummaryMaxTokens(600); p.setSummaryMaxChars(1200);
        p.setWorkerLeaseMs(30000); p.setRetryDelayMs(100);
        return p;
    }
    public static AiConfig ai() { var ai = new AiConfig(); ai.setModelName("fixture-model"); ai.setTimeout(1000); return ai; }
    public static String id() { return ConversationMemory.conversationId(42L, "summary-" + UUID.randomUUID()); }
    public static ArchivedTurn turn(String id, long sequence, String user, String answer) {
        return new ArchivedTurn(UUID.randomUUID().toString(), 42, id, 1, sequence, user, answer, System.currentTimeMillis());
    }
    public static SummaryState state(int count, int chars) {
        String id = id(); var turns = new ArrayList<SummaryState.RawTurn>();
        for (int i = 1; i <= count; i++) turns.add(SummaryState.RawTurn.of(turn(id, i, "q".repeat(chars), "a".repeat(chars))));
        return new SummaryState(1, "agent-transactions-integration-only", id, 42, 1, count, 0, 0,
                "", "", turns, "", null, 0, 0, 0, 0, 0, null, false);
    }
    public static MemorySummaryProperties.CachePolicy ttl(MemorySummaryProperties.Renewal renewal) {
        var c = new MemorySummaryProperties.CachePolicy(); c.setProvider("deepseek"); c.setModel("fixture-model");
        c.setMode("automatic"); c.setVersion("synthetic-v1"); c.setVendorBasis("TEST FIXTURE ONLY: not a real vendor TTL");
        c.setTtlSeconds(60); c.setRenewal(renewal); c.setClockOrigin(MemorySummaryProperties.ClockOrigin.RESPONSE_COMPLETED); return c;
    }
}
