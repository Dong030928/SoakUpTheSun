package org.hgc.suts.gateway.memory.summary;

import cn.hutool.json.JSONUtil;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.transactions.RedisTransactionLedger;
import org.hgc.suts.gateway.memory.transactions.TransactionTransport;
import org.springframework.ai.chat.messages.Message;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.hgc.suts.gateway.memory.RedisConversationMemoryRepository.key;

/** State and proof have independent lifetimes from the provider cache. No cross-slot Lua. */
public final class RedisSummaryStateStore implements AutoCloseable {
    public static final String IDLE_INDEX = "suts:agent:memory:summary:expiry";
    private static final DefaultRedisScript<Long> CAS = script("memory_summary_cas.lua");
    private static final DefaultRedisScript<Long> REGISTER = script("memory_summary_register.lua");
    private static final DefaultRedisScript<Long> PROOF = script("memory_summary_proof.lua");
    private final StringRedisTemplate redis;
    private final RedisTransactionLedger ledger;
    private final MemoryTransactionProperties tx;
    private final MemoryProperties window;
    private final SummaryPolicy policy;
    private org.springframework.data.redis.core.Cursor<String> cursor;
    public RedisSummaryStateStore(StringRedisTemplate redis, RedisTransactionLedger ledger,
            MemoryTransactionProperties tx, MemoryProperties window, SummaryPolicy policy) {
        this.redis = redis; this.ledger = ledger; this.tx = tx; this.window = window; this.policy = policy;
    }
    public SummaryState read(String id) {
        ledger.requireGeneration();
        String raw = redis.opsForValue().get(key(id, "summary"));
        if (raw == null) return null;
        Assert.state(raw.getBytes(StandardCharsets.UTF_8).length <= policy.current().getMaxStateBytes(), "Summary state exceeds capacity");
        SummaryState state = SummaryState.decode(raw);
        Assert.state(state.conversationId().equals(id) && state.generation().equals(tx.getGeneration()), "Corrupt summary identity");
        return state;
    }
    public String checked(SummaryState state) {
        String encoded = state.encode();
        Assert.state(state.raw().size() <= policy.current().getMaxRawTurns()
                && encoded.getBytes(StandardCharsets.UTF_8).length <= policy.current().getMaxStateBytes(), "Summary state capacity reached");
        return encoded;
    }
    public void initialize(SummaryState state, RedisConversationMemoryRepository.Lease lease) {
        Assert.state(cas(null, state, lease.fencingToken(), false), "Summary state changed during initialization");
        index(state);
    }
    public boolean cas(SummaryState expected, SummaryState next, String lease, boolean merge) {
        ledger.requireGeneration();
        String id = next.conversationId();
        Long result = redis.execute(CAS, List.of(key(id, "summary"), key(id, "lock"), key(id, "transactions"), key(id, "messages")),
                expected == null ? "" : expected.encode(), checked(next), lease == null ? "" : lease,
                merge ? tx.getGeneration() : "", Long.toString(next.epoch()), Long.toString(next.coveredThrough()),
                merge ? snapshot(next) : "");
        return Long.valueOf(1).equals(result);
    }
    public SummaryState update(String id, UnaryOperator<SummaryState> transform) {
        for (int attempt = 0; attempt < 16; attempt++) {
            SummaryState previous = read(id);
            Assert.notNull(previous, "Summary state is missing; verified recovery is required");
            SummaryState next = transform.apply(previous);
            if (next.equals(previous) || cas(previous, next, null, false)) return next;
        }
        throw new IllegalStateException("Summary state is busy");
    }
    public TransactionTransport.State register(SummaryEvent event, SummaryState expected, String lease) {
        try {
            ledger.requireGeneration();
            String body = event.body(tx.getMaxMessageBytes());
            var pending = new SummaryState.Pending(event.jobId(), body, 0, 0, System.currentTimeMillis());
            var next = expected.withPending(pending);
            Long result = redis.execute(REGISTER, List.of(key(event.conversationId(), "lock"), key(event.conversationId(), "summary"),
                            key(event.conversationId(), "summary-proof")), lease, expected.encode(), checked(next), event.jobId(), body,
                    tx.getGeneration(), Long.toString(System.currentTimeMillis()), Long.toString(tx.getEvidenceRetentionSeconds() * 1000),
                    Integer.toString(tx.getMaxEvidencePerConversation()), Integer.toString(tx.getMaxLedgerBytes()));
            if (Long.valueOf(1).equals(result)) return TransactionTransport.State.COMMITTED;
        } catch (RuntimeException ignored) { /* Lost response is resolved using the shared proof, never a guessed rollback. */ }
        return check(event);
    }
    public TransactionTransport.State check(SummaryEvent event) {
        try {
            ledger.requireGeneration();
            var proof = proof(event.conversationId());
            var entry = proof == null ? null : proof.getJSONObject("events").getJSONObject(event.jobId());
            if (entry != null && event.jobId().equals(entry.getStr("hash"))) return TransactionTransport.State.COMMITTED;
        } catch (RuntimeException ignored) { }
        return TransactionTransport.State.UNKNOWN;
    }
    public void acknowledge(SummaryEvent event) { proofMutation(event, "ACK", 0); }
    public boolean claimRedrive(SummaryEvent event) { return proofMutation(event, "CLAIM", tx.getRedriveAfterMs()); }
    private boolean proofMutation(SummaryEvent event, String operation, long delay) {
        ledger.requireGeneration();
        return Long.valueOf(1).equals(redis.execute(PROOF, List.of(key(event.conversationId(), "summary-proof")), tx.getGeneration(),
                event.jobId(), Long.toString(System.currentTimeMillis()), operation, Long.toString(delay)));
    }
    private cn.hutool.json.JSONObject proof(String id) {
        String raw = redis.opsForValue().get(key(id, "summary-proof"));
        if (raw == null) return null;
        Assert.state(raw.getBytes(StandardCharsets.UTF_8).length <= tx.getMaxLedgerBytes(), "Summary proof capacity exceeded");
        var proof = JSONUtil.parseObj(raw);
        Assert.state(tx.getGeneration().equals(proof.getStr("generation")) && proof.getJSONObject("events") != null, "Corrupt summary proof");
        return proof;
    }
    public List<SummaryEvent> pendingEvents(String id) {
        ledger.requireGeneration();
        var proof = proof(id);
        if (proof == null) return List.of();
        List<SummaryEvent> events = new ArrayList<>();
        for (Object item : proof.getJSONObject("events").values()) {
            var entry = JSONUtil.parseObj(item);
            String body = entry.getStr("body");
            if (body != null) events.add(SummaryEvent.decode(body.getBytes(StandardCharsets.UTF_8), tx.getMaxMessageBytes(), tx.getGeneration()));
        }
        return events;
    }
    public boolean fenceWorker(SummaryEvent event, long worker, long until) {
        var next = update(event.conversationId(), state -> {
            if (state.appliedJob().equals(event.jobId())) return state;
            var pending = state.pending();
            Assert.state(pending != null && pending.jobId().equals(event.jobId()) && pending.worker() <= worker,
                    "Summary worker no longer owns this intent");
            return state.withPending(new SummaryState.Pending(pending.jobId(), pending.body(), worker, until, pending.sentAt()));
        });
        return next.appliedJob().equals(event.jobId()) || next.pending().worker() == worker;
    }
    public boolean merge(SummaryEvent event, long worker, String result) {
        for (int attempt = 0; attempt < 16; attempt++) {
            var state = read(event.conversationId());
            Assert.notNull(state, "Missing summary state requires recovery");
            if (state.version() == event.baseVersion() + 1 && state.appliedJob().equals(event.jobId())) return true;
            if (state.epoch() != event.epoch() || state.version() != event.baseVersion() || state.coveredThrough() != event.from() - 1) return false;
            var pending = state.pending();
            Assert.state(pending != null && pending.jobId().equals(event.jobId()) && pending.worker() == worker
                    && pending.leaseUntil() > System.currentTimeMillis(), "Expired summary worker cannot merge");
            Assert.state(state.raw().size() >= event.turns().size()
                    && state.raw().subList(0, event.turns().size()).equals(event.turns()), "Summary coverage/input changed");
            if (cas(state, state.merged(event, result), null, true)) return true;
        }
        throw new IllegalStateException("Summary merge not confirmed; retry from the durable result");
    }
    public void finishMerge(SummaryEvent event) {
        var state = update(event.conversationId(), s -> s.appliedJob().equals(event.jobId()) && s.pending() != null
                && s.pending().jobId().equals(event.jobId()) ? s.withPending(null) : s);
        acknowledge(event);
        index(state);
    }
    public void index(SummaryState state) {
        if (state.expiresAt() > 0) redis.opsForZSet().add(IDLE_INDEX, state.conversationId(), state.expiresAt());
        else redis.opsForZSet().remove(IDLE_INDEX, state.conversationId());
    }
    public List<String> due(long now, int limit) {
        var set = redis.opsForZSet().rangeByScore(IDLE_INDEX, 0, now, 0, limit);
        return set == null ? List.of() : List.copyOf(set);
    }
    public synchronized List<String> scan(int limit) {
        if (cursor == null || cursor.isClosed()) cursor = redis.scan(ScanOptions.scanOptions()
                .match("suts:agent:memory:v2:*").count(limit).build());
        var ids = new java.util.LinkedHashSet<String>();
        int examined = 0;
        while (examined++ < limit && cursor.hasNext()) {
            String key = cursor.next(); int start = key.indexOf('{'), end = key.indexOf('}');
            if (start >= 0 && end > start) ids.add(key.substring(start + 1, end));
        }
        if (!cursor.hasNext()) { cursor.close(); cursor = null; }
        return List.copyOf(ids);
    }
    private String snapshot(SummaryState state) {
        List<Message> messages = new ArrayList<>();
        int remaining = window.getMaxContextChars();
        for (int i = state.raw().size() - 1; i >= 0 && messages.size() + 2 <= window.getMaxMessages(); i--) {
            var turn = state.raw().get(i);
            String user = turn.user().substring(0, Math.min(turn.user().length(), window.getMaxMessageChars()));
            String answer = turn.assistant().substring(0, Math.min(turn.assistant().length(), window.getMaxMessageChars()));
            if (user.length() + answer.length() > remaining) break;
            messages.addAll(0, List.of(new org.springframework.ai.chat.messages.UserMessage(user),
                    new org.springframework.ai.chat.messages.AssistantMessage(answer)));
            remaining -= user.length() + answer.length();
        }
        return RedisConversationMemoryRepository.encode(messages, state.conversationId());
    }
    @Override public synchronized void close() { if (cursor != null) cursor.close(); }
    private static DefaultRedisScript<Long> script(String file) {
        var script = new DefaultRedisScript<Long>(); script.setLocation(new ClassPathResource("lua/" + file)); script.setResultType(Long.class); return script;
    }
}
