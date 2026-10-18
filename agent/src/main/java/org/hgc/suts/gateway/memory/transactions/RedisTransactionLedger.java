package org.hgc.suts.gateway.memory.transactions;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hgc.suts.gateway.memory.RedisConversationMemoryRepository.key;
import static org.hgc.suts.gateway.memory.transactions.TransactionTransport.State;

/** Persistent, bounded per-conversation evidence. Every script key shares the existing conversation hash slot. */
public final class RedisTransactionLedger implements AutoCloseable {
    public static final String GENERATION_KEY = "suts:agent:memory:transactions:generation";
    private static final DefaultRedisScript<Long> INIT = script("memory_tx_init.lua");
    private static final DefaultRedisScript<Long> COMMIT = script("memory_tx_commit.lua");
    private static final DefaultRedisScript<Long> ACK = script("memory_tx_ack.lua");
    private static final DefaultRedisScript<Long> CLAIM = script("memory_tx_claim.lua");
    private static final DefaultRedisScript<Long> RELEASE = script("memory_tx_release.lua");
    private final StringRedisTemplate redis;
    private final MemoryTransactionProperties config;
    private final TransactionMetrics metrics;
    private org.springframework.data.redis.core.Cursor<String> scan;

    public RedisTransactionLedger(StringRedisTemplate redis, MemoryTransactionProperties config, TransactionMetrics metrics) {
        this.redis = redis; this.config = config; this.metrics = metrics;
    }

    public void verifyDeployment() {
        requireGeneration();
        try (var connection = redis.getConnectionFactory().getConnection()) {
            Assert.state(!(connection instanceof org.springframework.data.redis.connection.RedisClusterConnection),
                    "Transaction reconciliation requires standalone Redis, not a Cluster connection");
            var server = connection.serverCommands();
            var persistence = server.info("persistence");
            var policy = server.getConfig("maxmemory-policy");
            var memory = server.getConfig("maxmemory");
            var fsync = server.getConfig("appendfsync");
            Assert.state("1".equals(persistence.getProperty("aof_enabled"))
                            && "noeviction".equals(policy.getProperty("maxmemory-policy"))
                            && Long.parseLong(memory.getProperty("maxmemory", "0")) > 0
                            && "always".equals(fsync.getProperty("appendfsync")),
                    "Transactions require bounded noeviction Redis with AOF appendfsync always");
        }
    }

    public void requireGeneration() {
        Assert.state(config.getGeneration().equals(redis.opsForValue().get(GENERATION_KEY)),
                "Redis generation is absent or changed; stop traffic and reconcile before recovery");
    }

    public Ledger read(String id) {
        requireGeneration();
        String raw = redis.opsForValue().get(key(id, "transactions"));
        if (raw == null) return null;
        Assert.state(raw.getBytes(StandardCharsets.UTF_8).length <= config.getMaxLedgerBytes(), "Ledger exceeds capacity");
        JSONObject json = JSONUtil.parseObj(raw);
        Assert.state(Integer.valueOf(1).equals(json.getInt("schemaVersion")) && id.equals(json.getStr("conversationId"))
                        && config.getGeneration().equals(json.getStr("generation")) && json.getJSONObject("events") != null,
                "Corrupt transaction ledger");
        long head = json.getLong("head", -1L), through = json.getLong("archivedThrough", -1L);
        long epoch = json.getLong("epoch", -1L);
        Assert.state(head >= 0 && head <= 9999999999999L && through >= 0 && through <= head && epoch > 0 && epoch <= 9999999999999L,
                "Corrupt transaction watermarks");
        return new Ledger(epoch, head, through, json.getJSONObject("events"));
    }

    /** Only for an empty new conversation or an explicit, independently verified offline checkpoint. */
    public void initialize(String id, RedisConversationMemoryRepository.Lease lease, long epoch, long checkpoint) {
        Assert.isTrue(epoch > 0 && epoch <= 9999999999999L && checkpoint >= 0 && checkpoint <= 9999999999999L,
                "Invalid exact transaction checkpoint");
        requireGeneration();
        String payload = new JSONObject().set("schemaVersion", 1).set("generation", config.getGeneration())
                .set("conversationId", id).set("epoch", epoch).set("head", checkpoint).set("archivedThrough", checkpoint)
                .set("events", new JSONObject()).toString();
        Long result = redis.execute(INIT, List.of(key(id, "lock"), key(id, "transactions")), lease.fencingToken(), payload);
        Assert.state(result != null && result >= 0, "Cannot initialize transaction checkpoint without a lease");
    }

    public void admit(Ledger ledger) {
        int pending = 0;
        long now = System.currentTimeMillis();
        int retained = 0;
        for (String eventId : ledger.events.keySet()) {
            JSONObject entry = ledger.events.getJSONObject(eventId);
            if ("COMMITTED".equals(entry.getStr("state")) && !entry.getBool("archived", false)) pending++;
            if (!(entry.getBool("archived", false) || "ABORTED".equals(entry.getStr("state")))
                    || now - entry.getLong("at", now) < config.getEvidenceRetentionSeconds() * 1000) retained++;
        }
        if (pending >= config.getMaxPendingPerConversation() || retained >= config.getMaxEvidencePerConversation()
                || ledger.events.toString().getBytes(StandardCharsets.UTF_8).length + 2L * config.getMaxMessageBytes() + 1024 > config.getMaxLedgerBytes()) {
            metrics.capacityRejected.increment();
            throw new MemoryTransactionsUnavailableException();
        }
    }

    public State commit(ArchiveEvent event, String leaseToken, long expectedHead, String snapshot, long hotTtlMs) {
        try {
            requireGeneration();
            var turn = event.turn();
            String body = new String(event.encode(config.getMaxMessageBytes()), StandardCharsets.UTF_8);
            Long result = redis.execute(COMMIT, List.of(key(turn.conversationId(), "lock"),
                            key(turn.conversationId(), "transactions"), key(turn.conversationId(), "messages")),
                    leaseToken, config.getGeneration(), Long.toString(turn.epoch()), Long.toString(expectedHead),
                    turn.eventId(), event.payloadHash(), body, snapshot, Long.toString(hotTtlMs),
                    Long.toString(System.currentTimeMillis()), Integer.toString(config.getMaxPendingPerConversation()),
                    Integer.toString(config.getMaxEvidencePerConversation()), Integer.toString(config.getMaxLedgerBytes()),
                    Long.toString(config.getEvidenceRetentionSeconds() * 1000), turn.conversationId());
            if (Long.valueOf(1).equals(result)) { metrics.committed.increment(); return State.COMMITTED; }
            if (Long.valueOf(-1).equals(result)) { metrics.aborted.increment(); return State.ABORTED; }
        } catch (RuntimeException ex) {
            // A transport/script failure is not proof of rollback. Read back shared evidence, never reapply blindly.
        }
        return check(event);
    }

    public State check(ArchiveEvent event) {
        try {
            Assert.state(config.getGeneration().equals(event.generation()), "Wrong transaction generation");
            Ledger ledger = read(event.turn().conversationId());
            if (ledger != null && ledger.epoch == event.turn().epoch()) {
                JSONObject entry = ledger.events.getJSONObject(event.turn().eventId());
                if (entry != null && event.payloadHash().equals(entry.getStr("hash"))
                        && event.turn().sequence() == entry.getLong("sequence", -1L)) {
                    if ("COMMITTED".equals(entry.getStr("state")) && ledger.head >= event.turn().sequence()) return State.COMMITTED;
                    if ("ABORTED".equals(entry.getStr("state"))) return State.ABORTED;
                }
            }
        } catch (RuntimeException ex) { /* Redis outage, corruption and absent evidence all stay UNKNOWN. */ }
        metrics.unknown.increment();
        return State.UNKNOWN;
    }

    public void acknowledge(ArchiveEvent event, long through) {
        requireGeneration();
        String id = event.turn().conversationId();
        Long result = redis.execute(ACK, List.of(key(id, "transactions")), config.getGeneration(),
                Long.toString(event.turn().epoch()), event.turn().eventId(), event.payloadHash(),
                Long.toString(through), Long.toString(System.currentTimeMillis()));
        Assert.state(Long.valueOf(1).equals(result), "Cannot confirm the archive receipt against producer evidence");
    }

    public boolean claimRedrive(ArchiveEvent event) {
        requireGeneration();
        Long result = redis.execute(CLAIM, List.of(key(event.turn().conversationId(), "transactions")),
                config.getGeneration(), event.turn().eventId(), event.payloadHash(),
                Long.toString(System.currentTimeMillis()), Long.toString(config.getRedriveAfterMs()));
        return Long.valueOf(1).equals(result);
    }

    /** Caller holds the lease, all workers are drained, SQL is verified and the audit/check window has elapsed. */
    public void releaseToCache(String id, RedisConversationMemoryRepository.Lease lease, long checkpoint, boolean dryRun) {
        Ledger state = read(id);
        Assert.state(state != null && state.head == checkpoint && state.archivedThrough == checkpoint,
                "Unarchived producer checkpoint cannot be released");
        Assert.state(config.getRecoveryWindowSeconds() > 0 && config.getEvidenceRetentionSeconds() >= config.getRecoveryWindowSeconds(),
                "Explicit evidence retention covering the recovery window is required for release");
        long now = System.currentTimeMillis();
        for (String eventId : state.events.keySet()) {
            var entry = state.events.getJSONObject(eventId);
            Assert.state(entry.getBool("archived", false) || "ABORTED".equals(entry.getStr("state")),
                    "Pending transaction evidence cannot be released");
            Assert.state(now - entry.getLong("at", now) >= config.getEvidenceRetentionSeconds() * 1000,
                    "Transaction evidence is still inside the checker/retry/audit retention window");
        }
        if (dryRun) return;
        String raw = redis.opsForValue().get(key(id, "transactions"));
        // Recheck the exact bytes validated above; a background change invalidates the release.
        Assert.state(raw != null && state.equals(read(id)), "Transaction evidence changed during release");
        Long result = redis.execute(RELEASE, List.of(key(id, "lock"), key(id, "transactions")), lease.fencingToken(), raw);
        Assert.state(Long.valueOf(1).equals(result), "Transaction evidence changed during release");
    }

    /** Incremental SCAN, never KEYS or a process-local pending queue. Cursor progress survives between batches. */
    public synchronized List<String> nextConversations(int limit) {
        List<String> ids = new ArrayList<>();
        if (scan == null || scan.isClosed()) scan = redis.scan(ScanOptions.scanOptions()
                .match("suts:agent:memory:v2:*:transactions").count(limit).build());
        while (ids.size() < limit && scan.hasNext()) {
            String value = scan.next();
            int start = value.indexOf('{'), end = value.indexOf('}');
            if (start >= 0 && end > start) ids.add(value.substring(start + 1, end));
        }
        if (!scan.hasNext()) { scan.close(); scan = null; }
        return ids;
    }

    @Override public synchronized void close() { if (scan != null) scan.close(); }
    private static DefaultRedisScript<Long> script(String name) {
        var script = new DefaultRedisScript<Long>();
        script.setLocation(new ClassPathResource("lua/" + name)); script.setResultType(Long.class); return script;
    }
    public record Ledger(long epoch, long head, long archivedThrough, JSONObject events) { }
}
