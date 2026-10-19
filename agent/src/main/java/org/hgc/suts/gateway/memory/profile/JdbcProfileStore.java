package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.config.MemoryProfileProperties;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.summary.SummaryEvent;
import org.hgc.suts.gateway.memory.summary.SummaryPolicy;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.util.*;

import static org.hgc.suts.gateway.memory.profile.ProfileData.*;

/** Authoritative facts and durable work. Network calls never execute inside its SQL transactions. */
public final class JdbcProfileStore {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final MemoryProfileProperties config;
    private final String target;
    private final int maxMessageBytes;
    private final String generation;

    public JdbcProfileStore(JdbcConversationArchive archive, MemoryProfileProperties config, String target,
                            int maxMessageBytes, String generation) {
        this.jdbc = archive.jdbcOperations(); this.transactions = archive.transactionOperations();
        this.config = config; this.target = target; this.maxMessageBytes = maxMessageBytes; this.generation = generation;
        config.validate();
        jdbc.queryForObject("SELECT COUNT(*) FROM agent_profile_user WHERE 1 = 0", Long.class);
    }

    public String target() { return target; }
    public SummaryEvent event(Extraction extraction) {
        return SummaryEvent.decode(extraction.eventBody().getBytes(java.nio.charset.StandardCharsets.UTF_8), maxMessageBytes, generation);
    }

    /** Freeze profile revision/deletion epoch before the model request, including on retries. */
    public Snapshot capture(SummaryEvent event) {
        return transactions.execute(status -> {
            var existing = extraction(event.jobId(), false);
            if (existing.isPresent()) {
                Assert.state(existing.get().eventBody().equals(event.body(maxMessageBytes)), "Profile input identity conflict");
                return JSON.readValue(existing.get().snapshot(), Snapshot.class);
            }
            ensureUser(event.userId());
            long[] user = lockUser(event.userId());
            // Another worker may have captured this job while we waited for the user row.
            existing = extraction(event.jobId(), false);
            if (existing.isPresent()) {
                Assert.state(existing.get().eventBody().equals(event.body(maxMessageBytes)), "Profile input identity conflict");
                return JSON.readValue(existing.get().snapshot(), Snapshot.class);
            }
            var facts = list(event.userId(), true, config.getMaxFacts());
            Map<String, Long> versions = new TreeMap<>();
            facts.forEach(f -> versions.put(f.attribute(), f.version()));
            List<Fact> visible = new ArrayList<>(); int chars = 0;
            for (Fact fact : facts) {
                if (!"ACTIVE".equals(fact.state())) continue;
                int size = json(fact).length();
                if (visible.size() >= config.getMaxSnapshotFacts() || chars + size > config.getMaxSnapshotChars()) continue;
                visible.add(fact); chars += size;
            }
            var snapshot = new Snapshot(event.userId(), user[0], user[1], versions, visible);
            jdbc.update("INSERT INTO agent_profile_extraction (summary_job_id, user_id, snapshot_json, event_body, state, updated_at) "
                            + "VALUES (?, ?, ?, ?, 'CAPTURED', ?)", event.jobId(), event.userId(), json(snapshot),
                    event.body(maxMessageBytes), System.currentTimeMillis());
            return snapshot;
        });
    }

    /** Invoked inside the same archive transaction as saving the plain summary result. */
    public void result(SummaryEvent event, String payload, boolean rejected) {
        Assert.state(payload != null && payload.length() <= config.getMaxResultChars(), "Profile output exceeds its limit");
        int changed = jdbc.update("UPDATE agent_profile_extraction SET result_body = ?, state = ?, updated_at = ? "
                        + "WHERE summary_job_id = ? AND user_id = ? AND state = 'CAPTURED'", payload,
                rejected ? "REJECTED" : "RESULT", System.currentTimeMillis(), event.jobId(), event.userId());
        Assert.state(changed == 1, "Profile result has no frozen input");
    }

    /** Invoked in the same SQL transaction as confirming the merged summary projection. */
    public void merged(SummaryEvent event) {
        jdbc.update("UPDATE agent_profile_extraction SET state = 'READY', updated_at = ? "
                + "WHERE summary_job_id = ? AND user_id = ? AND state = 'RESULT'", System.currentTimeMillis(), event.jobId(), event.userId());
    }
    public void stale(String job) {
        jdbc.update("UPDATE agent_profile_extraction SET state = 'STALE', updated_at = ? "
                + "WHERE summary_job_id = ? AND state IN ('CAPTURED','RESULT')", System.currentTimeMillis(), job);
    }

    public Optional<Extraction> extraction(String id, boolean lock) {
        return jdbc.query("SELECT summary_job_id,user_id,snapshot_json,event_body,result_body,state,worker_epoch,lease_until "
                        + "FROM agent_profile_extraction WHERE summary_job_id = ?" + (lock ? " FOR UPDATE" : ""),
                (rs, i) -> new Extraction(rs.getString(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), rs.getLong(7), rs.getLong(8)), id).stream().findFirst();
    }

    public Extraction claimExtraction(String id) {
        return transactions.execute(status -> {
            var row = extraction(id, true).orElseThrow(); long now = System.currentTimeMillis();
            if (!"READY".equals(row.state()) || row.leaseUntil() > now) return null;
            int changed = jdbc.update("UPDATE agent_profile_extraction SET worker_epoch=worker_epoch+1,lease_until=?,"
                            + "attempts=attempts+1,updated_at=? WHERE summary_job_id=? AND retry_at<=?",
                    now + config.getWorkerLeaseMs(), now, id, now);
            return changed == 1 ? extraction(id, false).orElseThrow() : null;
        });
    }

    /** Validate the complete batch before applying any candidate. */
    public void validate(SummaryEvent event, List<Candidate> candidates) {
        Assert.isTrue(candidates != null && candidates.size() <= config.getMaxCandidates(), "Too many profile candidates");
        Set<String> keys = new HashSet<>();
        for (Candidate candidate : candidates) {
            Assert.notNull(candidate, "Missing profile candidate"); config.attribute(candidate.attribute());
            Assert.isTrue(keys.add(candidate.attribute()), "Only one final candidate per attribute and input range is allowed");
            Assert.isTrue(candidate.operation() != null && List.of("ASSERT", "CORRECT", "FORGET").contains(candidate.operation()), "Invalid profile operation");
            Assert.isTrue(candidate.value() != null && !candidate.value().isBlank()
                    && candidate.value().length() <= config.getMaxValueChars(), "Invalid profile value");
            Assert.isTrue(candidate.quote() != null && !candidate.quote().isBlank()
                    && candidate.quote().length() <= config.getMaxQuoteChars(), "Invalid profile source quote");
            var turn = event.turns().stream().filter(t -> t.sequence() == candidate.sourceSequence()).findFirst().orElseThrow();
            Assert.isTrue(turn.user().contains(candidate.quote()), "Profile quote must come from the user's raw words");
        }
    }

    public void apply(Extraction claimed, List<Candidate> candidates) {
        var event = event(claimed); validate(event, candidates);
        transactions.executeWithoutResult(status -> {
            ensureUser(claimed.userId());
            long[] user = lockUser(claimed.userId());
            var current = extraction(claimed.id(), true).orElseThrow(); requireWorker(current.worker(), current.leaseUntil(), claimed.worker());
            Assert.state("READY".equals(current.state()), "Profile extraction already completed");
            Snapshot snapshot = JSON.readValue(current.snapshot(), Snapshot.class);
            if (snapshot.memoryEpoch() != user[1]) { finishExtraction(claimed, "STALE", null); return; }
            boolean changed = false, forgot = false;
            for (Candidate candidate : candidates) {
                Source source = source(event, candidate);
                var old = fact(claimed.userId(), factId(claimed.userId(), candidate.attribute()));
                if (old.isEmpty()) {
                    Assert.state(count(claimed.userId()) < config.getMaxFacts(), "Profile capacity reached");
                    boolean remove = "FORGET".equals(candidate.operation());
                    write(claimed.userId(), candidate.attribute(), 1, remove ? "DELETED" : "ACTIVE", candidate.value(), source);
                    forgot |= remove; changed = true; continue;
                }
                Fact prior = old.get();
                // A durable deletion is never automatically undone by extraction, even from a newly created job.
                if ("DELETED".equals(prior.state())) continue;
                Source previous = prior.source();
                boolean unchanged = snapshot.versions().getOrDefault(candidate.attribute(), 0L) == prior.version();
                // A delayed extraction must not downgrade a direct correction/conflict resolution.
                if ("DIRECT".equals(previous.kind()) && (!unchanged || source.occurredAt() <= previous.occurredAt())) continue;
                boolean sameConversation = "SUMMARY".equals(previous.kind()) && previous.conversationId().equals(source.conversationId())
                        && previous.epoch() == source.epoch();
                if (sameConversation && source.sequence() <= previous.sequence()) continue;
                if (prior.value().equals(candidate.value()) && "ACTIVE".equals(prior.state())
                        && !"FORGET".equals(candidate.operation())) continue;
                String next;
                if ("FORGET".equals(candidate.operation()) && unchanged) { next = "DELETED"; forgot = true; }
                else if (sameConversation && "ACTIVE".equals(prior.state())) next = "ACTIVE";
                else next = "CONFLICT"; // Cross-session ambiguity, stale snapshot or direct human correction wins no silent race.
                write(claimed.userId(), candidate.attribute(), prior.version() + 1, next, candidate.value(), source); changed = true;
            }
            if (changed) jdbc.update("UPDATE agent_profile_user SET revision=revision+1,memory_epoch=memory_epoch+? WHERE user_id=?",
                    forgot ? 1 : 0, claimed.userId());
            finishExtraction(claimed, "APPLIED", null);
        });
    }

    private Source source(SummaryEvent event, Candidate candidate) {
        var expected = event.turns().stream().filter(t -> t.sequence() == candidate.sourceSequence()).findFirst().orElseThrow();
        List<Source> result = jdbc.query("SELECT t.user_text,t.payload_hash,t.occurred_at FROM agent_memory_turn t "
                        + "JOIN agent_memory_session s ON s.conversation_id=t.conversation_id WHERE t.conversation_id=? "
                        + "AND t.epoch=? AND t.turn_seq=? AND s.user_id=? AND s.epoch=? AND s.retired=FALSE",
                (rs, i) -> {
                    Assert.state(rs.getString(1).contains(candidate.quote()), "Durable user source no longer matches");
                    Assert.state(expected.payloadHash().equals(rs.getString(2)), "Durable profile source hash changed");
                    return new Source("SUMMARY", event.conversationId(), event.epoch(), candidate.sourceSequence(), candidate.quote(),
                            rs.getString(2), rs.getLong(3), event.jobId());
                }, event.conversationId(), event.epoch(), candidate.sourceSequence(), event.userId(), event.epoch());
        Assert.state(result.size() == 1, "Wait for the exact archived user source"); return result.get(0);
    }

    public void reject(Extraction claimed, RuntimeException failure) {
        transactions.executeWithoutResult(s -> {
            var row = extraction(claimed.id(), true).orElseThrow(); requireWorker(row.worker(), row.leaseUntil(), claimed.worker());
            finishExtraction(claimed, "REJECTED", failure.getClass().getSimpleName());
        });
    }
    public void retryExtraction(Extraction row, RuntimeException failure) {
        jdbc.update("UPDATE agent_profile_extraction SET lease_until=0,retry_at=?,last_error=?,updated_at=? "
                        + "WHERE summary_job_id=? AND worker_epoch=? AND state='READY'", System.currentTimeMillis() + config.getRetryDelayMs(),
                failure.getClass().getSimpleName(), System.currentTimeMillis(), row.id(), row.worker());
    }
    private void finishExtraction(Extraction row, String state, String error) {
        Assert.state(jdbc.update("UPDATE agent_profile_extraction SET state=?,lease_until=0,last_error=?,updated_at=? "
                        + "WHERE summary_job_id=? AND worker_epoch=? AND lease_until>?", state, error, System.currentTimeMillis(),
                row.id(), row.worker(), System.currentTimeMillis()) == 1, "Stale profile worker");
    }

    public List<Fact> list(long user, boolean includeInactive, int limit) {
        owner(user); Assert.isTrue(limit > 0 && limit <= config.getMaxFacts(), "Invalid profile page limit");
        return jdbc.query("SELECT fact_id,user_id,attribute_key,fact_version,state,value_text,source_json,updated_at "
                        + "FROM agent_profile_fact WHERE user_id=?" + (includeInactive ? "" : " AND state='ACTIVE'")
                        + " ORDER BY updated_at DESC,fact_id LIMIT ?", (rs, i) -> readFact(rs), user, limit);
    }
    public Optional<Fact> fact(long user, String id) {
        owner(user); id(id);
        return jdbc.query("SELECT fact_id,user_id,attribute_key,fact_version,state,value_text,source_json,updated_at "
                        + "FROM agent_profile_fact WHERE user_id=? AND fact_id=?", (rs, i) -> readFact(rs), user, id).stream().findFirst();
    }
    public List<Fact> versions(long user, String id, long before, int limit) {
        owner(user); id(id); Assert.isTrue(before > 0 && limit > 0 && limit <= 100, "Invalid history page");
        return jdbc.query("SELECT fact_id,user_id,attribute_key,fact_version,state,value_text,source_json,updated_at "
                        + "FROM agent_profile_version WHERE user_id=? AND fact_id=? AND fact_version<? ORDER BY fact_version DESC LIMIT ?",
                (rs, i) -> readFact(rs), user, id, before, limit);
    }

    /** Authenticated explicit corrections can resolve conflicts/restore a deleted slot. Idempotency spans retries. */
    public Fact command(long user, String id, long expectedVersion, String requestId, String value, String statement, boolean forget) {
        owner(user); id(id);
        Assert.isTrue(expectedVersion > 0 && requestId != null && requestId.matches("[a-zA-Z0-9_-]{8,96}"), "Invalid command identity");
        Assert.isTrue(statement != null && !statement.isBlank() && statement.length() <= config.getMaxQuoteChars(), "Invalid user statement");
        if (!forget) Assert.isTrue(value != null && !value.isBlank() && value.length() <= config.getMaxValueChars()
                && statement.contains(value), "The correction must be present in the user's statement");
        String command = SummaryPolicy.hash(user + "|" + requestId);
        String hash = SummaryPolicy.hash(json(List.of(id, expectedVersion, forget, value == null ? "" : value, statement)));
        return transactions.execute(status -> {
            ensureUser(user); lockUser(user);
            var duplicate = jdbc.query("SELECT payload_hash,fact_id,fact_version FROM agent_profile_command WHERE command_id=? AND user_id=?",
                    (rs, i) -> new String[]{rs.getString(1), rs.getString(2), rs.getString(3)}, command, user);
            if (!duplicate.isEmpty()) {
                Assert.isTrue(duplicate.get(0)[0].equals(hash), "Request ID reused with different profile content");
                return version(user, duplicate.get(0)[1], Long.parseLong(duplicate.get(0)[2]));
            }
            Fact old = fact(user, id).orElseThrow(() -> new IllegalArgumentException("Profile fact not found"));
            if (old.version() != expectedVersion) throw new ProfileConflictException();
            Source source = new Source("DIRECT", "", 0, 0, statement, hash, System.currentTimeMillis(), command);
            Fact next = write(user, old.attribute(), old.version() + 1, forget ? "DELETED" : "ACTIVE", forget ? old.value() : value, source);
            jdbc.update("UPDATE agent_profile_user SET revision=revision+1,memory_epoch=memory_epoch+? WHERE user_id=?", forget ? 1 : 0, user);
            jdbc.update("INSERT INTO agent_profile_command (command_id,user_id,payload_hash,fact_id,fact_version) VALUES (?,?,?,?,?)",
                    command, user, hash, id, next.version());
            return next;
        });
    }

    private Fact version(long user, String id, long version) {
        return jdbc.queryForObject("SELECT fact_id,user_id,attribute_key,fact_version,state,value_text,source_json,updated_at "
                + "FROM agent_profile_version WHERE user_id=? AND fact_id=? AND fact_version=?", (rs, i) -> readFact(rs), user, id, version);
    }
    private Fact write(long user, String attribute, long version, String state, String value, Source source) {
        String id = factId(user, attribute); long now = System.currentTimeMillis();
        if (version == 1) jdbc.update("INSERT INTO agent_profile_fact (fact_id,user_id,attribute_key,fact_version,state,value_text,source_json,updated_at) "
                + "VALUES (?,?,?,?,?,?,?,?)", id, user, attribute, version, state, value, json(source), now);
        else jdbc.update("UPDATE agent_profile_fact SET fact_version=?,state=?,value_text=?,source_json=?,updated_at=? WHERE fact_id=? AND user_id=?",
                version, state, value, json(source), now, id, user);
        jdbc.update("INSERT INTO agent_profile_version (fact_id,user_id,attribute_key,fact_version,state,value_text,source_json,updated_at) "
                + "VALUES (?,?,?,?,?,?,?,?)", id, user, attribute, version, state, value, json(source), now);
        // Wake old-version cleanup and fence workers that were writing obsolete versions.
        jdbc.update("UPDATE agent_profile_projection SET state='PENDING',lease_until=0,retry_at=0,worker_epoch=worker_epoch+1,updated_at=? WHERE fact_id=?", now, id);
        enqueue(id, user, version);
        return new Fact(id, user, attribute, version, state, value, source, now);
    }

    private void enqueue(String fact, long user, long version) {
        String id = SummaryPolicy.hash(fact + "|" + version + "|" + target);
        try {
            jdbc.update("INSERT INTO agent_profile_projection (projection_id,fact_id,user_id,fact_version,target_tag,state,updated_at) "
                    + "VALUES (?,?,?,?,?,'PENDING',?)", id, fact, user, version, target, System.currentTimeMillis());
        } catch (DuplicateKeyException duplicate) {
            jdbc.update("UPDATE agent_profile_projection SET state='PENDING',retry_at=0,updated_at=? WHERE projection_id=?",
                    System.currentTimeMillis(), id);
        }
    }
    public int reindex(long user) {
        owner(user);
        return transactions.execute(status -> {
            ensureUser(user); lockUser(user);
            var facts = list(user, true, config.getMaxFacts());
            for (Fact fact : facts) enqueue(fact.id(), user, fact.version());
            jdbc.update("UPDATE agent_profile_projection SET state='PENDING',retry_at=0,updated_at=? WHERE user_id=? AND target_tag=?",
                    System.currentTimeMillis(), user, target);
            return facts.size();
        });
    }
    public Projection claimProjection(String id) {
        return transactions.execute(status -> {
            var rows = projections(" WHERE projection_id=? FOR UPDATE", id);
            if (rows.isEmpty()) return null;
            var row = rows.get(0); long now = System.currentTimeMillis();
            if (!row.target().equals(target) || !row.state().equals("PENDING") || row.leaseUntil() > now) return null;
            int changed = jdbc.update("UPDATE agent_profile_projection SET worker_epoch=worker_epoch+1,lease_until=?,attempts=attempts+1,updated_at=? "
                    + "WHERE projection_id=? AND retry_at<=?", now + config.getWorkerLeaseMs(), now, id, now);
            return changed == 1 ? projections(" WHERE projection_id=?", id).get(0) : null;
        });
    }
    public void finishProjection(Projection row, String state) {
        Assert.isTrue(List.of("WRITTEN", "OBSOLETE").contains(state), "Invalid projection status");
        Assert.state(jdbc.update("UPDATE agent_profile_projection SET state=?,lease_until=0,last_error=NULL,updated_at=? "
                        + "WHERE projection_id=? AND worker_epoch=? AND lease_until>?", state, System.currentTimeMillis(), row.id(),
                row.worker(), System.currentTimeMillis()) == 1, "Stale profile projection worker");
    }
    public void retryProjection(Projection row, RuntimeException failure) {
        jdbc.update("UPDATE agent_profile_projection SET lease_until=0,retry_at=?,last_error=?,updated_at=? WHERE projection_id=? AND worker_epoch=?",
                System.currentTimeMillis() + config.getRetryDelayMs(), failure.getClass().getSimpleName(), System.currentTimeMillis(), row.id(), row.worker());
    }
    public List<Projection> projectionStatus(long user, String id) {
        owner(user); id(id);
        return projections(" WHERE user_id=? AND fact_id=? AND target_tag=? ORDER BY fact_version DESC LIMIT 100", user, id, target);
    }
    private List<Projection> projections(String suffix, Object... args) {
        return jdbc.query("SELECT projection_id,fact_id,user_id,fact_version,target_tag,state,worker_epoch,lease_until FROM agent_profile_projection" + suffix,
                (rs, i) -> new Projection(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getString(5), rs.getString(6),
                        rs.getLong(7), rs.getLong(8)), args);
    }
    public List<String> pendingExtractions() {
        long now = System.currentTimeMillis();
        return jdbc.query("SELECT summary_job_id FROM agent_profile_extraction WHERE state='READY' AND lease_until<=? AND retry_at<=? "
                + "ORDER BY updated_at,summary_job_id LIMIT ?", (rs, i) -> rs.getString(1), now, now, config.getScanBatch());
    }
    public List<ExtractionStatus> extractionStatus(long user, int limit) {
        owner(user); Assert.isTrue(limit > 0 && limit <= 100, "Invalid extraction status limit");
        return jdbc.query("SELECT summary_job_id,state,attempts,retry_at,last_error,updated_at FROM agent_profile_extraction "
                        + "WHERE user_id=? ORDER BY updated_at DESC,summary_job_id LIMIT ?",
                (rs, i) -> new ExtractionStatus(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getString(5), rs.getLong(6)), user, limit);
    }
    public List<String> pendingProjections() {
        long now = System.currentTimeMillis();
        return jdbc.query("SELECT projection_id FROM agent_profile_projection WHERE target_tag=? AND state='PENDING' AND lease_until<=? AND retry_at<=? "
                + "ORDER BY updated_at,projection_id LIMIT ?", (rs, i) -> rs.getString(1), target, now, now, config.getScanBatch());
    }
    private long count(long user) { return jdbc.queryForObject("SELECT COUNT(*) FROM agent_profile_fact WHERE user_id=?", Long.class, user); }
    private void ensureUser(long user) {
        owner(user);
        if (jdbc.queryForObject("SELECT COUNT(*) FROM agent_profile_user WHERE user_id=?", Long.class, user) == 0)
            try { jdbc.update("INSERT INTO agent_profile_user (user_id) VALUES (?)", user); } catch (DuplicateKeyException ignored) { }
    }
    private long[] lockUser(long user) {
        return jdbc.queryForObject("SELECT revision,memory_epoch FROM agent_profile_user WHERE user_id=? FOR UPDATE",
                (rs, i) -> new long[]{rs.getLong(1), rs.getLong(2)}, user);
    }
    private static Fact readFact(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new Fact(rs.getString(1), rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getString(5), rs.getString(6),
                JSON.readValue(rs.getString(7), Source.class), rs.getLong(8));
    }
    private static void requireWorker(long worker, long lease, long expected) { Assert.state(worker == expected && lease > System.currentTimeMillis(), "Profile worker lost its lease"); }
    private static void owner(long user) { Assert.isTrue(user > 0, "Invalid profile owner"); }
    private static void id(String id) { Assert.isTrue(id != null && id.matches("[a-f0-9]{64}"), "Invalid profile ID"); }
}
