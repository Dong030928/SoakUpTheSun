package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Optional;

/** Short DB transactions only. The model, Redis, MQ and embedding are called outside all DB locks. */
public final class JdbcSummaryJobs {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final int maxMessageBytes;
    private final String generation;
    private final org.hgc.suts.gateway.memory.profile.ProfileSummarySupport profiles;
    public JdbcSummaryJobs(JdbcConversationArchive archive, int maxMessageBytes, String generation) {
        this(archive, maxMessageBytes, generation, null);
    }
    public JdbcSummaryJobs(JdbcConversationArchive archive, int maxMessageBytes, String generation,
                           org.hgc.suts.gateway.memory.profile.ProfileSummarySupport profiles) {
        jdbc = archive.jdbcOperations(); transactions = archive.transactionOperations();
        this.maxMessageBytes = maxMessageBytes; this.generation = generation; this.profiles = profiles;
    }
    public void register(SummaryEvent event) {
        String body = event.body(maxMessageBytes);
        try {
            jdbc.update("INSERT INTO agent_memory_job (job_id, conversation_id, user_id, epoch, base_version, covered_from, "
                            + "covered_to, input_body, state, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?)",
                    event.jobId(), event.conversationId(), event.userId(), event.epoch(), event.baseVersion(), event.from(),
                    event.to(), body, System.currentTimeMillis());
        } catch (DuplicateKeyException duplicate) {
            Assert.state(read(event.jobId()).map(j -> j.body.equals(body)).orElse(false), "Summary identity/base-version conflict");
        }
    }
    public Optional<Job> read(String jobId) {
        return rows(jobId, false).stream().findFirst();
    }
    public Job claim(String id, long now, long leaseMs) {
        return transactions.execute(status -> {
            var row = rows(id, true).stream().findFirst().orElseThrow();
            if (row.state.equals("DONE") || row.state.equals("STALE") || row.leaseUntil > now || row.retryAt > now) return null;
            jdbc.update("UPDATE agent_memory_job SET worker_epoch = worker_epoch + 1, lease_until = ?, "
                    + "attempts = attempts + 1, updated_at = ? WHERE job_id = ?", now + leaseMs, now, id);
            return rows(id, false).get(0);
        });
    }
    public void saveResult(Job job, String result, long inputTokens, long outputTokens) {
        saveResult(job, new SummaryModel.Result(result, inputTokens, outputTokens));
    }
    public void requireProfileCompatibility(Job job) {
        Assert.state(profiles != null || jdbc.queryForObject("SELECT COUNT(*) FROM agent_profile_extraction "
                        + "WHERE summary_job_id=? AND state IN ('CAPTURED','RESULT')", Long.class, job.id) == 0,
                "Enable profile processing to resume structured summary jobs");
    }
    public void saveResult(Job job, SummaryModel.Result result) {
        transactions.executeWithoutResult(status -> {
        int updated = jdbc.update("UPDATE agent_memory_job SET result_text = ?, state = 'RESULT', prompt_tokens = ?, "
                        + "completion_tokens = ?, updated_at = ? WHERE job_id = ? AND worker_epoch = ? AND lease_until > ? "
                        + "AND state = 'READY'", result.text(), result.promptTokens(), result.completionTokens(), System.currentTimeMillis(), job.id,
                job.worker, System.currentTimeMillis());
        Assert.state(updated == 1, "Summary worker lease expired before persisting its result");
        if (result.profilePayload() != null) {
            Assert.notNull(profiles, "Profile result processor is missing");
            profiles.result(event(job), result.profilePayload(), result.profileRejected());
        }
        });
    }
    public void project(Job job, SummaryEvent event, String result) {
        transactions.executeWithoutResult(status -> {
            Job current = rows(job.id, true).get(0);
            requireWorker(current, job);
            Assert.state("RESULT".equals(current.state) || "MERGED".equals(current.state), "Summary result must be durable before projection");
            var existing = projection(event.userId(), event.conversationId(), event.epoch());
            if (existing.isPresent()) {
                var old = existing.get();
                Assert.state(old.version <= event.baseVersion() + 1, "A newer summary already exists");
                if (old.version == event.baseVersion() + 1) {
                    Assert.state(old.jobId.equals(event.jobId()) && old.text.equals(result), "Summary version conflict");
                } else {
                    Assert.state(old.version == event.baseVersion() && old.through == event.from() - 1,
                            "Summary projection has a coverage gap");
                    jdbc.update("UPDATE agent_memory_summary SET summary_version = ?, covered_through = ?, summary_text = ?, "
                                    + "job_id = ?, updated_at = ? WHERE conversation_id = ? AND epoch = ? AND summary_version = ?",
                            event.baseVersion() + 1, event.to(), result, event.jobId(), System.currentTimeMillis(),
                            event.conversationId(), event.epoch(), event.baseVersion());
                }
            } else {
                Assert.state(event.baseVersion() == 0 && event.from() == 1, "Initial summary projection has a gap");
                jdbc.update("INSERT INTO agent_memory_summary (conversation_id, user_id, epoch, summary_version, covered_through, "
                                + "summary_text, job_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", event.conversationId(),
                        event.userId(), event.epoch(), 1, event.to(), result, event.jobId(), System.currentTimeMillis());
            }
            jdbc.update("UPDATE agent_memory_job SET state = 'MERGED', updated_at = ? WHERE job_id = ?",
                    System.currentTimeMillis(), job.id);
            if (profiles != null) profiles.merged(event);
        });
    }
    public void finish(Job job, String vectorState) {
        Assert.isTrue(List.of("WRITTEN", "SKIPPED", "OBSOLETE").contains(vectorState), "Invalid vector completion state");
        updateFenced(job, "UPDATE agent_memory_job SET state = 'DONE', vector_state = ?, lease_until = 0, last_error = NULL, "
                + "updated_at = ? WHERE job_id = ? AND worker_epoch = ? AND lease_until > ?", vectorState);
    }
    public void stale(Job job) {
        transactions.executeWithoutResult(status -> {
        updateFenced(job, "UPDATE agent_memory_job SET state = 'STALE', vector_state = ?, lease_until = 0, "
                + "updated_at = ? WHERE job_id = ? AND worker_epoch = ? AND lease_until > ?", "OBSOLETE");
        if (profiles != null) profiles.stale(job.id);
        });
    }
    private void updateFenced(Job job, String sql, String value) {
        Assert.state(jdbc.update(sql, value, System.currentTimeMillis(), job.id, job.worker, System.currentTimeMillis()) == 1,
                "Stale summary worker cannot complete a job");
    }
    public void retry(Job job, long retryDelay, RuntimeException failure) {
        jdbc.update("UPDATE agent_memory_job SET lease_until = 0, retry_at = ?, last_error = ?, updated_at = ? "
                        + "WHERE job_id = ? AND worker_epoch = ?", System.currentTimeMillis() + retryDelay,
                failure.getClass().getSimpleName(), System.currentTimeMillis(), job.id, job.worker);
    }
    public List<String> recoverable(long now, int limit) {
        return jdbc.query("SELECT job_id FROM agent_memory_job WHERE state NOT IN ('DONE', 'STALE') AND lease_until <= ? "
                + "AND retry_at <= ? ORDER BY updated_at, job_id LIMIT ?", (rs, i) -> rs.getString(1), now, now, limit);
    }
    public List<Job> unfinished(String id, long epoch) {
        var ids = jdbc.query("SELECT job_id FROM agent_memory_job WHERE conversation_id = ? AND epoch = ? "
                + "AND state NOT IN ('DONE', 'STALE') ORDER BY base_version DESC LIMIT 100", (rs, i) -> rs.getString(1), id, epoch);
        return ids.stream().map(job -> read(job).orElseThrow()).toList();
    }
    public Optional<Projection> projection(long user, String id, long epoch) {
        return jdbc.query("SELECT summary_version, covered_through, summary_text, job_id FROM agent_memory_summary "
                        + "WHERE conversation_id = ? AND user_id = ? AND epoch = ?",
                (rs, i) -> new Projection(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4)), id, user, epoch)
                .stream().findFirst();
    }
    public boolean isCurrent(long user, String id, long epoch, long version) {
        return projection(user, id, epoch).map(p -> p.version == version).orElse(false);
    }
    public boolean hasWork(String id, long epoch) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_job WHERE conversation_id = ? AND epoch = ? "
                + "AND state NOT IN ('DONE', 'STALE')", Long.class, id, epoch) > 0;
    }
    private List<Job> rows(String id, boolean lock) {
        return jdbc.query("SELECT job_id, input_body, state, worker_epoch, lease_until, retry_at, result_text, vector_state "
                        + "FROM agent_memory_job WHERE job_id = ?" + (lock ? " FOR UPDATE" : ""),
                (rs, i) -> new Job(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4), rs.getLong(5),
                        rs.getLong(6), rs.getString(7), rs.getString(8)), id);
    }
    private static void requireWorker(Job current, Job claimed) {
        Assert.state(current.worker == claimed.worker && current.leaseUntil > System.currentTimeMillis(), "Summary worker lost its lease");
    }
    public SummaryEvent event(Job job) {
        return SummaryEvent.decode(job.body.getBytes(java.nio.charset.StandardCharsets.UTF_8), maxMessageBytes, generation);
    }
    public record Job(String id, String body, String state, long worker, long leaseUntil, long retryAt,
                      String result, String vectorState) { }
    public record Projection(long version, long through, String text, String jobId) { }
}
