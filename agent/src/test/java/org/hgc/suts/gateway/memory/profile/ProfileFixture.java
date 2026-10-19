package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.config.MemoryProfileProperties;
import org.hgc.suts.gateway.memory.ConversationMemory;
import org.hgc.suts.gateway.memory.archive.ArchiveDatabaseFixture;
import org.hgc.suts.gateway.memory.archive.ArchivedTurn;
import org.hgc.suts.gateway.memory.summary.*;

import java.util.List;
import java.util.UUID;

import static org.hgc.suts.gateway.memory.profile.ProfileData.*;

public final class ProfileFixture {
    public static final String GENERATION = "profile-test-generation";
    public final ArchiveDatabaseFixture db;
    public final MemoryProfileProperties config = new MemoryProfileProperties();
    public final JdbcProfileStore profiles;
    public final ProfileSummarySupport support;
    public final JdbcSummaryJobs jobs;
    public ProfileFixture() { this(ArchiveDatabaseFixture.h2(), "fixture"); }
    public ProfileFixture(ArchiveDatabaseFixture db, String target) {
        this.db = db; config.setEnabled(true); config.setRetryDelayMs(1);
        profiles = new JdbcProfileStore(db.archive, config, target, 262144, GENERATION);
        support = new ProfileSummarySupport(profiles, config);
        jobs = new JdbcSummaryJobs(db.archive, 262144, GENERATION, support);
    }
    public SummaryEvent event(String input) { return event(42, null, 1, 0, input); }
    public SummaryEvent event(long user, String conversation, long sequence, long baseVersion, String input) {
        String id = conversation == null ? ConversationMemory.conversationId(user, "profile-" + UUID.randomUUID()) : conversation;
        var turn = new ArchivedTurn(UUID.randomUUID().toString(), user, id, 1, sequence, input, "助手未确认的推测", System.currentTimeMillis());
        db.archive.archive(turn);
        return new SummaryEvent(1, GENERATION, null, user, id, 1, baseVersion, sequence, sequence,
                baseVersion == 0 ? "" : "old summary", List.of(SummaryState.RawTurn.of(turn)));
    }
    public JdbcSummaryJobs.Job result(SummaryEvent event, Candidate... candidates) {
        jobs.register(event); var claimed = jobs.claim(event.jobId(), System.currentTimeMillis(), 30000);
        support.prepare(event);
        jobs.saveResult(claimed, new SummaryModel.Result("用户画像摘要", 100, 20, json(List.of(candidates)), false));
        return claimed;
    }
    public void ready(SummaryEvent event, Candidate... candidates) {
        var claimed = result(event, candidates); jobs.project(claimed, event, "用户画像摘要");
    }
    public void apply(SummaryEvent event, Candidate... candidates) {
        ready(event, candidates); var work = profiles.claimExtraction(event.jobId()); profiles.apply(work, List.of(candidates));
    }
    public static Candidate tea(String value, long seq, String quote) { return new Candidate("preference.drink", value, "ASSERT", seq, quote); }
}
