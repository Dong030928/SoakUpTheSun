package org.hgc.suts.gateway.memory.profile;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.hgc.suts.gateway.memory.profile.ProfileFixture.*;

class ProfileWorkerTest {
    private final ProfileFixture f = new ProfileFixture();
    private final ProfileVectorIndex vectors = mock(ProfileVectorIndex.class);
    @Test void projectionFailureRetainsAppliedFactsAndRetriesOnlyTheIndexStage() {
        var event = f.event("我喜欢绿茶"); f.ready(event, tea("绿茶", 1, "我喜欢绿茶"));
        when(vectors.enabled()).thenReturn(true);
        try (var worker = new ProfileWorker(f.profiles, vectors, f.config)) {
            worker.extract(event.jobId()); var fact = f.profiles.list(42, false, 20).get(0);
            var projection = f.profiles.projectionStatus(42, fact.id()).get(0);
            doThrow(new IllegalStateException("embedding unavailable")).doNothing().when(vectors).write(any());
            assertThatThrownBy(() -> worker.project(projection.id())).hasMessageContaining("embedding");
            f.db.jdbc.update("UPDATE agent_profile_projection SET retry_at=0 WHERE projection_id=?", projection.id());
            worker.project(projection.id());
            assertThat(f.profiles.projectionStatus(42, fact.id()).get(0).state()).isEqualTo("WRITTEN");
            assertThat(f.profiles.versions(42, fact.id(), Long.MAX_VALUE, 20)).hasSize(1);
            worker.extract(event.jobId()); verify(vectors, times(2)).write(any());
        }
    }
    @Test void forgetDuringAnInFlightEmbeddingDeletesTheLateOldDocument() {
        f.apply(f.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        when(vectors.enabled()).thenReturn(true); var fact = f.profiles.list(42, false, 20).get(0);
        var projection = f.profiles.projectionStatus(42, fact.id()).get(0);
        doAnswer(call -> {
            f.profiles.command(42, fact.id(), fact.version(), "forget-inflight", null, "忘掉饮品偏好", true); return null;
        }).when(vectors).write(any());
        try (var worker = new ProfileWorker(f.profiles, vectors, f.config)) {
            assertThatThrownBy(() -> worker.project(projection.id())).hasMessageContaining("Stale");
            verify(vectors).delete(fact.id(), fact.version());
            assertThat(f.profiles.list(42, false, 20)).isEmpty();
            worker.project(projection.id());
            assertThat(f.profiles.projectionStatus(42, fact.id()).stream().filter(p -> p.version() == 1).findFirst().orElseThrow().state())
                    .isEqualTo("OBSOLETE");
        }
    }
    @Test void disabledVectorsKeepDurableWorkPendingForResume() {
        f.apply(f.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        var fact = f.profiles.list(42, false, 20).get(0);
        try (var worker = new ProfileWorker(f.profiles, vectors, f.config)) {
            worker.project(f.profiles.projectionStatus(42, fact.id()).get(0).id());
            verify(vectors, never()).write(any());
            assertThat(f.profiles.pendingProjections()).hasSize(1);
        }
    }
}
