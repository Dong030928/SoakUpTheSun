package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.memory.profile.ProfileData;
import org.hgc.suts.gateway.memory.profile.ProfileFixture;
import org.hgc.suts.gateway.memory.profile.ProfileVectorIndex;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.memory.profile.ProfileFixture.tea;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ProfileRagAccessTest {
    private final ProfileFixture fixture = new ProfileFixture();
    private final RedisClient redis = mock(RedisClient.class);
    private final ProfileVectorIndex vectors = mock(ProfileVectorIndex.class);
    private final ProfileRagAccess access = new ProfileRagAccess(redis, vectors, fixture.profiles, 3, 0.7);

    @Test void searchesTheOriginalProfileIndexWithoutInvokingItsEmbeddingSearch() {
        fixture.apply(fixture.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        var fact = fixture.profiles.list(42, false, 20).get(0); var result = mock(SearchResult.class);
        when(vectors.enabled()).thenReturn(true); when(vectors.indexName()).thenReturn("profile-index"); when(vectors.prefix()).thenReturn("profile-data:");
        when(redis.ftInfo("profile-index")).thenReturn(RedisRagIndexTest.schema("profile-data:", 3, true));
        when(result.getDocuments()).thenReturn(List.of(RedisRagIndexTest.nativeDocument("profile-data:" + ProfileData.documentId(fact.id(), 1),
                Map.of("factId", fact.id(), "factVersion", "1", "vector_score", "0.2"))));
        when(redis.ftSearch(eq("profile-index"), any(Query.class))).thenReturn(result);
        assertThat(access.search(42, "偏好", new float[]{1, 0, 0}, 2)).extracting(RagData.Hit::id)
                .containsExactly(ProfileRagAccess.candidateId(fact));
        verify(vectors, never()).search(anyLong(), anyString(), anyInt());
        fixture.profiles.command(42, fact.id(), 1, "forget-rag-profile", null, "忘掉饮品偏好", true);
        assertThat(access.search(42, "偏好", new float[]{1, 0, 0}, 2)).isEmpty();
    }

    @Test void hydrationChecksOwnerCurrentStateAndTheExactHitVersion() {
        fixture.apply(fixture.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
        var fact = fixture.profiles.list(42, false, 20).get(0); String first = ProfileRagAccess.candidateId(fact);
        assertThat(access.hydrate(42, List.of(first, first, "bad-id"))).hasSize(1);
        assertThat(access.hydrate(43, List.of(first))).isEmpty();
        var corrected = fixture.profiles.command(42, fact.id(), 1, "correct-rag-profile", "红茶", "我喜欢红茶", false);
        assertThat(access.hydrate(42, List.of(first))).isEmpty();
        String current = ProfileRagAccess.candidateId(corrected);
        assertThat(access.hydrate(42, List.of(current))).extracting(RagData.Candidate::text).containsExactly("preference.drink: 红茶");
        fixture.profiles.command(42, fact.id(), 2, "forget-rag-profile", null, "忘掉饮品偏好", true);
        assertThat(access.hydrate(42, List.of(first, current))).isEmpty();
        verifyNoInteractions(redis, vectors);
    }
}
