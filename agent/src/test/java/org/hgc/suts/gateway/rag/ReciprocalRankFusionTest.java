package org.hgc.suts.gateway.rag;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

class ReciprocalRankFusionTest {
    @Test void sharedCandidateReceivesBothRankContributions() {
        var result = ReciprocalRankFusion.fuse(List.of("a", "b"), List.of("b", "c"), 60);
        assertThat(result).extracting(ReciprocalRankFusion.Hit::id).containsExactly("b", "a", "c");
        assertThat(result.get(0).score()).isCloseTo(1.0 / 62 + 1.0 / 61, within(1e-12));
        assertThat(result.get(0).vectorRank()).isEqualTo(2);
        assertThat(result.get(0).keywordRank()).isEqualTo(1);
        assertThat(result.get(1).keywordRank()).isZero();
    }

    @Test void duplicatesDoNotAddVotesOrConsumeAnotherRank() {
        var duplicated = ReciprocalRankFusion.fuse(List.of("a", "a", "b"), List.of("b", "b", "c"), 60);
        assertThat(duplicated).isEqualTo(ReciprocalRankFusion.fuse(List.of("a", "b"), List.of("b", "c"), 60));
    }

    @Test void tiedScoresUseStableCandidateIds() {
        var result = ReciprocalRankFusion.fuse(List.of("z", "a"), List.of("a", "z"), 60);
        assertThat(result).extracting(ReciprocalRankFusion.Hit::id).containsExactly("a", "z");
    }

    @Test void twoTop50ChannelsReturnAtMostTwentyCandidates() {
        var vector = IntStream.range(0, 50).mapToObj(i -> "v" + i).toList();
        var keyword = IntStream.range(0, 50).mapToObj(i -> "k" + i).toList();
        assertThat(ReciprocalRankFusion.fuse(vector, keyword, 60)).hasSize(20);
        assertThatThrownBy(() -> ReciprocalRankFusion.fuse(IntStream.range(0, 51).mapToObj(Integer::toString).toList(), keyword, 60))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void availableChannelStillHasItsOwnRanksAndEmptyChannelsAreValid() {
        assertThat(ReciprocalRankFusion.fuse(List.of(), List.of("a"), 60))
                .containsExactly(new ReciprocalRankFusion.Hit("a", 1.0 / 61, 0, 1));
        assertThat(ReciprocalRankFusion.fuse(List.of(), List.of(), 60)).isEmpty();
    }

    @Test void invalidCandidatesAndConfigurationAreRejected() {
        assertThatThrownBy(() -> ReciprocalRankFusion.fuse(List.of(" "), List.of(), 60)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReciprocalRankFusion.fuse(null, List.of(), 60)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReciprocalRankFusion.fuse(List.of(), List.of(), 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
