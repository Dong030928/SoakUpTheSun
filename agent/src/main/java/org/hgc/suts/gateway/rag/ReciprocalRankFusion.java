package org.hgc.suts.gateway.rag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Combines independent ranks, never incomparable vector and BM25 scores. */
public final class ReciprocalRankFusion {
    public static final int CHANNEL_LIMIT = 50;
    public static final int FUSION_LIMIT = 20;

    private ReciprocalRankFusion() { }

    public record Hit(String id, double score, int vectorRank, int keywordRank) { }

    public static List<Hit> fuse(List<String> vectors, List<String> keywords, int rankConstant) {
        if (rankConstant < 1 || rankConstant > 10000) throw new IllegalArgumentException("Invalid RRF rank constant");
        Map<String, int[]> ranks = new LinkedHashMap<>();
        collect(vectors, ranks, 0);
        collect(keywords, ranks, 1);
        var result = new ArrayList<Hit>(ranks.size());
        ranks.forEach((id, rank) -> result.add(new Hit(id,
                contribution(rank[0], rankConstant) + contribution(rank[1], rankConstant), rank[0], rank[1])));
        result.sort(Comparator.comparingDouble(Hit::score).reversed().thenComparing(Hit::id));
        return List.copyOf(result.subList(0, Math.min(FUSION_LIMIT, result.size())));
    }

    private static void collect(List<String> ids, Map<String, int[]> ranks, int channel) {
        if (ids == null) throw new IllegalArgumentException("RRF channels must be explicit lists");
        if (ids.size() > CHANNEL_LIMIT) throw new IllegalArgumentException("RRF channel exceeds Top50");
        int position = 0;
        for (String id : ids) {
            if (id == null || id.isBlank() || id.length() > 256) throw new IllegalArgumentException("Invalid candidate ID");
            int[] rank = ranks.computeIfAbsent(id, ignored -> new int[2]);
            if (rank[channel] == 0) rank[channel] = ++position;
        }
    }

    private static double contribution(int rank, int rankConstant) {
        return rank == 0 ? 0 : 1.0 / (rankConstant + rank);
    }
}
