package org.hgc.suts.gateway.rag;

import java.util.List;

/** Internal retrieval contracts. Authenticated identity is supplied separately by the caller. */
public final class RagData {
    private RagData() { }
    public enum Scope { PROFILE, DOCUMENT, BOTH, NONE }
    public enum Kind { PROFILE_FACT, KNOWLEDGE_CHILD }
    public enum Visibility { PUBLIC, OWNER }

    public record Plan(String originalQuestion, String query, Scope scope, List<String> documentIds) {
        public Plan {
            documentIds = documentIds == null ? List.of() : List.copyOf(documentIds);
        }
    }
    public record Hit(String id, double score) { }
    public record Candidate(String id, Kind kind, String resourceId, long version, String parentId,
                            String title, String text, String source, int pageStart, int pageEnd, int ordinal) { }
    public record Reranked(int index, double score) { }
    public record Evidence(String id, Kind kind, String title, String text, String source, long version,
                           int pageStart, int pageEnd, List<String> matchedChildren) { }
    public record Trace(int vectorHits, int keywordHits, int fusedHits, int rerankedHits,
                        List<String> degradedChannels, long elapsedMillis) { }
    public record Result(List<Evidence> evidence, Trace trace) { }
}
