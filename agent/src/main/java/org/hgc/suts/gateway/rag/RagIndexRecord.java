package org.hgc.suts.gateway.rag;

/** One stable, independently searchable projection of an authoritative RAG resource. */
public record RagIndexRecord(String id, RagData.Kind kind, long ownerId, RagData.Visibility visibility,
                             String resourceId, long version, String parentId, String title, String text,
                             String source, int pageStart, int pageEnd, int ordinal, boolean searchable) {
}
