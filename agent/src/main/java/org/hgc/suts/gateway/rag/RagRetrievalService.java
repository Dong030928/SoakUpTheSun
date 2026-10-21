package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.manager.CheckedEmbeddingModel;
import org.springframework.ai.embedding.EmbeddingModel;

import java.util.*;
import java.util.concurrent.*;

import static org.hgc.suts.gateway.rag.RagData.*;

/** Independent execution of a validated plan. Original conversation text is never rewritten here. */
public final class RagRetrievalService {
    @FunctionalInterface public interface VectorChannel { List<Hit> search(long user, String query, float[] vector, Kind kind, List<String> documents, int limit); }
    @FunctionalInterface public interface KeywordChannel { List<Hit> search(long user, String query, Kind kind, List<String> documents, int limit); }
    public interface Resolver {
        List<Candidate> hydrate(long user, List<String> ids, List<String> documents);
        Evidence expand(long user, Candidate candidate);
    }
    @FunctionalInterface public interface Ranker { List<Reranked> rank(String query, List<Candidate> candidates, int limit); }

    private final RagProperties config;
    private final ExecutorService executor;
    private final EmbeddingModel embedding;
    private final VectorChannel vectors;
    private final KeywordChannel keywords;
    private final Resolver resolver;
    private final Ranker ranker;

    public RagRetrievalService(RagProperties config, ExecutorService executor, EmbeddingModel embedding, int dimension,
                               VectorChannel vectors, KeywordChannel keywords, Resolver resolver, Ranker ranker) {
        config.validate(); this.config = config; this.executor = executor;
        this.embedding = new CheckedEmbeddingModel(embedding, dimension);
        this.vectors = vectors; this.keywords = keywords; this.resolver = resolver; this.ranker = ranker;
    }

    public Result retrieve(long user, Plan plan) {
        validate(user, plan);
        long started = System.nanoTime(), deadline = started + TimeUnit.MILLISECONDS.toNanos(config.getTimeoutMs());
        if (plan.scope() == Scope.NONE) return new Result(List.of(), new Trace(0, 0, 0, 0, List.of(), 0));
        Set<String> degraded = new ConcurrentSkipListSet<>();
        var vectorFuture = executor.submit(() -> {
            float[] vector = embedding.embed(plan.query()); // Exactly one remote query embedding for both namespaces.
            return search(user, plan, (kind, limit) -> vectors.search(user, plan.query(), vector, kind, plan.documentIds(), limit), degraded, "VECTOR");
        });
        Future<List<Hit>> keywordFuture;
        try { keywordFuture = executor.submit(() -> search(user, plan,
                (kind, limit) -> keywords.search(user, plan.query(), kind, plan.documentIds(), limit), degraded, "KEYWORD")); }
        catch (RejectedExecutionException rejected) { vectorFuture.cancel(true); throw rejected; }
        long channelDeadline = Math.min(deadline, started + TimeUnit.MILLISECONDS.toNanos(config.getChannelTimeoutMs()));
        List<Hit> vectorHits = await(vectorFuture, channelDeadline, degraded, "VECTOR");
        List<Hit> keywordHits = await(keywordFuture, channelDeadline, degraded, "KEYWORD");
        var fused = ReciprocalRankFusion.fuse(vectorHits.stream().map(Hit::id).toList(),
                keywordHits.stream().map(Hit::id).toList(), config.getRrfConstant());
        List<Candidate> hydrated = fused.isEmpty() ? List.of() : await(executor.submit(() -> resolver.hydrate(user,
                fused.stream().map(ReciprocalRankFusion.Hit::id).toList(), plan.documentIds())), deadline, degraded, "SQL");
        Map<String, Candidate> current = new HashMap<>();
        for (Candidate candidate : hydrated) if (candidate != null) current.putIfAbsent(candidate.id(), candidate);
        List<Candidate> candidates = fused.stream().map(hit -> current.get(hit.id())).filter(Objects::nonNull).toList();
        List<Reranked> ranking = List.of();
        if (!candidates.isEmpty() && System.nanoTime() < deadline) {
            Future<List<Reranked>> rerank = executor.submit(() -> ranker.rank(plan.query(), candidates, Math.min(5, candidates.size())));
            ranking = await(rerank, deadline, degraded, "RERANKER");
        }
        int reranked = ranking.size();
        if (ranking.isEmpty() && !candidates.isEmpty()) {
            degraded.add("RERANKER");
            ranking = java.util.stream.IntStream.range(0, Math.min(5, candidates.size())).mapToObj(i -> new Reranked(i, 0)).toList();
        }
        Set<Integer> seenRanks = new HashSet<>();
        LinkedHashMap<String, Evidence> evidence = new LinkedHashMap<>();
        int chars = 0;
        for (Reranked ranked : ranking) {
            if (System.nanoTime() >= deadline) { degraded.add("SQL"); break; }
            if (ranked.index() < 0 || ranked.index() >= candidates.size() || !Double.isFinite(ranked.score()) || !seenRanks.add(ranked.index()))
                throw new IllegalStateException("Invalid reranker result");
            Candidate candidate = candidates.get(ranked.index());
            // Recheck the exact version immediately before parent expansion; never substitute newer content.
            List<Evidence> expansion = await(executor.submit(() -> {
                Evidence value = resolver.expand(user, candidate);
                return value == null ? List.of() : List.of(value);
            }), deadline, degraded, "SQL");
            if (expansion.isEmpty()) continue;
            Evidence expanded = expansion.get(0);
            Evidence existing = evidence.get(expanded.id());
            if (existing != null) {
                LinkedHashSet<String> children = new LinkedHashSet<>(existing.matchedChildren()); children.addAll(expanded.matchedChildren());
                evidence.put(existing.id(), new Evidence(existing.id(), existing.kind(), existing.title(), existing.text(), existing.source(),
                        existing.version(), existing.pageStart(), existing.pageEnd(), List.copyOf(children)));
            } else if (chars + expanded.text().length() <= config.getMaxEvidenceChars()) {
                evidence.put(expanded.id(), expanded); chars += expanded.text().length();
            }
        }
        return new Result(List.copyOf(evidence.values()), new Trace(vectorHits.size(), keywordHits.size(), fused.size(), reranked,
                List.copyOf(degraded), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)));
    }

    @FunctionalInterface private interface Search { List<Hit> apply(Kind kind, int limit); }
    private List<Hit> search(long user, Plan plan, Search channel, Set<String> degraded, String name) {
        // At most 50 per type internally, then enforce the shared channel cap and quotas before RRF.
        List<Hit> profile = plan.scope() == Scope.DOCUMENT ? List.of() : safe(channel, Kind.PROFILE_FACT, degraded, name);
        List<Hit> documents = plan.scope() == Scope.PROFILE ? List.of() : safe(channel, Kind.KNOWLEDGE_CHILD, degraded, name);
        int profileLimit = plan.scope() == Scope.PROFILE ? 50 : config.getProfileQuota();
        int documentLimit = plan.scope() == Scope.DOCUMENT ? 50 : config.getDocumentQuota();
        return allocate(profile, documents, profileLimit, documentLimit);
    }
    private List<Hit> safe(Search channel, Kind kind, Set<String> degraded, String name) {
        try {
            List<Hit> result = channel.apply(kind, 50);
            if (result == null || result.size() > 50 || result.stream().anyMatch(hit -> hit == null || hit.id() == null || hit.id().isBlank() || !Double.isFinite(hit.score())))
                throw new IllegalStateException("Invalid retrieval channel result");
            return result;
        } catch (RuntimeException failure) { degraded.add(name + "_" + kind.name()); return List.of(); }
    }

    static List<Hit> allocate(List<Hit> profile, List<Hit> documents, int profileLimit, int documentLimit) {
        profile = distinct(profile); documents = distinct(documents);
        int p = Math.min(profileLimit, profile.size()), d = Math.min(documentLimit, documents.size());
        int remaining = 50 - p - d;
        int extraP = Math.min(remaining, profile.size() - p); p += extraP; remaining -= extraP;
        d += Math.min(remaining, documents.size() - d);
        List<Hit> result = new ArrayList<>(profile.subList(0, p)); result.addAll(documents.subList(0, d));
        return result.stream().sorted(Comparator.comparingDouble(Hit::score).reversed().thenComparing(Hit::id)).limit(50).toList();
    }
    private static List<Hit> distinct(List<Hit> input) {
        LinkedHashMap<String, Hit> unique = new LinkedHashMap<>();
        input.stream().sorted(Comparator.comparingDouble(Hit::score).reversed().thenComparing(Hit::id)).forEach(hit -> unique.putIfAbsent(hit.id(), hit));
        return List.copyOf(unique.values());
    }
    private <T> List<T> await(Future<List<T>> future, long deadline, Set<String> degraded, String name) {
        try { return future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); future.cancel(true); degraded.add(name); return List.of(); }
        catch (ExecutionException | TimeoutException failure) { future.cancel(true); degraded.add(name); return List.of(); }
    }
    private void validate(long user, Plan plan) {
        if (user <= 0 || plan == null || plan.scope() == null || plan.originalQuestion() == null || plan.originalQuestion().isBlank()
                || plan.originalQuestion().length() > config.getMaxQueryChars() || plan.query() == null || plan.query().isBlank()
                || plan.query().length() > config.getMaxQueryChars() || plan.documentIds().size() > 20
                || plan.documentIds().stream().anyMatch(id -> id == null || !id.matches("[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}")))
            throw new IllegalArgumentException("Invalid RAG retrieval plan");
    }
}
