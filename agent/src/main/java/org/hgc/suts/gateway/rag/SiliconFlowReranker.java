package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;

import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Single-attempt SiliconFlow rerank adapter. */
public class SiliconFlowReranker {
    private final URI endpoint;
    private final String apiKey;
    private final String model;
    private final String instruction;
    private final int maxQueryChars;
    private final int maxTextChars;
    private final RagIndexHttp http;

    public SiliconFlowReranker(RagProperties properties) {
        Objects.requireNonNull(properties, "RAG properties are required");
        var settings = Objects.requireNonNull(properties.getReranker(), "RAG reranker settings are required");
        URI base = RagProperties.validateEndpoint(settings.getEndpoint());
        String address = base.toString().replaceAll("/+$", "");
        endpoint = URI.create(address.endsWith("/rerank") ? address : address + "/rerank");
        apiKey = Objects.requireNonNullElse(settings.getApiKey(), "");
        model = Objects.requireNonNullElse(settings.getModel(), "");
        instruction = Objects.requireNonNullElse(settings.getInstruction(), "");
        maxQueryChars = properties.getMaxQueryChars();
        maxTextChars = properties.getMaxParentChars();
        if (apiKey.length() > 8192 || apiKey.indexOf('\r') >= 0 || apiKey.indexOf('\n') >= 0
                || model.isBlank() || model.length() > 256 || instruction.length() > 4000
                || maxQueryChars < 1 || maxQueryChars > 4000 || maxTextChars < 1 || maxTextChars > 16000) {
            throw new IllegalArgumentException("Invalid RAG reranker configuration");
        }
        http = new RagIndexHttp(settings.getTimeoutMs(), settings.getMaxResponseBytes(),
                apiKey.isBlank() ? Map.of() : Map.of("Authorization", "Bearer " + apiKey));
    }

    public List<RagData.Reranked> rerank(String query, List<RagData.Candidate> candidates, int topN) {
        if (query == null || query.isBlank() || query.length() > maxQueryChars || candidates == null
                || candidates.size() > 20 || topN < 1 || topN > 5) {
            throw new IllegalArgumentException("Invalid RAG reranker input");
        }
        if (candidates.isEmpty()) return List.of();
        var documents = new ArrayList<String>(candidates.size());
        var candidateIds = new HashSet<String>();
        for (var candidate : candidates) {
            if (candidate == null || candidate.id() == null || candidate.id().isBlank()
                    || !candidateIds.add(candidate.id()) || candidate.text() == null || candidate.text().isBlank()
                    || candidate.text().length() > maxTextChars) {
                throw new IllegalArgumentException("Invalid RAG reranker candidate");
            }
            documents.add(candidate.text());
        }
        if (apiKey.isBlank()) throw new IllegalStateException("RAG reranker credentials are unavailable");
        int expected = Math.min(topN, candidates.size());
        var payload = new LinkedHashMap<String, Object>();
        payload.put("model", model);
        payload.put("query", query);
        payload.put("documents", documents);
        payload.put("top_n", expected);
        payload.put("return_documents", false);
        if (model.startsWith("Qwen/Qwen3-Reranker-") && !instruction.isBlank()) payload.put("instruction", instruction);
        var response = http.exchange(endpoint, "POST", payload);
        if (!response.successful()) throw RagIndexHttp.failure();
        JsonNode results = RagIndexHttp.parse(response).path("results");
        if (!results.isArray() || results.size() != expected) throw RagIndexHttp.failure();
        var ranked = new ArrayList<RagData.Reranked>(expected);
        var seen = new HashSet<Integer>();
        double previousScore = Double.POSITIVE_INFINITY;
        for (JsonNode result : results) {
            JsonNode index = result.path("index"), score = result.path("relevance_score");
            if (!index.isIntegralNumber() || !index.canConvertToInt() || index.asInt() < 0 || index.asInt() >= candidates.size()
                    || !score.isNumber() || !Double.isFinite(score.asDouble()) || !seen.add(index.asInt())
                    || score.asDouble() > previousScore) throw RagIndexHttp.failure();
            previousScore = score.asDouble();
            ranked.add(new RagData.Reranked(index.asInt(), score.asDouble()));
        }
        return List.copyOf(ranked);
    }
}
