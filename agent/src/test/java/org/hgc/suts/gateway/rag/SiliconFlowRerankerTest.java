package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.RagProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SiliconFlowRerankerTest {
    private RagIndexHttpTestServer server;
    private RagProperties properties;

    @BeforeEach void setup() throws Exception {
        server = new RagIndexHttpTestServer();
        properties = new RagProperties();
        properties.getReranker().setEndpoint(server.endpoint() + "/v1/");
        properties.getReranker().setApiKey("integration-only-key");
    }
    @AfterEach void cleanup() { server.close(); }

    @Test void sendsOneQwenRequestWithInstructionAndMapsOriginalIndexes() {
        server.responder = request -> new RagIndexHttpTestServer.Reply(200,
                "{\"results\":[{\"index\":1,\"relevance_score\":0.9},{\"index\":0,\"relevance_score\":0.2}]}");
        assertThat(new SiliconFlowReranker(properties).rerank("原始问题", candidates(2), 5))
                .containsExactly(new RagData.Reranked(1, .9), new RagData.Reranked(0, .2));
        assertThat(server.requests).hasSize(1);
        var request = server.requests.get(0);
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.path()).isEqualTo("/v1/rerank");
        assertThat(request.authorization()).isEqualTo("Bearer integration-only-key");
        assertThat(request.body().path("model").asText()).isEqualTo("Qwen/Qwen3-Reranker-0.6B");
        assertThat(request.body().path("query").asText()).isEqualTo("原始问题");
        assertThat(request.body().path("documents").get(0).asText()).isEqualTo("passage 0");
        assertThat(request.body().path("top_n").asInt()).isEqualTo(2);
        assertThat(request.body().path("return_documents").asBoolean()).isFalse();
        assertThat(request.body().path("instruction").asText()).isEqualTo(properties.getReranker().getInstruction());
        assertThat(request.body().has("max_chunks_per_doc")).isFalse();
        assertThat(request.body().has("overlap_tokens")).isFalse();
    }

    @Test void otherModelsDoNotReceiveQwenOrBgeSpecificOptions() {
        properties.getReranker().setModel("BAAI/bge-reranker-v2-m3");
        properties.getReranker().setEndpoint(server.endpoint() + "/v1/rerank");
        server.responder = request -> new RagIndexHttpTestServer.Reply(200, "{\"results\":[{\"index\":0,\"relevance_score\":-1.2}]}");
        assertThat(new SiliconFlowReranker(properties).rerank("question", candidates(1), 1)).hasSize(1);
        var body = server.requests.get(0).body();
        assertThat(body.has("instruction")).isFalse();
        assertThat(body.has("max_chunks_per_doc")).isFalse();
        assertThat(body.has("overlap_tokens")).isFalse();
    }

    @Test void rejectsMalformedIndexesScoresCardinalityAndOrderingWithoutResponseLeakage() {
        var reranker = new SiliconFlowReranker(properties);
        for (String body : List.of(
                "{\"results\":[{\"index\":0,\"relevance_score\":0.9},{\"index\":0,\"relevance_score\":0.2}]}",
                "{\"results\":[{\"index\":-1,\"relevance_score\":0.9},{\"index\":0,\"relevance_score\":0.2}]}",
                "{\"results\":[{\"index\":2,\"relevance_score\":0.9},{\"index\":0,\"relevance_score\":0.2}]}",
                "{\"results\":[{\"index\":4294967296,\"relevance_score\":0.9},{\"index\":1,\"relevance_score\":0.2}]}",
                "{\"results\":[{\"index\":0.0,\"relevance_score\":0.9},{\"index\":1,\"relevance_score\":0.2}]}",
                "{\"results\":[{\"index\":0,\"relevance_score\":\"NaN\"},{\"index\":1,\"relevance_score\":0.2}]}",
                "{\"results\":[{\"index\":0,\"relevance_score\":1e999},{\"index\":1,\"relevance_score\":0.2}]}",
                "{\"results\":[{\"index\":0,\"relevance_score\":0.1},{\"index\":1,\"relevance_score\":0.9}]}",
                "{\"results\":[{\"index\":0,\"relevance_score\":0.9}]}",
                "{\"results\":[{\"index\":0,\"relevance_score\":0.9},{\"index\":1,\"relevance_score\":0.2},{\"index\":0,\"relevance_score\":0.1}]}",
                "{\"results\":null,\"private\":\"sensitive-provider-body\"}",
                "sensitive-provider-body",
                "{\"results\":[{\"index\":0,\"relevance_score\":0.9},{\"index\":1,\"relevance_score\":0.2}]} sensitive-provider-body")) {
            server.responder = request -> new RagIndexHttpTestServer.Reply(200, body);
            int before = server.requests.size();
            assertThatThrownBy(() -> reranker.rerank("question", candidates(2), 2))
                    .isInstanceOf(IllegalStateException.class).hasMessage("RAG remote service request failed")
                    .hasNoCause();
            assertThat(server.requests).hasSize(before + 1);
        }
    }

    @Test void serverErrorsAreNotRetriedAndTheirBodyIsNeverExposed() {
        server.responder = request -> new RagIndexHttpTestServer.Reply(503, "sensitive-provider-body");
        assertThatThrownBy(() -> new SiliconFlowReranker(properties).rerank("question", candidates(1), 1))
                .hasMessage("RAG remote service request failed").hasNoCause();
        assertThat(server.requests).hasSize(1);
    }

    @Test void chunkedResponseSizeAndWholeResponseTimeAreBounded() {
        properties.getReranker().setMaxResponseBytes(1024);
        server.responder = request -> new RagIndexHttpTestServer.Reply(200, "x".repeat(4096), true, 0);
        assertThatThrownBy(() -> new SiliconFlowReranker(properties).rerank("question", candidates(1), 1))
                .hasMessage("RAG remote service request failed").hasNoCause();
        properties.getReranker().setTimeoutMs(250);
        server.responder = request -> new RagIndexHttpTestServer.Reply(200, "{}", false, 1000);
        long started = System.nanoTime();
        assertThatThrownBy(() -> new SiliconFlowReranker(properties).rerank("question", candidates(1), 1))
                .hasMessage("RAG remote service request failed").hasNoCause();
        assertThat((System.nanoTime() - started) / 1_000_000).isLessThan(3000);
        assertThat(server.requests).hasSize(2);
    }

    @Test void invalidBudgetsDuplicatesAndMissingCredentialsNeverReachProvider() {
        var reranker = new SiliconFlowReranker(properties);
        assertThatThrownBy(() -> reranker.rerank("question", candidates(21), 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reranker.rerank("question", candidates(1), 6)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reranker.rerank("question", List.of(candidates(1).get(0), candidates(1).get(0)), 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(reranker.rerank("question", List.of(), 1)).isEmpty();
        properties.getReranker().setApiKey("");
        assertThatThrownBy(() -> new SiliconFlowReranker(properties).rerank("question", candidates(1), 1))
                .hasMessage("RAG reranker credentials are unavailable");
        assertThat(server.requests).isEmpty();
    }

    private static List<RagData.Candidate> candidates(int count) {
        return IntStream.range(0, count).mapToObj(i -> new RagData.Candidate("candidate-" + i,
                RagData.Kind.KNOWLEDGE_CHILD, "document", 1, "parent", "title", "passage " + i,
                "local.md", 0, 0, i)).toList();
    }
}
