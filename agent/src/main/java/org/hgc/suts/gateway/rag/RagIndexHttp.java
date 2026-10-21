package org.hgc.suts.gateway.rag;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

/** Small bounded transport. Provider text, credentials and Jackson parser context never escape it. */
final class RagIndexHttp {
    static final JsonMapper JSON = JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private static final int MAX_REQUEST_BYTES = 1024 * 1024;
    private final HttpClient client;
    private final int timeoutMs;
    private final int maxResponseBytes;
    private final Map<String, String> headers;

    RagIndexHttp(int timeoutMs, int maxResponseBytes, Map<String, String> headers) {
        if (timeoutMs < 100 || timeoutMs > 30000 || maxResponseBytes < 1024 || maxResponseBytes > 1024 * 1024) {
            throw new IllegalArgumentException("Invalid RAG HTTP budget");
        }
        this.timeoutMs = timeoutMs;
        this.maxResponseBytes = maxResponseBytes;
        this.headers = Map.copyOf(headers);
        client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs))
                .followRedirects(HttpClient.Redirect.NEVER).version(HttpClient.Version.HTTP_1_1).build();
    }

    Response exchange(URI uri, String method, Object payload) {
        CompletableFuture<HttpResponse<byte[]>> pending = null;
        try {
            byte[] bytes = payload == null ? null : JSON.writeValueAsBytes(payload);
            if (bytes != null && bytes.length > MAX_REQUEST_BYTES) throw failure();
            var request = HttpRequest.newBuilder(uri).timeout(Duration.ofMillis(timeoutMs))
                    .header("Accept", "application/json");
            headers.forEach(request::header);
            if (bytes != null) request.header("Content-Type", "application/json; charset=UTF-8");
            request.method(method, bytes == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(bytes));
            // Exactly one application-level attempt; redirects and retry policies are intentionally absent.
            pending = client.sendAsync(request.build(), ignored -> new LimitedBody(maxResponseBytes));
            var response = pending.get(timeoutMs, TimeUnit.MILLISECONDS);
            return new Response(response.statusCode(), response.body());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw failure();
        } catch (Exception ignored) {
            throw failure();
        } finally {
            if (pending != null && !pending.isDone()) pending.cancel(true);
        }
    }

    static JsonNode parse(Response response) {
        try {
            JsonNode json = JSON.readTree(response.body());
            if (json == null || !json.isObject()) throw failure();
            return json;
        } catch (Exception ignored) {
            throw failure();
        }
    }

    static IllegalStateException failure() { return new IllegalStateException("RAG remote service request failed"); }

    record Response(int status, byte[] body) {
        boolean successful() { return status >= 200 && status < 300; }
    }

    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final int limit;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;

        private LimitedBody(int limit) { this.limit = limit; }
        @Override public CompletionStage<byte[]> getBody() { return result; }
        @Override public void onSubscribe(Flow.Subscription subscription) {
            if (this.subscription != null) { subscription.cancel(); return; }
            this.subscription = subscription;
            subscription.request(1);
        }
        @Override public void onNext(List<ByteBuffer> buffers) {
            if (result.isDone()) return;
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > limit - bytes.size()) {
                    subscription.cancel();
                    result.completeExceptionally(failure());
                    return;
                }
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                bytes.writeBytes(chunk);
            }
            subscription.request(1);
        }
        @Override public void onError(Throwable ignored) { result.completeExceptionally(failure()); }
        @Override public void onComplete() { result.complete(bytes.toByteArray()); }
    }
}
