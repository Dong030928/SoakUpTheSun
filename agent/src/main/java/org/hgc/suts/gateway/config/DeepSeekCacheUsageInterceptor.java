package org.hgc.suts.gateway.config;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.*;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.*;

/** Bridge the documented DeepSeek hit field into the native usage shape understood by Spring AI 2.0.1. */
final class DeepSeekCacheUsageInterceptor implements ClientHttpRequestInterceptor {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    @Override public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        var response = execution.execute(request, body);
        if (!response.getStatusCode().is2xxSuccessful()) return response;
        byte[] original;
        try { original = response.getBody().readAllBytes(); }
        catch (IOException failure) { response.close(); throw failure; }
        byte[] adapted = original;
        try {
            var root = JSON.readTree(original);
            if (root != null && root.get("usage") instanceof ObjectNode usage) {
                var hit = usage.get("prompt_cache_hit_tokens");
                if (hit != null && hit.isIntegralNumber() && hit.asLong() >= 0) {
                    ObjectNode details = usage.get("prompt_tokens_details") instanceof ObjectNode node
                            ? node : usage.putObject("prompt_tokens_details");
                    if (!details.hasNonNull("cached_tokens")) {
                        details.put("cached_tokens", hit.asLong()); adapted = JSON.writeValueAsBytes(root);
                    }
                }
            }
        } catch (RuntimeException invalidJson) { /* Preserve provider decoding/error behavior; never invent a cache hit. */ }
        final byte[] content = adapted;
        return new ClientHttpResponse() {
            public HttpStatusCode getStatusCode() throws IOException { return response.getStatusCode(); }
            public String getStatusText() throws IOException { return response.getStatusText(); }
            public HttpHeaders getHeaders() {
                var headers = new HttpHeaders(); headers.putAll(response.getHeaders()); headers.setContentLength(content.length); return headers;
            }
            public InputStream getBody() { return new ByteArrayInputStream(content); }
            public void close() { response.close(); }
        };
    }
}
