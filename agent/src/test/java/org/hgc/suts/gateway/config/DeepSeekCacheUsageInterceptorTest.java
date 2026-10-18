package org.hgc.suts.gateway.config;

import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.mock.http.client.*;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

class DeepSeekCacheUsageInterceptorTest {
    private byte[] adapt(String json) throws Exception {
        var request = new MockClientHttpRequest(HttpMethod.POST, URI.create("https://fixture.invalid/chat/completions"));
        try (var response = new DeepSeekCacheUsageInterceptor().intercept(request, new byte[0],
                (r, b) -> new MockClientHttpResponse(json.getBytes(StandardCharsets.UTF_8), HttpStatus.OK))) {
            return response.getBody().readAllBytes();
        }
    }
    @Test void documentedLegacyHitCounterIsKeptInNativeUsageWithoutChangingMessageContent() throws Exception {
        var root = JsonMapper.builder().build().readTree(adapt("{\"usage\":{\"prompt_tokens\":100,\"prompt_cache_hit_tokens\":80},\"choices\":[{\"message\":{\"content\":\"原始内容\"}}]}"));
        assertThat(root.path("usage").path("prompt_tokens_details").path("cached_tokens").asInt()).isEqualTo(80);
        assertThat(root.path("choices").get(0).path("message").path("content").asText()).isEqualTo("原始内容");
    }
    @Test void providerSuppliedNativeCounterWinsOverCompatibilityField() throws Exception {
        var root = JsonMapper.builder().build().readTree(adapt("{\"usage\":{\"prompt_cache_hit_tokens\":80,\"prompt_tokens_details\":{\"cached_tokens\":60}}}"));
        assertThat(root.path("usage").path("prompt_tokens_details").path("cached_tokens").asInt()).isEqualTo(60);
    }
    @Test void absentOrInvalidUsageAndNonJsonBodiesAreNotTurnedIntoHits() throws Exception {
        for (String json : java.util.List.of("{\"choices\":[]}", "{\"usage\":{\"prompt_cache_hit_tokens\":-1}}", "not json"))
            assertThat(adapt(json)).isEqualTo(json.getBytes(StandardCharsets.UTF_8));
    }
}
