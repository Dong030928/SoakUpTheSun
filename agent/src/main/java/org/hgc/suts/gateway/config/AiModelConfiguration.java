package org.hgc.suts.gateway.config;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.deepseek.DeepSeekChatModel;
import org.springframework.ai.deepseek.DeepSeekChatOptions;
import org.springframework.ai.deepseek.api.DeepSeekApi;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.openai.OpenAiEmbeddingModel;
import org.springframework.ai.openai.OpenAiEmbeddingOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

/** Bridges existing suts.ai properties to Spring AI, without changing the business chain. */
@Configuration(proxyBeanMethods = false)
public class AiModelConfiguration {
    @Bean
    public ChatModel agentChatModel(AiConfig config) {
        validateUrl(config.getApiHost(), "suts.ai.api-host");
        Assert.hasText(config.getApiKey(), "suts.ai.api-key must be configured");
        Assert.hasText(config.getModelName(), "suts.ai.model-name must be configured");
        Assert.isTrue(config.getTimeout() != null && config.getTimeout() > 0, "suts.ai.timeout must be positive");
        Assert.isTrue(config.getTemperature() != null && Double.isFinite(config.getTemperature())
                && config.getTemperature() >= 0 && config.getTemperature() <= 2, "suts.ai.temperature must be between 0 and 2");
        Duration timeout = Duration.ofMillis(config.getTimeout());
        var factory = new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(timeout).build());
        factory.setReadTimeout(timeout);
        DeepSeekApi api = DeepSeekApi.builder().baseUrl(config.getApiHost()).apiKey(config.getApiKey())
                .completionsPath("/chat/completions")
                .restClientBuilder(RestClient.builder().requestFactory(factory)).build();
        return DeepSeekChatModel.builder().deepSeekApi(api)
                .options(DeepSeekChatOptions.builder().model(config.getModelName()).disableThinking()
                        .temperature(config.getTemperature()).build())
                .retryTemplate(new RetryTemplate(RetryPolicy.withMaxRetries(0))).build();
    }

    @Bean
    public ChatClient agentChatClient(ChatModel agentChatModel) {
        return ChatClient.builder(agentChatModel).build();
    }

    @Bean
    public EmbeddingModel agentEmbeddingModel(AiConfig config) {
        Assert.isTrue(config.getVector() != null && config.getVector().getDimension() > 0,
                "suts.ai.vector.dimension must be positive");
        if (!config.isEmbeddingEnabled()) {
            return new EmbeddingModel() {
                @Override
                public org.springframework.ai.embedding.EmbeddingResponse call(org.springframework.ai.embedding.EmbeddingRequest request) {
                    throw new IllegalStateException("Embedding is disabled");
                }
                @Override
                public float[] embed(org.springframework.ai.document.Document document) {
                    throw new IllegalStateException("Embedding is disabled");
                }
            };
        }
        validateUrl(config.getEmbeddingHost(), "suts.ai.embedding-host");
        Assert.hasText(config.getEmbeddingApiKey(), "suts.ai.embedding-api-key must be configured or embedding-enabled=false");
        Assert.hasText(config.getEmbeddingModelName(), "suts.ai.embedding-model-name must be configured");
        Assert.isTrue(config.getEmbeddingTimeout() > 0, "suts.ai.embedding-timeout must be positive");
        return OpenAiEmbeddingModel.builder().options(OpenAiEmbeddingOptions.builder()
                .baseUrl(config.getEmbeddingHost()).apiKey(config.getEmbeddingApiKey()).model(config.getEmbeddingModelName())
                .encodingFormat(OpenAiEmbeddingOptions.EncodingFormat.FLOAT)
                .timeout(Duration.ofMillis(config.getEmbeddingTimeout())).maxRetries(0).build()).build();
    }

    private static void validateUrl(String value, String property) {
        Assert.hasText(value, property + " must be configured");
        URI uri;
        try { uri = URI.create(value); }
        catch (IllegalArgumentException ex) { throw new IllegalArgumentException(property + " must be an HTTP(S) URL"); }
        Assert.isTrue(("https".equals(uri.getScheme()) || "http".equals(uri.getScheme()))
                && uri.getHost() != null && uri.getUserInfo() == null && uri.getQuery() == null && uri.getFragment() == null,
                property + " must be an HTTP(S) URL without credentials, query or fragment");
    }
}
