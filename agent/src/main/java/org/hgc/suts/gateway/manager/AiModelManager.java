package org.hgc.suts.gateway.manager;

import lombok.RequiredArgsConstructor;
import org.hgc.suts.gateway.config.AiConfig;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/** Compatibility facade for the existing chain; model HTTP and JSON are owned by Spring AI. */
@Component
@RequiredArgsConstructor
public class AiModelManager {
    private final ChatClient agentChatClient;
    private final EmbeddingModel agentEmbeddingModel;
    private final AiConfig config;

    public String chat(String systemPrompt, String userContent) {
        Assert.hasText(userContent, "Model input must not be empty");
        var request = agentChatClient.prompt().user(userContent);
        if (StringUtils.hasText(systemPrompt)) request.system(systemPrompt);
        String answer = request.call().content();
        if (!StringUtils.hasText(answer)) throw new IllegalStateException("Model returned an empty answer");
        return answer;
    }

    /** Failure is handled at the memory boundary; invalid vectors are never invented or saved. */
    public List<Double> embed(String text) {
        Assert.hasText(text, "Embedding input must not be empty");
        float[] vector = agentEmbeddingModel.embed(text);
        if (vector == null || vector.length != config.getVector().getDimension()) {
            throw new IllegalStateException("Embedding dimension does not match suts.ai.vector.dimension");
        }
        List<Double> result = new ArrayList<>(vector.length);
        boolean nonZero = false;
        for (float value : vector) {
            if (!Float.isFinite(value)) throw new IllegalStateException("Embedding contains a non-finite value");
            nonZero |= value != 0;
            result.add((double) value);
        }
        if (!nonZero) throw new IllegalStateException("Embedding returned a zero vector");
        return result;
    }
}
