package org.hgc.suts.gateway.memory.summary;

import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.metadata.Usage;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.DoubleAccumulator;

/** Explicit conservative UTF-8 byte estimator, NOT a DeepSeek tokenizer or an exact provider-token guarantee. */
public final class MemoryTokenBudget {
    private final DoubleAccumulator calibration = new DoubleAccumulator(Math::max, 1.0);
    private final java.util.concurrent.atomic.LongAdder observed = new java.util.concurrent.atomic.LongAdder();
    public int text(String text) {
        if (text == null) return 0;
        return (int) Math.min(Integer.MAX_VALUE, Math.ceil(text.getBytes(StandardCharsets.UTF_8).length * calibration.get()));
    }
    public int message(Message message) {
        int cost = Math.addExact(16, text(message.getText()));
        if (message instanceof org.springframework.ai.chat.messages.AssistantMessage assistant && assistant.hasToolCalls())
            cost = Math.addExact(cost, text(tools.jackson.databind.json.JsonMapper.builder().build().writeValueAsString(assistant.getToolCalls())));
        if (message instanceof org.springframework.ai.chat.messages.ToolResponseMessage response)
            cost = Math.addExact(cost, text(tools.jackson.databind.json.JsonMapper.builder().build().writeValueAsString(response.getResponses())));
        return cost;
    }
    public int messages(List<Message> messages) {
        long total = 16;
        for (Message message : messages) total += message(message);
        return (int) Math.min(Integer.MAX_VALUE, total);
    }
    public int request(List<Message> messages, String toolSchema) {
        return Math.addExact(messages(messages), text(toolSchema));
    }
    public void observe(int estimated, Usage usage) {
        if (estimated > 0 && usage != null && usage.getPromptTokens() != null && usage.getPromptTokens() > 0) {
            calibration.accumulate((double) usage.getPromptTokens() / estimated);
            observed.increment();
        }
    }
    public long observations() { return observed.sum(); }
    public String estimator() { return "conservative-utf8-bytes-with-usage-calibration"; }
}
