package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.common.exception.ClientException;
import org.springframework.ai.chat.client.ChatClientRequest;
import org.springframework.ai.chat.client.ChatClientResponse;
import org.springframework.ai.chat.client.advisor.api.Advisor;
import org.springframework.ai.chat.client.advisor.api.AdvisorChain;
import org.springframework.ai.chat.client.advisor.api.BaseAdvisor;
import org.springframework.ai.model.tool.ToolCallingChatOptions;

/** Inside the tool loop: tool results and schemas count again before EVERY provider call. */
@lombok.extern.slf4j.Slf4j
public final class SummaryBudgetAdvisor implements BaseAdvisor {
    private final SummaryPolicy policy;
    private final MemoryTokenBudget tokens;
    private final SummaryCoordinator coordinator;
    public SummaryBudgetAdvisor(SummaryPolicy policy, MemoryTokenBudget tokens) { this(policy, tokens, null); }
    public SummaryBudgetAdvisor(SummaryPolicy policy, MemoryTokenBudget tokens, SummaryCoordinator coordinator) {
        this.policy = policy; this.tokens = tokens; this.coordinator = coordinator;
    }
    @Override public int getOrder() { return Advisor.DEFAULT_CHAT_MEMORY_PRECEDENCE_ORDER + 150; }
    @Override public ChatClientRequest before(ChatClientRequest request, AdvisorChain chain) {
        String schema = "";
        if (request.prompt().getOptions() instanceof ToolCallingChatOptions options) {
            schema = options.getToolCallbacks().stream().map(c -> c.getToolDefinition().toString()).reduce("", String::concat);
        }
        int estimate = tokens.request(request.prompt().getInstructions(), schema);
        if (estimate > policy.current().getInputBudgetTokens()) throw new ClientException("本轮模型上下文超过输入预算，已停止继续调用");
        return request.mutate().context("summary.estimatedTokens", estimate)
                .context("summary.requestStartedAt", System.currentTimeMillis()).build();
    }
    @Override public ChatClientResponse after(ChatClientResponse response, AdvisorChain chain) {
        Object estimated = response.context().get("summary.estimatedTokens");
        if (estimated instanceof Integer value && response.chatResponse() != null)
            tokens.observe(value, response.chatResponse().getMetadata().getUsage());
        if (coordinator != null && response.chatResponse() != null) {
            Object id = response.context().get(org.springframework.ai.chat.memory.ChatMemory.CONVERSATION_ID);
            Object version = response.context().get("summary.version");
            Object started = response.context().get("summary.requestStartedAt");
            Object selectedPolicy = response.context().get("summary.policy");
            if (id instanceof String conversation && version instanceof Number v && started instanceof Number time) {
                var usage = response.chatResponse().getMetadata().getUsage();
                Long cacheRead = org.hgc.suts.gateway.manager.ModelCacheUsage.cacheReadTokens(usage);
                try {
                    coordinator.observe(conversation, new SummaryContext.CacheObservation(System.currentTimeMillis(),
                            cacheRead != null && cacheRead > 0,
                            null, v.longValue(), time.longValue(), selectedPolicy instanceof String fingerprint ? fingerprint : null));
                } catch (RuntimeException ex) { log.warn("厂商缓存观测未保存，后续请求将重新校验 ({})", ex.getClass().getSimpleName()); }
            }
        }
        return response;
    }
}
