package org.hgc.suts.gateway.manager;

import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.deepseek.api.DeepSeekApi;

/** Spring AI 2.0.1 preserves DeepSeek's native details but does not fill the canonical cache-read counter. */
public final class ModelCacheUsage {
    private ModelCacheUsage() { }
    public static Long cacheReadTokens(Usage usage) {
        if (usage == null) return null;
        Long canonical = usage.getCacheReadInputTokens();
        if (canonical != null) return canonical >= 0 ? canonical : null;
        if (usage.getNativeUsage() instanceof DeepSeekApi.Usage nativeUsage && nativeUsage.promptTokensDetails() != null) {
            Integer cached = nativeUsage.promptTokensDetails().cachedTokens();
            return cached != null && cached >= 0 ? cached.longValue() : null;
        }
        return null;
    }
}
