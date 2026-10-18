package org.hgc.suts.gateway.manager;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.service.VectorStorageService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/** Long-term semantic recall through Spring AI; writes have their own bounded worker pool. */
@Slf4j
@Component
public class ChatMemoryManager {
    private final VectorStorageService vectors;
    private final Executor writer;
    private final MemoryProperties properties;
    private final MemoryVectorProperties vectorProperties;

    public ChatMemoryManager(VectorStorageService vectors, @Qualifier("memoryTaskExecutor") Executor writer,
                             MemoryProperties properties, MemoryVectorProperties vectorProperties) {
        this.vectors = vectors;
        this.writer = writer;
        this.properties = properties;
        this.vectorProperties = vectorProperties;
    }

    public String getRelatedHistory(Long userId, String query) {
        if (properties.getLongTermChars() == 0) return "";
        try {
            var result = new StringBuilder();
            for (String content : new LinkedHashSet<>(vectors.searchMemory(userId, query, 6))) {
                if (content == null || content.isBlank()) continue;
                int available = properties.getLongTermChars() - result.length();
                if (available <= 0) break;
                result.append(content, 0, Math.min(content.length(), available));
                if (result.length() < properties.getLongTermChars()) result.append('\n');
            }
            return result.toString();
        } catch (Exception ex) {
            log.warn("长期记忆检索不可用，跳过本轮检索 ({})", ex.getClass().getSimpleName());
            return "";
        }
    }

    public void saveHistory(Long userId, String userInput, String answer) {
        if (!properties.isWritesEnabled()) return;
        String question = bounded(userInput);
        int limit = vectorProperties.getMaxDocumentChars();
        if (limit <= 11) return;
        question = question.substring(0, Math.min(question.length(), limit - 11));
        String prefix = "User: " + question + "\nAI: ";
        String reply = bounded(answer);
        String content = prefix + reply.substring(0, Math.min(reply.length(), limit - prefix.length()));
        try {
            writer.execute(() -> {
                try { vectors.saveMemory(userId, content); }
                catch (Exception ex) { log.warn("长期记忆保存不可用，跳过本轮写入 ({})", ex.getClass().getSimpleName()); }
            });
        } catch (RejectedExecutionException ex) {
            // A full memory queue must never invalidate an already executed client/business action.
            log.warn("长期记忆写入队列已满，跳过本轮写入");
        }
    }

    private String bounded(String value) {
        if (value == null) return "";
        return value.substring(0, Math.min(value.length(), properties.getMaxMessageChars()));
    }
}
