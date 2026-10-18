package org.hgc.suts.gateway.service;

import java.util.List;

/**
 * 向量存储服务接口
 */
public interface VectorStorageService {

    /**
     * 保存记忆
     * @param userId  用户ID (用于数据隔离，不同用户的记忆互不可见)
     * @param content 原始记忆文本 (例如："用户提到他对花生过敏")
     */
    void saveMemory(Long userId, String content);

    /**
     * 语义检索
     * @param userId      用户ID (强制过滤条件)
     * @param query 当前问题（由 Spring AI 完成向量化）
     * @param limit       返回条数限制
     * @return 匹配到的文本列表
     */
    List<String> searchMemory(Long userId, String query, int limit);
}
