package org.hgc.suts.gateway.manager;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

/** Reads only fresh image evidence, including the previous Picture service cache layout. */
@Slf4j
@Component
public class VisionContextManager {
    private final StringRedisTemplate redis;
    private final Clock clock;
    @Value("${suts.ai.vision.max-age-ms:300000}")
    private long maxAgeMs = 300000;
    @Value("${suts.ai.vision.max-chars:2000}")
    private int maxChars = 2000;

    @Autowired
    public VisionContextManager(StringRedisTemplate redis) { this(redis, Clock.systemUTC()); }

    VisionContextManager(StringRedisTemplate redis, Clock clock) { this.redis = redis; this.clock = clock; }

    public String getRecentVision(Long userId) {
        for (String pattern : new String[]{RedisCacheConstant.AGENT_VISION_CONTEXT_KEY, RedisCacheConstant.LEGACY_VISION_CONTEXT_KEY}) {
            String key = String.format(pattern, userId);
            try {
                String raw = redis.opsForValue().get(key);
                String text = read(raw, key, userId);
                if (StrUtil.isNotBlank(text)) return text.substring(0, Math.min(text.length(), Math.max(0, maxChars)));
            } catch (Exception ex) {
                log.warn("视觉上下文不可用，跳过该记录 ({})", ex.getClass().getSimpleName());
            }
        }
        return "";
    }

    private String read(String raw, String key, Long userId) {
        if (StrUtil.isBlank(raw)) return "";
        if (!raw.stripLeading().startsWith("{")) {
            // Pre-schema plain text is accepted only while its original five-minute TTL is still active.
            Long ttl = redis.getExpire(key, TimeUnit.MILLISECONDS);
            return ttl != null && ttl > 0 && ttl <= maxAgeMs ? raw : "";
        }
        JSONObject record = JSONUtil.parseObj(raw);
        Integer version = record.getInt("schemaVersion");
        if (version != null && version != 1) return "";
        Long timestamp = record.getLong("timestamp");
        if (timestamp == null || timestamp < clock.millis() - maxAgeMs || timestamp > clock.millis() + 5000) return "";
        Long owner = record.getLong("userId");
        if (version != null && owner == null) return "";
        if (owner != null && !owner.equals(userId)) return "";
        if (!(record.get("text") instanceof String text)) return "";
        if (StrUtil.isBlank(text)) return "";
        if (version == null && text.stripLeading().startsWith("{")) {
            // Old Picture stored the complete {question, answer} response in text.
            Object answer = JSONUtil.parseObj(text).get("answer");
            text = answer instanceof String ? (String) answer : "";
        }
        return text;
    }
}
