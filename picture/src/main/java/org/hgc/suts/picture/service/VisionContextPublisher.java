package org.hgc.suts.picture.service;

import com.alibaba.fastjson2.JSONObject;
import lombok.RequiredArgsConstructor;
import org.hgc.suts.picture.common.constant.RedisCacheConstant;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;

@Component
@RequiredArgsConstructor
public class VisionContextPublisher {
    private static final long MAX_AGE_MS = 300000;
    private static final DefaultRedisScript<Long> UPDATE = updateScript();
    private final StringRedisTemplate redis;

    private static DefaultRedisScript<Long> updateScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/update_vision_context.lua"));
        script.setResultType(Long.class);
        return script;
    }

    public void publish(Long userId, Long pictureId, String answer, Long requestedAt) {
        Assert.hasText(answer, "Vision response must contain answer");
        if (userId == null) return;
        long timestamp = requestedAt == null ? System.currentTimeMillis() : requestedAt;
        if (timestamp < System.currentTimeMillis() - MAX_AGE_MS || timestamp > System.currentTimeMillis() + 5000) return;
        JSONObject record = new JSONObject();
        record.put("schemaVersion", 1);
        record.put("userId", userId);
        record.put("pictureId", pictureId);
        record.put("text", answer);
        record.put("timestamp", timestamp);
        redis.execute(UPDATE, List.of(String.format(RedisCacheConstant.AGENT_VISION_CONTEXT_KEY, userId)),
                record.toJSONString(), String.valueOf(timestamp), String.valueOf(MAX_AGE_MS));
    }
}
