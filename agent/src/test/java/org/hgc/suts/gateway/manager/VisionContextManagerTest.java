package org.hgc.suts.gateway.manager;

import cn.hutool.json.JSONObject;
import org.hgc.suts.gateway.common.constant.RedisCacheConstant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class VisionContextManagerTest {
    private static final long NOW = 1_800_000_000_000L;
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class, RETURNS_DEEP_STUBS);
    private final VisionContextManager reader = new VisionContextManager(redis, Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC));
    private final String current = String.format(RedisCacheConstant.AGENT_VISION_CONTEXT_KEY, 42L);
    private final String legacy = String.format(RedisCacheConstant.LEGACY_VISION_CONTEXT_KEY, 42L);

    @BeforeEach
    void setUp() { when(redis.opsForValue().get(current)).thenReturn(record("一扇门", NOW)); }

    @Test
    void newSchemaWinsOverLegacy() {
        when(redis.opsForValue().get(legacy)).thenReturn(record("旧图片", NOW));
        assertThat(reader.getRecentVision(42L)).isEqualTo("一扇门");
    }

    @Test
    void readsOldKeyAndExtractsNestedAnswer() {
        when(redis.opsForValue().get(current)).thenReturn(null);
        when(redis.opsForValue().get(legacy)).thenReturn(record("{\"question\":\"看什么\",\"answer\":\"一张桌子\"}", NOW));
        assertThat(reader.getRecentVision(42L)).isEqualTo("一张桌子");
    }

    @Test
    void ignoresExpiredFutureCorruptAndUnknownSchemaRecords() {
        for (String invalid : new String[]{record("旧图片", NOW - 300001), record("未来图片", NOW + 5001),
                "{broken", "{\"text\":\"缺时间戳\"}", "{\"timestamp\":" + NOW + ",\"schemaVersion\":2,\"text\":\"unknown\"}"}) {
            when(redis.opsForValue().get(current)).thenReturn(invalid);
            assertThat(reader.getRecentVision(42L)).isEmpty();
        }
    }

    @Test
    void userKeyAndOwnerPreventCrossUserReads() {
        assertThat(reader.getRecentVision(7L)).isEmpty();
        when(redis.opsForValue().get(current)).thenReturn(new JSONObject().set("timestamp", NOW)
                .set("userId", 7L).set("text", "其他用户").toString());
        assertThat(reader.getRecentVision(42L)).isEmpty();
    }

    @Test
    void acceptsPlainLegacyTextOnlyWithBoundedTtl() {
        when(redis.opsForValue().get(current)).thenReturn(null);
        when(redis.opsForValue().get(legacy)).thenReturn("路牌");
        when(redis.getExpire(legacy, TimeUnit.MILLISECONDS)).thenReturn(10000L);
        assertThat(reader.getRecentVision(42L)).isEqualTo("路牌");
        when(redis.getExpire(legacy, TimeUnit.MILLISECONDS)).thenReturn(-1L);
        assertThat(reader.getRecentVision(42L)).isEmpty();
    }

    @Test
    void limitsVisualEvidenceAndDegradesOnCacheFailure() {
        when(redis.opsForValue().get(current)).thenReturn(record("字".repeat(4000), NOW));
        assertThat(reader.getRecentVision(42L)).hasSize(2000);
        when(redis.opsForValue().get(current)).thenThrow(new IllegalStateException("unavailable"));
        assertThat(reader.getRecentVision(42L)).isEmpty();
    }

    @Test
    void versionedEvidenceIsNotReinterpretedAsProviderJson() {
        when(redis.opsForValue().get(current)).thenReturn(new JSONObject().set("schemaVersion", 1).set("userId", 42L)
                .set("timestamp", NOW).set("text", "{这是一段图中文字}").toString());
        assertThat(reader.getRecentVision(42L)).isEqualTo("{这是一段图中文字}");
    }

    private String record(String text, long time) { return new JSONObject().set("text", text).set("timestamp", time).toString(); }
}
