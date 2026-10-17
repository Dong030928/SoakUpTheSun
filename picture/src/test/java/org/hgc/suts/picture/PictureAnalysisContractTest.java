package org.hgc.suts.picture;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.qcloud.cos.model.COSObject;
import com.sun.net.httpserver.HttpServer;
import org.hgc.suts.picture.common.constant.RedisCacheConstant;
import org.hgc.suts.picture.common.tensentCos.CosManager;
import org.hgc.suts.picture.mq.base.MessageWrapper;
import org.hgc.suts.picture.mq.consumer.PictureAnalysisConsumer;
import org.hgc.suts.picture.mq.event.UploadPictureAnalysisEvent;
import org.hgc.suts.picture.service.VisionContextPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PictureAnalysisContractTest {
    @Test
    @SuppressWarnings("unchecked")
    void multipartResponsePublishesOnlyAnswerWithCaptureTimestamp() throws Exception {
        StringRedisTemplate redis = mock(StringRedisTemplate.class, RETURNS_DEEP_STUBS);
        CosManager cos = mock(CosManager.class);
        COSObject image = new COSObject();
        image.setObjectContent(new com.qcloud.cos.model.COSObjectInputStream(
                new ByteArrayInputStream(new byte[]{(byte) 0xff, (byte) 0xd8, (byte) 0xff}),
                new org.apache.http.client.methods.HttpGet("http://example.invalid/fixture")));
        when(cos.getObject("space/test.jpg")).thenReturn(image);
        AtomicReference<String> multipart = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/vision-qa/qa", exchange -> {
            multipart.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = "{\"question\":\"这是什么\",\"answer\":\"一扇门\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            PictureAnalysisConsumer consumer = new PictureAnalysisConsumer(cos, redis, new VisionContextPublisher(redis));
            ReflectionTestUtils.setField(consumer, "aiApiUrl", "http://127.0.0.1:" + server.getAddress().getPort() + "/vision-qa/qa");
            long requestedAt = System.currentTimeMillis() - 1000;
            UploadPictureAnalysisEvent event = UploadPictureAnalysisEvent.builder().userId(42L).pictureId(7L)
                    .requestedAt(requestedAt).imageKey("space/test.jpg").descriptionContent("这是什么").build();
            consumer.onMessage(new MessageWrapper<>("fixture", event));
            assertThat(multipart.get()).contains("name=\"file\"", "filename=\"test.jpg\"", "name=\"description\"", "这是什么");
            ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
            verify(redis).execute(any(RedisScript.class), eq(List.of("suts:agent:vision:context:42")),
                    payload.capture(), eq(String.valueOf(requestedAt)), eq("300000"));
            JSONObject record = JSON.parseObject(payload.getValue());
            assertThat(record.getString("text")).isEqualTo("一扇门");
            assertThat(record.getIntValue("schemaVersion")).isEqualTo(1);
            assertThat(record.getLongValue("userId")).isEqualTo(42);
            assertThat(record.getLongValue("pictureId")).isEqualTo(7);
            assertThat(record.getLongValue("timestamp")).isEqualTo(requestedAt);
            verify(redis.opsForValue()).set(eq(String.format(RedisCacheConstant.PICTURE_ANALYSIS_RESPONSE_KEY, 7L)),
                    contains("answer"), eq(5L), eq(java.util.concurrent.TimeUnit.MINUTES));
        } finally { server.stop(0); }
    }

    @Test
    void staleAnalysisDoesNotRefreshVisionContext() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        new VisionContextPublisher(redis).publish(42L, 7L, "旧图像", System.currentTimeMillis() - 300001);
        verifyNoInteractions(redis);
    }

    @Test
    void blankAnswerIsRejectedBeforeAnyCacheWrite() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        assertThatThrownBy(() -> new VisionContextPublisher(redis).publish(42L, 7L, " ", null))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(redis);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"{}", "{\"answer\":\" \"}", "{\"answer\":{\"invalid\":1}}", "not json"})
    void invalidUpstreamAnswerNeverReachesCaches(String body) throws Exception {
        StringRedisTemplate redis = mock(StringRedisTemplate.class, RETURNS_DEEP_STUBS);
        CosManager cos = mock(CosManager.class);
        COSObject image = new COSObject();
        image.setObjectContent(new com.qcloud.cos.model.COSObjectInputStream(new ByteArrayInputStream(new byte[]{1, 2, 3}),
                new org.apache.http.client.methods.HttpGet("http://example.invalid/fixture")));
        when(cos.getObject("test.jpg")).thenReturn(image);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/vision-qa/qa", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            PictureAnalysisConsumer consumer = new PictureAnalysisConsumer(cos, redis, new VisionContextPublisher(redis));
            ReflectionTestUtils.setField(consumer, "aiApiUrl", "http://127.0.0.1:" + server.getAddress().getPort() + "/vision-qa/qa");
            assertThatThrownBy(() -> consumer.onMessage(new MessageWrapper<>("fixture",
                    UploadPictureAnalysisEvent.builder().userId(42L).pictureId(7L).imageKey("test.jpg").build())))
                    .isInstanceOf(RuntimeException.class);
            verifyNoInteractions(redis);
        } finally { server.stop(0); }
    }

    @Test
    void defaultUrlMatchesPythonRouteAndRedisUsesSharedEnvironment() throws Exception {
        String yaml = new ClassPathResource("application.yaml").getContentAsString(StandardCharsets.UTF_8);
        assertThat(yaml).contains("${VISION_QA_URL:http://127.0.0.1:20000/vision-qa/qa}",
                "${REDIS_HOST:127.0.0.1}", "${REDIS_PORT:6379}", "${REDIS_PASSWORD:}");
    }
}
