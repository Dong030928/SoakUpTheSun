package org.hgc.suts.gateway.memory;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * A conversation-scoped Spring AI repository. The renewable lease serializes requests
 * across application instances; token-checked writes fence expired lease holders out.
 */
@Slf4j
@Component
public class RedisConversationMemoryRepository implements ConversationMemoryStore {
    private static final Pattern CONVERSATION_ID = Pattern.compile("user:[0-9]+:[a-f0-9]{64}");
    private static final String PREFIX = "suts:agent:memory:v2:";
    private static final int MAX_MESSAGES = 1000;
    private static final int MAX_MESSAGE_CHARS = 65536;
    private static final int MAX_PAYLOAD_CHARS = 1_048_576;
    private static final DefaultRedisScript<Long> RENEW = script("memory_renew.lua");
    private static final DefaultRedisScript<Long> RELEASE = script("memory_release.lua");
    private static final DefaultRedisScript<Long> SAVE = script("memory_save.lua");
    private static final DefaultRedisScript<Long> DELETE = script("memory_delete.lua");
    private static final DefaultRedisScript<Long> RESTORE = script("memory_restore.lua");

    private final StringRedisTemplate redis;
    private final long leaseMs;
    private final long ttlMs;
    private final ScheduledExecutorService scheduler;

    @Autowired
    public RedisConversationMemoryRepository(StringRedisTemplate redis,
            @Value("${suts.ai.memory.lease-ms:30000}") long leaseMs,
            @Value("${suts.ai.memory.ttl-seconds:86400}") long ttlSeconds) {
        this(redis, leaseMs, ttlSeconds, createScheduler());
    }

    RedisConversationMemoryRepository(StringRedisTemplate redis, long leaseMs, long ttlSeconds,
            ScheduledExecutorService scheduler) {
        Assert.notNull(redis, "Redis template must be provided");
        Assert.notNull(scheduler, "Memory lease scheduler must be provided");
        Assert.isTrue(leaseMs >= 1000, "suts.ai.memory.lease-ms must be at least 1000");
        Assert.isTrue(ttlSeconds > 0 && ttlSeconds <= Long.MAX_VALUE / 1000,
                "suts.ai.memory.ttl-seconds must be positive and fit in milliseconds");
        this.redis = redis;
        this.leaseMs = leaseMs;
        this.ttlMs = ttlSeconds * 1000;
        this.scheduler = scheduler;
    }

    public Lease open(String conversationId) {
        validateConversationId(conversationId);
        String token = UUID.randomUUID().toString();
        String lockKey = key(conversationId, "lock");
        Boolean acquired;
        try {
            acquired = redis.opsForValue().setIfAbsent(lockKey, token, Duration.ofMillis(leaseMs));
        } catch (RuntimeException ex) {
            log.warn("无法取得会话处理锁 ({})", ex.getClass().getSimpleName());
            throw new MemoryLeaseLostException();
        }
        if (Boolean.FALSE.equals(acquired)) throw new ConversationBusyException();
        if (!Boolean.TRUE.equals(acquired)) throw new MemoryLeaseLostException();
        Lease lease = new Lease(conversationId, token);
        try {
            lease.renewal = scheduler.scheduleWithFixedDelay(lease::renewInBackground,
                    leaseMs / 3, leaseMs / 3, TimeUnit.MILLISECONDS);
            return lease;
        } catch (RuntimeException ex) {
            lease.close();
            throw new MemoryLeaseLostException();
        }
    }

    @PreDestroy
    public void destroy() {
        scheduler.shutdownNow();
    }

    public final class Lease implements ConversationMemoryStore.Session {
        private final String conversationId;
        private final String token;
        private final AtomicBoolean valid = new AtomicBoolean(true);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ChatMemoryRepository repository = new BoundRepository();
        private ScheduledFuture<?> renewal;

        private Lease(String conversationId, String token) {
            this.conversationId = conversationId;
            this.token = token;
        }

        public ChatMemoryRepository repository() {
            return repository;
        }

        /** Internal capability used by the transaction script; never sent to MQ or an HTTP client. */
        public String fencingToken() { assertValid(); return token; }

        @Override public void prepare() {
            assertValid();
            if (!Boolean.FALSE.equals(redis.hasKey(key(conversationId, "transactions")))) {
                throw new org.hgc.suts.gateway.memory.transactions.MemoryTransactionsUnavailableException();
            }
        }

        public void assertNoTransactionEvidence() {
            assertValid();
            Assert.state(Boolean.FALSE.equals(redis.hasKey(key(conversationId, "transactions"))),
                    "Transaction evidence exists: use verified transaction maintenance after draining MQ");
        }

        public boolean hasStoredSnapshot() {
            assertValid();
            // Corruption is not absence: recovery must not silently overwrite an unreadable record.
            return !Boolean.FALSE.equals(redis.hasKey(key(conversationId, "messages")));
        }

        /** Offline recovery only: never replace an existing (even corrupt) hot snapshot. */
        public boolean restoreIfMissing(List<Message> messages) {
            assertValid();
            Long result = redis.execute(RESTORE, List.of(key(conversationId, "lock"),
                    key(conversationId, "messages")), token, encode(messages, conversationId), Long.toString(ttlMs));
            if (result == null || result < 0) throw new MemoryLeaseLostException();
            return result == 1;
        }

        public void assertValid() {
            if (closed.get() || !valid.get()) throw new MemoryLeaseLostException();
            Long renewed;
            try {
                renewed = redis.execute(RENEW, List.of(key(conversationId, "lock")), token,
                        Long.toString(leaseMs));
            } catch (RuntimeException ex) {
                valid.set(false);
                log.warn("无法确认会话处理锁 ({})", ex.getClass().getSimpleName());
                throw new MemoryLeaseLostException();
            }
            if (!Long.valueOf(1).equals(renewed) || closed.get() || !valid.get()) {
                valid.set(false);
                throw new MemoryLeaseLostException();
            }
        }

        private void renewInBackground() {
            try {
                assertValid();
            } catch (MemoryLeaseLostException ex) {
                if (renewal != null) renewal.cancel(false);
            }
        }

        private void requireBoundConversation(String candidate) {
            Assert.isTrue(conversationId.equals(candidate), "Repository is bound to a different conversation");
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) return;
            valid.set(false);
            if (renewal != null) renewal.cancel(false);
            try {
                redis.execute(RELEASE, List.of(key(conversationId, "lock")), token);
            } catch (RuntimeException ex) {
                // The server TTL still releases this lease if Redis cannot be reached.
                log.warn("会话处理锁释放失败，等待过期 ({})", ex.getClass().getSimpleName());
            }
        }

        private final class BoundRepository implements ChatMemoryRepository {
            private boolean snapshotReadable = true;
            @Override
            public List<String> findConversationIds() {
                assertValid();
                return List.of(conversationId);
            }

            @Override
            public List<Message> findByConversationId(String candidate) {
                requireBoundConversation(candidate);
                assertValid();
                try {
                    return decode(redis.opsForValue().get(key(conversationId, "messages")), conversationId);
                } catch (RuntimeException ex) {
                    snapshotReadable = false;
                    log.warn("会话记忆读取不可用，忽略该记录 ({})", ex.getClass().getSimpleName());
                    return List.of();
                }
            }

            @Override
            public void saveAll(String candidate, List<Message> messages) {
                requireBoundConversation(candidate);
                assertValid();
                assertNoTransactionEvidence();
                Assert.state(snapshotReadable, "Cannot replace a conversation snapshot that could not be read");
                String payload = encode(messages, conversationId);
                fencedMutation(SAVE, token, payload, Long.toString(ttlMs));
            }

            @Override
            public void deleteByConversationId(String candidate) {
                requireBoundConversation(candidate);
                assertValid();
                assertNoTransactionEvidence();
                fencedMutation(DELETE, token);
            }

            private void fencedMutation(DefaultRedisScript<Long> script, String... args) {
                Long result;
                try {
                    result = redis.execute(script, List.of(key(conversationId, "lock"),
                            key(conversationId, "messages")), (Object[]) args);
                } catch (RuntimeException ex) {
                    valid.set(false);
                    log.warn("会话记忆提交不可用 ({})", ex.getClass().getSimpleName());
                    throw new MemoryLeaseLostException();
                }
                if (!Long.valueOf(1).equals(result)) {
                    valid.set(false);
                    throw new MemoryLeaseLostException();
                }
            }
        }
    }

    public static String encode(List<Message> messages, String conversationId) {
        Assert.notNull(messages, "Messages must be provided");
        Assert.isTrue(messages.size() <= MAX_MESSAGES && messages.size() % 2 == 0,
                "Memory must contain bounded, complete user and assistant pairs");
        JSONArray records = new JSONArray();
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            boolean user = i % 2 == 0;
            Assert.isTrue(user ? message instanceof UserMessage : message instanceof AssistantMessage,
                    "Memory must alternate user and assistant messages");
            if (message instanceof UserMessage input) {
                Assert.isTrue(input.getMedia().isEmpty(), "Media cannot be persisted in conversation memory");
            }
            if (message instanceof AssistantMessage answer) {
                Assert.isTrue(!answer.hasToolCalls() && answer.getMedia().isEmpty(),
                        "Tool calls and media cannot be persisted in conversation memory");
            }
            String text = message.getText();
            Assert.isTrue(StringUtils.hasText(text) && text.length() <= MAX_MESSAGE_CHARS,
                    "Memory text must be nonempty and bounded");
            records.add(new JSONObject().set("role", user ? "USER" : "ASSISTANT").set("text", text));
        }
        String payload = new JSONObject().set("schemaVersion", 2).set("conversationId", conversationId)
                .set("messages", records).toString();
        Assert.isTrue(payload.length() <= MAX_PAYLOAD_CHARS, "Memory payload exceeds the bounded size");
        return payload;
    }

    private static List<Message> decode(String raw, String conversationId) {
        if (!StringUtils.hasText(raw)) return List.of();
        Assert.isTrue(raw.length() <= MAX_PAYLOAD_CHARS && raw.stripLeading().startsWith("{"),
                "Invalid memory payload");
        JSONObject record = JSONUtil.parseObj(raw);
        Assert.isTrue(Integer.valueOf(2).equals(record.get("schemaVersion"))
                && conversationId.equals(record.get("conversationId")), "Invalid memory owner or version");
        Object value = record.get("messages");
        Assert.isTrue(value instanceof JSONArray, "Invalid message collection");
        JSONArray records = (JSONArray) value;
        Assert.isTrue(records.size() <= MAX_MESSAGES && records.size() % 2 == 0,
                "Memory requires complete, bounded turns");
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            Object entry = records.get(i);
            Assert.isTrue(entry instanceof JSONObject, "Invalid memory message");
            JSONObject message = (JSONObject) entry;
            boolean user = i % 2 == 0;
            Assert.isTrue((user ? "USER" : "ASSISTANT").equals(message.get("role")), "Invalid memory role");
            Object textValue = message.get("text");
            Assert.isTrue(textValue instanceof String, "Invalid memory text");
            String text = (String) textValue;
            Assert.isTrue(StringUtils.hasText(text) && text.length() <= MAX_MESSAGE_CHARS,
                    "Memory text must be nonempty and bounded");
            messages.add(user ? new UserMessage(text) : new AssistantMessage(text));
        }
        return List.copyOf(messages);
    }

    public static String key(String conversationId, String suffix) {
        return PREFIX + "{" + conversationId + "}:" + suffix;
    }

    private static void validateConversationId(String conversationId) {
        Assert.isTrue(conversationId != null && CONVERSATION_ID.matcher(conversationId).matches(),
                "Conversation ID must identify an authenticated user and hashed session");
    }

    private static DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/" + name));
        script.setResultType(Long.class);
        return script;
    }

    private static ScheduledExecutorService createScheduler() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2, task -> {
            Thread thread = new Thread(task, "suts-memory-lease");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }
}
