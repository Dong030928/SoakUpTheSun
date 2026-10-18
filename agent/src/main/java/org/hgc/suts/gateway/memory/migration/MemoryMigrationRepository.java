package org.hgc.suts.gateway.memory.migration;

import org.springframework.stereotype.Component;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Raw legacy-format access is confined to the offline migration boundary. */
@Component
public class MemoryMigrationRepository {
    private final RedisClient redis;

    public MemoryMigrationRepository(RedisClient memoryRedisClient) {
        this.redis = memoryRedisClient;
    }

    public record Page(String nextCursor, List<String> keys) { }

    public Page scan(String cursor, String prefix, int count) {
        var result = redis.scan(cursor, new ScanParams().match(prefix + "*").count(count));
        return new Page(result.getCursor(), result.getResult());
    }

    public Map<byte[], byte[]> readLegacyHash(String key) {
        return redis.hgetAll(bytes(key));
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> readDocument(String key) {
        return redis.jsonGet(key, Map.class);
    }

    public boolean exists(String key) {
        return redis.exists(key);
    }

    public Map<String, String> readCheckpoint(String key) {
        return redis.hgetAll(key);
    }

    /** The lease test and mutation are atomic, so an expired worker cannot corrupt its successor's progress. */
    public boolean saveCheckpoint(String key, String lockKey, String token, Map<String, String> values) {
        var arguments = new ArrayList<String>();
        arguments.add(token);
        values.forEach((field, value) -> {
            arguments.add(field);
            arguments.add(value);
        });
        Object result = redis.eval("if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end "
                        + "for i = 2, #ARGV, 2 do redis.call('HSET', KEYS[2], ARGV[i], ARGV[i+1]) end return 1",
                List.of(lockKey, key), arguments);
        return ((Number) result).longValue() == 1;
    }

    /** Return false only for an identical previous export, never overwrite another record. */
    public boolean writeLegacyIfAbsent(String key, String userId, String content, byte[] vector) {
        String script = """
                if redis.call('EXISTS', KEYS[1]) == 1 then
                    if redis.call('TYPE', KEYS[1]).ok ~= 'hash' then return -1 end
                    if redis.call('HGET', KEYS[1], 'userId') == ARGV[1]
                      and redis.call('HGET', KEYS[1], 'content') == ARGV[2]
                      and redis.call('HGET', KEYS[1], 'vector') == ARGV[3] then return 0 end
                    return -1
                end
                redis.call('HSET', KEYS[1], 'userId', ARGV[1], 'content', ARGV[2], 'vector', ARGV[3])
                return 1
                """;
        Object result = redis.eval(bytes(script), List.of(bytes(key)),
                List.of(bytes(userId), bytes(content), vector));
        long status = ((Number) result).longValue();
        if (status < 0) throw new IllegalStateException("Rollback target conflicts with existing data");
        return status == 1;
    }

    public boolean acquireLock(String key, String token) {
        return "OK".equals(redis.set(key, token, SetParams.setParams().nx().px(600_000)));
    }

    public void renewLock(String key, String token) {
        Object result = redis.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then "
                        + "return redis.call('PEXPIRE', KEYS[1], 600000) end return 0",
                List.of(key), List.of(token));
        if (((Number) result).longValue() != 1) throw new IllegalStateException("Migration lock lost");
    }

    public void releaseLock(String key, String token) {
        redis.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then "
                + "return redis.call('DEL', KEYS[1]) end return 0", List.of(key), List.of(token));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
