package org.hgc.suts.gateway;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.manager.ManagedMemoryVectorStore;
import org.hgc.suts.gateway.memory.migration.MemoryMigrationProperties;
import org.hgc.suts.gateway.memory.migration.MemoryMigrationRepository;
import org.hgc.suts.gateway.memory.migration.MemoryMigrationService;
import org.hgc.suts.gateway.service.impl.RedisVectorStorageServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.vectorstore.redis.RedisVectorStore;
import org.springframework.mock.env.MockEnvironment;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.ScanParams;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Opt-in Query Engine/JSON test. Only a deliberately marked private Docker fixture is accepted. */
class RedisVectorIT {
    private static final String PASSWORD = "agent-vector-integration-only";
    private static final String FIXTURE_MARKER = "suts:agent:test:redis-vector-fixture";
    private RedisClient redis;
    private AiConfig ai;
    private MemoryVectorProperties properties;
    private DeterministicEmbeddings embedding;
    private ManagedMemoryVectorStore managed;
    private RedisVectorStorageServiceImpl vectors;
    private final List<String> legacyCleanupPrefixes = new ArrayList<>();

    @BeforeEach
    void connectOnlyToExplicitPrivateFixture() {
        String configuredPort = System.getProperty("redis.vector.port");
        if (configuredPort == null || !configuredPort.matches("[0-9]+")) {
            throw new IllegalStateException("Set -Dredis.vector.port=<private Docker port> for -Predis-vector");
        }
        int port = Integer.parseInt(configuredPort);
        assertThat(port).isBetween(1024, 65535);
        redis = RedisClient.builder().hostAndPort("127.0.0.1", port)
                .clientConfig(DefaultJedisClientConfig.builder().password(PASSWORD)
                        .connectionTimeoutMillis(2000).socketTimeoutMillis(2000).build()).build();
        // Never create the marker here: it must be installed by the private-fixture launcher.
        assertThat(redis.get(FIXTURE_MARKER)).as("Refuse to write to a Redis without the private fixture marker")
                .isEqualTo(PASSWORD);
        ai = new AiConfig();
        ai.setEmbeddingHost("https://fixture.invalid/v1");
        ai.setEmbeddingModelName("fixture/model-with-slash");
        ai.getVector().setDimension(3);
        properties = new MemoryVectorProperties();
        String namespace = UUID.randomUUID().toString();
        properties.setIndexName("test:agent:vector:index:" + namespace);
        properties.setPrefix("test:agent:vector:data:" + namespace + ":");
        properties.setSimilarityThreshold(0.9);
        embedding = new DeterministicEmbeddings();
        managed = new ManagedMemoryVectorStore(redis, embedding, ai, properties);
        vectors = service(ai, properties, managed);
    }

    @AfterEach
    void closeClient() {
        if (redis == null) return;
        try {
            for (String prefix : legacyCleanupPrefixes) {
                for (String key : keys(prefix)) redis.del(key);
            }
        } finally {
            redis.close();
        }
    }

    @Test
    void createsRealJsonIndexAndReusesPersistedDocumentsWithoutDimensionProbe() throws Exception {
        assertThat(redis.ftList()).doesNotContain(properties.effectiveIndexName(ai));
        managed.getStore();
        assertThat(embedding.calls).isZero();
        assertThat(redis.ftList()).contains(properties.effectiveIndexName(ai));
        assertThat(redis.get(properties.manifestKey(ai))).isEqualTo(properties.identity(ai));

        vectors.saveMemory(42L, "MATCH remembered text");
        await(() -> vectors.searchMemory(42L, "MATCH query", 3).size() == 1);
        String key = keys(properties.effectivePrefix(ai)).get(0);
        Map<String, Object> document = new MemoryMigrationRepository(redis).readDocument(key);
        assertThat(document).containsEntry("userId", "42").containsEntry("schemaVersion", "2")
                .containsEntry("embeddingModel", ai.getEmbeddingModelName())
                .containsEntry("content", "MATCH remembered text");
        assertThat(((Number) document.get("embeddingDimension")).intValue()).isEqualTo(3);
        assertThat((List<?>) document.get("embedding")).hasSize(3);
        assertThat(((Number) document.get("createdAt")).longValue()).isPositive();

        var restarted = service(ai, properties, new ManagedMemoryVectorStore(redis, embedding, ai, properties));
        assertThat(restarted.searchMemory(42L, "MATCH query", 3)).containsExactly("MATCH remembered text");
    }

    @Test
    void filtersOwnerAndSchemaBeforeTopKInsteadOfLeakingNearestOtherUsers() throws Exception {
        properties.setTopK(1);
        for (int i = 0; i < 5; i++) vectors.saveMemory(7L, "MATCH other owner " + i);
        // The authenticated user's less exact match must still survive the server-side top-k.
        vectors.saveMemory(42L, "NEAR owned text");
        List<Document> invalid = new ArrayList<>();
        for (String field : List.of("userId", "schemaVersion", "embeddingModel", "embeddingDimension")) {
            Map<String, Object> metadata = metadata(42);
            if (field.equals("embeddingDimension")) metadata.put(field, 4);
            else metadata.put(field, "wrong");
            if (field.equals("embeddingModel")) metadata.put("embeddingModelTag", "wrong");
            invalid.add(Document.builder().id(UUID.randomUUID().toString()).text("MATCH invalid " + field)
                    .metadata(metadata).build());
        }
        managed.getStore().add(invalid);
        await(() -> vectors.searchMemory(42L, "MATCH query", 1).equals(List.of("NEAR owned text")));
        assertThat(vectors.searchMemory(99L, "MATCH query", 1)).isEmpty();
        assertThatThrownBy(() -> vectors.searchMemory(null, "MATCH query", 1))
                .hasMessageContaining("Authenticated memory owner");
    }

    @Test
    void appliesSimilarityThresholdTopKDeduplicationAndRecallBudget() throws Exception {
        vectors.saveMemory(42L, "MATCH duplicate");
        vectors.saveMemory(42L, "MATCH duplicate");
        vectors.saveMemory(42L, "NEAR relevant");
        vectors.saveMemory(42L, "ORTHOGONAL irrelevant");
        vectors.saveMemory(42L, "OPPOSITE irrelevant");
        await(() -> vectors.searchMemory(42L, "MATCH query", 3).size() == 2);
        assertThat(vectors.searchMemory(42L, "MATCH query", 3))
                .containsExactly("MATCH duplicate", "NEAR relevant");
        assertThat(vectors.searchMemory(42L, "MATCH query", 1)).containsExactly("MATCH duplicate");
        properties.setMaxRecallChars(5);
        assertThat(vectors.searchMemory(42L, "MATCH query", 3)).containsExactly("MATCH");
    }

    @Test
    void aDifferentModelCannotReadOrOverwriteAnEqualDimensionEmbeddingSpace() throws Exception {
        vectors.saveMemory(42L, "MATCH original model");
        await(() -> vectors.searchMemory(42L, "MATCH query", 3).size() == 1);
        String originalPrefix = properties.effectivePrefix(ai);
        var other = new AiConfig();
        other.setEmbeddingHost(ai.getEmbeddingHost());
        other.setEmbeddingModelName("fixture/other-model");
        other.getVector().setDimension(3);
        var otherVectors = service(other, properties,
                new ManagedMemoryVectorStore(redis, embedding, other, properties));
        assertThat(properties.effectivePrefix(other)).isNotEqualTo(originalPrefix);
        assertThat(otherVectors.searchMemory(42L, "MATCH query", 3)).isEmpty();
        otherVectors.saveMemory(42L, "MATCH second model");
        await(() -> otherVectors.searchMemory(42L, "MATCH query", 3).size() == 1);
        assertThat(vectors.searchMemory(42L, "MATCH query", 3)).containsExactly("MATCH original model");
        assertThat(otherVectors.searchMemory(42L, "MATCH query", 3)).containsExactly("MATCH second model");
    }

    @Test
    void rejectsMalformedVectorsBeforeAnyJsonDocumentIsWritten() {
        managed.getStore();
        for (float[] invalid : List.of(new float[]{1, 0}, new float[]{0, 0, 0},
                new float[]{Float.NaN, 0, 1}, new float[]{Float.POSITIVE_INFINITY, 0, 1})) {
            embedding.forced = invalid;
            assertThatThrownBy(() -> vectors.saveMemory(42L, "MATCH bad vector"))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(keys(properties.effectivePrefix(ai))).isEmpty();
        }
        embedding.forced = new float[]{Float.MAX_VALUE, Float.MAX_VALUE, 0};
        vectors.saveMemory(42L, "MATCH large finite vector");
        Map<String, Object> stored = new MemoryMigrationRepository(redis)
                .readDocument(keys(properties.effectivePrefix(ai)).get(0));
        assertThat((List<?>) stored.get("embedding")).hasSize(3);
        assertThat(((List<?>) stored.get("embedding")).stream().map(value -> ((Number) value).doubleValue())
                .allMatch(value -> Double.isFinite(value) && value >= 0)).isTrue();
    }

    @Test
    void rejectsAnExistingIndexWhoseRealSchemaUsesTheWrongDimension() {
        EmbeddingModel fourDimensions = new DeterministicEmbeddings() {
            @Override public int dimensions() { return 4; }
        };
        var wrong = RedisVectorStore.builder(redis, fourDimensions)
                .indexName(properties.effectiveIndexName(ai)).prefix(properties.effectivePrefix(ai))
                .metadataFields(RedisVectorStore.MetadataField.tag("userId"),
                        RedisVectorStore.MetadataField.tag("schemaVersion"),
                        RedisVectorStore.MetadataField.tag("embeddingModel"),
                        RedisVectorStore.MetadataField.tag("embeddingModelTag"),
                        RedisVectorStore.MetadataField.numeric("embeddingDimension"),
                        RedisVectorStore.MetadataField.numeric("createdAt"))
                .initializeSchema(true).build();
        wrong.afterPropertiesSet();
        assertThatThrownBy(managed::getStore).hasMessageContaining("unavailable")
                .hasRootCauseMessage("Memory index dimension does not match configuration");
        assertThat(keys(properties.effectivePrefix(ai))).isEmpty();
    }

    @Test
    void migratesLegacyTextResumablyAndRollsBackNewDocumentsWithoutDeletingSources() throws Exception {
        properties.setWritesEnabled(false);
        String legacyKey = MemoryMigrationService.LEGACY_PREFIX + "424242:" + UUID.randomUUID();
        legacyCleanupPrefixes.add(legacyKey);
        legacyCleanupPrefixes.add(MemoryMigrationService.LEGACY_PREFIX + "424242:rollback:");
        byte[] originalVector = ByteBuffer.allocate(3 * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putFloat(0).putFloat(1).putFloat(0).array();
        redis.hset(legacyKey.getBytes(StandardCharsets.UTF_8), Map.of(
                "userId".getBytes(StandardCharsets.UTF_8), "424242".getBytes(StandardCharsets.UTF_8),
                "content".getBytes(StandardCharsets.UTF_8), "MATCH legacy text".getBytes(StandardCharsets.UTF_8),
                "vector".getBytes(StandardCharsets.UTF_8), originalVector));
        var environment = new MockEnvironment().withProperty("suts.ai.memory.writes-enabled", "false")
                .withProperty("suts.ai.memory.vector.writes-enabled", "false");
        var migration = new MemoryMigrationService(new MemoryMigrationRepository(redis), vectors,
                properties, ai, environment);
        var options = new MemoryMigrationProperties();
        options.setEnabled(true);
        options.setWritesFrozen(true);
        options.setWritersDrained(true);
        options.setCheckpointName("vector-it-" + UUID.randomUUID());
        int callsBefore = embedding.calls;
        var dryRun = migration.migrate(options);
        assertThat(dryRun.completed()).isTrue();
        assertThat(dryRun.planned()).isEqualTo(1);
        assertThat(dryRun.written()).isZero();
        assertThat(embedding.calls).isEqualTo(callsBefore);

        options.setDryRun(false);
        var forward = migration.migrate(options);
        assertThat(forward.completed()).isTrue();
        assertThat(forward.written()).isEqualTo(1);
        await(() -> vectors.searchMemory(424242L, "MATCH query", 3).size() == 1);
        assertThat(vectors.searchMemory(424242L, "MATCH query", 3)).containsExactly("MATCH legacy text");
        int callsAfter = embedding.calls;
        assertThat(migration.migrate(options).written()).isEqualTo(1);
        assertThat(embedding.calls).isEqualTo(callsAfter);
        options.setCheckpointName("vector-it-repeat-" + UUID.randomUUID());
        assertThat(migration.migrate(options).skipped()).isEqualTo(1);
        assertThat(embedding.calls).isEqualTo(callsAfter);

        properties.setWritesEnabled(true);
        vectors.saveMemory(424242L, "NEAR new cutover memory");
        properties.setWritesEnabled(false);
        options.setDirection(MemoryMigrationProperties.Direction.ROLLBACK);
        options.setRollbackEmbeddingHost(ai.getEmbeddingHost());
        options.setRollbackEmbeddingModel(ai.getEmbeddingModelName());
        options.setRollbackEmbeddingDimension(3);
        var rollback = migration.migrate(options);
        assertThat(rollback.completed()).isTrue();
        assertThat(rollback.written()).isEqualTo(1);
        assertThat(rollback.skipped()).isEqualTo(1);
        assertThat(redis.hget(legacyKey, "content")).isEqualTo("MATCH legacy text");
        assertThat(redis.hget(legacyKey.getBytes(StandardCharsets.UTF_8), "vector".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(originalVector);
        List<String> exported = keys(MemoryMigrationService.LEGACY_PREFIX + "424242:rollback:");
        assertThat(exported).hasSize(1);
        assertThat(redis.hget(exported.get(0), "content")).isEqualTo("NEAR new cutover memory");
        assertThat(keys(properties.effectivePrefix(ai))).hasSize(2);
        var repository = new MemoryMigrationRepository(redis);
        assertThat(repository.writeLegacyIfAbsent(legacyKey, "424242", "MATCH legacy text", originalVector)).isFalse();
        assertThatThrownBy(() -> repository.writeLegacyIfAbsent(legacyKey, "424242", "conflicting text", originalVector))
                .hasMessageContaining("conflicts");
        assertThat(redis.hget(legacyKey, "content")).isEqualTo("MATCH legacy text");
        // Only this test writes legacy fixtures, keeping another local run independent.
        redis.del(legacyKey);
        redis.del(exported.get(0));
    }

    @Test
    void fencesCheckpointWritesAndLockReleaseAgainstAnExpiredOrForeignWorker() {
        var repository = new MemoryMigrationRepository(redis);
        String namespace = "test:agent:migration:{" + UUID.randomUUID() + "}:";
        String lockKey = namespace + "lock";
        String checkpointKey = namespace + "checkpoint";
        assertThat(repository.acquireLock(lockKey, "current-worker")).isTrue();
        try {
            assertThat(repository.saveCheckpoint(checkpointKey, lockKey, "current-worker",
                    Map.of("status", "RUNNING", "cursor", "7"))).isTrue();
            assertThat(repository.saveCheckpoint(checkpointKey, lockKey, "stale-worker",
                    Map.of("status", "FAILED", "cursor", "0"))).isFalse();
            assertThat(repository.readCheckpoint(checkpointKey))
                    .containsEntry("status", "RUNNING").containsEntry("cursor", "7");
            assertThatThrownBy(() -> repository.renewLock(lockKey, "stale-worker"))
                    .hasMessageContaining("lock lost");
            repository.releaseLock(lockKey, "stale-worker");
            assertThat(redis.get(lockKey)).isEqualTo("current-worker");
        } finally {
            repository.releaseLock(lockKey, "current-worker");
            redis.del(checkpointKey);
        }
        assertThat(redis.exists(lockKey)).isFalse();
    }

    private RedisVectorStorageServiceImpl service(AiConfig config, MemoryVectorProperties props,
                                                   ManagedMemoryVectorStore store) {
        return new RedisVectorStorageServiceImpl(store, props, config, redis);
    }

    private Map<String, Object> metadata(long owner) {
        Map<String, Object> result = new HashMap<>();
        result.put("userId", Long.toString(owner));
        result.put("schemaVersion", "2");
        result.put("embeddingModel", ai.getEmbeddingModelName());
        result.put("embeddingModelTag", properties.embeddingModelTag(ai));
        result.put("embeddingDimension", ai.getVector().getDimension());
        result.put("createdAt", System.currentTimeMillis());
        return result;
    }

    private List<String> keys(String prefix) {
        List<String> result = new ArrayList<>();
        String cursor = "0";
        do {
            var page = redis.scan(cursor, new ScanParams().match(prefix + "*").count(100));
            result.addAll(page.getResult());
            cursor = page.getCursor();
        } while (!cursor.equals("0"));
        return result;
    }

    private static void await(Supplier<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.get()) return;
            Thread.sleep(25);
        }
        assertThat(condition.get()).as("Redis JSON index caught up within 10 seconds").isTrue();
    }

    private static class DeterministicEmbeddings implements EmbeddingModel {
        private int calls;
        private float[] forced;

        @Override
        public EmbeddingResponse call(EmbeddingRequest request) {
            calls++;
            List<Embedding> results = new ArrayList<>();
            for (int i = 0; i < request.getInstructions().size(); i++) {
                String text = request.getInstructions().get(i);
                float[] value = forced != null ? forced : text.startsWith("NEAR") ? new float[]{0.99f, 0.1f, 0}
                        : text.startsWith("ORTHOGONAL") ? new float[]{0, 1, 0}
                        : text.startsWith("OPPOSITE") ? new float[]{-1, 0, 0} : new float[]{1, 0, 0};
                results.add(new Embedding(value.clone(), i));
            }
            return new EmbeddingResponse(results);
        }

        @Override public float[] embed(Document document) { return embed(document.getText()); }
        @Override public int dimensions() { return 3; }
    }
}
