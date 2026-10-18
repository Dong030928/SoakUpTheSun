package org.hgc.suts.gateway.memory.migration;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.service.impl.RedisVectorStorageServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.env.MockEnvironment;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MemoryMigrationServiceTest {
    private final MemoryMigrationRepository repository = mock(MemoryMigrationRepository.class);
    private final RedisVectorStorageServiceImpl vectors = mock(RedisVectorStorageServiceImpl.class);
    private final MemoryVectorProperties vectorProperties = new MemoryVectorProperties();
    private final AiConfig ai = new AiConfig();
    private final MockEnvironment environment = new MockEnvironment()
            .withProperty("suts.ai.memory.writes-enabled", "false")
            .withProperty("suts.ai.memory.vector.writes-enabled", "false");
    private final MemoryMigrationProperties options = new MemoryMigrationProperties();
    private MemoryMigrationService migration;

    @BeforeEach
    void setup() {
        ai.setEmbeddingHost("https://embedding.example/v1");
        ai.setEmbeddingModelName("test-embedding");
        ai.getVector().setDimension(3);
        options.setEnabled(true);
        options.setWritesFrozen(true);
        options.setWritersDrained(true);
        when(repository.acquireLock(anyString(), anyString())).thenReturn(true);
        when(repository.readCheckpoint(anyString())).thenReturn(Map.of());
        when(repository.saveCheckpoint(anyString(), anyString(), anyString(), anyMap())).thenReturn(true);
        migration = new MemoryMigrationService(repository, vectors, vectorProperties, ai, environment);
    }

    @Test
    void defaultsAreDisabledAndDryRun() {
        var defaults = new MemoryMigrationProperties();
        assertThat(defaults.isEnabled()).isFalse();
        assertThat(defaults.isDryRun()).isTrue();
        assertThatThrownBy(() -> migration.migrate(defaults)).hasMessageContaining("enabled=true");
        verifyNoInteractions(repository, vectors);
    }

    @ParameterizedTest
    @ValueSource(strings = {"declaration", "drain", "short-writes", "vector-writes"})
    void requiresAllWritersFrozenBeforeAnyRedisAccess(String missing) {
        if (missing.equals("declaration")) options.setWritesFrozen(false);
        if (missing.equals("drain")) options.setWritersDrained(false);
        if (missing.equals("short-writes")) environment.setProperty("suts.ai.memory.writes-enabled", "true");
        if (missing.equals("vector-writes")) environment.setProperty("suts.ai.memory.vector.writes-enabled", "true");
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("Freeze and drain");
        verifyNoInteractions(repository, vectors);
    }

    @ParameterizedTest
    @ValueSource(strings = {"null-direction", "zero-batch", "large-batch", "null-checkpoint", "unsafe-checkpoint",
            "disabled-embedding", "disabled-vector", "invalid-endpoint", "invalid-dimension", "legacy-prefix"})
    void rejectsInvalidMigrationConfigurationBeforeRedisAccess(String invalid) {
        switch (invalid) {
            case "null-direction" -> options.setDirection(null);
            case "zero-batch" -> options.setBatchSize(0);
            case "large-batch" -> options.setBatchSize(1001);
            case "null-checkpoint" -> options.setCheckpointName(null);
            case "unsafe-checkpoint" -> options.setCheckpointName("../all");
            case "disabled-embedding" -> ai.setEmbeddingEnabled(false);
            case "disabled-vector" -> vectorProperties.setEnabled(false);
            case "invalid-endpoint" -> ai.setEmbeddingHost("ftp://embedding.example/v1");
            case "invalid-dimension" -> ai.getVector().setDimension(0);
            case "legacy-prefix" -> vectorProperties.setPrefix(MemoryMigrationService.LEGACY_PREFIX);
            default -> throw new AssertionError();
        }
        assertThatThrownBy(() -> migration.migrate(options)).isInstanceOf(RuntimeException.class);
        verifyNoInteractions(repository, vectors);
    }

    @Test
    void dryRunValidatesCountsAndCharacterCostWithoutWritesOrEmbedding() {
        String key = legacyKey(42, "abc");
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100))
                .thenReturn(page("0", key));
        when(repository.readLegacyHash(key)).thenReturn(legacyHash("42", "中文记忆"));

        var report = migration.migrate(options);

        assertThat(report.completed()).isTrue();
        assertThat(report.dryRun()).isTrue();
        assertThat(report.scannedAttempts()).isEqualTo(1);
        assertThat(report.planned()).isEqualTo(1);
        assertThat(report.embeddingTextCharacters()).isEqualTo(4);
        assertThat(report.written()).isZero();
        verify(vectors, never()).importMemory(anyLong(), anyString(), anyString(), anyString());
        verify(repository, never()).saveCheckpoint(anyString(), anyString(), anyString(), anyMap());
        verify(repository, never()).acquireLock(anyString(), anyString());
    }

    @Test
    void forwardUsesStableIdsAndOriginalTextForReembeddingAcrossUsers() {
        options.setDryRun(false);
        String first = legacyKey(42, "abc");
        String second = legacyKey(7, "abc");
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100))
                .thenReturn(page("0", first, second));
        when(repository.readLegacyHash(first)).thenReturn(legacyHash("42", "完整原文"));
        when(repository.readLegacyHash(second)).thenReturn(legacyHash("7", "其他用户"));

        var report = migration.migrate(options);

        assertThat(report.written()).isEqualTo(2);
        verify(vectors).importMemory(42L, "完整原文", MemoryMigrationService.deterministicDocumentId(first), first);
        verify(vectors).importMemory(7L, "其他用户", MemoryMigrationService.deterministicDocumentId(second), second);
        assertThat(MemoryMigrationService.deterministicDocumentId(first))
                .isNotEqualTo(MemoryMigrationService.deterministicDocumentId(second));
        verify(repository).releaseLock(anyString(), anyString());
    }

    @Test
    void repeatedScanResultsSkipOnlyAfterCheckingExistingTextAndOwner() {
        options.setDryRun(false);
        String key = legacyKey(42, "abc");
        String id = MemoryMigrationService.deterministicDocumentId(key);
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100))
                .thenReturn(page("0", key, key));
        when(repository.readLegacyHash(key)).thenReturn(legacyHash("42", "相同文本"));
        when(vectors.containsDocument(id)).thenReturn(false, true);
        when(repository.readDocument(v2Key(id))).thenReturn(document(42, "相同文本", key));

        var report = migration.migrate(options);

        assertThat(report.written()).isEqualTo(1);
        assertThat(report.skipped()).isEqualTo(1);
        verify(vectors, times(1)).importMemory(42L, "相同文本", id, key);
    }

    @Test
    void conflictingDeterministicTargetStopsWithoutOverwrite() {
        String key = legacyKey(42, "abc");
        String id = MemoryMigrationService.deterministicDocumentId(key);
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0", key));
        when(repository.readLegacyHash(key)).thenReturn(legacyHash("42", "原文"));
        when(vectors.containsDocument(id)).thenReturn(true);
        when(repository.readDocument(v2Key(id))).thenReturn(document(7, "其他用户文本", key));

        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining(id).hasMessageNotContaining("原文");
        verify(vectors, never()).importMemory(anyLong(), anyString(), anyString(), anyString());
    }

    @Test
    void failurePersistsCursorAndResumeSkipsSuccessfulPartialPageWithoutDuplicateWrite() {
        options.setDryRun(false);
        String first = legacyKey(42, "first");
        String second = legacyKey(42, "second");
        String third = legacyKey(42, "third");
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("7", first));
        when(repository.scan("7", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0", second, third));
        for (String key : List.of(first, second, third)) {
            when(repository.readLegacyHash(key)).thenReturn(legacyHash("42", key.substring(key.lastIndexOf(':') + 1)));
        }
        Map<String, Map<String, Object>> imported = new HashMap<>();
        Map<String, String> checkpoint = new HashMap<>();
        when(repository.readCheckpoint(anyString())).thenAnswer(invocation -> new HashMap<>(checkpoint));
        when(repository.saveCheckpoint(anyString(), anyString(), anyString(), anyMap())).thenAnswer(invocation -> {
            checkpoint.clear();
            checkpoint.putAll(invocation.getArgument(3));
            return true;
        });
        when(vectors.containsDocument(anyString())).thenAnswer(invocation -> imported.containsKey(invocation.getArgument(0)));
        when(repository.readDocument(anyString())).thenAnswer(invocation ->
                imported.get(((String) invocation.getArgument(0)).substring(vectorProperties.effectivePrefix(ai).length())));
        AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            String key = invocation.getArgument(3);
            if (key.equals(third) && fail.get()) throw new IllegalStateException("Sensitive provider payload");
            imported.put(invocation.getArgument(2), document(42, invocation.getArgument(1), key));
            return null;
        }).when(vectors).importMemory(anyLong(), anyString(), anyString(), anyString());

        assertThatThrownBy(() -> migration.migrate(options)).hasMessageNotContaining("Sensitive provider payload");
        assertThat(checkpoint).containsEntry("cursor", "7").containsEntry("status", "FAILED")
                .containsEntry("written", "2");
        fail.set(false);
        var report = migration.migrate(options);

        assertThat(report.completed()).isTrue();
        assertThat(report.written()).isEqualTo(3);
        assertThat(checkpoint).containsEntry("cursor", "0").containsEntry("status", "COMPLETED");
        verify(vectors, times(1)).importMemory(42L, "second", MemoryMigrationService.deterministicDocumentId(second), second);
        verify(vectors, times(2)).importMemory(42L, "third", MemoryMigrationService.deterministicDocumentId(third), third);
    }

    @Test
    void emptyScanPagesAreNotTreatedAsCompletionWhileCursorRemainsNonzero() {
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("12"));
        when(repository.scan("12", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0"));
        assertThat(migration.migrate(options).completed()).isTrue();
        verify(repository).scan("12", MemoryMigrationService.LEGACY_PREFIX, 100);
        verifyNoInteractions(vectors);
    }

    @Test
    void failedScanDoesNotAdvanceItsCheckpointOrExposeRedisErrorPayload() {
        options.setDryRun(false);
        when(repository.readCheckpoint(anyString())).thenReturn(Map.of("status", "RUNNING", "cursor", "17"));
        when(repository.scan("17", MemoryMigrationService.LEGACY_PREFIX, 100))
                .thenThrow(new IllegalStateException("Sensitive Redis endpoint details"));
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped at scan")
                .hasMessageNotContaining("Sensitive Redis");
        verify(repository).saveCheckpoint(anyString(), anyString(), anyString(), argThat(checkpoint ->
                "17".equals(checkpoint.get("cursor")) && "FAILED".equals(checkpoint.get("status"))));
        verifyNoInteractions(vectors);
    }

    @Test
    void disappearingSourceStopsWithoutSkippingTheRecord() {
        options.setDryRun(false);
        String key = legacyKey(42, "gone");
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0", key));
        when(repository.readLegacyHash(key)).thenReturn(Map.of());
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped");
        verifyNoInteractions(vectors);
        verify(repository).saveCheckpoint(anyString(), anyString(), anyString(), argThat(checkpoint ->
                "0".equals(checkpoint.get("cursor")) && "FAILED".equals(checkpoint.get("status"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"cursor", "status", "negative-count", "non-numeric-count", "completed-cursor"})
    void rejectsCorruptedCheckpointWithoutScanningOrWriting(String corruption) {
        options.setDryRun(false);
        Map<String, String> checkpoint = new HashMap<>(Map.of("cursor", "0", "status", "FAILED"));
        switch (corruption) {
            case "cursor" -> checkpoint.put("cursor", "-1");
            case "status" -> checkpoint.put("status", "UNKNOWN");
            case "negative-count" -> checkpoint.put("written", "-5");
            case "non-numeric-count" -> checkpoint.put("planned", "broken");
            case "completed-cursor" -> {
                checkpoint.put("status", "COMPLETED");
                checkpoint.put("cursor", "7");
            }
            default -> throw new AssertionError();
        }
        when(repository.readCheckpoint(anyString())).thenReturn(checkpoint);
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("checkpoint");
        verify(repository, never()).scan(anyString(), anyString(), anyInt());
        verifyNoInteractions(vectors);
        verify(repository).releaseLock(anyString(), anyString());
    }

    @Test
    void customDestinationsCannotReuseAnotherNamespacesCompletedCheckpoint() {
        options.setDryRun(false);
        List<String> checkpointKeys = new ArrayList<>();
        when(repository.readCheckpoint(anyString())).thenAnswer(invocation -> {
            checkpointKeys.add(invocation.getArgument(0));
            return Map.of("status", "COMPLETED");
        });
        migration.migrate(options);
        vectorProperties.setPrefix("test:another:memory:");
        migration.migrate(options);
        vectorProperties.setIndexName("test:another:index");
        migration.migrate(options);
        assertThat(checkpointKeys).doesNotHaveDuplicates();
    }

    @Test
    void lockLossStopsBeforeEmbeddingAndDoesNotContinueThePage() {
        options.setDryRun(false);
        String key = legacyKey(42, "abc");
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0", key));
        doThrow(new IllegalStateException("Migration lock lost"))
                .when(repository).renewLock(anyString(), anyString());
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped");
        verifyNoInteractions(vectors);
        verify(repository, never()).readLegacyHash(anyString());
    }

    @Test
    void failedFencedCheckpointStopsAndUsesTheFenceForFailureReportingToo() {
        options.setDryRun(false);
        String key = legacyKey(42, "abc");
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0", key));
        when(repository.readLegacyHash(key)).thenReturn(legacyHash("42", "完整原文"));
        when(repository.saveCheckpoint(anyString(), anyString(), anyString(), anyMap())).thenReturn(false);
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped");
        // Both RUNNING and FAILED attempts go through the token-checked mutation, never an unfenced HSET.
        verify(repository).saveCheckpoint(anyString(), anyString(), anyString(),
                argThat(checkpoint -> "RUNNING".equals(checkpoint.get("status"))));
        verify(repository).saveCheckpoint(anyString(), anyString(), anyString(),
                argThat(checkpoint -> "FAILED".equals(checkpoint.get("status"))));
        verify(vectors, times(1)).importMemory(anyLong(), anyString(), anyString(), anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"foreign:42:record", "suts:vector:data:memory:0:record",
            "suts:vector:data:memory:042:record", "suts:vector:data:memory:42:",
            "suts:vector:data:memory:9223372036854775808:record"})
    void rejectsInvalidOrForeignSourceKeyBeforeHashRead(String key) {
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0", key));
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped");
        verify(repository, never()).readLegacyHash(anyString());
        verifyNoInteractions(vectors);
    }

    @ParameterizedTest
    @ValueSource(strings = {"ownership", "utf8", "blank", "budget"})
    void refusesInvalidLegacySourceInsteadOfTruncatingOrMisassigning(String corruption) {
        String key = legacyKey(42, "abc");
        when(repository.scan("0", MemoryMigrationService.LEGACY_PREFIX, 100)).thenReturn(page("0", key));
        var fields = legacyHash(corruption.equals("ownership") ? "7" : "42",
                corruption.equals("blank") ? " " : "文本");
        if (corruption.equals("utf8")) putField(fields, "content", new byte[]{(byte) 0xC3, 0x28});
        if (corruption.equals("budget")) vectorProperties.setMaxDocumentChars(1);
        when(repository.readLegacyHash(key)).thenReturn(fields);
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped");
        verify(vectors, never()).importMemory(anyLong(), anyString(), anyString(), anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "host", "model", "dimension"})
    void rollbackRequiresExplicitMatchingModelProvenance(String mismatch) {
        rollbackOptions();
        if (mismatch.equals("missing")) options.setRollbackEmbeddingHost(null);
        if (mismatch.equals("host")) options.setRollbackEmbeddingHost("https://other.example/v1");
        if (mismatch.equals("model")) options.setRollbackEmbeddingModel("other-model");
        if (mismatch.equals("dimension")) options.setRollbackEmbeddingDimension(1024);
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("explicitly matching");
        verifyNoInteractions(repository, vectors);
    }

    @Test
    void rollbackSupplementsPostCutoverRecordsWithDeterministicLegacyKeysAndPreservesV2() {
        rollbackOptions();
        options.setDryRun(false);
        String key = v2Key("new-record");
        when(repository.scan("0", vectorProperties.effectivePrefix(ai), 100)).thenReturn(page("0", key));
        when(repository.readDocument(key)).thenReturn(document(42, "切换后的新记忆", null));
        List<String> targetKeys = new ArrayList<>();
        doAnswer(invocation -> {
            targetKeys.add(invocation.getArgument(0));
            assertThat((String) invocation.getArgument(1)).isEqualTo("42");
            assertThat((String) invocation.getArgument(2)).isEqualTo("切换后的新记忆");
            var buffer = ByteBuffer.wrap((byte[]) invocation.getArgument(3)).order(ByteOrder.LITTLE_ENDIAN);
            assertThat(buffer.getFloat()).isEqualTo(1.0f);
            assertThat(buffer.getFloat()).isEqualTo(0.25f);
            assertThat(buffer.getFloat()).isEqualTo(-0.5f);
            return targetKeys.size() == 1;
        }).when(repository).writeLegacyIfAbsent(anyString(), anyString(), anyString(), any());

        assertThat(migration.migrate(options).written()).isEqualTo(1);
        options.setCheckpointName("repeat");
        assertThat(migration.migrate(options).skipped()).isEqualTo(1);
        assertThat(targetKeys).hasSize(2);
        assertThat(targetKeys.get(0)).startsWith(legacyKey(42, "rollback:")).isEqualTo(targetKeys.get(1));
        verifyNoInteractions(vectors);
    }

    @Test
    void rollbackSkipsUnchangedImportOriginsInsteadOfDuplicatingThem() {
        rollbackOptions();
        String source = legacyKey(42, "original");
        String key = v2Key("imported-record");
        when(repository.scan("0", vectorProperties.effectivePrefix(ai), 100)).thenReturn(page("0", key));
        when(repository.readDocument(key)).thenReturn(document(42, "原有记忆", source));
        when(repository.exists(source)).thenReturn(true);
        when(repository.readLegacyHash(source)).thenReturn(legacyHash("42", "原有记忆"));

        var report = migration.migrate(options);

        assertThat(report.skipped()).isEqualTo(1);
        assertThat(report.planned()).isZero();
        verify(repository, never()).writeLegacyIfAbsent(anyString(), anyString(), anyString(), any());
    }

    @Test
    void rollbackRefusesChangedOriginalHashEvenWithTheSameOwner() {
        rollbackOptions();
        String source = legacyKey(42, "original");
        String key = v2Key("imported-record");
        when(repository.scan("0", vectorProperties.effectivePrefix(ai), 100)).thenReturn(page("0", key));
        when(repository.readDocument(key)).thenReturn(document(42, "原有记忆", source));
        when(repository.exists(source)).thenReturn(true);
        when(repository.readLegacyHash(source)).thenReturn(legacyHash("42", "已经更改"));
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped")
                .hasMessageNotContaining("已经更改");
        verify(repository, never()).writeLegacyIfAbsent(anyString(), anyString(), anyString(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"unchanged", "owner", "text", "vector"})
    void rollbackDryRunChecksExistingDeterministicExportWithoutOverwriting(String existingState) {
        rollbackOptions();
        String key = v2Key("post-cutover");
        when(repository.scan("0", vectorProperties.effectivePrefix(ai), 100)).thenReturn(page("0", key));
        when(repository.readDocument(key)).thenReturn(document(42, "新增记忆", null));
        when(repository.exists(anyString())).thenReturn(true);
        var previous = legacyHash(existingState.equals("owner") ? "7" : "42",
                existingState.equals("text") ? "冲突文本" : "新增记忆");
        byte[] vector = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
                .putFloat(1).putFloat(.25f).putFloat(-.5f).array();
        putField(previous, "vector", existingState.equals("vector") ? new byte[]{0, 0, 0} : vector);
        when(repository.readLegacyHash(anyString())).thenReturn(previous);
        if (existingState.equals("unchanged")) {
            assertThat(migration.migrate(options).skipped()).isEqualTo(1);
        } else {
            assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped");
        }
        verify(repository, never()).writeLegacyIfAbsent(anyString(), anyString(), anyString(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"owner", "schema", "model", "model-tag", "dimension", "zero", "nan", "legacy-owner"})
    void rollbackRejectsInvalidV2MetadataOrVectors(String corruption) {
        rollbackOptions();
        String key = v2Key("record");
        var record = document(42, "记忆", null);
        switch (corruption) {
            case "owner" -> record.put("userId", "-1");
            case "schema" -> record.put("schemaVersion", "1");
            case "model" -> record.put("embeddingModel", "wrong-model");
            case "model-tag" -> record.put("embeddingModelTag", "foreign-model-tag");
            case "dimension" -> record.put("embeddingDimension", 1024);
            case "zero" -> record.put("embedding", List.of(0, 0, 0));
            case "nan" -> record.put("embedding", List.of(1, Double.NaN, 1));
            case "legacy-owner" -> record.put("legacyKey", legacyKey(7, "wrong-owner"));
            default -> throw new AssertionError();
        }
        when(repository.scan("0", vectorProperties.effectivePrefix(ai), 100)).thenReturn(page("0", key));
        when(repository.readDocument(key)).thenReturn(record);

        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("stopped");
        verify(repository, never()).writeLegacyIfAbsent(anyString(), anyString(), anyString(), any());
    }

    @Test
    void completedCheckpointIsNoOpAndConcurrentMigrationIsRejected() {
        options.setDryRun(false);
        when(repository.readCheckpoint(anyString())).thenReturn(Map.of("status", "COMPLETED", "written", "5"));
        assertThat(migration.migrate(options).written()).isEqualTo(5);
        verify(repository, never()).scan(anyString(), anyString(), anyInt());
        when(repository.acquireLock(anyString(), anyString())).thenReturn(false);
        assertThatThrownBy(() -> migration.migrate(options)).hasMessageContaining("Another memory migration");
    }

    private void rollbackOptions() {
        options.setDirection(MemoryMigrationProperties.Direction.ROLLBACK);
        options.setRollbackEmbeddingHost(ai.getEmbeddingHost() + "/");
        options.setRollbackEmbeddingModel(ai.getEmbeddingModelName());
        options.setRollbackEmbeddingDimension(ai.getVector().getDimension());
    }

    private Map<String, Object> document(long userId, String content, String legacyKey) {
        Map<String, Object> document = new HashMap<>();
        document.put("content", content);
        document.put("embedding", List.of(1.0, 0.25, -0.5));
        document.put("userId", Long.toString(userId));
        document.put("schemaVersion", "2");
        document.put("embeddingModel", ai.getEmbeddingModelName());
        document.put("embeddingModelTag", vectorProperties.embeddingModelTag(ai));
        document.put("embeddingDimension", 3);
        document.put("createdAt", 1_760_000_000_000L);
        if (legacyKey != null) document.put("legacyKey", legacyKey);
        return document;
    }

    private String v2Key(String id) { return vectorProperties.effectivePrefix(ai) + id; }

    private static MemoryMigrationRepository.Page page(String cursor, String... keys) {
        return new MemoryMigrationRepository.Page(cursor, List.of(keys));
    }

    private static String legacyKey(long owner, String id) {
        return MemoryMigrationService.LEGACY_PREFIX + owner + ":" + id;
    }

    private static Map<byte[], byte[]> legacyHash(String owner, String content) {
        var fields = new HashMap<byte[], byte[]>();
        fields.put(bytes("userId"), bytes(owner));
        fields.put(bytes("content"), bytes(content));
        // Invalid original bytes are deliberately ignored: forward migration always re-embeds text.
        fields.put(bytes("vector"), new byte[]{1, 2, 3});
        return fields;
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    private static void putField(Map<byte[], byte[]> fields, String name, byte[] value) {
        fields.keySet().removeIf(key -> java.util.Arrays.equals(key, bytes(name)));
        fields.put(bytes(name), value);
    }
}
