package org.hgc.suts.gateway.memory.migration;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.service.impl.RedisVectorStorageServiceImpl;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Offline, resumable text migration. Existing legacy and v2 records are never deleted. */
@Component
public class MemoryMigrationService {
    public static final String LEGACY_PREFIX = "suts:vector:data:memory:";
    private static final String CHECKPOINT_PREFIX = "suts:agent:memory:migration:v2:";
    private final MemoryMigrationRepository repository;
    private final RedisVectorStorageServiceImpl vectors;
    private final MemoryVectorProperties vectorProperties;
    private final AiConfig ai;
    private final Environment environment;

    public MemoryMigrationService(MemoryMigrationRepository repository, RedisVectorStorageServiceImpl vectors,
            MemoryVectorProperties vectorProperties, AiConfig ai, Environment environment) {
        this.repository = repository;
        this.vectors = vectors;
        this.vectorProperties = vectorProperties;
        this.ai = ai;
        this.environment = environment;
    }

    public record Report(long scannedAttempts, long written, long skipped, long planned,
                         long embeddingTextCharacters, boolean completed, boolean dryRun) { }

    public Report migrate(MemoryMigrationProperties options) {
        validateOptions(options);
        String fingerprint = vectorProperties.fingerprint(ai);
        String direction = options.getDirection().name().toLowerCase(java.util.Locale.ROOT);
        // A custom Redis prefix/index must never inherit another destination's completed checkpoint.
        String destination = digest(vectorProperties.effectivePrefix(ai) + "\n" + vectorProperties.effectiveIndexName(ai));
        String namespace = CHECKPOINT_PREFIX + "{" + fingerprint + "}:";
        String checkpointKey = namespace + destination.substring(0, 16)
                + ":" + direction + ":" + options.getCheckpointName();
        String lockKey = namespace + "lock";
        String token = UUID.randomUUID().toString();
        if (!options.isDryRun() && !repository.acquireLock(lockKey, token)) {
            throw new IllegalStateException("Another memory migration is running for this namespace");
        }
        try {
            return execute(options, checkpointKey, lockKey, token);
        } finally {
            if (!options.isDryRun()) repository.releaseLock(lockKey, token);
        }
    }

    private Report execute(MemoryMigrationProperties options, String checkpointKey, String lockKey, String token) {
        Progress progress = options.isDryRun() ? new Progress() : Progress.restore(repository.readCheckpoint(checkpointKey));
        if (progress.completed) return progress.report(options.isDryRun());
        String prefix = options.getDirection() == MemoryMigrationProperties.Direction.FORWARD
                ? LEGACY_PREFIX : vectorProperties.effectivePrefix(ai);
        String failedId = "scan";
        try {
            do {
                String pageCursor = progress.cursor;
                var page = repository.scan(pageCursor, prefix, options.getBatchSize());
                if (page == null || page.nextCursor() == null || !page.nextCursor().matches("[0-9]+")
                        || page.keys() == null) {
                    throw new IllegalStateException("Invalid Redis scan response");
                }
                for (String key : page.keys()) {
                    failedId = deterministicDocumentId(key);
                    progress.scannedAttempts++;
                    if (!options.isDryRun()) repository.renewLock(lockKey, token);
                    if (options.getDirection() == MemoryMigrationProperties.Direction.FORWARD) {
                        importLegacy(key, options, progress);
                    } else {
                        exportDocument(key, options, progress);
                    }
                    if (!options.isDryRun()) {
                        repository.renewLock(lockKey, token);
                        saveCheckpoint(checkpointKey, lockKey, token, progress.fields("RUNNING", "", ""));
                    }
                }
                progress.cursor = page.nextCursor();
                progress.completed = "0".equals(progress.cursor);
                if (!options.isDryRun()) {
                    repository.renewLock(lockKey, token);
                    saveCheckpoint(checkpointKey, lockKey, token,
                            progress.fields(progress.completed ? "COMPLETED" : "RUNNING", "", ""));
                }
            } while (!progress.completed);
            return progress.report(options.isDryRun());
        } catch (Exception failure) {
            if (!options.isDryRun()) {
                try {
                    // A failed lease renewal must not let this worker alter a successor's checkpoint.
                    repository.saveCheckpoint(checkpointKey, lockKey, token,
                            progress.fields("FAILED", failedId, failure.getClass().getSimpleName()));
                } catch (RuntimeException ignored) {
                    // Preserve the sanitized original failure if Redis itself is no longer available.
                }
            }
            // Provider exceptions can contain request/response data; retain only a type and opaque record id.
            throw new IllegalStateException("Memory migration stopped at " + failedId
                    + " (" + failure.getClass().getSimpleName() + "); fix the source/configuration and resume the same checkpoint");
        }
    }

    private void saveCheckpoint(String key, String lockKey, String token, Map<String, String> values) {
        if (!repository.saveCheckpoint(key, lockKey, token, values)) {
            throw new IllegalStateException("Migration lock lost before checkpoint update");
        }
    }

    private void importLegacy(String key, MemoryMigrationProperties options, Progress progress) {
        LegacyRecord record = readLegacy(key);
        // A source key may disappear between SCAN and HGETALL only if the declared freeze was violated.
        if (record == null) throw new IllegalStateException("Legacy record disappeared during a frozen migration");
        if (record.content.length() > vectorProperties.getMaxDocumentChars()) {
            throw new IllegalArgumentException("Legacy text exceeds destination document budget; no truncation performed");
        }
        String id = deterministicDocumentId(key);
        if (vectors.containsDocument(id)) {
            validateExistingImport(id, key, record);
            progress.skipped++;
            return;
        }
        progress.planned++;
        progress.embeddingTextCharacters += record.content.length();
        if (!options.isDryRun()) {
            // Legacy vectors have no reliable model provenance. Always re-embed original text.
            vectors.importMemory(record.userId, record.content, id, key);
            progress.written++;
        }
    }

    private void validateExistingImport(String id, String legacyKey, LegacyRecord legacy) {
        Map<String, Object> document = repository.readDocument(vectorProperties.effectivePrefix(ai) + id);
        var record = readV2(document);
        if (record.userId != legacy.userId || !record.content.equals(legacy.content)
                || !legacyKey.equals(record.legacyKey)) {
            throw new IllegalStateException("Existing migrated document conflicts with source");
        }
    }

    private void exportDocument(String key, MemoryMigrationProperties options, Progress progress) {
        String prefix = vectorProperties.effectivePrefix(ai);
        if (!key.startsWith(prefix) || key.length() == prefix.length()) {
            throw new IllegalArgumentException("Document outside active v2 namespace");
        }
        V2Record record = readV2(repository.readDocument(key));
        if (record.legacyKey != null) {
            if (parseLegacyUser(record.legacyKey) != record.userId) {
                throw new IllegalArgumentException("Migrated source owner does not match document");
            }
            if (repository.exists(record.legacyKey)) {
                LegacyRecord original = readLegacy(record.legacyKey);
                if (original == null || original.userId != record.userId || !original.content.equals(record.content)) {
                    throw new IllegalStateException("Existing legacy source conflicts with v2 text");
                }
                progress.skipped++;
                return;
            }
        }
        // Import-origin documents recover their original key; post-cutover additions use a stable export key.
        String target = record.legacyKey != null ? record.legacyKey
                : LEGACY_PREFIX + record.userId + ":rollback:" + digest(key);
        byte[] encoded = legacyVector(record.embedding);
        if (repository.exists(target)) {
            Map<byte[], byte[]> existing = repository.readLegacyHash(target);
            LegacyRecord previous = readLegacy(target, existing);
            if (previous == null || previous.userId != record.userId || !previous.content.equals(record.content)
                    || !java.util.Arrays.equals(requiredField(existing, "vector"), encoded)) {
                throw new IllegalStateException("Rollback target conflicts with existing data");
            }
            progress.skipped++;
            return;
        }
        progress.planned++;
        if (!options.isDryRun()) {
            if (repository.writeLegacyIfAbsent(target, Long.toString(record.userId), record.content, encoded)) {
                progress.written++;
            } else {
                progress.skipped++;
            }
        }
    }

    private LegacyRecord readLegacy(String key) {
        parseLegacyUser(key);
        return readLegacy(key, repository.readLegacyHash(key));
    }

    private LegacyRecord readLegacy(String key, Map<byte[], byte[]> fields) {
        long userId = parseLegacyUser(key);
        if (fields == null || fields.isEmpty()) return null;
        String owner = decode(requiredField(fields, "userId"));
        if (!Long.toString(userId).equals(owner)) throw new IllegalArgumentException("Legacy key/hash owner mismatch");
        String content = decode(requiredField(fields, "content"));
        requireText(content);
        return new LegacyRecord(userId, content);
    }

    private V2Record readV2(Map<String, Object> document) {
        if (document == null) {
            throw new IllegalArgumentException("Missing v2 document metadata");
        }
        // Spring AI RedisVectorStore stores user metadata as fields on the root JSON object.
        Map<String, Object> metadata = document;
        String owner = requireString(metadata.get("userId"), "userId");
        long userId = positiveUser(owner);
        if (!MemoryVectorProperties.SCHEMA_VERSION.equals(metadata.get("schemaVersion"))
                || !ai.getEmbeddingModelName().equals(metadata.get("embeddingModel"))
                || !vectorProperties.embeddingModelTag(ai).equals(metadata.get("embeddingModelTag"))
                || exactLong(metadata.get("embeddingDimension")) != ai.getVector().getDimension()
                || exactLong(metadata.get("createdAt")) <= 0) {
            throw new IllegalArgumentException("V2 schema/model/dimension mismatch");
        }
        String content = requireString(document.get("content"), "content");
        requireText(content);
        if (!(document.get("embedding") instanceof List<?> embedding)
                || embedding.size() != ai.getVector().getDimension()) {
            throw new IllegalArgumentException("Invalid v2 embedding dimension");
        }
        String legacyKey = metadata.get("legacyKey") == null ? null
                : requireString(metadata.get("legacyKey"), "legacyKey");
        // Validate every vector even if an existing source means the rollback can skip this document.
        legacyVector(embedding);
        return new V2Record(userId, content, embedding, legacyKey);
    }

    private void validateOptions(MemoryMigrationProperties options) {
        if (!options.isEnabled()) throw new IllegalStateException("Explicit migration.enabled=true is required");
        if (!options.isWritesFrozen() || !options.isWritersDrained()
                || environment.getProperty("suts.ai.memory.writes-enabled", Boolean.class, true)
                || environment.getProperty("suts.ai.memory.vector.writes-enabled", Boolean.class, true)) {
            throw new IllegalStateException("Freeze and drain all writers, set migration.writes-frozen=true, "
                    + "migration.writers-drained=true, "
                    + "memory.writes-enabled=false and memory.vector.writes-enabled=false before migration");
        }
        if (options.getDirection() == null || options.getBatchSize() < 1 || options.getBatchSize() > 1000
                || options.getCheckpointName() == null
                || !options.getCheckpointName().matches("[A-Za-z0-9_-]{1,64}")) {
            throw new IllegalArgumentException("Invalid migration direction, batch size or checkpoint name");
        }
        if (!ai.isEmbeddingEnabled() || ai.getEmbeddingModelName() == null || ai.getEmbeddingModelName().isBlank()) {
            throw new IllegalStateException("Configure and enable the destination Embedding model before migration");
        }
        vectorProperties.validate();
        vectorProperties.identity(ai);
        if (!vectorProperties.isEnabled()) throw new IllegalStateException("Enable vector memory before migration");
        normalizeEndpoint(ai.getEmbeddingHost());
        if (options.getDirection() == MemoryMigrationProperties.Direction.ROLLBACK
                && (!normalizeEndpoint(ai.getEmbeddingHost()).equals(normalizeEndpoint(options.getRollbackEmbeddingHost()))
                || !ai.getEmbeddingModelName().equals(options.getRollbackEmbeddingModel())
                || options.getRollbackEmbeddingDimension() == null
                || ai.getVector().getDimension() != options.getRollbackEmbeddingDimension())) {
            throw new IllegalStateException("Rollback requires explicitly matching legacy embedding host, model and dimension; "
                    + "different embedding spaces require re-embedding with the legacy model before cutback");
        }
    }

    private static String normalizeEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) return "";
        URI uri = URI.create(endpoint);
        if (!("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                || uri.getHost() == null || uri.getUserInfo() != null
                || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalArgumentException("Invalid embedding endpoint");
        }
        return (uri.getScheme() + "://" + uri.getAuthority() + (uri.getPath() == null ? "" : uri.getPath()))
                .replaceAll("/+$", "");
    }

    private static long parseLegacyUser(String key) {
        if (key == null || !key.startsWith(LEGACY_PREFIX)) throw new IllegalArgumentException("Invalid legacy prefix");
        String remainder = key.substring(LEGACY_PREFIX.length());
        int separator = remainder.indexOf(':');
        if (separator < 1 || separator == remainder.length() - 1 || remainder.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("Invalid legacy memory key");
        }
        return positiveUser(remainder.substring(0, separator));
    }

    private static long positiveUser(String owner) {
        if (!owner.matches("[1-9][0-9]*")) throw new IllegalArgumentException("Invalid memory owner");
        return Long.parseLong(owner);
    }

    private static byte[] requiredField(Map<byte[], byte[]> fields, String name) {
        byte[] expected = name.getBytes(StandardCharsets.UTF_8);
        return fields.entrySet().stream().filter(entry -> java.util.Arrays.equals(entry.getKey(), expected))
                .map(Map.Entry::getValue).findFirst().orElseThrow(() -> new IllegalArgumentException("Missing legacy " + name));
    }

    private static String decode(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException("Legacy text is not valid UTF-8");
        }
    }

    private static String requireString(Object value, String field) {
        if (!(value instanceof String string)) throw new IllegalArgumentException("Invalid " + field);
        return string;
    }

    private static void requireText(String text) {
        if (text.isBlank() || text.indexOf('\0') >= 0) throw new IllegalArgumentException("Empty or binary memory text");
    }

    private static long exactLong(Object value) {
        if (!(value instanceof Number number) || !Double.isFinite(number.doubleValue())
                || number.doubleValue() != number.longValue()) {
            throw new IllegalArgumentException("Invalid numeric metadata");
        }
        return number.longValue();
    }

    private static byte[] legacyVector(List<?> values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.size() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        double norm = 0;
        for (Object value : values) {
            if (!(value instanceof Number number) || !Float.isFinite(number.floatValue())) {
                throw new IllegalArgumentException("Non-finite v2 embedding");
            }
            float component = number.floatValue();
            norm += (double) component * component;
            buffer.putFloat(component);
        }
        if (norm == 0) throw new IllegalArgumentException("Zero v2 embedding");
        return buffer.array();
    }

    public static String deterministicDocumentId(String key) {
        return "legacy-" + digest(key);
    }

    private static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private record LegacyRecord(long userId, String content) { }
    private record V2Record(long userId, String content, List<?> embedding, String legacyKey) { }

    private static final class Progress {
        private String cursor = "0";
        private long scannedAttempts;
        private long written;
        private long skipped;
        private long planned;
        private long embeddingTextCharacters;
        private boolean completed;

        static Progress restore(Map<String, String> values) {
            Progress result = new Progress();
            if (values == null || values.isEmpty()) return result;
            result.cursor = values.getOrDefault("cursor", "0");
            if (!result.cursor.matches("[0-9]+")) throw new IllegalStateException("Invalid migration checkpoint cursor");
            String status = values.get("status");
            if (status == null || !List.of("RUNNING", "FAILED", "COMPLETED").contains(status)) {
                throw new IllegalStateException("Invalid migration checkpoint status");
            }
            result.scannedAttempts = nonNegative(values, "scannedAttempts");
            result.written = nonNegative(values, "written");
            result.skipped = nonNegative(values, "skipped");
            result.planned = nonNegative(values, "planned");
            result.embeddingTextCharacters = nonNegative(values, "embeddingTextCharacters");
            result.completed = "COMPLETED".equals(values.get("status"));
            if (result.completed && !"0".equals(result.cursor)) {
                throw new IllegalStateException("Completed migration checkpoint has a nonzero cursor");
            }
            return result;
        }

        private static long nonNegative(Map<String, String> values, String name) {
            String value = values.getOrDefault(name, "0");
            if (!value.matches("[0-9]+")) throw new IllegalStateException("Invalid migration checkpoint counter");
            return Long.parseLong(value);
        }

        Map<String, String> fields(String status, String failureId, String failureType) {
            Map<String, String> values = new LinkedHashMap<>();
            values.put("cursor", cursor);
            values.put("status", status);
            values.put("scannedAttempts", Long.toString(scannedAttempts));
            values.put("written", Long.toString(written));
            values.put("skipped", Long.toString(skipped));
            values.put("planned", Long.toString(planned));
            values.put("embeddingTextCharacters", Long.toString(embeddingTextCharacters));
            values.put("lastFailureId", failureId);
            values.put("lastFailureType", failureType);
            return values;
        }

        Report report(boolean dryRun) {
            return new Report(scannedAttempts, written, skipped, planned, embeddingTextCharacters, completed, dryRun);
        }
    }
}
