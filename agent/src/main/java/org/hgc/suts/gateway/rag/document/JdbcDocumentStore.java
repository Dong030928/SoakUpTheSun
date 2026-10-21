package org.hgc.suts.gateway.rag.document;

import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.rag.RagData.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.util.*;

import static org.hgc.suts.gateway.rag.document.DocumentEtl.*;

/** SQL owns content, ACL and publication; both search engines are recoverable projections. */
public final class JdbcDocumentStore {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final RagProperties config;
    public JdbcDocumentStore(JdbcTemplate jdbc, TransactionTemplate transactions, RagProperties config) {
        this.jdbc = jdbc; this.transactions = transactions; this.config = config;
    }

    /** Physical destinations are immutable for a target fingerprint, including after process restarts. */
    public void registerTarget(String targetTag, String redisPrefix, String esEndpoint, String esIndex) {
        target(targetTag); bounded(redisPrefix, 512, "Redis projection prefix"); bounded(esEndpoint, 2048, "Elasticsearch endpoint");
        RagProperties.validateEndpoint(esEndpoint);
        Assert.isTrue(esIndex != null && esIndex.matches("[a-z0-9][a-z0-9_-]{0,100}"), "Invalid Elasticsearch projection index");
        String endpoint = esEndpoint.replaceAll("/+$", "");
        var descriptor = new Target(targetTag, redisPrefix, endpoint, esIndex);
        try { jdbc.update("INSERT INTO agent_rag_target (target_tag,redis_prefix,es_endpoint,es_index) VALUES (?,?,?,?)", targetTag, redisPrefix, endpoint, esIndex); }
        catch (DuplicateKeyException duplicate) { Assert.state(descriptor.equals(getTarget(targetTag)), "Projection target is already bound to different physical destinations"); }
    }
    public Target getTarget(String targetTag) {
        target(targetTag);
        return jdbc.query("SELECT target_tag,redis_prefix,es_endpoint,es_index FROM agent_rag_target WHERE target_tag=?",
                (rs, i) -> new Target(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)), targetTag)
                .stream().findFirst().orElseThrow(() -> new NoSuchElementException("Document projection target descriptor is missing"));
    }

    public ImportResult importDocument(String logicalId, long ownerId, Visibility visibility, String title,
                                       String source, ParsedDocument parsed, String targetTag) {
        identity(logicalId, ownerId); target(targetTag);
        Assert.notNull(visibility, "Document visibility is required");
        bounded(title, 512, "Document title"); bounded(source, 2048, "Document source");
        validateParsed(parsed);
        String bodyHash = hash(JSON.writeValueAsString(List.of(visibility.name(), title, source, parsed.sourceHash(),
                parsed.pipelineTag(), parsed.parents(), parsed.children())));
        return transactions.execute(status -> {
            DocumentRow document = ensureDocument(logicalId, ownerId);
            if (!document.deleted() && document.desiredVersion() > 0) {
                var latest = version(logicalId, document.desiredVersion());
                if (latest.payloadHash().equals(bodyHash)) {
                    if (!document.desiredTarget().equals(targetTag)) return registerReindex(document, targetTag, false);
                    return result(logicalId, latest.version(), targetTag, true);
                }
            }
            long number = document.desiredVersion() + 1; long now = System.currentTimeMillis();
            jdbc.update("INSERT INTO agent_rag_version (document_id,document_version,payload_hash,source_hash,pipeline_tag,visibility,title,"
                            + "source_ref,parent_count,child_count,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    logicalId, number, bodyHash, parsed.sourceHash(), parsed.pipelineTag(), visibility.name(), title, source,
                    parsed.parents().size(), parsed.children().size(), now);
            Map<String, String> parents = new HashMap<>();
            for (Parent parent : parsed.parents()) {
                String id = parentId(logicalId, number, parent.id()); parents.put(parent.id(), id);
                jdbc.update("INSERT INTO agent_rag_parent (parent_id,document_id,document_version,ordinal_no,body,heading_path,page_start,page_end,line_start,line_end) "
                                + "VALUES (?,?,?,?,?,?,?,?,?,?)", id, logicalId, number, parent.ordinal(), parent.text(), parent.headingPath(),
                        parent.pageStart(), parent.pageEnd(), parent.lineStart(), parent.lineEnd());
            }
            for (Child child : parsed.children()) jdbc.update("INSERT INTO agent_rag_child (child_id,parent_id,document_id,document_version,ordinal_no,body,"
                            + "heading_path,page_start,page_end,line_start,line_end,content_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    childId(logicalId, number, child.id()), parents.get(child.parentId()), logicalId, number, child.ordinal(), child.text(),
                    child.headingPath(), child.pageStart(), child.pageEnd(), child.lineStart(), child.lineEnd(), hash(child.text()));
            register(logicalId, number, targetTag);
            jdbc.update("UPDATE agent_rag_document SET desired_version=?,desired_target=?,deleted=FALSE,revision=revision+1,updated_at=? WHERE document_id=?",
                    number, targetTag, now, logicalId);
            return result(logicalId, number, targetTag, false);
        });
    }

    public ImportResult reindex(long ownerId, String logicalId, String targetTag) {
        identity(logicalId, ownerId); target(targetTag);
        return transactions.execute(status -> {
            var row = owned(logicalId, ownerId);
            Assert.state(!row.deleted() && row.desiredVersion() > 0, "Cannot reindex a deleted or missing document");
            return registerReindex(row, targetTag, true);
        });
    }
    private ImportResult registerReindex(DocumentRow row, String targetTag, boolean force) {
        String id = jobId(row.id(), row.desiredVersion(), targetTag);
        var existing = job(id, false);
        if (force || !row.desiredTarget().equals(targetTag)) {
            Assert.state(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_cleanup x JOIN agent_rag_child c ON c.child_id=x.child_id "
                            + "WHERE x.target_tag=? AND c.document_id=? AND c.document_version=?", Long.class,
                    targetTag, row.id(), row.desiredVersion()) == 0,
                    "Finish pending cleanup before reusing the same immutable projection target");
        }
        if (existing.isPresent() && (force || List.of("DONE", "SUPERSEDED").contains(existing.get().state())
                && !(row.currentVersion() == row.desiredVersion() && row.currentTarget().equals(targetTag)))) {
            // Reusing a former target must rebuild documents that a previous cleanup may have removed.
            jdbc.update("UPDATE agent_rag_job SET state='WRITING',redis_done=FALSE,es_done=FALSE,worker_epoch=worker_epoch+1,lease_until=0,"
                    + "retry_at=0,last_error=NULL,updated_at=? WHERE job_id=?", System.currentTimeMillis(), id);
        } else if (existing.isEmpty()) register(row.id(), row.desiredVersion(), targetTag);
        jdbc.update("UPDATE agent_rag_document SET desired_target=?,revision=revision+1,updated_at=? WHERE document_id=?", targetTag, System.currentTimeMillis(), row.id());
        return result(row.id(), row.desiredVersion(), targetTag, existing.isPresent());
    }

    /** Visibility is revoked synchronously; physical projection removal remains durable work. */
    public void delete(long ownerId, String logicalId) {
        identity(logicalId, ownerId);
        transactions.executeWithoutResult(status -> {
            var row = owned(logicalId, ownerId); if (row.deleted()) return;
            jdbc.update("UPDATE agent_rag_document SET deleted=TRUE,current_version=0,current_target='',revision=revision+1,updated_at=? WHERE document_id=?",
                    System.currentTimeMillis(), logicalId);
            if (row.desiredVersion() == 0) return;
            String controller = jobId(logicalId, row.desiredVersion(), row.desiredTarget());
            for (Job projection : documentJobs(logicalId)) {
                enqueueCleanup(controller, projection);
                if (!projection.id().equals(controller)) jdbc.update("UPDATE agent_rag_job SET state='SUPERSEDED',worker_epoch=worker_epoch+1,lease_until=0,updated_at=? WHERE job_id=?",
                        System.currentTimeMillis(), projection.id());
            }
            jdbc.update("UPDATE agent_rag_job SET state='CLEANUP',worker_epoch=worker_epoch+1,lease_until=0,retry_at=0,updated_at=? WHERE job_id=?",
                    System.currentTimeMillis(), controller);
        });
    }

    public ImportStatus status(long ownerId, String logicalId) {
        identity(logicalId, ownerId);
        var row = document(logicalId, false).orElseThrow(() -> new NoSuchElementException("Document not found"));
        Assert.isTrue(row.owner() == ownerId, "Document belongs to another owner");
        return new ImportStatus(row.id(), row.desiredVersion(), row.currentVersion(), row.desiredTarget(), row.currentTarget(), row.deleted(),
                documentJobs(logicalId).stream().map(j -> new JobStatus(j.id(), j.version(), j.targetTag(), j.state(), j.redisDone(), j.esDone())).toList());
    }
    public List<String> pending(String targetTag) {
        target(targetTag); long now = System.currentTimeMillis();
        return jdbc.query("SELECT job_id FROM agent_rag_job WHERE target_tag=? AND state IN ('WRITING','CLEANUP') AND lease_until<=? AND retry_at<=? "
                        + "ORDER BY updated_at,job_id LIMIT ?", (rs, i) -> rs.getString(1), targetTag, now, now, config.getScanBatch());
    }
    public Job claim(String jobId, String targetTag) {
        target(targetTag);
        return transactions.execute(status -> {
            var optional = job(jobId, true); if (optional.isEmpty()) return null;
            var row = optional.get(); long now = System.currentTimeMillis();
            if (!row.targetTag().equals(targetTag) || !List.of("WRITING", "CLEANUP").contains(row.state()) || row.leaseUntil() > now) return null;
            int changed = jdbc.update("UPDATE agent_rag_job SET worker_epoch=worker_epoch+1,lease_until=?,attempts=attempts+1,updated_at=? WHERE job_id=? AND retry_at<=?",
                    now + config.getWorkerLeaseMs(), now, jobId, now);
            return changed == 1 ? job(jobId, false).orElseThrow() : null;
        });
    }
    public Job renew(Job claimed) {
        Assert.state(jdbc.update("UPDATE agent_rag_job SET lease_until=?,updated_at=? WHERE job_id=? AND worker_epoch=? AND lease_until>? AND state IN ('WRITING','CLEANUP')",
                System.currentTimeMillis() + config.getWorkerLeaseMs(), System.currentTimeMillis(), claimed.id(), claimed.workerEpoch(), System.currentTimeMillis()) == 1,
                "Document worker lost its lease");
        return job(claimed.id(), false).orElseThrow();
    }
    public List<ProjectionChunk> chunks(Job claimed) {
        requireWorker(claimed, false);
        return jdbc.query("SELECT c.child_id,c.parent_id,c.document_id,c.document_version,d.owner_id,v.visibility,v.title,c.heading_path,c.body,v.source_ref,"
                        + "c.page_start,c.page_end,c.ordinal_no FROM agent_rag_child c JOIN agent_rag_document d ON d.document_id=c.document_id "
                        + "JOIN agent_rag_version v ON v.document_id=c.document_id AND v.document_version=c.document_version "
                        + "WHERE c.document_id=? AND c.document_version=? ORDER BY c.ordinal_no",
                (rs, i) -> new ProjectionChunk(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4), rs.getLong(5),
                        Visibility.valueOf(rs.getString(6)), displayTitle(rs.getString(7), rs.getString(8)), rs.getString(9), rs.getString(10),
                        rs.getInt(11), rs.getInt(12), rs.getInt(13)), claimed.documentId(), claimed.version());
    }

    public boolean isCurrent(Job job) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_document WHERE document_id=? AND current_version=? AND current_target=? AND deleted=FALSE",
                Long.class, job.documentId(), job.version(), job.targetTag()) == 1;
    }

    /** Internal compensation only; callers serving users must use hydrate/expand for ACL enforcement. */
    public Optional<ProjectionChunk> publishedChunk(String childId) {
        return jdbc.query("SELECT c.child_id,c.parent_id,c.document_id,c.document_version,d.owner_id,v.visibility,v.title,c.heading_path,c.body,v.source_ref,"
                        + "c.page_start,c.page_end,c.ordinal_no FROM agent_rag_child c JOIN agent_rag_document d ON d.document_id=c.document_id "
                        + "JOIN agent_rag_version v ON v.document_id=c.document_id AND v.document_version=c.document_version "
                        + "WHERE c.child_id=? AND d.deleted=FALSE AND d.current_version=c.document_version",
                (rs, i) -> new ProjectionChunk(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4), rs.getLong(5),
                        Visibility.valueOf(rs.getString(6)), displayTitle(rs.getString(7), rs.getString(8)), rs.getString(9), rs.getString(10),
                        rs.getInt(11), rs.getInt(12), rs.getInt(13)), childId).stream().findFirst();
    }

    public float[] embedding(String childId, String targetTag) {
        target(targetTag);
        return jdbc.query("SELECT vector_dimension,vector_payload FROM agent_rag_embedding WHERE child_id=? AND target_tag=?", (rs, i) -> {
            int dimension = rs.getInt(1); byte[] bytes = Base64.getDecoder().decode(rs.getString(2));
            Assert.state(dimension > 0 && dimension <= 65536 && bytes.length == dimension * 4, "Corrupt cached document embedding");
            var buffer = ByteBuffer.wrap(bytes); float[] vector = new float[dimension];
            for (int j = 0; j < dimension; j++) { vector[j] = buffer.getFloat(); Assert.state(Float.isFinite(vector[j]), "Non-finite cached document embedding"); }
            return vector;
        }, childId, targetTag).stream().findFirst().orElse(null);
    }
    public void cacheEmbedding(Job claimed, String childId, float[] vector) {
        Assert.isTrue(vector != null && vector.length > 0 && vector.length <= 65536, "Invalid document embedding dimension");
        var buffer = ByteBuffer.allocate(vector.length * 4);
        for (float value : vector) { Assert.isTrue(Float.isFinite(value), "Document embedding must be finite"); buffer.putFloat(value); }
        String payload = Base64.getEncoder().encodeToString(buffer.array());
        transactions.executeWithoutResult(status -> {
            requireWorker(claimed, true);
            Assert.state(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_child WHERE child_id=? AND document_id=? AND document_version=?", Long.class,
                    childId, claimed.documentId(), claimed.version()) == 1, "Embedding does not belong to this immutable version");
            try { jdbc.update("INSERT INTO agent_rag_embedding (child_id,target_tag,vector_dimension,vector_payload) VALUES (?,?,?,?)", childId, claimed.targetTag(), vector.length, payload); }
            catch (DuplicateKeyException duplicate) {
                float[] previous = embedding(childId, claimed.targetTag());
                Assert.state(previous != null && Arrays.equals(previous, vector), "Conflicting cached embedding for a fixed projection target");
            }
        });
    }

    /** The caller confirms an entire immutable child set only after the engine reports successful searchable writes. */
    public void complete(Job claimed, String engine) {
        Assert.isTrue(List.of("REDIS", "ES").contains(engine), "Unknown document projection engine");
        transactions.executeWithoutResult(status -> {
            // All mutations lock the document before jobs, including delete/import/publication.
            var doc = document(claimed.documentId(), true).orElseThrow();
            var current = requireWorker(claimed, true);
            if (current.state().equals("CLEANUP")) return;
            Assert.state(current.state().equals("WRITING"), "Document projection is no longer writable");
            jdbc.update("UPDATE agent_rag_job SET " + (engine.equals("REDIS") ? "redis_done" : "es_done") + "=TRUE,updated_at=? WHERE job_id=?",
                    System.currentTimeMillis(), claimed.id());
            current = job(claimed.id(), false).orElseThrow();
            if (!current.redisDone() || !current.esDone()) return;
            boolean publish = !doc.deleted() && doc.desiredVersion() == current.version() && doc.desiredTarget().equals(current.targetTag());
            if (publish) {
                var expected = version(current.documentId(), current.version());
                Assert.state(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_child WHERE document_id=? AND document_version=?", Long.class,
                        current.documentId(), current.version()) == expected.childCount(), "Immutable document child manifest is incomplete");
                jdbc.update("UPDATE agent_rag_document SET current_version=?,current_target=?,revision=revision+1,updated_at=? WHERE document_id=? "
                                + "AND desired_version=? AND desired_target=? AND deleted=FALSE", current.version(), current.targetTag(), System.currentTimeMillis(),
                        current.documentId(), current.version(), current.targetTag());
                for (Job previous : documentJobs(current.documentId())) if (!previous.id().equals(current.id())) {
                    enqueueCleanup(current.id(), previous);
                    // Transfer unfinished cleanup first; a new publication must not strand obsolete targets.
                    transferCleanup(previous.id(), current.id());
                    jdbc.update("UPDATE agent_rag_job SET state='SUPERSEDED',worker_epoch=worker_epoch+1,lease_until=0,updated_at=? WHERE job_id=?",
                            System.currentTimeMillis(), previous.id());
                }
                jdbc.update("DELETE FROM agent_rag_cleanup WHERE job_id=? AND target_tag=? AND child_id IN "
                                + "(SELECT child_id FROM agent_rag_child WHERE document_id=? AND document_version=?)",
                        current.id(), current.targetTag(), current.documentId(), current.version());
            } else enqueueCleanup(current.id(), current);
            jdbc.update("UPDATE agent_rag_job SET state='CLEANUP',updated_at=? WHERE job_id=?", System.currentTimeMillis(), current.id());
        });
    }
    public List<Cleanup> cleanup(Job claimed) {
        var row = requireWorker(claimed, false);
        if (!row.state().equals("CLEANUP")) return List.of();
        return jdbc.query("SELECT child_id,target_tag FROM agent_rag_cleanup WHERE job_id=? ORDER BY target_tag,child_id",
                (rs, i) -> new Cleanup(rs.getString(1), rs.getString(2)), claimed.id());
    }
    /** Same immutable child may share one ES physical ID across model-target reindexes. */
    public boolean safeToDeleteKeyword(Cleanup cleanup) {
        Target obsolete = getTarget(cleanup.targetTag());
        return protectedTargets(cleanup.childId()).stream().noneMatch(current -> current.esEndpoint().equals(obsolete.esEndpoint())
                && current.esIndex().equals(obsolete.esIndex()));
    }
    public boolean safeToDeleteVector(Cleanup cleanup) {
        Target obsolete = getTarget(cleanup.targetTag());
        return protectedTargets(cleanup.childId()).stream().noneMatch(current -> current.redisPrefix().equals(obsolete.redisPrefix()));
    }

    /** Persist before attempting compensation; a failed original job need not remain claimable. */
    public void recordOrphan(String childId, String targetTag) {
        target(targetTag); getTarget(targetTag);
        Assert.state(jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_child WHERE child_id=?", Long.class, childId) == 1,
                "Orphan projection must refer to an immutable document child");
        transactions.executeWithoutResult(status -> {
            long now = System.currentTimeMillis();
            try { jdbc.update("INSERT INTO agent_rag_orphan (child_id,target_tag,revision,retry_at,updated_at) VALUES (?,?,1,0,?)", childId, targetTag, now); }
            catch (DuplicateKeyException duplicate) {
                Assert.state(jdbc.update("UPDATE agent_rag_orphan SET revision=revision+1,state='PENDING',retry_at=0,last_error=NULL,updated_at=? WHERE child_id=? AND target_tag=?",
                        now, childId, targetTag) == 1, "Orphan cleanup registration raced its acknowledgement; retry registration");
            }
        });
    }
    public List<Orphan> pendingOrphans() {
        return jdbc.query("SELECT child_id,target_tag,revision FROM agent_rag_orphan WHERE state='PENDING' AND retry_at<=? ORDER BY updated_at,child_id,target_tag LIMIT ?",
                (rs, i) -> new Orphan(rs.getString(1), rs.getString(2), rs.getLong(3)), System.currentTimeMillis(), config.getScanBatch());
    }
    /** A takeover may still be writing the same IDs; defer cleanup until its publication has settled. */
    public boolean orphanInFlight(Orphan orphan) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_child c JOIN agent_rag_document d ON d.document_id=c.document_id "
                        + "JOIN agent_rag_job j ON j.document_id=d.document_id AND j.document_version=d.desired_version AND j.target_tag=d.desired_target "
                        + "WHERE c.child_id=? AND d.deleted=FALSE AND d.desired_version=c.document_version AND j.state='WRITING'",
                Long.class, orphan.childId()) > 0;
    }
    public void completeOrphan(Orphan orphan) {
        // Retain the revision tombstone: deleting/reinserting revision 1 would permit an old ACK to consume new work.
        jdbc.update("UPDATE agent_rag_orphan SET state='DONE',last_error=NULL,updated_at=? WHERE child_id=? AND target_tag=? AND revision=?",
                System.currentTimeMillis(), orphan.childId(), orphan.targetTag(), orphan.revision());
    }
    public void retryOrphan(Orphan orphan, RuntimeException failure) {
        jdbc.update("UPDATE agent_rag_orphan SET retry_at=?,attempts=attempts+1,last_error=?,updated_at=? WHERE child_id=? AND target_tag=? AND revision=? AND state='PENDING'",
                System.currentTimeMillis() + config.getRetryDelayMs(), failure.getClass().getSimpleName(), System.currentTimeMillis(), orphan.childId(), orphan.targetTag(), orphan.revision());
    }
    private List<Target> protectedTargets(String childId) {
        String published = "SELECT t.target_tag,t.redis_prefix,t.es_endpoint,t.es_index FROM agent_rag_child c "
                        + "JOIN agent_rag_document d ON d.document_id=c.document_id LEFT JOIN agent_rag_target t ON t.target_tag=d.current_target "
                        + "WHERE c.child_id=? AND d.deleted=FALSE AND d.current_version=c.document_version";
        String writing = "SELECT t.target_tag,t.redis_prefix,t.es_endpoint,t.es_index FROM agent_rag_child c "
                + "JOIN agent_rag_document d ON d.document_id=c.document_id LEFT JOIN agent_rag_target t ON t.target_tag=d.desired_target "
                + "JOIN agent_rag_job j ON j.document_id=d.document_id AND j.document_version=d.desired_version AND j.target_tag=d.desired_target "
                + "WHERE c.child_id=? AND d.deleted=FALSE AND d.desired_version=c.document_version AND j.state='WRITING'";
        return jdbc.query(published + " UNION ALL " + writing,
                (rs, i) -> {
                    Assert.state(rs.getString(1) != null, "Current document projection target descriptor is missing");
                    return new Target(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
                }, childId, childId);
    }
    public void completeCleanup(Job claimed) {
        transactions.executeWithoutResult(status -> {
            var row = requireWorker(claimed, true); Assert.state(row.state().equals("CLEANUP"), "Projection receipts must precede cleanup completion");
            jdbc.update("DELETE FROM agent_rag_cleanup WHERE job_id=?", row.id());
            jdbc.update("UPDATE agent_rag_job SET state='DONE',lease_until=0,last_error=NULL,updated_at=? WHERE job_id=?", System.currentTimeMillis(), row.id());
        });
    }
    public void retry(Job claimed, RuntimeException failure) {
        jdbc.update("UPDATE agent_rag_job SET lease_until=0,retry_at=?,last_error=?,updated_at=? WHERE job_id=? AND worker_epoch=? AND state IN ('WRITING','CLEANUP')",
                System.currentTimeMillis() + config.getRetryDelayMs(), failure.getClass().getSimpleName(), System.currentTimeMillis(), claimed.id(), claimed.workerEpoch());
    }

    public List<Candidate> hydrate(long userId, List<String> childIds, List<String> docIds) { return hydrate(userId, childIds, docIds, null); }
    public List<Candidate> hydrate(long userId, List<String> childIds, List<String> docIds, String targetTag) {
        Assert.isTrue(userId > 0, "Authenticated user is required");
        Assert.isTrue(childIds != null && childIds.size() <= 200 && docIds != null && docIds.size() <= 100, "Invalid document hydration bounds");
        if (targetTag != null) target(targetTag); if (childIds.isEmpty()) return List.of();
        List<String> ids = childIds.stream().filter(Objects::nonNull).distinct().toList(); if (ids.isEmpty()) return List.of();
        List<Object> args = new ArrayList<>(ids); args.add(userId);
        String sql = "SELECT c.child_id,c.document_id,c.document_version,c.parent_id,v.title,c.heading_path,c.body,v.source_ref,c.page_start,c.page_end,c.ordinal_no "
                + "FROM agent_rag_child c JOIN agent_rag_document d ON d.document_id=c.document_id "
                + "JOIN agent_rag_version v ON v.document_id=c.document_id AND v.document_version=c.document_version "
                + "WHERE c.child_id IN (" + marks(ids.size()) + ") AND d.deleted=FALSE AND d.current_version=c.document_version "
                + "AND (v.visibility='PUBLIC' OR d.owner_id=?)";
        if (targetTag != null) { sql += " AND d.current_target=?"; args.add(targetTag); }
        if (!docIds.isEmpty()) { sql += " AND c.document_id IN (" + marks(docIds.size()) + ")"; args.addAll(docIds); }
        var rows = jdbc.query(sql, (rs, i) -> new Candidate(rs.getString(1), Kind.KNOWLEDGE_CHILD, rs.getString(2), rs.getLong(3), rs.getString(4),
                displayTitle(rs.getString(5), rs.getString(6)), rs.getString(7), rs.getString(8), rs.getInt(9), rs.getInt(10), rs.getInt(11)), args.toArray());
        Map<String, Candidate> byId = new HashMap<>(); rows.forEach(c -> byId.put(c.id(), c));
        return ids.stream().map(byId::get).filter(Objects::nonNull).toList();
    }
    public Evidence expand(long userId, Candidate candidate) {
        Assert.notNull(candidate, "Document candidate is required");
        Assert.isTrue(candidate.kind() == Kind.KNOWLEDGE_CHILD, "Only knowledge children may be expanded");
        var visible = hydrate(userId, List.of(candidate.id()), List.of(candidate.resourceId()));
        if (visible.isEmpty()) return null;
        Candidate canonical = visible.get(0);
        // The candidate cannot substitute another version or a parent outside its own immutable membership.
        if (canonical.version() != candidate.version() || !canonical.parentId().equals(candidate.parentId())) return null;
        return jdbc.query("SELECT p.body,p.page_start,p.page_end FROM agent_rag_parent p JOIN agent_rag_document d ON d.document_id=p.document_id "
                        + "JOIN agent_rag_version v ON v.document_id=p.document_id AND v.document_version=p.document_version "
                        + "WHERE p.parent_id=? AND p.document_id=? AND p.document_version=? AND d.deleted=FALSE AND d.current_version=p.document_version "
                        + "AND (v.visibility='PUBLIC' OR d.owner_id=?)",
                (rs, i) -> new Evidence(canonical.parentId(), Kind.KNOWLEDGE_CHILD, canonical.title(), rs.getString(1), canonical.source(), canonical.version(),
                        rs.getInt(2), rs.getInt(3), List.of(canonical.id())), canonical.parentId(), canonical.resourceId(), canonical.version(), userId)
                .stream().findFirst().orElse(null);
    }

    private DocumentRow ensureDocument(String id, long owner) {
        try { jdbc.update("INSERT INTO agent_rag_document (document_id,owner_id,updated_at) VALUES (?,?,?)", id, owner, System.currentTimeMillis()); }
        catch (DuplicateKeyException exists) { /* Lock and check the original owner below. */ }
        return owned(id, owner);
    }
    private DocumentRow owned(String id, long owner) {
        var row = document(id, true).orElseThrow(() -> new NoSuchElementException("Document not found"));
        Assert.isTrue(row.owner() == owner, "Document belongs to another owner"); return row;
    }
    private Optional<DocumentRow> document(String id, boolean lock) {
        return jdbc.query("SELECT document_id,owner_id,desired_version,desired_target,current_version,current_target,deleted FROM agent_rag_document WHERE document_id=?"
                        + (lock ? " FOR UPDATE" : ""),
                (rs, i) -> new DocumentRow(rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getLong(5), rs.getString(6), rs.getBoolean(7)), id).stream().findFirst();
    }
    private Version version(String id, long number) {
        return jdbc.query("SELECT document_version,payload_hash,child_count FROM agent_rag_version WHERE document_id=? AND document_version=?",
                (rs, i) -> new Version(rs.getLong(1), rs.getString(2), rs.getLong(3)), id, number).stream().findFirst().orElseThrow();
    }
    private Optional<Job> job(String id, boolean lock) {
        return jobs(" WHERE job_id=?" + (lock ? " FOR UPDATE" : ""), id).stream().findFirst();
    }
    private List<Job> documentJobs(String id) { return jobs(" WHERE document_id=? ORDER BY document_version,target_tag", id); }
    private List<Job> jobs(String suffix, Object... args) {
        return jdbc.query("SELECT job_id,document_id,document_version,target_tag,worker_epoch,lease_until,redis_done,es_done,state FROM agent_rag_job" + suffix,
                (rs, i) -> new Job(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getString(4), rs.getLong(5), rs.getLong(6),
                        rs.getBoolean(7), rs.getBoolean(8), rs.getString(9)), args);
    }
    private Job requireWorker(Job claimed, boolean lock) {
        var row = job(claimed.id(), lock).orElseThrow();
        Assert.state(row.workerEpoch() == claimed.workerEpoch() && row.leaseUntil() > System.currentTimeMillis()
                && List.of("WRITING", "CLEANUP").contains(row.state()), "Document worker lost its lease"); return row;
    }
    private void register(String document, long version, String target) {
        jdbc.update("INSERT INTO agent_rag_job (job_id,document_id,document_version,target_tag,state,updated_at) VALUES (?,?,?,?,'WRITING',?)",
                jobId(document, version, target), document, version, target, System.currentTimeMillis());
    }
    private ImportResult result(String document, long version, String target, boolean duplicate) {
        return new ImportResult(document, version, jobId(document, version, target), duplicate);
    }
    private void enqueueCleanup(String controller, Job previous) {
        for (String id : jdbc.query("SELECT child_id FROM agent_rag_child WHERE document_id=? AND document_version=?", (rs, i) -> rs.getString(1),
                previous.documentId(), previous.version())) addCleanup(controller, id, previous.targetTag());
    }
    private void transferCleanup(String from, String to) {
        for (Cleanup row : jdbc.query("SELECT child_id,target_tag FROM agent_rag_cleanup WHERE job_id=?", (rs, i) -> new Cleanup(rs.getString(1), rs.getString(2)), from))
            addCleanup(to, row.childId(), row.targetTag());
        jdbc.update("DELETE FROM agent_rag_cleanup WHERE job_id=?", from);
    }
    private void addCleanup(String controller, String child, String target) {
        try { jdbc.update("INSERT INTO agent_rag_cleanup (job_id,child_id,target_tag) VALUES (?,?,?)", controller, child, target); }
        catch (DuplicateKeyException duplicate) { /* Idempotent cleanup transfer. */ }
    }
    private void validateParsed(ParsedDocument parsed) {
        Assert.notNull(parsed, "Parsed document is required"); target(parsed.sourceHash()); target(parsed.pipelineTag());
        Assert.isTrue(!parsed.parents().isEmpty() && !parsed.children().isEmpty() && parsed.parents().size() <= config.getMaxChunks()
                && parsed.children().size() <= config.getMaxChunks(), "Invalid immutable document chunk manifest");
        Map<String, Parent> parents = new HashMap<>(); Set<Integer> positions = new HashSet<>();
        long length = 0;
        for (Parent parent : parsed.parents()) {
            target(parent.id()); bounded(parent.text(), config.getMaxParentChars(), "Parent text");
            Assert.isTrue(parent.headingPath() != null && parent.headingPath().length() <= 2048 && parent.ordinal() >= 0
                    && positions.add(parent.ordinal()) && parents.put(parent.id(), parent) == null, "Duplicate parent identity/position");
            locator(parent.pageStart(), parent.pageEnd(), parent.lineStart(), parent.lineEnd()); length += parent.text().length();
        }
        Assert.isTrue(length <= config.getMaxDocumentChars(), "Document text budget exceeded");
        Set<String> ids = new HashSet<>(); positions.clear();
        for (Child child : parsed.children()) {
            target(child.id()); bounded(child.text(), config.getMaxChildChars(), "Child text");
            Parent parent = parents.get(child.parentId());
            Assert.isTrue(parent != null && parent.text().contains(child.text()) && child.ordinal() >= 0 && positions.add(child.ordinal()) && ids.add(child.id()),
                    "Child must belong to one immutable parent with a unique position");
            Assert.isTrue(child.headingPath() != null && child.headingPath().length() <= 2048, "Invalid child heading path");
            locator(child.pageStart(), child.pageEnd(), child.lineStart(), child.lineEnd());
            Assert.isTrue(child.pageStart() >= parent.pageStart() && child.pageEnd() <= parent.pageEnd(), "Child page range escapes parent");
        }
    }
    private static void locator(int pageStart, int pageEnd, int lineStart, int lineEnd) {
        Assert.isTrue(pageStart >= 0 && pageEnd >= pageStart && lineStart >= 0 && lineEnd >= lineStart
                && (pageStart == 0 ? pageEnd == 0 && lineStart > 0 : lineStart == 0 && lineEnd == 0), "Invalid source locator");
    }
    private static void bounded(String text, int max, String label) { Assert.isTrue(text != null && !text.isBlank() && text.length() <= max, label + " is empty or exceeds its limit"); }
    private static void identity(String id, long owner) {
        Assert.isTrue(owner > 0 && id != null && id.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"), "Document requires an owner and a stable logical ID");
    }
    private static void target(String target) { Assert.isTrue(target != null && target.matches("[a-f0-9]{64}"), "Document target/hash must be SHA-256"); }
    private static String marks(int count) { return String.join(",", Collections.nCopies(count, "?")); }
    private static String displayTitle(String title, String heading) {
        return heading == null || heading.isBlank() ? title : title + " / " + heading;
    }
    private static String jobId(String document, long version, String target) { return hash("rag-job|" + document + "|" + version + "|" + target); }
    private static String parentId(String document, long version, String parsed) { return "parent_" + hash(document + "|" + version + "|" + parsed); }
    private static String childId(String document, long version, String parsed) { return "doc_" + hash(document + "|" + version + "|" + parsed); }

    private record DocumentRow(String id, long owner, long desiredVersion, String desiredTarget, long currentVersion, String currentTarget, boolean deleted) { }
    private record Version(long version, String payloadHash, long childCount) { }
    public record ImportResult(String documentId, long version, String jobId, boolean duplicate) { }
    public record ImportStatus(String documentId, long desiredVersion, long currentVersion, String desiredTarget, String currentTarget, boolean deleted, List<JobStatus> jobs) { }
    public record JobStatus(String id, long version, String targetTag, String state, boolean redisDone, boolean esDone) { }
    public record Job(String id, String documentId, long version, String targetTag, long workerEpoch, long leaseUntil, boolean redisDone, boolean esDone, String state) { }
    public record ProjectionChunk(String id, String parentId, String documentId, long version, long ownerId, Visibility visibility, String title, String text, String source,
                                  int pageStart, int pageEnd, int ordinal) { }
    public record Cleanup(String childId, String targetTag) { }
    public record Target(String targetTag, String redisPrefix, String esEndpoint, String esIndex) { }
    public record Orphan(String childId, String targetTag, long revision) { }
}
