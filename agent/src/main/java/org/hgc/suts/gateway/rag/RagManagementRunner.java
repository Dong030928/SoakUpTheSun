package org.hgc.suts.gateway.rag;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.memory.profile.ProfileKeywordWorker;
import org.hgc.suts.gateway.rag.document.DocumentEtl;
import org.hgc.suts.gateway.rag.document.JdbcDocumentStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.Set;

/** Trusted local administration only; document owner/visibility cannot be changed over the search API. */
@Slf4j
@Component
@ConditionalOnProperty(name = {"suts.ai.rag.enabled", "suts.ai.rag.management.enabled"}, havingValue = "true")
public final class RagManagementRunner implements ApplicationRunner {
    private final JdbcDocumentStore documents;
    private final DocumentEtl etl;
    private final RedisRagIndex vectors;
    private final DocumentIndexWorker worker;
    private final ObjectProvider<ProfileKeywordWorker> profiles;
    private final Environment environment;
    private final ConfigurableApplicationContext context;
    public RagManagementRunner(JdbcDocumentStore documents, DocumentEtl etl, RedisRagIndex vectors, DocumentIndexWorker worker,
            ObjectProvider<ProfileKeywordWorker> profiles, Environment environment, ConfigurableApplicationContext context) {
        this.documents = documents; this.etl = etl; this.vectors = vectors; this.worker = worker;
        this.profiles = profiles; this.environment = environment; this.context = context;
    }
    @Override public void run(ApplicationArguments args) {
        if (!"none".equalsIgnoreCase(environment.getProperty("spring.main.web-application-type", "")))
            throw new IllegalStateException("RAG management requires --spring.main.web-application-type=none");
        String action = required("action");
        if (!Set.of("import", "status", "delete", "reindex", "retry", "profile-backfill").contains(action))
            throw new IllegalArgumentException("Unknown RAG management action");
        if (action.equals("profile-backfill")) {
            var profile = profiles.getIfAvailable();
            if (profile == null) throw new IllegalStateException("Profile memory is disabled");
            profile.resetBackfill(); profile.sweep(); log.info("画像关键词回填已重置，剩余任务由服务扫描继续");
        } else if (action.equals("retry")) {
            worker.sweep(); log.info("已执行一批待恢复文档任务");
        } else {
            String id = required("document-id"); long owner = Long.parseLong(required("owner-id"));
            if (action.equals("import")) {
                Path path = Path.of(required("file"));
                var visibility = RagData.Visibility.valueOf(environment.getProperty("suts.ai.rag.management.visibility", "OWNER"));
                String title = environment.getProperty("suts.ai.rag.management.title", path.getFileName().toString());
                String source = environment.getProperty("suts.ai.rag.management.source", path.getFileName().toString());
                var result = documents.importDocument(id, owner, visibility, title, source, etl.parse(path), vectors.target());
                worker.process(result.jobId());
                log.info("文档导入 id={}, version={}, duplicate={}", result.documentId(), result.version(), result.duplicate());
            } else if (action.equals("reindex")) {
                var result = documents.reindex(owner, id, vectors.target()); worker.process(result.jobId());
                log.info("文档索引重建 id={}, version={}", result.documentId(), result.version());
            } else if (action.equals("delete")) {
                documents.delete(owner, id); worker.sweep(); log.info("已撤销文档可见性 id={}", id);
            }
            var status = documents.status(owner, id);
            log.info("文档状态 id={}, desired={}, current={}, deleted={}, jobs={}", status.documentId(), status.desiredVersion(),
                    status.currentVersion(), status.deleted(), status.jobs());
        }
        context.close();
    }
    private String required(String key) {
        String value = environment.getProperty("suts.ai.rag.management." + key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing RAG management argument: " + key);
        return value;
    }
}
