package org.hgc.suts.gateway.rag.document;

import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.rag.RagData.Visibility;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.rag.document.DocumentEtl.hash;

/** Opt-in only: the existing private MySQL fixture marker is verified before any RAG migration. */
class MysqlDocumentStoreIT {
    @Test void productionMysqlPersistsTwoReceiptsVersionSwapCleanupAndDelete() {
        String port = System.getProperty("mysql.archive.port");
        Assert.isTrue(port != null && port.matches("[0-9]{1,5}") && Integer.parseInt(port) > 0
                && Integer.parseInt(port) < 65536, "Pass an explicit private MySQL fixture port");
        var source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port
                + "/agent_archive_it?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=5000&socketTimeout=10000",
                "root", "agent-archive-integration-only");
        var jdbc = new JdbcTemplate(source);
        Assert.state("agent-archive-integration-only".equals(jdbc.queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class)),
                "Refusing an unmarked MySQL database");
        Flyway.configure().dataSource(source).table("agent_rag_schema_history").locations("classpath:db/rag")
                .baselineOnMigrate(true).baselineVersion("0")
                .placeholders(Map.of("tableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")).load().migrate();
        var properties = new RagProperties();
        var store = new JdbcDocumentStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(source)), properties);
        var etl = new DocumentEtl(properties); String target = hash("mysql-document-fixture"); String id = "mysql-" + UUID.randomUUID();
        store.registerTarget(target, "fixture:rag:", "http://127.0.0.1:9200", "fixture-rag");
        var first = store.importDocument(id, 42, Visibility.OWNER, "申请指南", "fixture:guide.md",
                etl.parse("# 申请\n1. 身份证\n2. 窗口".getBytes(StandardCharsets.UTF_8), "guide.md"), target);
        var job = store.claim(first.jobId(), target); var children = store.chunks(job);
        store.cacheEmbedding(job, children.get(0).id(), new float[]{1, 2, 3}); store.complete(job, "REDIS");
        assertThat(store.hydrate(42, children.stream().map(JdbcDocumentStore.ProjectionChunk::id).toList(), List.of(id))).isEmpty();
        store.complete(job, "ES"); store.completeCleanup(job);
        assertThat(store.status(42, id).currentVersion()).isEqualTo(1);
        assertThat(store.embedding(children.get(0).id(), target)).containsExactly(1, 2, 3);
        var second = store.importDocument(id, 42, Visibility.PUBLIC, "申请指南", "fixture:guide.md",
                etl.parse("# 申请\n1. 更新的材料清单".getBytes(StandardCharsets.UTF_8), "guide.md"), target);
        var replacement = store.claim(second.jobId(), target); store.complete(replacement, "REDIS"); store.complete(replacement, "ES");
        assertThat(store.status(42, id).currentVersion()).isEqualTo(2);
        assertThat(store.cleanup(replacement)).hasSize(children.size());
        assertThat(store.safeToDeleteKeyword(store.cleanup(replacement).get(0))).isTrue();
        store.completeCleanup(replacement); store.delete(42, id);
        assertThat(store.status(42, id).deleted()).isTrue();
        var deleted = store.claim(second.jobId(), target); assertThat(deleted.state()).isEqualTo("CLEANUP"); store.completeCleanup(deleted);
    }
}
