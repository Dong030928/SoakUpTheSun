package org.hgc.suts.gateway.config;

import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.memory.profile.*;
import org.hgc.suts.gateway.rag.*;
import org.hgc.suts.gateway.rag.document.*;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import redis.clients.jedis.RedisClient;

import java.util.*;
import java.util.concurrent.*;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "suts.ai.rag.enabled", havingValue = "true")
@EnableConfigurationProperties(RagProperties.class)
public class RagConfiguration {
    @Bean public JdbcDocumentStore ragDocumentStore(RagProperties config) {
        config.validate(); Assert.hasText(config.getJdbcUrl(), "RAG JDBC URL is required");
        Assert.isTrue(config.getJdbcUrl().startsWith("jdbc:mysql:"), "RAG requires a MySQL JDBC URL");
        var datasource = new DriverManagerDataSource(config.getJdbcUrl(), config.getUsername(), config.getPassword());
        var connection = new Properties(); connection.setProperty("connectTimeout", "5000"); connection.setProperty("socketTimeout", "10000");
        datasource.setConnectionProperties(connection);
        var flyway = Flyway.configure().dataSource(datasource).table("agent_rag_schema_history").locations("classpath:db/rag")
                .baselineOnMigrate(true).baselineVersion("0")
                .placeholders(Map.of("tableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")).load();
        if (config.isMigrate()) flyway.migrate(); else flyway.validate();
        var jdbc = new JdbcTemplate(datasource); jdbc.setQueryTimeout(10);
        jdbc.queryForObject("SELECT COUNT(*) FROM agent_rag_document WHERE 1=0", Long.class);
        return new JdbcDocumentStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(datasource)), config);
    }
    @Bean public DocumentEtl ragDocumentEtl(RagProperties config) { return new DocumentEtl(config); }
    @Bean public RedisRagIndex ragDocumentVectors(RedisClient redis, EmbeddingModel agentEmbeddingModel, AiConfig ai, RagProperties config) {
        Assert.state(ai.isEmbeddingEnabled(), "RAG requires enabled embeddings");
        return new RedisRagIndex(redis, agentEmbeddingModel, config, ai.getVector().getDimension(), new MemoryVectorProperties().fingerprint(ai));
    }
    @Bean public ElasticsearchRagIndex ragKeywords(RagProperties config) { return new ElasticsearchRagIndex(config); }
    @Bean public SiliconFlowReranker ragReranker(RagProperties config) { return new SiliconFlowReranker(config); }
    @Bean(initMethod = "start", destroyMethod = "close") public DocumentIndexWorker ragDocumentWorker(JdbcDocumentStore documents,
            RedisRagIndex vectors, ElasticsearchRagIndex keywords, RedisClient redis, EmbeddingModel agentEmbeddingModel, AiConfig ai, RagProperties config) {
        return new DocumentIndexWorker(documents, vectors, keywords, redis, agentEmbeddingModel, ai.getVector().getDimension(), config);
    }
    @Bean @ConditionalOnProperty(name = "suts.ai.memory.profile.enabled", havingValue = "true")
    public ProfileRagAccess ragProfiles(RedisClient redis, ProfileVectorIndex vectors, JdbcProfileStore store, AiConfig ai, MemoryProfileProperties config) {
        return new ProfileRagAccess(redis, vectors, store, ai.getVector().getDimension(), config.getSimilarityThreshold());
    }
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnProperty(name = "suts.ai.memory.profile.enabled", havingValue = "true")
    public ProfileKeywordWorker profileKeywordWorker(JdbcProfileStore store, ElasticsearchRagIndex keywords, RagProperties config) {
        return new ProfileKeywordWorker(store, keywords, config);
    }
    @Bean(name = "ragQueryExecutor", destroyMethod = "shutdownNow") public ExecutorService ragQueryExecutor() {
        return new ThreadPoolExecutor(4, 8, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(64), run -> {
            Thread thread = new Thread(() -> {
                org.hgc.suts.gateway.common.biz.user.UserContext.remove();
                try { run.run(); } finally { org.hgc.suts.gateway.common.biz.user.UserContext.remove(); }
            }, "agent-rag-query"); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }
    @Bean public RagRetrievalService ragRetrievalService(RagProperties config, @Qualifier("ragQueryExecutor") ExecutorService executor,
            EmbeddingModel agentEmbeddingModel, AiConfig ai, RedisRagIndex vectors, ElasticsearchRagIndex keywords,
            SiliconFlowReranker reranker, JdbcDocumentStore documents, ObjectProvider<ProfileRagAccess> optionalProfiles) {
        var profiles = optionalProfiles.getIfAvailable();
        return new RagRetrievalService(config, executor, agentEmbeddingModel, ai.getVector().getDimension(),
                (user, query, vector, kind, ids, limit) -> kind == RagData.Kind.KNOWLEDGE_CHILD
                        ? vectors.search(user, query, vector, ids, limit) : requireProfiles(profiles).search(user, query, vector, limit),
                (user, query, kind, ids, limit) -> {
                    if (kind == RagData.Kind.PROFILE_FACT) requireProfiles(profiles);
                    return keywords.search(user, query, kind, ids, limit);
                }, new RagRetrievalService.Resolver() {
                    public List<RagData.Candidate> hydrate(long user, List<String> ids, List<String> docIds) {
                        var rows = new ArrayList<>(documents.hydrate(user, ids, docIds, vectors.target()));
                        if (profiles != null) rows.addAll(profiles.hydrate(user, ids));
                        return rows;
                    }
                    public RagData.Evidence expand(long user, RagData.Candidate candidate) {
                        if (candidate.kind() == RagData.Kind.KNOWLEDGE_CHILD) return documents.expand(user, candidate);
                        var current = requireProfiles(profiles).hydrate(user, List.of(candidate.id()));
                        if (current.isEmpty()) return null;
                        var c = current.get(0);
                        return new RagData.Evidence(c.id(), c.kind(), c.title(), c.text(), c.source(), c.version(), 0, 0, List.of());
                    }
                }, reranker::rerank);
    }
    private static ProfileRagAccess requireProfiles(ProfileRagAccess profiles) {
        Assert.state(profiles != null, "Profile memory is disabled"); return profiles;
    }
}
