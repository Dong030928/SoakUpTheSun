package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.RagConfiguration;
import org.hgc.suts.gateway.config.RagProperties;
import org.hgc.suts.gateway.controller.RagRetrievalController;
import org.hgc.suts.gateway.memory.profile.JdbcProfileStore;
import org.hgc.suts.gateway.memory.profile.ProfileKeywordWorker;
import org.hgc.suts.gateway.rag.document.JdbcDocumentStore;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import redis.clients.jedis.RedisClient;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RagConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RagConfiguration.class, RagRetrievalController.class);

    @Test void disabledByDefaultCreatesNoDatabaseIndexWorkerExecutorOrController() {
        runner.withPropertyValues("suts.ai.rag.jdbc-url=jdbc:must-not-open:fixture",
                "suts.ai.rag.elasticsearch.endpoint=must-not-contact").run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(DataSource.class).doesNotHaveBean(JdbcDocumentStore.class)
                    .doesNotHaveBean(RedisRagIndex.class).doesNotHaveBean(ElasticsearchRagIndex.class)
                    .doesNotHaveBean(DocumentIndexWorker.class).doesNotHaveBean(ProfileKeywordWorker.class)
                    .doesNotHaveBean(ExecutorService.class).doesNotHaveBean(RagRetrievalService.class)
                    .doesNotHaveBean(RagRetrievalController.class);
        });
        var properties = new RagProperties();
        assertThat(properties.isEnabled()).isFalse(); assertThat(properties.isMigrate()).isFalse();
    }

    @Test void explicitlyDisabledIgnoresInvalidInfrastructureSettings() {
        runner.withPropertyValues("suts.ai.rag.enabled=false", "suts.ai.rag.jdbc-url=jdbc:must-not-open:fixture",
                "suts.ai.rag.profile-quota=-1").run(context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(RagRetrievalService.class).doesNotHaveBean(JdbcDocumentStore.class));
    }

    @Test void documentRetrievalWorksWithoutProfileBeansAndNeverCreatesRealInfrastructure() {
        var documents = mock(JdbcDocumentStore.class); var vectors = mock(RedisRagIndex.class);
        var keywords = mock(ElasticsearchRagIndex.class); var reranker = mock(SiliconFlowReranker.class);
        var redis = mock(RedisClient.class); var embedding = mock(EmbeddingModel.class);
        var ai = new AiConfig(); ai.setEmbeddingEnabled(true); ai.getVector().setDimension(2);
        String target = "a".repeat(64), child = "doc_" + "b".repeat(64);
        var candidate = new RagData.Candidate(child, RagData.Kind.KNOWLEDGE_CHILD, "guide", 1, "parent",
                "读屏指南", "按回车键", "guide.txt", 0, 0, 0);
        var evidence = new RagData.Evidence("parent", RagData.Kind.KNOWLEDGE_CHILD, "读屏指南", "按回车键打开文档。",
                "guide.txt", 1, 0, 0, List.of(child));
        when(embedding.call(any())).thenReturn(new EmbeddingResponse(List.of(new Embedding(new float[]{1, 0}, 0))));
        when(vectors.target()).thenReturn(target);
        when(vectors.search(eq(42L), eq("怎么打开文档"), any(float[].class), eq(List.of("guide")), eq(50)))
                .thenReturn(List.of(new RagData.Hit(child, .9)));
        when(keywords.search(42, "怎么打开文档", RagData.Kind.KNOWLEDGE_CHILD, List.of("guide"), 50)).thenReturn(List.of());
        when(documents.hydrate(eq(42L), anyList(), eq(List.of("guide")), eq(target))).thenReturn(List.of(candidate));
        when(documents.expand(42, candidate)).thenReturn(evidence);
        when(reranker.rerank(eq("怎么打开文档"), anyList(), eq(1))).thenReturn(List.of(new RagData.Reranked(0, .95)));
        runner.withPropertyValues("suts.ai.rag.enabled=true", "suts.ai.memory.profile.enabled=false",
                        "suts.ai.rag.jdbc-url=jdbc:must-not-open:fixture")
                .withBean("fixtureDocuments", JdbcDocumentStore.class, () -> documents, definition -> definition.setPrimary(true))
                .withBean("fixtureVectors", RedisRagIndex.class, () -> vectors, definition -> definition.setPrimary(true))
                .withBean("fixtureKeywords", ElasticsearchRagIndex.class, () -> keywords, definition -> definition.setPrimary(true))
                .withBean("fixtureReranker", SiliconFlowReranker.class, () -> reranker, definition -> definition.setPrimary(true))
                .withBean("agentEmbeddingModel", EmbeddingModel.class, () -> embedding)
                .withBean(RedisClient.class, () -> redis).withBean(AiConfig.class, () -> ai)
                .withInitializer(context -> context.addBeanFactoryPostProcessor(factory -> {
                    // Primary mocks isolate infrastructure; leave the real service wiring under test.
                    for (String name : List.of("ragDocumentStore", "ragDocumentVectors", "ragKeywords", "ragReranker", "ragDocumentWorker"))
                        factory.getBeanDefinition(name).setLazyInit(true);
                })).run(context -> {
                    assertThat(context).hasNotFailed().doesNotHaveBean(ProfileRagAccess.class)
                            .doesNotHaveBean(ProfileKeywordWorker.class).doesNotHaveBean(JdbcProfileStore.class).doesNotHaveBean(DataSource.class);
                    var result = context.getBean(RagRetrievalService.class).retrieve(42,
                            new RagData.Plan("怎么打开文档", "怎么打开文档", RagData.Scope.DOCUMENT, List.of("guide")));
                    assertThat(result.evidence()).containsExactly(evidence);
                    verify(embedding, times(1)).call(any()); verifyNoInteractions(redis);
                    verify(documents, never()).registerTarget(anyString(), anyString(), anyString(), anyString());
                    verify(keywords, never()).initialize(); verify(vectors, never()).initialize();
                });
    }

    @Test void validatesQuotasBudgetsAndServiceEndpointsBeforeInfrastructureUse() {
        new RagProperties().validate();
        var quotas = new RagProperties(); quotas.setProfileQuota(11);
        assertThatThrownBy(quotas::validate).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("quotas");
        var timeout = new RagProperties(); timeout.setChannelTimeoutMs(timeout.getTimeoutMs() + 1);
        assertThatThrownBy(timeout::validate).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("time budget");
        var chunks = new RagProperties(); chunks.setOverlapChars(chunks.getMaxChildChars() / 2);
        assertThatThrownBy(chunks::validate).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("import budget");
        var endpoint = new RagProperties(); endpoint.getElasticsearch().setEndpoint("http://secret@example.invalid:9200");
        assertThatThrownBy(endpoint::validate).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("endpoint");
    }
}
