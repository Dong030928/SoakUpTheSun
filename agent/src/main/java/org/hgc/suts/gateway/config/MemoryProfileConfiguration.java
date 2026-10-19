package org.hgc.suts.gateway.config;

import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.profile.*;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import redis.clients.jedis.RedisClient;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "suts.ai.memory.profile.enabled", havingValue = "true")
@EnableConfigurationProperties(MemoryProfileProperties.class)
public class MemoryProfileConfiguration {
    @Bean public JdbcProfileStore profileStore(JdbcConversationArchive archive, MemoryProfileProperties config, AiConfig ai,
                                               MemoryTransactionProperties tx, Environment environment) {
        Assert.state(environment.getProperty("suts.ai.memory.summary.enabled", Boolean.class, false), "Profiles require enabled M03 summary memory");
        return new JdbcProfileStore(archive, config, ProfileVectorIndex.target(ai, config), tx.getMaxMessageBytes(), tx.getGeneration());
    }
    @Bean public ProfileSummarySupport profileSummarySupport(JdbcProfileStore profiles, MemoryProfileProperties config) {
        return new ProfileSummarySupport(profiles, config);
    }
    @Bean public ProfileVectorIndex profileVectorIndex(RedisClient redis, EmbeddingModel agentEmbeddingModel, AiConfig ai,
                                                       MemoryProfileProperties config, JdbcProfileStore profiles) {
        return new ProfileVectorIndex(redis, agentEmbeddingModel, ai, config, profiles);
    }
    @Bean(initMethod = "start", destroyMethod = "close") public ProfileWorker profileWorker(JdbcProfileStore profiles,
            ProfileVectorIndex vectors, MemoryProfileProperties config) { return new ProfileWorker(profiles, vectors, config); }
}
