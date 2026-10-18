package org.hgc.suts.gateway.config;

import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.summary.*;
import org.hgc.suts.gateway.memory.transactions.RedisTransactionLedger;
import org.hgc.suts.gateway.memory.transactions.RocketMqTransactionTransport;
import org.hgc.suts.gateway.service.VectorStorageService;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "suts.ai.memory.summary.enabled", havingValue = "true")
public class MemorySummaryConfiguration {
    @Bean public SummaryPolicy summaryPolicy(Environment environment, AiConfig ai, MemoryVectorProperties vector, MemoryTransactionProperties tx) {
        Assert.state(environment.getProperty("suts.ai.memory.transactions.enabled", Boolean.class, false),
                "Summary memory requires enabled transaction archival");
        var policy = new SummaryPolicy(() -> {
            var properties = Binder.get(environment).bind("suts.ai.memory.summary", Bindable.of(MemorySummaryProperties.class))
                    .orElseThrow(() -> new IllegalStateException("Summary configuration is missing"));
            Assert.state(!ai.isEmbeddingEnabled() || !vector.isEnabled() || properties.getSummaryMaxChars() <= vector.getMaxDocumentChars(),
                    "Summary output must fit the existing vector document budget");
            Assert.state(properties.getMaxStateBytes() > 2L * tx.getMaxMessageBytes() + 4096,
                    "Summary state requires room for one complete turn and an immutable pending task");
            return properties;
        }, ai);
        policy.current();
        return policy;
    }
    @Bean public MemoryTokenBudget memoryTokenBudget() { return new MemoryTokenBudget(); }
    @Bean public JdbcSummaryJobs summaryJobs(JdbcConversationArchive archive, MemoryTransactionProperties tx) {
        return new JdbcSummaryJobs(archive, tx.getMaxMessageBytes(), tx.getGeneration());
    }
    @Bean(destroyMethod = "close") public RedisSummaryStateStore summaryStates(StringRedisTemplate redis,
            RedisTransactionLedger ledger, MemoryTransactionProperties tx, MemoryProperties window, SummaryPolicy policy) {
        return new RedisSummaryStateStore(redis, ledger, tx, window, policy);
    }
    @Bean public SummaryCoordinator summaryCoordinator(RedisSummaryStateStore states, RedisConversationMemoryRepository redis,
            RedisTransactionLedger ledger, JdbcConversationArchive archive, JdbcSummaryJobs jobs, MemoryTransactionProperties tx,
            SummaryPolicy policy, MemoryTokenBudget tokens, ObjectProvider<RocketMqTransactionTransport> messaging, MemoryProperties window) {
        return new SummaryCoordinator(states, redis, ledger, archive, jobs, tx, policy, tokens, messaging::getObject, window);
    }
    @Bean public SummaryModel summaryModel(ChatModel agentChatModel, SummaryPolicy policy, MemoryTokenBudget tokens) {
        return new SummaryModel(agentChatModel, policy, tokens);
    }
    @Bean public SummaryWorker summaryWorker(JdbcSummaryJobs jobs, JdbcConversationArchive archive, RedisSummaryStateStore states,
            SummaryCoordinator coordinator, SummaryPolicy policy, SummaryModel model, MemoryTransactionProperties tx, VectorStorageService vectors) {
        return new SummaryWorker(jobs, archive, states, coordinator, policy, model, tx, vectors);
    }
    @Bean public SummaryBudgetAdvisor summaryBudgetAdvisor(SummaryPolicy policy, MemoryTokenBudget tokens, SummaryCoordinator coordinator) {
        return new SummaryBudgetAdvisor(policy, tokens, coordinator);
    }
    @Bean(initMethod = "start", destroyMethod = "close") public SummaryScanner summaryScanner(RedisSummaryStateStore states,
            JdbcSummaryJobs jobs, SummaryWorker worker, SummaryCoordinator coordinator, SummaryPolicy policy,
            ObjectProvider<RocketMqTransactionTransport> messaging) {
        return new SummaryScanner(states, jobs, worker, coordinator, policy, messaging::getObject);
    }
}
