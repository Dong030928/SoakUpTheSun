package org.hgc.suts.gateway.config;

import org.hgc.suts.gateway.memory.ConversationMemoryStore;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.hgc.suts.gateway.memory.transactions.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "suts.ai.memory.transactions.enabled", havingValue = "true")
@EnableConfigurationProperties(MemoryTransactionProperties.class)
public class MemoryTransactionConfiguration {
    @Bean public TransactionMetrics transactionMetrics(MemoryTransactionProperties properties, MemoryProperties memory,
                                                       Environment environment) {
        properties.validate(environment.getProperty("suts.ai.memory.archive.enabled", Boolean.class, false), memory.isWritesEnabled());
        Assert.state(!environment.containsProperty("spring.data.redis.cluster.nodes"),
                "M02 reconciliation currently requires a dedicated standalone Redis deployment");
        Assert.state(!environment.getProperty("suts.ai.memory.archive.maintenance.enabled", Boolean.class, false)
                        && !environment.getProperty("suts.ai.memory.migration.enabled", Boolean.class, false),
                "Stop and drain transaction workers before offline maintenance");
        return new TransactionMetrics();
    }
    @Bean(destroyMethod = "close") public RedisTransactionLedger redisTransactionLedger(StringRedisTemplate redis,
            MemoryTransactionProperties properties, TransactionMetrics metrics) {
        return new RedisTransactionLedger(redis, properties, metrics);
    }
    @Bean public ArchiveMessageConsumer archiveMessageConsumer(JdbcConversationArchive archive,
            RedisTransactionLedger ledger, MemoryTransactionProperties properties, TransactionMetrics metrics) {
        return new ArchiveMessageConsumer(archive, ledger, properties, metrics);
    }
    @Bean(initMethod = "start", destroyMethod = "close") public RocketMqTransactionTransport transactionTransport(
            MemoryTransactionProperties properties, RedisTransactionLedger ledger, ArchiveMessageConsumer listener, TransactionMetrics metrics) {
        return new RocketMqTransactionTransport(properties, ledger, listener, metrics);
    }
    @Bean @Primary public ConversationMemoryStore transactionalConversationMemoryStore(RedisConversationMemoryRepository redis,
            RedisTransactionLedger ledger, JdbcConversationArchive archive, TransactionTransport transport,
            MemoryTransactionProperties properties, @Value("${suts.ai.memory.ttl-seconds:86400}") long ttlSeconds) {
        return new TransactionalConversationMemoryStore(redis, ledger, archive, transport, properties, Math.multiplyExact(ttlSeconds, 1000));
    }
    @Bean(initMethod = "start", destroyMethod = "close") public ArchiveReconciler archiveReconciler(RedisTransactionLedger ledger,
            JdbcConversationArchive archive, TransactionTransport transport, MemoryTransactionProperties properties, TransactionMetrics metrics) {
        return new ArchiveReconciler(ledger, archive, transport, properties, metrics);
    }
}
