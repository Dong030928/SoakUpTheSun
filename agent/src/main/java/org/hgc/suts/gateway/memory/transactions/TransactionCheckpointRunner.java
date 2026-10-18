package org.hgc.suts.gateway.memory.transactions;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Slf4j
@Component
@ConditionalOnProperty(name = "suts.ai.memory.transactions.maintenance.enabled", havingValue = "true")
@EnableConfigurationProperties({MemoryTransactionProperties.class, TransactionCheckpointRunner.Properties.class})
public class TransactionCheckpointRunner implements ApplicationRunner {
    private final TransactionCheckpointMaintenance maintenance;
    private final Properties properties;
    private final Environment environment;
    private final ConfigurableApplicationContext context;
    public TransactionCheckpointRunner(JdbcConversationArchive archive, RedisConversationMemoryRepository redis,
            StringRedisTemplate template, MemoryTransactionProperties transactions, MemoryProperties memory,
            Properties properties, Environment environment, ConfigurableApplicationContext context) {
        this.maintenance = new TransactionCheckpointMaintenance(archive, redis,
                new RedisTransactionLedger(template, transactions, new TransactionMetrics()), memory);
        this.properties = properties; this.environment = environment; this.context = context;
    }
    @Override public void run(ApplicationArguments args) {
        Assert.state("none".equalsIgnoreCase(environment.getProperty("spring.main.web-application-type"))
                        && !environment.getProperty("suts.ai.memory.transactions.enabled", Boolean.class, false)
                        && !environment.getProperty("suts.ai.memory.archive.maintenance.enabled", Boolean.class, false)
                        && !environment.getProperty("suts.ai.memory.migration.enabled", Boolean.class, false),
                "Checkpoint adoption requires an isolated offline process with all workers disabled");
        Assert.notNull(properties.action, "An explicit transaction maintenance action is required");
        boolean result = switch (properties.action) {
            case ADOPT -> maintenance.adopt(properties.userId, properties.conversationId, properties.epoch,
                    properties.producerCheckpoint, properties.writersDrained, properties.dryRun);
            case RELEASE_TO_CACHE -> maintenance.releaseToCache(properties.userId, properties.conversationId, properties.epoch,
                    properties.producerCheckpoint, properties.writersDrained, properties.dryRun);
        };
        log.info("Transaction maintenance action={}, checkpoint={}, dryRun={}, result={}", properties.action,
                properties.producerCheckpoint, properties.dryRun, result);
        context.close();
    }
    @Data
    @ConfigurationProperties("suts.ai.memory.transactions.maintenance")
    public static class Properties {
        public enum Action { ADOPT, RELEASE_TO_CACHE }
        private Action action;
        private long userId, epoch, producerCheckpoint;
        private String conversationId;
        private boolean writersDrained, dryRun = true;
    }
}
