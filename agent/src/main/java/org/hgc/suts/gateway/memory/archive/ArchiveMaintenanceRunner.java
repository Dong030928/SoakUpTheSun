package org.hgc.suts.gateway.memory.archive;

import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/** No public import/restore API: operator must opt into an offline, frozen-writer process. */
@Slf4j
@Component
@ConditionalOnProperty(name = "suts.ai.memory.archive.maintenance.enabled", havingValue = "true")
@EnableConfigurationProperties(ArchiveMaintenanceProperties.class)
public class ArchiveMaintenanceRunner implements ApplicationRunner {
    private final ArchiveMaintenance operations;
    private final ArchiveMaintenanceProperties properties;
    private final Environment environment;
    private final ConfigurableApplicationContext context;

    public ArchiveMaintenanceRunner(JdbcConversationArchive archive, RedisConversationMemoryRepository redis,
            MemoryProperties memory, ArchiveMaintenanceProperties properties, Environment environment,
            ConfigurableApplicationContext context) {
        this.operations = new ArchiveMaintenance(archive, redis, memory);
        this.properties = properties;
        this.environment = environment;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        Assert.state("none".equalsIgnoreCase(environment.getProperty("spring.main.web-application-type")),
                "Archive maintenance requires --spring.main.web-application-type=none");
        Assert.state(!environment.getProperty("suts.ai.memory.migration.enabled", Boolean.class, false),
                "Do not run vector migration and archive maintenance in the same process");
        Assert.notNull(properties.getAction(), "An explicit archive maintenance action is required");
        Object result = switch (properties.getAction()) {
            case IMPORT_RECENT -> operations.importRecent(properties.getUserId(), properties.getConversationId(),
                    properties.getEpoch(), properties.isWritersDrained(), properties.isDryRun());
            case RESTORE -> operations.restore(properties.getUserId(), properties.getConversationId(), properties.getEpoch(),
                    properties.getProducerCheckpoint(), properties.isWritersDrained(), properties.isDryRun());
            case RETIRE -> operations.retireBefore(properties.getUserId(), properties.getConversationId(), properties.getEpoch(),
                    properties.getCutoff(), properties.isWritersDrained(), properties.isDryRun());
        };
        log.info("Archive maintenance action={}, dryRun={}, result={}", properties.getAction(), properties.isDryRun(), result);
        context.close();
    }
}
