package org.hgc.suts.gateway.memory.migration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Explicitly enabled offline CLI; the process closes its context after a successful report. */
@Slf4j
@Component
@ConditionalOnProperty(name = "suts.ai.memory.migration.enabled", havingValue = "true")
public class MemoryMigrationRunner implements ApplicationRunner {
    private final MemoryMigrationService migration;
    private final MemoryMigrationProperties properties;
    private final ConfigurableApplicationContext context;
    private final Environment environment;

    public MemoryMigrationRunner(MemoryMigrationService migration, MemoryMigrationProperties properties,
            ConfigurableApplicationContext context, Environment environment) {
        this.migration = migration;
        this.properties = properties;
        this.context = context;
        this.environment = environment;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!"none".equalsIgnoreCase(environment.getProperty("spring.main.web-application-type", ""))) {
            throw new IllegalStateException("Run memory migration offline with --spring.main.web-application-type=none");
        }
        var report = migration.migrate(properties);
        log.info("Memory migration direction={}, dryRun={}, scannedAttempts={}, written={}, skipped={}, planned={}, "
                        + "embeddingTextCharacters={}, completed={}", properties.getDirection(), report.dryRun(),
                report.scannedAttempts(), report.written(), report.skipped(), report.planned(),
                report.embeddingTextCharacters(), report.completed());
        context.close();
    }
}
