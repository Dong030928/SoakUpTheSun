package org.hgc.suts.gateway.memory.archive;

import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArchiveMaintenanceRunnerTest {
    @Test void rejectsWebModeBeforeOpeningStorage() {
        var redis = mock(RedisConversationMemoryRepository.class);
        var runner = new ArchiveMaintenanceRunner(mock(JdbcConversationArchive.class), redis, new MemoryProperties(),
                new ArchiveMaintenanceProperties(), new MockEnvironment(), mock(ConfigurableApplicationContext.class));
        assertThatThrownBy(() -> runner.run(new DefaultApplicationArguments())).hasMessageContaining("web-application-type=none");
        verifyNoInteractions(redis);
    }

    @Test void refusesTwoDifferentMigrationCommandsInTheSameProcess() {
        var environment = new MockEnvironment().withProperty("spring.main.web-application-type", "none")
                .withProperty("suts.ai.memory.migration.enabled", "true");
        var runner = new ArchiveMaintenanceRunner(mock(JdbcConversationArchive.class), mock(RedisConversationMemoryRepository.class),
                new MemoryProperties(), new ArchiveMaintenanceProperties(), environment, mock(ConfigurableApplicationContext.class));
        assertThatThrownBy(() -> runner.run(new DefaultApplicationArguments())).hasMessageContaining("same process");
    }

    @Test void requiresExplicitActionRatherThanDefaultingToImportOrDeletion() {
        var runner = new ArchiveMaintenanceRunner(mock(JdbcConversationArchive.class), mock(RedisConversationMemoryRepository.class),
                new MemoryProperties(), new ArchiveMaintenanceProperties(),
                new MockEnvironment().withProperty("spring.main.web-application-type", "none"), mock(ConfigurableApplicationContext.class));
        assertThatThrownBy(() -> runner.run(new DefaultApplicationArguments())).hasMessageContaining("explicit");
    }
}
