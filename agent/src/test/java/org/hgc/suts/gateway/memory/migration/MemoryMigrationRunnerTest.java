package org.hgc.suts.gateway.memory.migration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class MemoryMigrationRunnerTest {
    private final MemoryMigrationService migration = mock(MemoryMigrationService.class);
    private final MemoryMigrationProperties properties = new MemoryMigrationProperties();
    private final ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);

    @Test
    void refusesToStartAlongsideTheWebApplication() {
        var runner = new MemoryMigrationRunner(migration, properties, context, new MockEnvironment());
        assertThatThrownBy(() -> runner.run(new DefaultApplicationArguments()))
                .hasMessageContaining("web-application-type=none");
        verifyNoInteractions(migration, context);
    }

    @Test
    void successfulOfflineRunClosesTheProcessAfterReporting() {
        var environment = new MockEnvironment().withProperty("spring.main.web-application-type", "none");
        var runner = new MemoryMigrationRunner(migration, properties, context, environment);
        when(migration.migrate(properties))
                .thenReturn(new MemoryMigrationService.Report(2, 0, 0, 2, 20, true, true));
        runner.run(new DefaultApplicationArguments());
        var order = inOrder(migration, context);
        order.verify(migration).migrate(properties);
        order.verify(context).close();
    }
}
