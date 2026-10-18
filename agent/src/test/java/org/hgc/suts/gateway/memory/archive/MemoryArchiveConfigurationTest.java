package org.hgc.suts.gateway.memory.archive;

import org.hgc.suts.gateway.config.MemoryArchiveConfiguration;
import org.hgc.suts.gateway.config.MemoryArchiveProperties;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.*;

class MemoryArchiveConfigurationTest {
    @Test void disabledModeCreatesNoDatabaseInfrastructureOrArchive() {
        new ApplicationContextRunner().withUserConfiguration(MemoryArchiveConfiguration.class)
                .withBean(MemoryProperties.class, MemoryProperties::new)
                .run(context -> {
                    assertThat(context).hasNotFailed().doesNotHaveBean(JdbcConversationArchive.class)
                            .doesNotHaveBean(javax.sql.DataSource.class);
                });
    }

    @Test void enabledModeFailsFastWithoutExplicitConfiguration() {
        new ApplicationContextRunner().withUserConfiguration(MemoryArchiveConfiguration.class)
                .withBean(MemoryProperties.class, MemoryProperties::new)
                .withPropertyValues("suts.ai.memory.archive.enabled=true")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test void jdbcConfigurationDoesNotAcceptAnotherVendorOrInvalidGap() {
        var properties = new MemoryArchiveProperties();
        properties.setJdbcUrl("jdbc:h2:mem:unexpected");
        properties.setUsername("owner");
        assertThatThrownBy(properties::validate).hasMessageContaining("MySQL");
        properties.setJdbcUrl("jdbc:mysql://localhost/archive");
        properties.setMaxSequenceGap(0);
        assertThatThrownBy(properties::validate).hasMessageContaining("gap");
    }
}
