package org.hgc.suts.gateway.memory.transactions;

import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.config.MemoryTransactionConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.*;

class MemoryTransactionConfigurationTest {
    @Test void defaultModeNeedsNoMqOrArchiveBeans() {
        new ApplicationContextRunner().withUserConfiguration(MemoryTransactionConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(RedisTransactionLedger.class)
                    .doesNotHaveBean(TransactionTransport.class);
        });
    }
    @Test void explicitEnableCannotSilentlyFallbackWhenArchiveIsMissing() {
        new ApplicationContextRunner().withUserConfiguration(MemoryTransactionConfiguration.class)
                .withBean(MemoryProperties.class, MemoryProperties::new)
                .withPropertyValues("suts.ai.memory.transactions.enabled=true").run(context -> assertThat(context).hasFailed());
    }
}
