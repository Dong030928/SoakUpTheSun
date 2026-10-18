package org.hgc.suts.gateway.memory.archive;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.Assert;

/** Explicit port + fixture marker: never run migrations against an unmarked user database. */
class MysqlArchiveIT extends ArchiveContract {
    @org.junit.jupiter.api.Test
    void optInProductionConfigurationValidatesThePreparedMysqlSchema() {
        new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withUserConfiguration(org.hgc.suts.gateway.config.MemoryArchiveConfiguration.class)
                .withBean(org.hgc.suts.gateway.config.MemoryProperties.class, org.hgc.suts.gateway.config.MemoryProperties::new)
                .withPropertyValues("suts.ai.memory.archive.enabled=true",
                        "suts.ai.memory.archive.jdbc-url=jdbc:mysql://127.0.0.1:" + System.getProperty("mysql.archive.port")
                                + "/agent_archive_it?allowPublicKeyRetrieval=true&useSSL=false",
                        "suts.ai.memory.archive.username=root", "suts.ai.memory.archive.password=agent-archive-integration-only")
                .run(context -> {
                    org.assertj.core.api.Assertions.assertThat(context).hasNotFailed();
                    var target = context.getBean(JdbcConversationArchive.class);
                    target.archive(turn(1));
                    org.assertj.core.api.Assertions.assertThat(target.snapshot(owner, id, 1).orElseThrow().matchesCheckpoint(1)).isTrue();
                    org.assertj.core.api.Assertions.assertThat(target.statistics().appliedEvents()).isEqualTo(1);
                });
    }
    private static ArchiveDatabaseFixture fixture;

    @Override protected synchronized ArchiveDatabaseFixture database() {
        if (fixture == null) {
            String port = System.getProperty("mysql.archive.port");
            Assert.isTrue(port != null && port.matches("[0-9]{1,5}") && Integer.parseInt(port) > 0
                    && Integer.parseInt(port) < 65536, "Pass an explicit private MySQL fixture port");
            var source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port
                    + "/agent_archive_it?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=5000&socketTimeout=10000",
                    "root", "agent-archive-integration-only");
            String marker = new JdbcTemplate(source).queryForObject("SELECT marker FROM suts_agent_test_fixture", String.class);
            Assert.state("agent-archive-integration-only".equals(marker), "Refusing an unmarked MySQL database");
            fixture = new ArchiveDatabaseFixture(source, true);
        }
        return fixture;
    }
}
