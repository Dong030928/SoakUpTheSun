package org.hgc.suts.gateway.config;

import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.memory.archive.JdbcConversationArchive;
import org.springframework.ai.chat.memory.repository.jdbc.JdbcChatMemoryRepository;
import org.springframework.ai.chat.memory.repository.jdbc.MysqlChatMemoryRepositoryDialect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

/** No JDBC starter/auto-config: disabled mode neither opens a connection nor creates database beans. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "suts.ai.memory.archive.enabled", havingValue = "true")
@EnableConfigurationProperties(MemoryArchiveProperties.class)
public class MemoryArchiveConfiguration {
    @Bean
    public JdbcConversationArchive conversationArchive(MemoryArchiveProperties properties, MemoryProperties window) {
        properties.validate();
        DriverManagerDataSource dataSource = new DriverManagerDataSource(properties.getJdbcUrl(),
                properties.getUsername(), properties.getPassword());
        var connection = new java.util.Properties();
        connection.setProperty("connectTimeout", "5000");
        connection.setProperty("socketTimeout", "10000");
        dataSource.setConnectionProperties(connection);
        var flyway = Flyway.configure().dataSource(dataSource).table("agent_memory_schema_history")
                .locations("classpath:db/archive")
                .placeholders(java.util.Map.of("tableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
                .load();
        if (properties.isMigrate()) flyway.migrate();
        else flyway.validate();
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.setQueryTimeout(10);
        // Fail at startup on an unprepared Schema; no implicit fallback to cache-only guarantees.
        jdbc.queryForObject("SELECT COUNT(*) FROM agent_memory_session WHERE 1 = 0", Long.class);
        var manager = new DataSourceTransactionManager(dataSource);
        var repository = JdbcChatMemoryRepository.builder().jdbcTemplate(jdbc)
                .dialect(new MysqlChatMemoryRepositoryDialect()).transactionManager(manager).build();
        return new JdbcConversationArchive(jdbc, repository, new TransactionTemplate(manager), window,
                properties.getMaxSequenceGap());
    }
}
