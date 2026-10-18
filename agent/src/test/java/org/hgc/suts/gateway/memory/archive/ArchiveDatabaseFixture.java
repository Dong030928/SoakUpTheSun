package org.hgc.suts.gateway.memory.archive;

import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.memory.repository.jdbc.JdbcChatMemoryRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.Map;
import java.util.UUID;

public final class ArchiveDatabaseFixture {
    public final DataSource dataSource;
    public final JdbcTemplate jdbc;
    public final ChatMemoryRepository snapshots;
    public final TransactionTemplate transactions;
    public final MemoryProperties window = new MemoryProperties();
    public final JdbcConversationArchive archive;

    public static ArchiveDatabaseFixture h2() {
        return new ArchiveDatabaseFixture(new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1", "sa", ""), false);
    }

    public ArchiveDatabaseFixture(DataSource dataSource, boolean mysql) {
        this.dataSource = dataSource;
        Flyway.configure().dataSource(dataSource).table("agent_memory_schema_history").locations("classpath:db/archive")
                .baselineOnMigrate(mysql).baselineVersion("0")
                .placeholders(Map.of("tableOptions", mysql ? "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin" : ""))
                .load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        jdbc.setQueryTimeout(10);
        var manager = new DataSourceTransactionManager(dataSource);
        transactions = new TransactionTemplate(manager);
        snapshots = JdbcChatMemoryRepository.builder().jdbcTemplate(jdbc).transactionManager(manager).build();
        archive = new JdbcConversationArchive(jdbc, snapshots, transactions, window, 10000);
    }
}
