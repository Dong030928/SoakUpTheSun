package org.hgc.suts.gateway.memory.profile;

import org.flywaydb.core.Flyway;
import org.hgc.suts.gateway.config.MemoryProfileConfiguration;
import org.hgc.suts.gateway.config.MemoryProfileProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

class ProfileConfigurationTest {
    @Test void disabledFeatureDoesNotCreateSqlOrVectorInfrastructure() {
        new ApplicationContextRunner().withUserConfiguration(MemoryProfileConfiguration.class).run(context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(JdbcProfileStore.class).doesNotHaveBean(ProfileVectorIndex.class));
    }
    @Test void profileNamespaceAndCandidateBoundsAreValidated() {
        var config = new MemoryProfileProperties(); config.validate();
        config.setPrefix("suts:agent:memory:vector:v2:");
        assertThatThrownBy(config::validate).hasMessageContaining("namespace");
        config.setPrefix("suts:agent:profile:vector:v1:"); config.setMaxCandidates(100000);
        assertThatThrownBy(config::validate).hasMessageContaining("capacity");
    }
    @Test void projectionTargetIncludesThePhysicalIndexNamespace() {
        var ai = new org.hgc.suts.gateway.config.AiConfig(); ai.setEmbeddingEnabled(false);
        var first = new MemoryProfileProperties(); var next = new MemoryProfileProperties();
        next.setPrefix("suts:agent:profile:other:");
        assertThat(ProfileVectorIndex.target(ai, first)).isNotEqualTo(ProfileVectorIndex.target(ai, next));
    }
    @Test void v3MigrationKeepsV2ResultsAndOriginalHistoryWithoutRewritingThem() {
        var source = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        Flyway.configure().dataSource(source).table("agent_memory_schema_history").locations("classpath:db/archive")
                .placeholders(Map.of("tableOptions", "")).target("2").load().migrate();
        var jdbc = new JdbcTemplate(source);
        jdbc.update("INSERT INTO agent_memory_job (job_id,conversation_id,user_id,epoch,base_version,covered_from,covered_to,input_body,state,result_text,updated_at) "
                + "VALUES ('old-job','u:42:old',42,1,0,1,1,'old immutable input','RESULT','旧格式纯摘要',1)");
        Flyway.configure().dataSource(source).table("agent_memory_schema_history").locations("classpath:db/archive")
                .placeholders(Map.of("tableOptions", "")).load().migrate();
        assertThat(jdbc.queryForObject("SELECT result_text FROM agent_memory_job WHERE job_id='old-job'", String.class)).isEqualTo("旧格式纯摘要");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM agent_profile_fact", Long.class)).isZero();
    }
    @Test void existingIndexMustHaveCorrectTypesAndPathsForProfileFilters() {
        List<Object> fields = new ArrayList<>();
        for (String key : List.of("factId", "factVersion", "targetTag", "recordType")) fields.add(Map.of(
                "attribute", key, "type", key.equals("factVersion") ? "NUMERIC" : "TAG", "identifier", "$." + key));
        fields.add(Map.of("attribute", "embedding", "algorithm", "HNSW"));
        ProfileVectorIndex.validateProfileFields(Map.of("attributes", fields));
        fields.set(0, Map.of("attribute", "factId", "type", "TEXT", "identifier", "$.other"));
        assertThatThrownBy(() -> ProfileVectorIndex.validateProfileFields(Map.of("attributes", fields)))
                .hasMessageContaining("invalid profile field");
    }
}
