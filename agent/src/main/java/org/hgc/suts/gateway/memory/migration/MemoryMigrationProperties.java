package org.hgc.suts.gateway.memory.migration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Offline migration only. Nothing runs unless explicitly enabled. */
@Data
@Component
@ConfigurationProperties(prefix = "suts.ai.memory.migration")
public class MemoryMigrationProperties {
    public enum Direction { FORWARD, ROLLBACK }

    private boolean enabled;
    private boolean dryRun = true;
    private boolean writesFrozen;
    private boolean writersDrained;
    private Direction direction = Direction.FORWARD;
    private String checkpointName = "default";
    private int batchSize = 100;
    private String rollbackEmbeddingHost;
    private String rollbackEmbeddingModel;
    private Integer rollbackEmbeddingDimension;
}
