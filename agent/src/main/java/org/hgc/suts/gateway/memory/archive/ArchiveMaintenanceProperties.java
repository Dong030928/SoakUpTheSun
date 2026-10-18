package org.hgc.suts.gateway.memory.archive;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("suts.ai.memory.archive.maintenance")
public class ArchiveMaintenanceProperties {
    public enum Action { IMPORT_RECENT, RESTORE, RETIRE }
    private boolean enabled;
    private Action action;
    private long userId;
    private String conversationId;
    private long epoch;
    private long producerCheckpoint;
    private long cutoff;
    private boolean writersDrained;
    private boolean dryRun = true;
}
