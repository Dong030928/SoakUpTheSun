package org.hgc.suts.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

@Data
@ConfigurationProperties("suts.ai.memory.archive")
public class MemoryArchiveProperties {
    private boolean enabled;
    private String jdbcUrl;
    private String username;
    private String password;
    private boolean migrate;
    private int maxSequenceGap = 10000;

    public void validate() {
        Assert.isTrue(jdbcUrl != null && jdbcUrl.startsWith("jdbc:mysql://"), "Archive requires an explicit MySQL JDBC URL");
        Assert.hasText(username, "Archive username is required");
        Assert.isTrue(maxSequenceGap >= 1 && maxSequenceGap <= 100000, "Archive sequence gap must be 1..100000");
    }
}
