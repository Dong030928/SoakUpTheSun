package org.hgc.suts.gateway.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Data
@Component
@ConfigurationProperties(prefix = "suts.ai.memory")
public class MemoryProperties {
    private boolean writesEnabled = true;
    private int maxMessages = 20;
    private int maxInputChars = 4000;
    private int maxMessageChars = 4000;
    private int maxContextChars = 12000;
    private int longTermChars = 2000;

    @PostConstruct
    public void validate() {
        Assert.isTrue(maxMessages >= 2 && maxMessages <= 200, "suts.ai.memory.max-messages must be 2..200");
        Assert.isTrue(maxInputChars > 0 && maxInputChars <= 65536 && maxMessageChars > 0 && maxMessageChars <= 65536
                        && maxContextChars >= maxInputChars + 1000 && maxContextChars <= 262144,
                "suts.ai.memory text budgets must be positive, with room for the system prompt");
        Assert.isTrue(longTermChars >= 0 && longTermChars < maxContextChars,
                "suts.ai.memory.long-term-chars must fit the context budget");
    }
}
