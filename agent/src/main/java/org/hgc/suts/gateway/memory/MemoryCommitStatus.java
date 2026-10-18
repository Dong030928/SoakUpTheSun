package org.hgc.suts.gateway.memory;

/** Acceptance is not proof of asynchronous database archival. */
public enum MemoryCommitStatus {
    ACCEPTED, PENDING, DEGRADED, CACHED, DISABLED;

    public String headerValue() { return name().toLowerCase(java.util.Locale.ROOT); }
    public boolean saved() { return this == ACCEPTED || this == PENDING || this == CACHED; }
}
