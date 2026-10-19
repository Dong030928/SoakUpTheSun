package org.hgc.suts.gateway.memory.profile;

/** A stale optimistic version, not an authorization or storage error. */
public final class ProfileConflictException extends RuntimeException {
    public ProfileConflictException() { super("画像已更新，请刷新后重试"); }
}
