package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.MemorySummaryProperties;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.function.Supplier;

/** Rebind on each use so a changed model/policy invalidates stale scanner observations. */
public final class SummaryPolicy {
    private final Supplier<MemorySummaryProperties> source;
    private final AiConfig ai;
    public SummaryPolicy(Supplier<MemorySummaryProperties> source, AiConfig ai) { this.source = source; this.ai = ai; }
    public MemorySummaryProperties current() {
        var properties = source.get();
        properties.validate(true, ai.getTimeout());
        return properties;
    }
    public Selected selected() {
        var p = current();
        var cache = p.getCachePolicies().stream().filter(c -> c.getProvider().equals(p.getProvider())
                && c.getModel().equals(ai.getModelName()) && c.getMode().equals(p.getCacheMode())).findFirst().orElse(null);
        String identity = p.getProvider() + "|" + ai.getModelName() + "|" + p.getCacheMode() + "|"
                + (cache == null ? "TTL_DISABLED" : cache.getVersion() + "|" + cache.getRenewal() + "|"
                + cache.getTtlSeconds() + "|" + cache.getClockOrigin() + "|" + cache.getVendorBasis());
        return new Selected(hash(identity), p.getProvider(), ai.getModelName(), p.getCacheMode(), cache);
    }
    public record Selected(String fingerprint, String provider, String model, String mode,
                           MemorySummaryProperties.CachePolicy cache) {
        public Description description() {
            return new Description(provider, model, mode, cache == null ? "" : cache.getVersion(),
                    cache == null ? "" : cache.getVendorBasis(), cache == null ? null : cache.getRenewal(),
                    cache == null ? 0 : cache.getTtlSeconds(), cache == null ? null : cache.getClockOrigin());
        }
    }
    public record Description(String provider, String model, String mode, String version, String vendorBasis,
                              MemorySummaryProperties.Renewal renewal, long ttlSeconds, MemorySummaryProperties.ClockOrigin clockOrigin) {
        public boolean sameClock(Description other) {
            return other != null && provider.equals(other.provider) && model.equals(other.model)
                    && mode.equals(other.mode) && renewal == other.renewal && clockOrigin == other.clockOrigin;
        }
    }
    public static String hash(String text) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }
}
