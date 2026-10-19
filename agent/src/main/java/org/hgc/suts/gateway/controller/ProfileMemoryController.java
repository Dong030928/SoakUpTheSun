package org.hgc.suts.gateway.controller;

import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.result.Result;
import org.hgc.suts.gateway.common.web.Results;
import org.hgc.suts.gateway.memory.profile.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** Independent authenticated profile API; it does not expose arbitrary owner IDs or affect chat DTOs. */
@RestController
@RequestMapping("/api/agent/profile")
@ConditionalOnProperty(name = "suts.ai.memory.profile.enabled", havingValue = "true")
public class ProfileMemoryController {
    private final JdbcProfileStore profiles;
    private final ProfileVectorIndex vectors;
    public ProfileMemoryController(JdbcProfileStore profiles, ProfileVectorIndex vectors) { this.profiles = profiles; this.vectors = vectors; }
    private long owner() {
        Long user = UserContext.getUserId();
        if (user == null || user <= 0) throw new IllegalArgumentException("用户未登录");
        return user;
    }
    @GetMapping public Result<List<ProfileData.Fact>> list(@RequestParam(defaultValue = "false") boolean includeInactive,
                                                          @RequestParam(defaultValue = "20") int limit) {
        return Results.success(profiles.list(owner(), includeInactive, limit));
    }
    @GetMapping("/{factId}/versions") public Result<List<ProfileData.Fact>> versions(@PathVariable String factId,
            @RequestParam(defaultValue = "9223372036854775807") long beforeVersion, @RequestParam(defaultValue = "20") int limit) {
        return Results.success(profiles.versions(owner(), factId, beforeVersion, limit));
    }
    @PostMapping("/search") public Result<List<ProfileData.Fact>> search(@RequestBody Search request) {
        return Results.success(vectors.search(owner(), request.query(), request.limit()));
    }
    @PostMapping("/{factId}/correct") public Result<ProfileData.Fact> correct(@PathVariable String factId, @RequestBody Correction request) {
        return Results.success(profiles.command(owner(), factId, request.expectedVersion(), request.requestId(), request.value(), request.statement(), false));
    }
    @PostMapping("/{factId}/forget") public Result<ProfileData.Fact> forget(@PathVariable String factId, @RequestBody Forget request) {
        return Results.success(profiles.command(owner(), factId, request.expectedVersion(), request.requestId(), null, request.statement(), true));
    }
    @GetMapping("/{factId}/projections") public Result<List<ProfileData.Projection>> projections(@PathVariable String factId) {
        return Results.success(profiles.projectionStatus(owner(), factId));
    }
    @PostMapping("/reindex") public Result<Integer> reindex() { return Results.success(profiles.reindex(owner())); }
    @GetMapping("/extractions") public Result<List<ProfileData.ExtractionStatus>> extractions(@RequestParam(defaultValue = "20") int limit) {
        return Results.success(profiles.extractionStatus(owner(), limit));
    }
    @ExceptionHandler(ProfileConflictException.class) public Result<Void> conflict() { return new Result<Void>().setCode("409").setMessage("画像已更新，请刷新后重试"); }
    @ExceptionHandler(IllegalArgumentException.class) public Result<Void> invalid() { return new Result<Void>().setCode("400").setMessage("画像请求参数无效或记录不存在"); }
    public record Search(String query, int limit) { }
    public record Correction(String requestId, long expectedVersion, String value, String statement) { }
    public record Forget(String requestId, long expectedVersion, String statement) { }
}
