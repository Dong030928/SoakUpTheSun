package org.hgc.suts.gateway.controller;

import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.result.Result;
import org.hgc.suts.gateway.common.web.Results;
import org.hgc.suts.gateway.rag.RagData;
import org.hgc.suts.gateway.rag.RagRetrievalService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/agent/rag")
@ConditionalOnProperty(name = "suts.ai.rag.enabled", havingValue = "true")
public class RagRetrievalController {
    private final RagRetrievalService retrieval;
    public RagRetrievalController(RagRetrievalService retrieval) { this.retrieval = retrieval; }
    @PostMapping("/search") public Result<RagData.Result> search(@RequestBody RagData.Plan plan) {
        Long user = UserContext.getUserId();
        if (user == null || user <= 0) throw new IllegalArgumentException("Authentication required");
        return Results.success(retrieval.retrieve(user, plan));
    }
    @ExceptionHandler({IllegalArgumentException.class, org.springframework.http.converter.HttpMessageNotReadableException.class})
    public Result<Void> invalid() {
        return new Result<Void>().setCode("400").setMessage("检索请求无效");
    }
    @ExceptionHandler(RuntimeException.class) public Result<Void> unavailable() {
        return new Result<Void>().setCode("503").setMessage("检索暂不可用，请稍后重试");
    }
}
