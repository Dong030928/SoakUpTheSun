package org.hgc.suts.gateway.common.web;

import org.hgc.suts.gateway.common.result.Result;
import org.hgc.suts.gateway.dto.resp.AiChatRespDTO;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

/** Also runs on the async dispatch; the JSON contract and controller signatures stay compatible. */
@RestControllerAdvice
public class MemoryStatusAdvice implements ResponseBodyAdvice<Object> {
    @Override public boolean supports(MethodParameter parameter, Class<? extends HttpMessageConverter<?>> converter) {
        return true;
    }
    @Override public Object beforeBodyWrite(Object body, MethodParameter parameter, MediaType mediaType,
            Class<? extends HttpMessageConverter<?>> converter, ServerHttpRequest request, ServerHttpResponse response) {
        if (body instanceof Result<?> result && result.getData() instanceof AiChatRespDTO chat && chat.getMemoryStatus() != null) {
            response.getHeaders().set("X-Agent-Memory-Status", chat.getMemoryStatus().headerValue());
        }
        return body;
    }
}
