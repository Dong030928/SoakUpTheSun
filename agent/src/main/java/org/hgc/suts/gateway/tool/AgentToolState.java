package org.hgc.suts.gateway.tool;

import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.dto.resp.AiChatRespDTO;
import org.hgc.suts.gateway.dto.resp.TargetRoomLinkInfoRespDTO;
import org.springframework.util.Assert;

import java.util.Objects;

/** One authenticated turn; never registered as a singleton or serialized to the model. */
public final class AgentToolState {
    public static final String CONTEXT_KEY = "suts.agent.turn";
    private final Long userId;
    private final String token;
    private String operation;
    private String toolResult;
    private String clientCommand = "NONE";
    private TargetRoomLinkInfoRespDTO commandData;

    public AgentToolState(Long userId, String token) {
        Assert.notNull(userId, "Authenticated user is required");
        this.userId = userId;
        this.token = token;
    }

    void verifyAuthentication() {
        Assert.state(userId.equals(UserContext.getUserId()) && Objects.equals(token, UserContext.getToken()),
                "Tool authentication does not match this turn");
    }

    /** A repeated call reuses its result; conflicting actions cannot replace an executed action. */
    boolean begin(String requestedOperation) {
        if (operation != null) return false;
        operation = requestedOperation;
        return true;
    }

    String previousResult(String requestedOperation) {
        return operation.equals(requestedOperation) ? toolResult : "本轮已有其他操作，请勿重复或切换操作。";
    }

    void complete(String result, String command, TargetRoomLinkInfoRespDTO data) {
        toolResult = result;
        clientCommand = command;
        commandData = data;
    }

    public boolean hasToolResult() { return toolResult != null; }

    public String fallbackText() { return toolResult; }

    public AiChatRespDTO response(String text) {
        return AiChatRespDTO.builder().text(text).clientCommand(clientCommand).commandData(commandData).build();
    }
}
