package org.hgc.suts.gateway.tool;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hgc.suts.gateway.dto.resp.TargetRoomLinkInfoRespDTO;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/** Stateless tool definitions. Authenticated state and private results travel via ToolContext. */
@Slf4j
@Component
@RequiredArgsConstructor
public class AgentTools {
    private final VolunteerRemoteService volunteer;

    public enum ClientAction { CONNECT_WS, DISCONNECT_WS, CAPTURE_UPLOAD }

    @Tool(name = "request_client_action", description = "仅当用户明确要求时下发客户端动作：CONNECT_WS 开启避障，"
            + "DISCONNECT_WS 关闭避障，CAPTURE_UPLOAD 拍照识别。已有视觉参考足以回答时不要重新拍照。"
            + "本工具只生成客户端指令，不代表设备已执行；每轮最多一种操作。")
    public String requestClientAction(@ToolParam(description = "用户明确请求的客户端动作") ClientAction action,
                                      ToolContext context) {
        AgentToolState state = state(context);
        Assert.notNull(action, "Client action is required");
        if (!state.begin(action.name())) return state.previousResult(action.name());
        String result = switch (action) {
            case CONNECT_WS -> "已生成开启实时避障的客户端指令，等待设备执行。";
            case DISCONNECT_WS -> "已生成关闭实时避障的客户端指令，等待设备执行。";
            case CAPTURE_UPLOAD -> "已生成拍照上传指令，等待设备上传图片后再分析。";
        };
        state.complete(result, action.name(), null);
        return result;
    }

    @Tool(name = "create_volunteer_help", description = "用户明确要求发布求助、找志愿者或接通志愿视频时调用。"
            + "为当前已登录用户创建求助，无需传用户ID或Token；每轮最多一种操作，不重复创建。")
    public String createVolunteerHelp(ToolContext context) {
        AgentToolState state = state(context);
        if (!state.begin("create_volunteer_help")) return state.previousResult("create_volunteer_help");
        String failure = "志愿者服务暂时不可用，尚未取得有效的通话信息，请稍后重试。";
        try {
            var result = volunteer.createHelpTask();
            TargetRoomLinkInfoRespDTO data = result == null ? null : result.getData();
            if (result != null && result.isSuccess() && data != null
                    && StringUtils.hasText(data.getTargetRoomLink()) && StringUtils.hasText(data.getAuthTicket())) {
                String success = "志愿求助已创建，通话信息已返回客户端，等待客户端发起视频通话。";
                state.complete(success, "START_VIDEO_CALL", data);
                // Room links and auth tickets belong only in the HTTP response, never the model transcript.
                return success;
            }
        } catch (Exception ex) {
            log.warn("志愿求助调用失败 ({})", ex.getClass().getSimpleName());
        }
        state.complete(failure, "NONE", null);
        return failure;
    }

    private static AgentToolState state(ToolContext context) {
        Assert.notNull(context, "Tool context is required");
        Object value = context.getContext().get(AgentToolState.CONTEXT_KEY);
        Assert.isInstanceOf(AgentToolState.class, value, "Authenticated turn is required");
        AgentToolState state = (AgentToolState) value;
        state.verifyAuthentication();
        return state;
    }
}
