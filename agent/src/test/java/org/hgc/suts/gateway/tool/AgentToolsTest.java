package org.hgc.suts.gateway.tool;

import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.biz.user.UserInfoDTO;
import org.hgc.suts.gateway.common.web.Results;
import org.hgc.suts.gateway.dto.resp.TargetRoomLinkInfoRespDTO;
import org.hgc.suts.gateway.remote.VolunteerRemoteService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ToolContext;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AgentToolsTest {
    private final VolunteerRemoteService volunteer = mock(VolunteerRemoteService.class);
    private final AgentTools tools = new AgentTools(volunteer);

    @AfterEach
    void cleanup() { UserContext.remove(); }

    @Test
    void missingOrMismatchedAuthenticationCannotExecuteTools() {
        var turn = new AgentToolState(42L, "user-42-token");
        var context = context(turn);
        assertThatThrownBy(() -> tools.createVolunteerHelp(context)).isInstanceOf(IllegalStateException.class);
        authenticate(43L);
        assertThatThrownBy(() -> tools.requestClientAction(AgentTools.ClientAction.CONNECT_WS, context))
                .isInstanceOf(IllegalStateException.class);
        authenticate(42L);
        UserContext.setToken("wrong-token");
        assertThatThrownBy(() -> tools.createVolunteerHelp(context)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> tools.createVolunteerHelp(new ToolContext(Map.of())))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(volunteer);
    }

    @Test
    void independentTurnsCanRunConcurrentlyWithoutSharingCommandsOrTickets() throws Exception {
        var bothEntered = new CountDownLatch(2);
        when(volunteer.createHelpTask()).thenAnswer(invocation -> {
            Long user = UserContext.getUserId();
            bothEntered.countDown();
            assertThat(bothEntered.await(5, TimeUnit.SECONDS)).isTrue();
            return Results.success(new TargetRoomLinkInfoRespDTO("https://example.invalid/room/" + user, "ticket-" + user));
        });
        var workers = Executors.newFixedThreadPool(2);
        try {
            var first = workers.submit(() -> help(42L));
            var second = workers.submit(() -> help(43L));
            var firstResult = first.get(10, TimeUnit.SECONDS);
            var secondResult = second.get(10, TimeUnit.SECONDS);
            assertThat(((TargetRoomLinkInfoRespDTO) firstResult.getCommandData()).getAuthTicket()).isEqualTo("ticket-42");
            assertThat(((TargetRoomLinkInfoRespDTO) secondResult.getCommandData()).getAuthTicket()).isEqualTo("ticket-43");
            assertThat(firstResult.getClientCommand()).isEqualTo("START_VIDEO_CALL");
            assertThat(secondResult.getClientCommand()).isEqualTo("START_VIDEO_CALL");
            verify(volunteer, times(2)).createHelpTask();
        } finally { workers.shutdownNow(); }
    }

    @Test
    void laterTurnsDoNotInheritAnEarlierClientCommand() {
        authenticate(42L);
        var first = new AgentToolState(42L, UserContext.getToken());
        tools.requestClientAction(AgentTools.ClientAction.CAPTURE_UPLOAD, context(first));
        var next = new AgentToolState(42L, UserContext.getToken());
        assertThat(first.response("拍照").getClientCommand()).isEqualTo("CAPTURE_UPLOAD");
        assertThat(next.response("你好").getClientCommand()).isEqualTo("NONE");
        assertThat(next.response("你好").getCommandData()).isNull();
    }

    @Test
    void lostConversationLeasePreventsNewSideEffects() {
        authenticate(42L);
        var turn = new AgentToolState(42L, UserContext.getToken(), () -> {
            throw new org.hgc.suts.gateway.memory.MemoryLeaseLostException();
        });
        assertThatThrownBy(() -> tools.createVolunteerHelp(context(turn)))
                .isInstanceOf(org.hgc.suts.gateway.memory.MemoryLeaseLostException.class);
        assertThat(turn.response("lease lost").getClientCommand()).isEqualTo("NONE");
        verifyNoInteractions(volunteer);
    }

    private org.hgc.suts.gateway.dto.resp.AiChatRespDTO help(Long userId) {
        authenticate(userId);
        try {
            var state = new AgentToolState(userId, UserContext.getToken());
            String result = tools.createVolunteerHelp(context(state));
            assertThat(result).doesNotContain("ticket-", "https://example.invalid/room/");
            return state.response(result);
        } finally { UserContext.remove(); }
    }

    private static ToolContext context(AgentToolState state) { return new ToolContext(Map.of(AgentToolState.CONTEXT_KEY, state)); }

    private static void authenticate(Long id) {
        UserContext.setUser(UserInfoDTO.builder().id(id).build());
        UserContext.setToken("user-" + id + "-token");
    }
}
