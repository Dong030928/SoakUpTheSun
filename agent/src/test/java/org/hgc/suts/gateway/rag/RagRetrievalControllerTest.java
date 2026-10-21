package org.hgc.suts.gateway.rag;

import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.biz.user.UserInfoDTO;
import org.hgc.suts.gateway.controller.RagRetrievalController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

class RagRetrievalControllerTest {
    private final RagRetrievalService retrieval = mock(RagRetrievalService.class);
    private MockMvc http;
    private static final String BODY = "{\"originalQuestion\":\"原始问题\",\"query\":\"检索词\",\"scope\":\"DOCUMENT\",\"documentIds\":[\"guide\"]}";

    @BeforeEach void setup() {
        var user = new UserInfoDTO(); user.setId(42L); UserContext.setUser(user);
        http = MockMvcBuilders.standaloneSetup(new RagRetrievalController(retrieval)).build();
    }
    @AfterEach void clearUser() { UserContext.remove(); }

    @Test void ownerComesOnlyFromTheLoginContextAndClientOwnerParametersAreIgnored() throws Exception {
        when(retrieval.retrieve(eq(42L), any())).thenReturn(new RagData.Result(List.of(), new RagData.Trace(0, 0, 0, 0, List.of(), 0)));
        http.perform(post("/api/agent/rag/search").param("userId", "43").param("ownerId", "43")
                        .contentType("application/json").content(BODY)).andExpect(jsonPath("$.code").value("0"));
        verify(retrieval).retrieve(42, new RagData.Plan("原始问题", "检索词", RagData.Scope.DOCUMENT, List.of("guide")));
        verify(retrieval, never()).retrieve(eq(43L), any());
    }

    @Test void unauthenticatedAndInvalidLoginOwnersNeverReachRetrieval() throws Exception {
        UserContext.remove();
        http.perform(post("/api/agent/rag/search").contentType("application/json").content(BODY)).andExpect(jsonPath("$.code").value("400"));
        var invalid = new UserInfoDTO(); invalid.setId(-1L); UserContext.setUser(invalid);
        http.perform(post("/api/agent/rag/search").contentType("application/json").content(BODY)).andExpect(jsonPath("$.code").value("400"));
        verifyNoInteractions(retrieval);
    }

    @Test void invalidPlansAndInfrastructureFailuresHaveSanitizedResponses() throws Exception {
        when(retrieval.retrieve(anyLong(), any())).thenThrow(new IllegalArgumentException("private request contents"))
                .thenThrow(new IllegalStateException("private provider response and credentials"));
        var invalid = http.perform(post("/api/agent/rag/search").contentType("application/json").content(BODY))
                .andExpect(jsonPath("$.code").value("400")).andReturn().getResponse().getContentAsString();
        var unavailable = http.perform(post("/api/agent/rag/search").contentType("application/json").content(BODY))
                .andExpect(jsonPath("$.code").value("503")).andReturn().getResponse().getContentAsString();
        assertThat(invalid).doesNotContain("private request contents");
        assertThat(unavailable).doesNotContain("private provider response", "credentials");
    }

    @Test void malformedJsonAndUnknownScopeAreClientErrorsBeforeRetrieval() throws Exception {
        for (String body : List.of("{\"originalQuestion\":", BODY.replace("\"DOCUMENT\"", "\"UNKNOWN\""))) {
            http.perform(post("/api/agent/rag/search").contentType("application/json").content(body))
                    .andExpect(jsonPath("$.code").value("400"));
        }
        verifyNoInteractions(retrieval);
    }
}
