package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.biz.user.UserInfoDTO;
import org.hgc.suts.gateway.controller.ProfileMemoryController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.mockito.Mockito.*;
import static org.hgc.suts.gateway.memory.profile.ProfileData.*;
import static org.hgc.suts.gateway.memory.profile.ProfileFixture.*;

class ProfileControllerTest {
    private final ProfileFixture f = new ProfileFixture();
    private final ProfileVectorIndex vectors = mock(ProfileVectorIndex.class);
    private MockMvc http;
    @BeforeEach void setup() {
        var user = new UserInfoDTO(); user.setId(42L); UserContext.setUser(user);
        http = MockMvcBuilders.standaloneSetup(new ProfileMemoryController(f.profiles, vectors)).build();
        f.apply(f.event("我喜欢绿茶"), tea("绿茶", 1, "我喜欢绿茶"));
    }
    @AfterEach void clearUser() { UserContext.remove(); }
    @Test void listsOwnFactsAndIgnoresCallerSuppliedOwner() throws Exception {
        http.perform(get("/api/agent/profile").param("userId", "43")).andExpect(jsonPath("$.data[0].userId").value(42));
        var user = new UserInfoDTO(); user.setId(43L); UserContext.setUser(user);
        http.perform(get("/api/agent/profile")).andExpect(jsonPath("$.data").isEmpty());
    }
    @Test void correctionAndForgetExposeStableVersionsAndHideDeletedFactFromDefaultList() throws Exception {
        String id = factId(42, "preference.drink");
        http.perform(post("/api/agent/profile/" + id + "/correct").contentType("application/json")
                .content("{\"requestId\":\"correction-http\",\"expectedVersion\":1,\"value\":\"红茶\",\"statement\":\"我喜欢红茶\"}"))
                .andExpect(jsonPath("$.data.version").value(2));
        http.perform(post("/api/agent/profile/" + id + "/forget").contentType("application/json")
                .content("{\"requestId\":\"forget-http\",\"expectedVersion\":2,\"statement\":\"忘掉饮品偏好\"}"))
                .andExpect(jsonPath("$.data.state").value("DELETED"));
        http.perform(get("/api/agent/profile")).andExpect(jsonPath("$.data").isEmpty());
        http.perform(get("/api/agent/profile/" + id + "/versions")).andExpect(jsonPath("$.data.length()").value(3));
    }
    @Test void staleVersionReturnsConflictWithoutChangingFact() throws Exception {
        String id = factId(42, "preference.drink");
        http.perform(post("/api/agent/profile/" + id + "/forget").contentType("application/json")
                .content("{\"requestId\":\"forget-stale\",\"expectedVersion\":2,\"statement\":\"忘掉饮品偏好\"}"))
                .andExpect(jsonPath("$.code").value("409"));
        assertThat(f.profiles.fact(42, id).orElseThrow().version()).isEqualTo(1);
    }
    @Test void invalidQueryDoesNotReachSqlOrVectorIndex() throws Exception {
        http.perform(get("/api/agent/profile").param("limit", "10000")).andExpect(jsonPath("$.code").value("400"));
        UserContext.remove();
        http.perform(get("/api/agent/profile")).andExpect(jsonPath("$.code").value("400"));
        verifyNoInteractions(vectors);
    }
}
