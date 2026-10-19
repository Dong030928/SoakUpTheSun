package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.memory.summary.*;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.*;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.deepseek.DeepSeekChatOptions;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;
import static org.hgc.suts.gateway.memory.profile.ProfileData.*;
import static org.hgc.suts.gateway.memory.profile.ProfileFixture.*;

class ProfileSummaryTest {
    private final ProfileFixture f = new ProfileFixture();
    @Test void sameToolFreeSpringAiCallProducesPlainSummaryAndValidatedStructuredCandidates() {
        var event = f.event("我喜欢绿茶"); var chat = model(json(new Envelope("用户喜欢绿茶", List.of(tea("绿茶", 1, "我喜欢绿茶")))));
        var summary = new SummaryModel(chat, new SummaryPolicy(SummaryTestFixture::config, SummaryTestFixture.ai()), new MemoryTokenBudget(), f.support);
        var result = summary.summarize(event);
        assertThat(result.text()).isEqualTo("用户喜欢绿茶"); assertThat(result.profileRejected()).isFalse();
        assertThat(JSON.readValue(result.profilePayload(), Candidate[].class)).hasSize(1);
        var prompt = org.mockito.ArgumentCaptor.forClass(Prompt.class); verify(chat).call(prompt.capture());
        assertThat(prompt.getValue().getContents()).contains("profileChanges", "completeTurns", "sourceSequence");
        var options = (org.springframework.ai.model.tool.ToolCallingChatOptions) prompt.getValue().getOptions();
        assertThat(options.getToolCallbacks() == null || options.getToolCallbacks().isEmpty()).isTrue();
    }
    @Test void invalidProfileQuoteDoesNotDiscardAUsableSummaryOrTriggerAnotherModelCall() {
        var event = f.event("我喜欢绿茶"); var chat = model(json(new Envelope("用户喜欢绿茶", List.of(tea("红茶", 1, "助手未确认的推测")))));
        var result = new SummaryModel(chat, new SummaryPolicy(SummaryTestFixture::config, SummaryTestFixture.ai()), new MemoryTokenBudget(), f.support).summarize(event);
        assertThat(result.text()).isEqualTo("用户喜欢绿茶"); assertThat(result.profileRejected()).isTrue(); verify(chat, times(1)).call(any(Prompt.class));
        f.jobs.register(event); var job = f.jobs.claim(event.jobId(), System.currentTimeMillis(), 30000);
        f.jobs.saveResult(job, result); f.jobs.project(job, event, result.text());
        assertThat(f.profiles.extraction(event.jobId(), false).orElseThrow().state()).isEqualTo("REJECTED");
        assertThat(f.profiles.pendingExtractions()).isEmpty();
    }
    @Test void acceptingOffRetainsCapturedWorkButDoesNotEnrollNewSummaryJobs() {
        var first = f.event("我喜欢绿茶"); var snapshot = f.support.prepare(first); f.config.setAccepting(false);
        assertThat(f.support.prepare(first)).isEqualTo(snapshot);
        assertThat(f.support.prepare(f.event("今天聊天"))).isNull();
    }
    @Test void profileSchemaAndSnapshotAreIncludedInTheActualModelInputBudget() {
        var chat = model("{}"); var config = SummaryTestFixture.config();
        config.setInputBudgetTokens(2400); config.setHighTokens(1800);
        var summary = new SummaryModel(chat, new SummaryPolicy(() -> config, SummaryTestFixture.ai()), new MemoryTokenBudget(), f.support);
        assertThatThrownBy(() -> summary.summarize(f.event("我喜欢绿茶"))).hasMessageContaining("budget");
        verify(chat, never()).call(any(Prompt.class));
    }
    @Test void invalidEnvelopeCannotBeUsedAsPlainSummary() {
        var chat = model("just text without structured output");
        assertThatThrownBy(() -> new SummaryModel(chat, new SummaryPolicy(SummaryTestFixture::config, SummaryTestFixture.ai()),
                new MemoryTokenBudget(), f.support).summarize(f.event("我喜欢绿茶"))).isInstanceOf(RuntimeException.class);
    }
    private ChatModel model(String text) {
        var model = mock(ChatModel.class);
        when(model.getOptions()).thenReturn(DeepSeekChatOptions.builder().disableThinking().build());
        when(model.call(any(Prompt.class))).thenReturn(new ChatResponse(List.of(new Generation(new AssistantMessage(text)))));
        return model;
    }
}
