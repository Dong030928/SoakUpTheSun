package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.common.exception.ClientException;
import org.hgc.suts.gateway.config.MemorySummaryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.ToolCallingAdvisor;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.model.*;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.deepseek.DeepSeekChatOptions;
import org.springframework.ai.tool.annotation.Tool;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class SummaryBudgetAdvisorTest {
    @Test void everyToolLoopIterationIsCheckedBeforeAnotherProviderCall() {
        MemorySummaryProperties p = SummaryTestFixture.config(); p.setInputBudgetTokens(3000);
        var policy = new SummaryPolicy(() -> p, SummaryTestFixture.ai());
        ChatModel model = mock(ChatModel.class);
        when(model.getOptions()).thenReturn(DeepSeekChatOptions.builder().build());
        when(model.call(any(Prompt.class))).thenReturn(new ChatResponse(List.of(new Generation(AssistantMessage.builder()
                .content("").toolCalls(List.of(new AssistantMessage.ToolCall("call-test", "function", "read_fixture", "{}"))).build()))));
        var tools = new LargeResultTool();
        var client = ChatClient.builder(model).defaultAdvisors(ToolCallingAdvisor.builder().build(),
                new SummaryBudgetAdvisor(policy, new MemoryTokenBudget())).build();
        assertThatThrownBy(() -> client.prompt().user("read the fixture").tools(tools).call().content())
                .isInstanceOf(ClientException.class).hasMessageContaining("输入预算");
        verify(model, times(1)).call(any(Prompt.class));
        assertThat(tools.calls.get()).isEqualTo(1);
    }
    @Test void assistantArgumentsAndToolResponsePayloadAreIncludedInTheEstimate() {
        var tokens = new MemoryTokenBudget();
        var shortCall = AssistantMessage.builder().content("").toolCalls(List.of(new AssistantMessage.ToolCall("a", "function", "x", "{}"))).build();
        var longCall = AssistantMessage.builder().content("").toolCalls(List.of(new AssistantMessage.ToolCall("a", "function", "x", "x".repeat(2000)))).build();
        assertThat(tokens.message(longCall) - tokens.message(shortCall)).isGreaterThan(1900);
        var response = ToolResponseMessage.builder().responses(List.of(new ToolResponseMessage.ToolResponse("a", "x", "x".repeat(3000)))).build();
        assertThat(tokens.message(response)).isGreaterThan(3000);
    }
    @Test void actualUsageCanIncreaseTheConservativeEstimateWithoutInferringACacheTtl() {
        var tokens = new MemoryTokenBudget();
        var usage = mock(org.springframework.ai.chat.metadata.Usage.class); when(usage.getPromptTokens()).thenReturn(200);
        tokens.observe(100, usage);
        assertThat(tokens.text("abc")).isEqualTo(6); assertThat(tokens.observations()).isEqualTo(1);
        when(usage.getPromptTokens()).thenReturn(10); tokens.observe(100, usage);
        assertThat(tokens.text("abc")).isEqualTo(6);
    }
    static final class LargeResultTool {
        final AtomicInteger calls = new AtomicInteger();
        @Tool(name = "read_fixture", description = "Read a synthetic test fixture")
        public String read() { calls.incrementAndGet(); return "x".repeat(5000); }
    }
}
