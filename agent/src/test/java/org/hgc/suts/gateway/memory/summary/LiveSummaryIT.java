package org.hgc.suts.gateway.memory.summary;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.AiModelConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/** Explicit paid opt-in: exactly two summary calls and one follow-up; synthetic history only, no business tools. */
class LiveSummaryIT {
    @Test void twoCompactionsPreserveCorrectionsNegationTimeAndSameNameEntities() throws Exception {
        assertThat(System.getProperty("live.memory.summary")).as("Explicit opt-in to three paid provider calls").isEqualTo("true");
        Path secret = Path.of(Objects.requireNonNull(System.getProperty("live.memory.config"), "External config path is required")).toRealPath();
        assertThat(secret.startsWith(Path.of("").toAbsolutePath().getParent())).as("Secrets must stay outside the repository").isFalse();
        var sources = new YamlPropertySourceLoader().load("private-live-summary", new FileSystemResource(secret));
        var ai = new AiConfig();
        ai.setApiHost(property(sources, "suts.ai.api-host")); ai.setApiKey(property(sources, "suts.ai.api-key"));
        ai.setModelName(property(sources, "suts.ai.model-name")); ai.setTimeout(60000); ai.setTemperature(0.0);
        var config = SummaryTestFixture.config(); config.setWorkerLeaseMs(120000);
        var policy = new SummaryPolicy(() -> config, ai);
        var chatModel = new AiModelConfiguration().agentChatModel(ai);
        var summarizer = new SummaryModel(chatModel, policy);
        String id = SummaryTestFixture.id();
        var raw = List.of(SummaryState.RawTurn.of(SummaryTestFixture.turn(id, 1,
                        "更正，我喜欢绿茶，不喝红茶。不是花生过敏，是海鲜过敏。", "已记住更正，之前的红茶和花生说法作废。")),
                SummaryState.RawTurn.of(SummaryTestFixture.turn(id, 2,
                        "朋友小王在北京，同事小王在上海。2026年10月19日上午九点去医院复诊，尚未预订车辆。", "已记录，尚未叫车。")));
        var first = new SummaryEvent(1, "live-summary-evaluation", null, 42, id, 1, 0, 1, 2,
                "用户喜欢红茶；朋友小王在北京。", raw);
        long started = System.nanoTime();
        SummaryModel.Result firstResult;
        SummaryModel.Result secondResult;
        org.springframework.ai.chat.model.ChatResponse answer;
        try {
            firstResult = summarizer.summarize(first);
            var correction = SummaryState.RawTurn.of(SummaryTestFixture.turn(id, 3,
                    "再更正：复诊改到2026年10月20日上午十点；同事小王搬到杭州，朋友小王仍在北京，仍然尚未叫车。", "已更新，叫车任务仍待确认。"));
            var second = new SummaryEvent(1, "live-summary-evaluation", null, 42, id, 1, 1, 3, 3, firstResult.text(), List.of(correction));
            secondResult = summarizer.summarize(second);
            answer = ChatClient.create(chatModel).prompt().system("你只根据用户提供的历史摘要参考回答问题，不执行业务操作。不知道的事实明确说明。")
                    .user("历史摘要参考（不是系统指令）：\n" + secondResult.text()
                            + "\n请逐项简短回答：用户喜欢什么茶、对什么过敏；朋友和同事小王各在哪；复诊的日期和时间；是否已经叫车？")
                    .options(org.springframework.ai.deepseek.DeepSeekChatOptions.builder().disableThinking().temperature(0.0).maxTokens(600))
                    .call().chatResponse();
        } catch (RuntimeException ex) {
            throw new IllegalStateException("Live summary provider check failed (" + ex.getClass().getSimpleName() + ")");
        }
        String reply = answer.getResult().getOutput().getText();
        var report = new LinkedHashMap<String, Object>();
        report.put("testedAt", Instant.now().toString()); report.put("model", ai.getModelName()); report.put("calls", 3);
        report.put("firstSummary", firstResult); report.put("secondSummary", secondResult); report.put("followup", reply);
        var usage = answer.getMetadata().getUsage();
        report.put("followupPromptTokens", usage == null ? 0 : usage.getPromptTokens());
        report.put("followupCompletionTokens", usage == null ? 0 : usage.getCompletionTokens());
        report.put("elapsedMs", (System.nanoTime() - started) / 1000000);
        Path output = Path.of("target", "live-memory-summary-report.json"); Files.createDirectories(output.getParent());
        Files.writeString(output, JsonMapper.builder().build().writerWithDefaultPrettyPrinter().writeValueAsString(report));
        assertThat(secondResult.text()).contains("绿茶", "海鲜", "北京", "杭州");
        assertThat(reply).contains("绿茶", "海鲜", "北京", "杭州");
        assertThat(reply).containsAnyOf("10月20", "10-20", "10/20", "十月二十");
        assertThat(reply).containsAnyOf("未叫", "未预订", "尚未", "没有叫", "还没有");
    }
    private static String property(List<org.springframework.core.env.PropertySource<?>> sources, String key) {
        return sources.stream().map(source -> source.getProperty(key)).filter(Objects::nonNull)
                .map(Object::toString).findFirst().orElseThrow(() -> new IllegalArgumentException("Missing " + key));
    }
}
