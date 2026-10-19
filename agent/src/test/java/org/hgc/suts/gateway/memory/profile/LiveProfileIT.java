package org.hgc.suts.gateway.memory.profile;

import org.hgc.suts.gateway.config.AiConfig;
import org.hgc.suts.gateway.config.AiModelConfiguration;
import org.hgc.suts.gateway.memory.summary.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.hgc.suts.gateway.memory.profile.ProfileData.*;

/** Explicit opt-in, exactly two paid summary/extraction calls, synthetic user data only. No business tools. */
class LiveProfileIT {
    @Test void realProviderExtractsRawUserFactsAndCorrectsThemInTheNextSummary() throws Exception {
        assertThat(System.getProperty("live.profile")).as("Explicit opt-in to two paid calls").isEqualTo("true");
        Path secret = Path.of(Objects.requireNonNull(System.getProperty("live.memory.config"))).toRealPath();
        assertThat(secret.startsWith(Path.of("").toAbsolutePath().getParent())).isFalse();
        var sources = new YamlPropertySourceLoader().load("private-live-profile", new FileSystemResource(secret));
        var ai = new AiConfig(); ai.setApiHost(property(sources, "suts.ai.api-host")); ai.setApiKey(property(sources, "suts.ai.api-key"));
        ai.setModelName(property(sources, "suts.ai.model-name")); ai.setTimeout(60000); ai.setTemperature(0.0);
        var f = new ProfileFixture(); var config = SummaryTestFixture.config(); config.setSummaryMaxTokens(1600); config.setWorkerLeaseMs(120000);
        var summary = new SummaryModel(new AiModelConfiguration().agentChatModel(ai), new SummaryPolicy(() -> config, ai), new MemoryTokenBudget(), f.support);
        var first = f.event("我喜欢绿茶。我使用的屏幕阅读器是 NVDA。朋友小王喜欢咖啡，那是朋友的偏好。");
        long started = System.nanoTime(); SummaryModel.Result firstResult, secondResult;
        try {
            firstResult = summary.summarize(first);
            assertThat(firstResult.profileRejected()).isFalse();
            apply(f, first, firstResult);
            assertThat(f.profiles.list(42, false, 20)).anySatisfy(fact -> assertThat(fact.value()).contains("绿茶"))
                    .anySatisfy(fact -> assertThat(fact.value()).contains("NVDA"))
                    .noneSatisfy(fact -> assertThat(fact.value()).contains("咖啡"));
            var correction = f.event(42, first.conversationId(), 2, 1, "更正一下，我现在喜欢红茶，不再喜欢绿茶。屏幕阅读器仍然是 NVDA。");
            var second = new SummaryEvent(1, first.generation(), null, 42, first.conversationId(), 1, 1, 2, 2,
                    firstResult.text(), correction.turns());
            secondResult = summary.summarize(second); apply(f, second, secondResult);
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Live profile provider check failed (" + failure.getClass().getSimpleName() + ")");
        }
        assertThat(secondResult.profileRejected()).isFalse();
        assertThat(f.profiles.list(42, false, 20)).anySatisfy(fact -> {
            assertThat(fact.value()).contains("红茶"); assertThat(fact.version()).isGreaterThan(1);
            assertThat(fact.source().sequence()).isEqualTo(2);
        });
        var report = new LinkedHashMap<String, Object>();
        report.put("testedAt", Instant.now().toString()); report.put("model", ai.getModelName()); report.put("calls", 2);
        report.put("firstPromptTokens", firstResult.promptTokens()); report.put("firstCompletionTokens", firstResult.completionTokens());
        report.put("secondPromptTokens", secondResult.promptTokens()); report.put("secondCompletionTokens", secondResult.completionTokens());
        report.put("facts", f.profiles.list(42, false, 20)); report.put("elapsedMs", (System.nanoTime() - started) / 1000000);
        Path output = Path.of("target", "live-profile-report.json"); Files.createDirectories(output.getParent());
        Files.writeString(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }
    private void apply(ProfileFixture f, SummaryEvent event, SummaryModel.Result result) {
        f.jobs.register(event); var job = f.jobs.claim(event.jobId(), System.currentTimeMillis(), 120000);
        f.jobs.saveResult(job, result); f.jobs.project(job, event, result.text());
        f.profiles.apply(f.profiles.claimExtraction(event.jobId()), Arrays.asList(JSON.readValue(result.profilePayload(), Candidate[].class)));
    }
    private static String property(List<org.springframework.core.env.PropertySource<?>> sources, String key) {
        return sources.stream().map(source -> source.getProperty(key)).filter(Objects::nonNull).map(Object::toString).findFirst().orElseThrow();
    }
}
