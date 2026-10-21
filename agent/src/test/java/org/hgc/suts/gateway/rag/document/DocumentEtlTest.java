package org.hgc.suts.gateway.rag.document;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.hgc.suts.gateway.config.RagProperties;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

class DocumentEtlTest {
    private final RagProperties config = new RagProperties();
    private DocumentEtl.ParsedDocument parse(String text, String filename) {
        return new DocumentEtl(config).parse(text.getBytes(StandardCharsets.UTF_8), filename);
    }

    @Test void markdownRetainsNestedNumberedStepsHeadingPathsAndCodeFenceContents() {
        String raw = "# 办证\n请先准备材料。\n## 申请步骤\n1. 携带身份证。\n   1. 复印正反面。\n2. 前往服务窗口。\n\n```text\n# 不是标题\n3. 代码内编号\n```\n## 领取\n1. 等待通知。\n";
        var parsed = parse(raw, "guide.md");
        assertThat(parsed.parents()).extracting(DocumentEtl.Parent::headingPath).containsExactly("办证", "办证 / 申请步骤", "办证 / 领取");
        assertThat(parsed.parents().get(1).text()).contains("1. 携带身份证。\n   1. 复印正反面。\n2. 前往服务窗口。", "# 不是标题", "3. 代码内编号");
        assertThat(parsed.parents().get(1).lineStart()).isEqualTo(3);
        assertThat(parsed.children()).allSatisfy(child -> {
            assertThat(child.pageStart()).isZero(); assertThat(child.lineStart()).isPositive();
            assertThat(parsed.parents().stream().anyMatch(parent -> parent.id().equals(child.parentId()) && parent.text().contains(child.text()))).isTrue();
        });
        assertThat(parse(raw, "guide.md")).isEqualTo(parsed);
    }

    @Test void setextAndNonAsciiTextRemainDeterministicAndDoNotInventPdfPages() {
        var markdown = parse("办理\n====\n说明\n\n步骤\n----\n1. 先申请\n2. 再领取", "guide.markdown");
        assertThat(markdown.parents()).extracting(DocumentEtl.Parent::headingPath).containsExactly("办理", "办理 / 步骤");
        var txt = parse("1. 先申请\n2. 再领取\n", "guide.txt");
        assertThat(txt.parents()).singleElement().satisfies(parent -> {
            assertThat(parent.text()).contains("1. 先申请\n2. 再领取"); assertThat(parent.pageStart()).isZero();
        });
    }

    @Test void budgetsBoundEveryChunkAndOverlapWithoutDroppingUnicodeCodePoints() {
        config.setMaxParentChars(256); config.setMaxChildChars(80); config.setOverlapChars(16);
        String text = "甲乙丙丁😀".repeat(100);
        var parsed = parse(text, "unicode.txt");
        assertThat(parsed.parents()).allSatisfy(parent -> assertThat(parent.text().length()).isLessThanOrEqualTo(256));
        assertThat(parsed.children()).allSatisfy(child -> {
            assertThat(child.text().length()).isLessThanOrEqualTo(80);
            assertThat(Character.isLowSurrogate(child.text().charAt(0))).isFalse();
            assertThat(Character.isHighSurrogate(child.text().charAt(child.text().length() - 1))).isFalse();
        });
        assertThat(String.join("", parsed.parents().stream().map(DocumentEtl.Parent::text).toList())).isEqualTo(text);
        var siblings = parsed.children().stream().filter(c -> c.parentId().equals(parsed.parents().get(0).id())).toList();
        assertThat(siblings.size()).isGreaterThan(1);
        assertThat(siblings.get(0).text()).endsWith(siblings.get(1).text().substring(0, 16));
        String originalPipeline = parsed.pipelineTag(); config.setOverlapChars(10);
        assertThat(parse(text, "unicode.txt").pipelineTag()).isNotEqualTo(originalPipeline);
    }

    @Test void pdfUsesPhysicalPagesAndRejectsImageOnlyOrBlankText() throws Exception {
        byte[] bytes;
        try (var pdf = new PDDocument(); var output = new ByteArrayOutputStream()) {
            page(pdf, "Step 1: prepare identity card."); pdf.addPage(new PDPage()); page(pdf, "Step 2: visit service desk.");
            pdf.save(output); bytes = output.toByteArray();
        }
        var parsed = new DocumentEtl(config).parse(bytes, "guide.pdf");
        assertThat(parsed.parents()).extracting(DocumentEtl.Parent::pageStart).containsExactly(1, 3);
        assertThat(parsed.parents()).allSatisfy(parent -> { assertThat(parent.pageEnd()).isEqualTo(parent.pageStart()); assertThat(parent.lineStart()).isZero(); });
        try (var pdf = new PDDocument(); var output = new ByteArrayOutputStream()) {
            pdf.addPage(new PDPage()); pdf.save(output);
            assertThatThrownBy(() -> new DocumentEtl(config).parse(output.toByteArray(), "blank.pdf")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("empty");
        }
    }
    private static void page(PDDocument pdf, String text) throws Exception {
        var page = new PDPage(); pdf.addPage(page);
        try (var content = new PDPageContentStream(pdf, page)) {
            content.beginText(); content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
            content.newLineAtOffset(60, 700); content.showText(text); content.endText();
        }
    }

    @Test void rejectsInvalidEncodingUnsupportedFormatsAndExceededBudgets() {
        assertThatThrownBy(() -> parse("", "empty.txt")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parse("\uFEFF", "bom-only.txt")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parse("text", "notes.docx")).hasMessageContaining("Only TXT");
        assertThatThrownBy(() -> new DocumentEtl(config).parse(new byte[]{(byte) 0xC3, 0x28}, "broken.txt")).hasMessageContaining("UTF-8");
        config.setMaxFileBytes(3); assertThatThrownBy(() -> parse("1234", "large.txt")).hasMessageContaining("byte budget");
        config.setMaxFileBytes(100000); config.setMaxChunks(1); config.setMaxParentChars(256); config.setMaxChildChars(64); config.setOverlapChars(0);
        assertThatThrownBy(() -> parse("text".repeat(200), "many.txt")).hasMessageContaining("chunk budget");
    }
}
