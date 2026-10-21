package org.hgc.suts.gateway.rag.document;

import org.hgc.suts.gateway.config.RagProperties;
import org.springframework.ai.document.Document;
import org.springframework.ai.document.DocumentReader;
import org.springframework.ai.document.DocumentTransformer;
import org.springframework.ai.reader.TextReader;
import org.springframework.ai.reader.pdf.PagePdfDocumentReader;
import org.springframework.ai.reader.pdf.config.PdfDocumentReaderConfig;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.util.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

/** Local, deterministic ETL. Character limits are storage budgets, never a model token estimate. */
public final class DocumentEtl {
    private final RagProperties config;
    private final String pipelineTag;
    public DocumentEtl(RagProperties config) {
        this.config = config;
        Assert.isTrue(config.getMaxParentChars() >= config.getMaxChildChars() && config.getMaxChildChars() > 0
                && config.getOverlapChars() >= 0 && config.getOverlapChars() < config.getMaxChildChars() / 2
                && config.getMaxChunks() > 0, "Invalid parent/child storage budgets");
        pipelineTag = hash("structural-etl-v1|" + config.getMaxParentChars() + "|" + config.getMaxChildChars()
                + "|" + config.getOverlapChars() + "|" + config.getMaxChunks());
    }

    public ParsedDocument parse(Path path) {
        Assert.notNull(path, "Import path is required");
        Assert.isTrue(Files.isRegularFile(path), "Import requires a regular local file");
        try (var input = Files.newInputStream(path)) {
            byte[] bytes = input.readNBytes(config.getMaxFileBytes() + 1);
            return parse(bytes, path.getFileName().toString());
        } catch (IOException error) { throw new IllegalArgumentException("Cannot read local document", error); }
    }

    public ParsedDocument parse(byte[] bytes, String filename) {
        Assert.isTrue(bytes != null && bytes.length > 0 && bytes.length <= config.getMaxFileBytes(), "Document file exceeds its byte budget or is empty");
        Assert.hasText(filename, "Document filename is required");
        String suffix = filename.substring(Math.max(0, filename.lastIndexOf('.'))).toLowerCase(Locale.ROOT);
        var resource = new ByteArrayResource(bytes) { @Override public String getFilename() { return filename; } };
        DocumentReader reader;
        if (suffix.equals(".pdf")) reader = new ClosingPdfReader(resource);
        else if (suffix.equals(".md") || suffix.equals(".markdown")) reader = new MarkdownReader(decode(bytes));
        else if (suffix.equals(".txt")) {
            String decoded = decode(bytes); // Reject invalid UTF-8 and remove a leading byte-order mark.
            var textResource = new ByteArrayResource(decoded.getBytes(StandardCharsets.UTF_8)) { @Override public String getFilename() { return filename; } };
            var text = new TextReader(textResource); text.setCharset(StandardCharsets.UTF_8); reader = text;
        } else throw new IllegalArgumentException("Only TXT, Markdown and text PDF imports are supported");
        List<Document> extracted = reader.read();
        long length = extracted.stream().map(Document::getText).filter(Objects::nonNull).mapToLong(String::length).sum();
        Assert.isTrue(length > 0 && length <= config.getMaxDocumentChars(), "Extracted document is empty or exceeds its text budget");
        Assert.isTrue(extracted.stream().anyMatch(d -> d.getText() != null && !d.getText().isBlank()),
                "Document contains no extractable text; scanned PDFs require text extraction before import");
        String sourceHash = hash(bytes);
        DocumentTransformer transformer = new ParentChildTransformer(config, sourceHash, pipelineTag);
        List<Document> chunks = transformer.transform(extracted);
        List<Parent> parents = new ArrayList<>(); List<Child> children = new ArrayList<>();
        for (Document document : chunks) {
            var m = document.getMetadata();
            if ("PARENT".equals(m.get("recordType"))) parents.add(new Parent(document.getId(), integer(m, "ordinal"),
                    document.getText(), string(m, "headingPath"), integer(m, "pageStart"), integer(m, "pageEnd"),
                    integer(m, "lineStart"), integer(m, "lineEnd")));
            else children.add(new Child(document.getId(), string(m, "parentId"), integer(m, "ordinal"), document.getText(),
                    string(m, "headingPath"), integer(m, "pageStart"), integer(m, "pageEnd"), integer(m, "lineStart"), integer(m, "lineEnd")));
        }
        Assert.isTrue(!parents.isEmpty() && !children.isEmpty(), "Document contains no searchable text");
        return new ParsedDocument(sourceHash, pipelineTag, parents, children);
    }

    private static String decode(byte[] bytes) {
        try {
            String text = StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes)).toString();
            return text.startsWith("\uFEFF") ? text.substring(1) : text;
        } catch (java.nio.charset.CharacterCodingException invalid) { throw new IllegalArgumentException("Text imports require valid UTF-8", invalid); }
    }
    static int integer(Map<String, Object> metadata, String key) { Object n = metadata.get(key); return n instanceof Number number ? number.intValue() : 0; }
    static String string(Map<String, Object> metadata, String key) { return Objects.toString(metadata.get(key), ""); }
    public static String hash(String text) { return hash(text.getBytes(StandardCharsets.UTF_8)); }
    public static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    public record ParsedDocument(String sourceHash, String pipelineTag, List<Parent> parents, List<Child> children) {
        public ParsedDocument { parents = List.copyOf(parents); children = List.copyOf(children); }
    }
    public record Parent(String id, int ordinal, String text, String headingPath, int pageStart, int pageEnd, int lineStart, int lineEnd) { }
    public record Child(String id, String parentId, int ordinal, String text, String headingPath, int pageStart, int pageEnd, int lineStart, int lineEnd) { }

    /** Keep the original Markdown bytes between headings, including numbered and nested list markers. */
    static final class MarkdownReader implements DocumentReader {
        private static final Pattern ATX = Pattern.compile("^ {0,3}(#{1,6})[ \\t]+(.+?)\\s*#*\\s*$");
        private static final Pattern FENCE = Pattern.compile("^ {0,3}(`{3,}|~{3,}).*$");
        private final String text;
        MarkdownReader(String text) { this.text = text; }
        @Override public List<Document> get() {
            String[] lines = text.replace("\r\n", "\n").replace('\r', '\n').split("\n", -1);
            List<Document> result = new ArrayList<>(); List<String> headings = new ArrayList<>();
            StringBuilder section = new StringBuilder(); int start = 1; char fence = 0; int fenceLength = 0;
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i]; var fenced = FENCE.matcher(line);
                if (fenced.matches()) {
                    String marker = fenced.group(1);
                    if (fence == 0) { fence = marker.charAt(0); fenceLength = marker.length(); }
                    else if (marker.charAt(0) == fence && marker.length() >= fenceLength
                            && line.strip().substring(marker.length()).isBlank()) fence = 0;
                }
                var heading = ATX.matcher(line); int level = 0; String title = null; int consumed = 0;
                if (fence == 0 && heading.matches()) { level = heading.group(1).length(); title = heading.group(2); }
                else if (fence == 0 && i + 1 < lines.length && !line.isBlank()
                        && lines[i + 1].matches(" {0,3}(=+|-+)\\s*") && !line.stripLeading().matches("[-*+]\\s+.*")) {
                    level = lines[i + 1].strip().charAt(0) == '=' ? 1 : 2; title = line.strip(); consumed = 1;
                }
                if (level > 0) {
                    flush(result, section, headings, start, i);
                    while (headings.size() >= level) headings.remove(headings.size() - 1);
                    while (headings.size() < level - 1) headings.add("");
                    headings.add(title); start = i + 1;
                }
                section.append(line); if (i + 1 < lines.length) section.append('\n');
                if (consumed == 1) { section.append(lines[++i]); if (i + 1 < lines.length) section.append('\n'); }
            }
            flush(result, section, headings, start, lines.length); return result;
        }
        private static void flush(List<Document> output, StringBuilder body, List<String> headings, int start, int end) {
            if (!body.toString().isBlank()) output.add(Document.builder().text(body.toString())
                    .metadata(Map.of("headingPath", String.join(" / ", headings.stream().filter(s -> !s.isBlank()).toList()),
                            "lineStart", start, "lineEnd", end)).build());
            body.setLength(0);
        }
    }

    static final class ClosingPdfReader extends PagePdfDocumentReader {
        ClosingPdfReader(ByteArrayResource resource) { super(resource, PdfDocumentReaderConfig.builder().withPagesPerDocument(1).build()); }
        @Override public List<Document> get() {
            try { return super.get(); }
            finally { try { document.close(); } catch (IOException error) { throw new IllegalArgumentException("Cannot close parsed PDF", error); } }
        }
    }

    /** Structure first, bounded children second. Raw text remains in order and code points are never bisected. */
    static final class ParentChildTransformer implements DocumentTransformer {
        private final RagProperties config; private final String sourceHash; private final String pipeline;
        ParentChildTransformer(RagProperties config, String sourceHash, String pipeline) { this.config = config; this.sourceHash = sourceHash; this.pipeline = pipeline; }
        @Override public List<Document> apply(List<Document> documents) {
            List<Document> output = new ArrayList<>(); int parentOrdinal = 0; int childOrdinal = 0;
            for (Document input : documents) {
                String text = Objects.requireNonNullElse(input.getText(), "").replace("\r\n", "\n").replace('\r', '\n');
                int[] lineBreaks = java.util.stream.IntStream.range(0, text.length()).filter(i -> text.charAt(i) == '\n').toArray();
                for (Span parent : split(text, config.getMaxParentChars(), 0)) {
                    if (parent.text().isBlank()) continue;
                    String parentId = hash(sourceHash + "|" + pipeline + "|parent|" + parentOrdinal + "|" + parent.text());
                    var metadata = metadata(input, lineBreaks, parent.start(), parent.end());
                    metadata.put("recordType", "PARENT"); metadata.put("ordinal", parentOrdinal++);
                    output.add(Document.builder().id(parentId).text(parent.text()).metadata(metadata).build());
                    for (Span child : split(parent.text(), config.getMaxChildChars(), config.getOverlapChars())) {
                        if (child.text().isBlank()) continue;
                        String childId = hash(parentId + "|child|" + childOrdinal + "|" + child.text());
                        var childMetadata = metadata(input, lineBreaks, parent.start() + child.start(), parent.start() + child.end());
                        childMetadata.put("recordType", "CHILD"); childMetadata.put("parentId", parentId); childMetadata.put("ordinal", childOrdinal++);
                        output.add(Document.builder().id(childId).text(child.text()).metadata(childMetadata).build());
                        Assert.isTrue(childOrdinal <= config.getMaxChunks(), "Document exceeds its child chunk budget");
                    }
                    Assert.isTrue(parentOrdinal <= config.getMaxChunks(), "Document exceeds its parent chunk budget");
                }
            }
            return output;
        }
        private static Map<String, Object> metadata(Document input, int[] lineBreaks, int start, int end) {
            Map<String, Object> m = new HashMap<>(); var original = input.getMetadata();
            int page = integer(original, "page_number");
            m.put("pageStart", page); m.put("pageEnd", original.containsKey("end_page_number") ? integer(original, "end_page_number") : page);
            m.put("headingPath", string(original, "headingPath"));
            int base = integer(original, "lineStart"); if (base == 0 && page == 0) base = 1;
            m.put("lineStart", page == 0 ? base + newlines(lineBreaks, start) : 0);
            m.put("lineEnd", page == 0 ? base + newlines(lineBreaks, Math.max(start, end - 1)) : 0); return m;
        }
        private static int newlines(int[] positions, int end) { int found = Arrays.binarySearch(positions, end); return found >= 0 ? found : -found - 1; }
        private static List<Span> split(String text, int limit, int overlap) {
            List<Span> spans = new ArrayList<>(); int start = 0;
            while (start < text.length()) {
                int end = Math.min(text.length(), start + limit);
                if (end < text.length()) {
                    if (Character.isHighSurrogate(text.charAt(end - 1)) && Character.isLowSurrogate(text.charAt(end))) end--;
                    int lower = start + Math.max(overlap + 1, (end - start) / 2);
                    for (int i = end - 1; i >= lower; i--) if ("\n。！？；.!?;".indexOf(text.charAt(i)) >= 0) { end = i + 1; break; }
                }
                spans.add(new Span(start, end, text.substring(start, end)));
                if (end == text.length()) break;
                int next = Math.max(start + 1, end - overlap);
                if (Character.isLowSurrogate(text.charAt(next)) && next > 0 && Character.isHighSurrogate(text.charAt(next - 1))) next++;
                start = next;
            }
            return spans;
        }
        private record Span(int start, int end, String text) { }
    }
}
