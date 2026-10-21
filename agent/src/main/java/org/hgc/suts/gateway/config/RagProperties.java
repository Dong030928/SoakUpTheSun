package org.hgc.suts.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

import java.net.URI;

@Data
@ConfigurationProperties("suts.ai.rag")
public class RagProperties {
    private boolean enabled;
    private String jdbcUrl = "";
    private String username = "";
    @lombok.ToString.Exclude private String password = "";
    private boolean migrate;
    private String indexName = "suts:agent:rag:documents:index:v1";
    private String prefix = "suts:agent:rag:documents:data:v1:";
    private int profileQuota = 10;
    private int documentQuota = 40;
    private int rrfConstant = 60;
    private int maxQueryChars = 2000;
    private int maxEvidenceChars = 12000;
    private long timeoutMs = 12000;
    private long channelTimeoutMs = 7000;
    private long workerLeaseMs = 120000;
    private long retryDelayMs = 10000;
    private long scanIntervalMs = 5000;
    private int scanBatch = 10;
    private int maxFileBytes = 10 * 1024 * 1024;
    private int maxDocumentChars = 500000;
    private int maxParentChars = 4000;
    private int maxChildChars = 1000;
    private int overlapChars = 120;
    private int maxChunks = 3000;
    private Elasticsearch elasticsearch = new Elasticsearch();
    private Reranker reranker = new Reranker();

    @Data public static class Elasticsearch {
        private String endpoint = "http://127.0.0.1:9200";
        private String indexName = "suts-agent-rag-v1";
        private String username = "";
        @lombok.ToString.Exclude private String password = "";
        private int timeoutMs = 5000;
    }
    @Data public static class Reranker {
        private String endpoint = "https://api.siliconflow.cn/v1";
        @lombok.ToString.Exclude private String apiKey = "";
        private String model = "Qwen/Qwen3-Reranker-0.6B";
        private String instruction = "Given the user's question, rank passages by how directly they provide relevant user facts or assistive-domain evidence. Preserve distinctions between people, devices, negation and time.";
        private int timeoutMs = 5000;
        private int maxResponseBytes = 256 * 1024;
    }

    public void validate() {
        Assert.notNull(elasticsearch, "RAG keyword settings are required");
        Assert.notNull(reranker, "RAG reranker settings are required");
        Assert.isTrue(profileQuota >= 0 && documentQuota >= 0 && profileQuota + documentQuota == 50,
                "RAG channel quotas must add up to 50");
        Assert.isTrue(rrfConstant > 0 && rrfConstant <= 10000, "Invalid RRF constant");
        Assert.isTrue(maxQueryChars > 0 && maxQueryChars <= 4000 && maxEvidenceChars >= maxParentChars
                && maxEvidenceChars <= 65536, "Invalid RAG input/evidence budget");
        Assert.isTrue(timeoutMs >= 100 && timeoutMs <= 60000 && channelTimeoutMs >= 100
                && channelTimeoutMs <= timeoutMs, "Invalid RAG time budget");
        Assert.isTrue(workerLeaseMs >= 30000 && workerLeaseMs <= 3600000 && retryDelayMs >= 100
                && scanIntervalMs >= 100 && scanBatch > 0 && scanBatch <= 100, "Invalid RAG worker budget");
        Assert.isTrue(maxFileBytes > 0 && maxFileBytes <= 50 * 1024 * 1024 && maxDocumentChars > 0
                && maxDocumentChars <= 2000000 && maxParentChars >= 256 && maxParentChars <= 16000
                && maxChildChars >= 64 && maxChildChars <= maxParentChars && overlapChars >= 0
                && overlapChars < maxChildChars / 2 && maxChunks > 0 && maxChunks <= 10000, "Invalid RAG import budget");
        Assert.hasText(indexName, "RAG vector index name is required");
        Assert.hasText(prefix, "RAG vector prefix is required");
        validateEndpoint(elasticsearch.endpoint);
        Assert.isTrue(elasticsearch.indexName != null && elasticsearch.indexName.matches("[a-z0-9][a-z0-9_-]{0,100}"), "Invalid RAG keyword index name");
        Assert.isTrue(elasticsearch.timeoutMs >= 100 && elasticsearch.timeoutMs <= 30000, "Invalid ES timeout");
        validateEndpoint(reranker.endpoint);
        Assert.hasText(reranker.model, "Reranker model is required");
        Assert.isTrue(reranker.timeoutMs >= 100 && reranker.timeoutMs <= 30000
                && reranker.maxResponseBytes >= 1024 && reranker.maxResponseBytes <= 1024 * 1024,
                "Invalid reranker response budget");
    }

    public static URI validateEndpoint(String endpoint) {
        Assert.hasText(endpoint, "Service endpoint is required");
        URI uri = URI.create(endpoint);
        Assert.isTrue(("https".equals(uri.getScheme()) || "http".equals(uri.getScheme())) && uri.getHost() != null
                && uri.getUserInfo() == null && uri.getQuery() == null && uri.getFragment() == null, "Invalid service endpoint");
        return uri;
    }
}
