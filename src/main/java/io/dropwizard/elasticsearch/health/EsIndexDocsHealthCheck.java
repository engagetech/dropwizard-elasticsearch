package io.dropwizard.elasticsearch.health;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link HealthCheck} which checks if one or more indices in Elasticsearch contain a given number of documents
 * in their primaries.
 *
 * @see <a href="http://www.elasticsearch.org/guide/reference/api/admin-indices-stats/">Admin Indices Stats</a>
 */
public class EsIndexDocsHealthCheck extends HealthCheck {

    private final Logger logger = LoggerFactory.getLogger(EsIndexDocsHealthCheck.class);
    private static final long DEFAULT_DOCUMENT_THRESHOLD = 1L;
    private final RestHighLevelClient client;
    private final String[] indices;
    private final long documentThreshold;

    /**
     * Construct a new Elasticsearch index document count health check.
     *
     * @param client            an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     * @param indices           a {@link List} of indices in Elasticsearch which should be checked
     * @param documentThreshold the minimal number of documents in an index
     * @throws IllegalArgumentException if {@code indices} was {@literal null} or empty,
     *                                  or {@code documentThreshold} was less than 1
     */
    public EsIndexDocsHealthCheck(RestHighLevelClient client, List<String> indices, long documentThreshold) {
        checkArgument(!indices.isEmpty(), "At least one index must be given");
        checkArgument(documentThreshold > 0L, "The document threshold must at least be 1");

        this.client = checkNotNull(client);
        this.indices = checkNotNull(indices.toArray(new String[indices.size()]));
        this.documentThreshold = documentThreshold;
    }


    /**
     * Construct a new Elasticsearch index document count health check.
     *
     * @param client  an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     * @param indices a {@link List} of indices in Elasticsearch which should be checked
     */
    public EsIndexDocsHealthCheck(RestHighLevelClient client, List<String> indices) {
        this(client, indices, DEFAULT_DOCUMENT_THRESHOLD);
    }

    /**
     * Construct a new Elasticsearch index document count health check.
     *
     * @param client            an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     * @param indexName         the index in Elasticsearch which should be checked
     * @param documentThreshold the minimal number of documents in an index
     */
    public EsIndexDocsHealthCheck(RestHighLevelClient client, String indexName, long documentThreshold) {
        this(client, ImmutableList.of(indexName), documentThreshold);
    }

    /**
     * Construct a new Elasticsearch index document count health check.
     *
     * @param client    an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     * @param indexName the index in Elasticsearch which should be checked
     */
    public EsIndexDocsHealthCheck(RestHighLevelClient client, String indexName) {
        this(client, indexName, DEFAULT_DOCUMENT_THRESHOLD);
    }

    /**
     * Perform a check of the number of documents in the Elasticsearch indices.
     *
     * @return if the Elasticsearch indices contain the minimal number of documents, a healthy
     * {@link com.codahale.metrics.health.HealthCheck.Result}; otherwise, an unhealthy
     * {@link com.codahale.metrics.health.HealthCheck.Result} with a descriptive error message or exception
     * @throws Exception if there is an unhandled error during the health check; this will result in
     *                   a failed health check
     */
    @Override
    protected Result check() throws Exception {
        Response response = client.getLowLevelClient().performRequest("GET", "/_stats");
        ObjectMapper objectMapper = new ObjectMapper();

        IndicesStatsResponse indicesStatsResponse;
        try (InputStream is = response.getEntity().getContent()) {
            indicesStatsResponse = objectMapper.readValue(is, IndicesStatsResponse.class);
        }

        final List<String> indexDetails = new ArrayList<String>(indices.length);
        boolean healthy = true;

        for (IndexStats indexStats : indicesStatsResponse.getIndices().values()) {
            long documentCount = indexStats.getPrimaries().getDocs().getCount();

            if (documentCount < documentThreshold) {
                healthy = false;
                indexDetails.add(String.format("%s (%d)", indexStats.getIndex(), documentCount));
            } else {
                indexDetails.add(String.format("%s (%d!)", indexStats.getIndex(), documentCount));
            }
        }

        final String resultDetails = String.format("Last stats: %s", indexDetails);

        if (healthy) {
            return Result.healthy(resultDetails);
        }

        logger.warn("Index docs health check status unhealthy. " + resultDetails);
        return Result.unhealthy(resultDetails);
    }
}