package io.dropwizard.elasticsearch.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link HealthCheck} which checks if one or more indices exist in Elasticsearch.
 *
 * @see <a href="http://www.elasticsearch.org/guide/reference/api/admin-indices-indices-exists/">Admin Indices Indices Exists</a>
 */
public class EsIndexExistsHealthCheck extends HealthCheck {

    private final Logger logger = LoggerFactory.getLogger(EsIndexExistsHealthCheck.class);
    private final RestHighLevelClient client;
    private final String[] indices;

    /**
     * Construct a new Elasticsearch index exists health check.
     *
     * @param client  an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     * @param indices a {@link List} of indices in Elasticsearch which should be checked
     * @throws IllegalArgumentException if {@code indices} was {@literal null} or empty
     */
    public EsIndexExistsHealthCheck(RestHighLevelClient client, List<String> indices) {
        checkArgument(!indices.isEmpty(), "At least one index must be given");

        this.client = checkNotNull(client);
        this.indices = checkNotNull(indices.toArray(new String[indices.size()]));
    }

    /**
     * Construct a new Elasticsearch index exists health check.
     *
     * @param client    an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     * @param indexName the index in Elasticsearch which should be checked
     */
    public EsIndexExistsHealthCheck(RestHighLevelClient client, String indexName) {
        this(client, ImmutableList.of(indexName));
    }

    /**
     * Perform a check of the number of documents in the Elasticsearch indices.
     *
     * @return if the Elasticsearch indices exist, a healthy {@link com.codahale.metrics.health.HealthCheck.Result};
     * otherwise, an unhealthy {@link com.codahale.metrics.health.HealthCheck.Result} with a descriptive error
     * message or exception
     * @throws Exception if there is an unhandled error during the health check; this will result in
     *                   a failed health check
     */
    @Override
    protected Result check() throws Exception {
        Response response = client.getLowLevelClient().performRequest("HEAD", Arrays.stream(indices).collect(Collectors.joining(",")));

        if (response.getStatusLine().getStatusCode() == 200) {
            return Result.healthy();
        }

        logger.warn("Index exists health check: One or more indices do not exist");
        return Result.unhealthy("One or more indices do not exist.");

    }
}