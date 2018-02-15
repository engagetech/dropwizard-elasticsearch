package io.dropwizard.elasticsearch.health;

import com.codahale.metrics.health.HealthCheck;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link HealthCheck} which checks the cluster state of an Elasticsearch cluster.
 *
 * @see <a href="http://www.elasticsearch.org/guide/reference/api/admin-cluster-health/">Admin Cluster Health</a>
 */
public class EsClusterHealthCheck extends HealthCheck {

    private final Logger logger = LoggerFactory.getLogger(EsClusterHealthCheck.class);
    private final RestHighLevelClient client;
    private final boolean failOnYellow;

    /**
     * Construct a new Elasticsearch cluster health check.
     *
     * @param client       an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     * @param failOnYellow whether the health check should fail if the cluster health state is yellow
     */
    public EsClusterHealthCheck(RestHighLevelClient client, boolean failOnYellow) {
        this.client = checkNotNull(client);
        this.failOnYellow = failOnYellow;
    }

    /**
     * Construct a new Elasticsearch cluster health check which will fail if the cluster health state is
     * {@link ClusterHealthStatus#RED}.
     *
     * @param client an Elasticsearch {@link RestHighLevelClient} instance connected to the cluster
     */
    public EsClusterHealthCheck(RestHighLevelClient client) {
        this(client, false);
    }

    /**
     * Perform a check of the Elasticsearch cluster health.
     *
     * @return if the Elasticsearch cluster is healthy, a healthy {@link com.codahale.metrics.health.HealthCheck.Result};
     * otherwise, an unhealthy {@link com.codahale.metrics.health.HealthCheck.Result} with a descriptive error
     * message or exception
     * @throws Exception if there is an unhandled error during the health check; this will result in
     *                   a failed health check
     */
    @Override
    protected Result check() throws Exception {
        try {
            logger.info("Retrieving cluster health status...");
            Response response = client.getLowLevelClient().performRequest("GET", "/_cluster/health");

            ClusterHealthStatus healthStatus;
            try (InputStream is = response.getEntity().getContent()) {
                Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                healthStatus = ClusterHealthStatus.fromString((String) map.get("status"));
            } catch (Exception ex) {
                logger.error("Could not retrieve cluster health status: " + ex.getMessage());
                return Result.unhealthy("Last status: %s", ClusterHealthStatus.RED);
            }

            if (healthStatus == ClusterHealthStatus.RED || (failOnYellow && healthStatus == ClusterHealthStatus.YELLOW)) {
                logger.warn("Cluster health status: " + healthStatus.name());
                return Result.unhealthy("Last status: %s", healthStatus.name());
            } else {
                logger.info("Cluster health status: " + healthStatus.name());
                return Result.healthy("Last status: %s", healthStatus.name());
            }
        } catch (Exception e) {
            logger.error("Could not retrieve cluster health status: ", e);
            throw e;
        }

    }
}
