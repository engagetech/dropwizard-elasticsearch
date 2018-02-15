package io.dropwizard.elasticsearch.managed;

import io.dropwizard.elasticsearch.config.EsConfiguration;
import io.dropwizard.lifecycle.Managed;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Dropwizard managed Elasticsearch {@link RestHighLevelClient} for Elasticsearch 6.
 */
public class ManagedEsClient implements Managed {

    private final Logger logger = LoggerFactory.getLogger(ManagedEsClient.class);
    private RestHighLevelClient client = null;

    /**
     * Create a new managed Elasticsearch {@link RestHighLevelClient}.
     * {@link RestHighLevelClient} is being created with {@link EsConfiguration#servers} as node addresses.
     *
     * @param config a valid {@link EsConfiguration} instance
     */
    public ManagedEsClient(final EsConfiguration config) {

        checkNotNull(config, "EsConfiguration must not be null");

        this.client = new RestHighLevelClient(RestClient.builder(
                config.getServers().stream()
                        .map(s -> HttpHost.create(s))
                        .toArray(HttpHost[]::new))
        );
    }


    /**
     * Create a new managed Elasticsearch {@link RestHighLevelClient} from the provided {@link Client}.
     *
     * @param client an initialized {@link RestHighLevelClient} instance
     */
    public ManagedEsClient(RestHighLevelClient client) {
        this.client = checkNotNull(client, "Elasticsearch client must not be null");
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Elasticsearch client...");
    }

    /**
     * Stops the Elasticsearch {@link RestHighLevelClient}.
     * Called <i>after</i> the service is no longer accepting requests.
     *
     * @throws Exception if something goes wrong.
     */
    @Override
    public void stop() throws Exception {
        logger.info("Stopping Elasticsearch client...");
        closeClient();
    }

    /**
     * Get the managed Elasticsearch {@link RestHighLevelClient} instance.
     *
     * @return a valid Elasticsearch {@link RestHighLevelClient} instance
     */
    public RestHighLevelClient getClient() {
        return client;
    }

    private void closeClient() throws IOException {
        if (null != client) {
            logger.info("Closing client " + client.toString());
            client.close();
        }
    }

}
