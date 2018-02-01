package io.dropwizard.elasticsearch.health;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.DefaultConfigurationFactoryFactory;
import io.dropwizard.elasticsearch.config.EsConfiguration;
import io.dropwizard.jackson.Jackson;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link EsClusterHealthCheck}
 */
public class EsClusterHealthCheckTest {
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    private final ConfigurationFactory<EsConfiguration> configFactory =
            new DefaultConfigurationFactoryFactory<EsConfiguration>()
                    .create(EsConfiguration.class, validator, Jackson.newObjectMapper(), "dw");

    @Test(expected = NullPointerException.class)
    public void initializationWithNullClientShouldFail() {
        new EsClusterHealthCheck(null);
    }

    @Test
    public void initializationWithClientShouldSucceed() {
        new EsClusterHealthCheck(mock(RestHighLevelClient.class));
    }
}
