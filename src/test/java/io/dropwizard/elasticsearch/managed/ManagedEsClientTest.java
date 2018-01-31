package io.dropwizard.elasticsearch.managed;

import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationValidationException;
import io.dropwizard.configuration.DefaultConfigurationFactoryFactory;
import io.dropwizard.elasticsearch.config.EsConfiguration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.lifecycle.Managed;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link ManagedEsClient}.
 */
public class ManagedEsClientTest {
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
    private final ConfigurationFactory<EsConfiguration> configFactory =
            new DefaultConfigurationFactoryFactory<EsConfiguration>()
                    .create(EsConfiguration.class, validator, Jackson.newObjectMapper(), "dw");

    private ManagedEsClient managedEsClient;

    @Before
    public void setup() {
        managedEsClient = null;
    }

    @After
    public void closeClient() throws Exception {
        if (managedEsClient != null) {
            managedEsClient.stop();
        }
    }

    @Test(expected = NullPointerException.class)
    public void ensureEsConfigurationIsNotNull() throws Exception {
        new ManagedEsClient((EsConfiguration) null);
    }

    @Test(expected = NullPointerException.class)
    public void ensureClientIsNotNull() {
        new ManagedEsClient((RestHighLevelClient) null);
    }

    @Test
    @Ignore
    public void stopShouldCloseTheClient() throws Exception {
        RestHighLevelClient client = mock(RestHighLevelClient.class);
        Managed managed = new ManagedEsClient(client);

        managed.start();
        managed.stop();

        verify(client).close();
    }

//    @Test
//    public void lifecycleMethodsShouldStartAndCloseTheNode() throws Exception {
//        Node node = mock(Node.class);
//        when(node.isClosed()).thenReturn(false);
//        Managed managed = new ManagedEsClient(node);
//
//        managed.start();
//        verify(node).start();
//
//        managed.stop();
//        verify(node).close();
//    }
//
//    @Test
//    public void managedEsClientWithNodeShouldReturnClient() throws Exception {
//        Client client = mock(Client.class);
//        Node node = mock(Node.class);
//        when(node.client()).thenReturn(client);
//
//        ManagedEsClient managed = new ManagedEsClient(node);
//
//        assertSame(client, managed.getClient());
//    }


    @Test
    public void restClientShouldBeCreatedFromConfig() throws URISyntaxException, IOException, ConfigurationException {
        URL configFileUrl = this.getClass().getResource("/rest_client.yml");
        File configFile = new File(configFileUrl.toURI());
        EsConfiguration config = configFactory.build(configFile);

        managedEsClient = new ManagedEsClient(config);
        RestHighLevelClient client = managedEsClient.getClient();

        assertNotNull(client);
    }

    @Test(expected = ConfigurationValidationException.class)
    public void restClientShouldNotBeCreatedFromConfigWithEmptyServerList() throws URISyntaxException, IOException, ConfigurationException {
        URL configFileUrl = this.getClass().getResource("/rest_client_with_empty_server_list.yml");
        File configFile = new File(configFileUrl.toURI());
        EsConfiguration config = configFactory.build(configFile);

        managedEsClient = new ManagedEsClient(config);
    }

    @Test
    public void restClientShouldBeCreatedFromConfigWithDefaultPort() throws URISyntaxException, IOException, ConfigurationException {
        URL configFileUrl = this.getClass().getResource("/rest_client_no_port.yml");
        File configFile = new File(configFileUrl.toURI());
        EsConfiguration config = configFactory.build(configFile);

        managedEsClient = new ManagedEsClient(config);
        RestHighLevelClient client = managedEsClient.getClient();

        assertNotNull(client);
    }

//    @Test @Ignore
//    public void managedClientShouldUseCustomElasticsearchConfig() throws URISyntaxException, IOException, ConfigurationException {
//        URL configFileUrl = this.getClass().getResource("/custom_settings_file.yml");
//        File configFile = new File(configFileUrl.toURI());
//        EsConfiguration config = configFactory.build(configFile);
//
//        managedEsClient = new ManagedEsClient(config);
//        Client client = managedEsClient.getClient();
//
//        assertNotNull(client);
//
//        NodeClient nodeClient = (NodeClient) client;
//        assertEquals(config.getClusterName(), nodeClient.settings().get("cluster.name"));
//        assertEquals("19300-19400", nodeClient.settings().get("transport.tcp.port"));
//    }
//
//    @Test @Ignore
//    public void managedClientObeysPrecedenceOfSettings() throws URISyntaxException, IOException, ConfigurationException {
//        URL configFileUrl = this.getClass().getResource("/custom_settings_precedence.yml");
//        File configFile = new File(configFileUrl.toURI());
//        EsConfiguration config = configFactory.build(configFile);
//
//        managedEsClient = new ManagedEsClient(config);
//        Client client = managedEsClient.getClient();
//
//        assertNotNull(client);
//        assertTrue(client instanceof NodeClient);
//
//        NodeClient nodeClient = (NodeClient) client;
//        assertEquals(config.getClusterName(), nodeClient.settings().get("cluster.name"));
//        assertEquals("29300-29400", nodeClient.settings().get("transport.tcp.port"));
//        assertEquals("target/data/yaml", nodeClient.settings().get("path.home"));
//    }

//    @Test @Ignore
//    public void managedClientObeysPrecedenceOfSettings() throws URISyntaxException, IOException, ConfigurationException {
//        URL configFileUrl = this.getClass().getResource("/custom_settings_precedence.yml");
//        File configFile = new File(configFileUrl.toURI());
//        EsConfiguration config = configFactory.build(configFile);
//
//        managedEsClient = new ManagedEsClient(config);
//        Client client = managedEsClient.getClient();
//
//        assertNotNull(client);
//        assertTrue(client instanceof NodeClient);
//
//        NodeClient nodeClient = (NodeClient) client;
//        assertEquals(config.getClusterName(), nodeClient.settings().get("cluster.name"));
//        assertEquals("29300-29400", nodeClient.settings().get("transport.tcp.port"));
//        assertEquals("target/data/yaml", nodeClient.settings().get("path.home"));
//    }
}
