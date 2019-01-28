Dropwizard Elasticsearch
========================

Forked from [mattflax/dropwizard-elasticsearch] [5]

A set of classes for using [Elasticsearch] [1] (version 2.3.0 and higher) in a [Dropwizard] [2] application.

The package provides a [lifecycle-managed] [3] client class (`ManagedEsClient`), a configuration class with the most
common options (`EsConfiguration`), and some [health checks] [4] which can instantly be used in any Dropwizard application.

[1]: http://www.elastic.co/
[2]: http://dropwizard.io/1.2.0/docs
[3]: http://dropwizard.io/1.2.0/docs/manual/core.html#managed-objects
[4]: http://dropwizard.io/1.2.0/docs/manual/core.html#health-checks
[5]: https://github.com/mattflax/dropwizard-elasticsearch/tree/elasticsearch-5.0


Usage
-----

Just add `EsConfiguration` to your [Configuration](http://dropwizard.io/1.2.0/docs/manual/core.html#configuration) class and
create an `ManagedEsClient` instance in the run method of your service.

You can also add one of the existing health checks to your [Environment](http://dropwizard.io/1.2.0/docs/manual/core.html#environments)
in the same method. At least the usage of `EsClusterHealthCheck` is strongly advised.


    public class DemoApplication extends Application<DemoConfiguration> {
        // [...]
        @Override
        public void run(DemoConfiguration config, Environment environment) {
            final ManagedEsClient managedClient = new ManagedEsClient(configuration.getEsConfiguration());
            environment.lifecycle().manage(managedClient);
            environment.healthChecks().register("ES cluster health", new EsClusterHealthCheck(managedClient.getClient()));
            // [...]
        }
    }


Configuration
-------------

The following configuration settings are supported by `EsConfiguration`:

* `servers`: A list of servers for usage with the created RestClient 
* `clusterName`: The name of the Elasticsearch cluster; default: "elasticsearch"
* `settings`: Any additional settings for Elasticsearch, see [Configuration](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/setup-configuration.html)
* `settingsFile`: Any additional settings file for Elasticsearch, see [Configuration](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/setup-configuration.html)

An example configuration file for creating a Node Client could like this:

    clusterName: MyClusterName
    settings:
      node.name: MyCustomNodeName

The order of precedence is: `nodeClient`/`servers`/`clusterName` > `settings` > `settingsFile`, meaning that
any setting in `settingsFile` can be overwritten with `settings` which in turn get overwritten by the specific settings
like `clusterName`.

### Notes for Elasticsearch 6.x

Elasticsearch 6 uses RestClient instead of TransportClient. It does not allow the creation of a NodeClient, and it is removed in this version of
the connector.


Maven Artifacts
---------------

This project is available on Maven Central. To add it to your project simply add the following dependencies to your
`pom.xml`:

    <dependency>
      <groupId>com.engagetech.dropwizard.modules</groupId>
      <artifactId>dropwizard-elasticsearch</artifactId>
      <version>1.2.8-ES6.0.1</version>
    </dependency>


Support
-------

Please file bug reports and feature requests in [GitHub issues](https://github.com/dropwizard/dropwizard-elasticsearch/issues).


Acknowledgements
----------------

Thanks to Alexander Reelsen (@spinscale) for his [Dropwizard Blog Sample](https://github.com/spinscale/dropwizard-blog-sample)
which sparked the idea for this project.


License
-------

Copyright (c) 2013-2017 Jochen Schalanda

This library is licensed under the Apache License, Version 2.0.

See http://www.apache.org/licenses/LICENSE-2.0.html or the LICENSE file in this repository for the full license text.
