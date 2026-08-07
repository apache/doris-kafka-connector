<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Doris-Kafka-Connector

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) is a scalable and reliable tool for data transmission between Apache Kafka and other systems. Connectors can be defined Move large amounts of data in and out of Kafka.

Doris provides the Sink Connector plugin, which can write data from Kafka topics to Doris.
More information about usage, please visit [Doris Kafka Connector](https://doris.apache.org/docs/ecosystem/doris-kafka-connector)

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## How to Build

After running the following command, the jar package of Doris-Kafka-Connector will be generated in the dist directory.
```
sh build.sh
```
Note: Confluent archive zip file can be packaged through `sh build.sh --confluent`

## Code formatting
Doris-Kafka-Connector uses the AOSP style in google-java-format version 1.7 as the formatting style of the project code.

When you need to format your code, you have two formatting options:

- Execute `sh format.sh` under the project
- Execute `mvn spotless:apply` under the project

After executing the above formatting command, you can use `mvn spotless:check` to check whether the code format meets the requirements.

## TLS configuration

The connector supports opt-in one-way TLS for Doris HTTP and MySQL/JDBC connections. TLS is
disabled by default. When it is enabled, the connector does not probe plaintext endpoints or fall
back to plaintext after a TLS failure.

| Property | Default | Description |
| --- | --- | --- |
| `doris.enable.tls` | `false` | Enables TLS for Doris HTTP and MySQL/JDBC connections. |
| `doris.tls.ca-certificate-path` | empty | Worker-local PEM CA certificate or certificate-chain path. When empty, the JVM default trust store is used. |
| `doris.tls.skip-hostname-verification` | `false` | Disables hostname verification while retaining CA and certificate validation. |
| `doris.tls.excluded-protocols` | empty | Comma-separated protocols that remain plaintext. Supported values are `http` and `mysql`. |

Example:

```properties
doris.urls=doris-fe.example.com
doris.http.port=8040
doris.query.port=9030
doris.enable.tls=true
doris.tls.ca-certificate-path=/etc/kafka-connect/certs/doris-ca.pem
doris.tls.skip-hostname-verification=false
doris.tls.excluded-protocols=
```

`doris.urls` continues to contain host names only; do not add `http://`, `https://`, or a port. The
configured CA file must exist at the same local path on every Kafka Connect worker that may run the
task. Client certificates and private keys are not supported. Copy-into uploads to external object
storage use an independent JVM-system trust context and do not inherit a private Doris CA.
Plaintext Doris URLs are rejected when HTTP TLS is enabled. MySQL TLS requires the bundled
Connector/J 8 driver and never falls back to the legacy driver.

## Help
For additional help, please [file an issue in the repository](https://github.com/apache/doris-kafka-connector/issues) or raise a question in [Apache Doris Public Slack](https://apachedoriscommunity.slack.com/).
