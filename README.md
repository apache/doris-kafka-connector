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

# Velodb-Kafka-Connector

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) is a scalable and reliable tool for data transmission between Apache Kafka and other systems. Connectors can be defined Move large amounts of data in and out of Kafka.

VeloDB provides the Sink Connector plugin, which can write data from Kafka topics to Apache Doris, VeloDB Cloud or VeloDB Enterprise.
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

## Help
For additional help, please [file an issue in the repository](https://github.com/apache/doris-kafka-connector/issues) or raise a question in [Apache Doris Public Slack](https://apachedoriscommunity.slack.com/).