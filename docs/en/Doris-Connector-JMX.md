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

# Monitor VeloDB Kafka Connector using Java Management Extensions (JMX)
This article describes how to monitor the VeloDB Kafka Connector using Java Management Extensions (JMX). 

Kafka Connect provides some default JMX Metrics, and VeloDB Kafka Connector also extends these Metrics to obtain more monitoring indicators.
You can also import these indicators into third-party monitoring tools, including (Prometheus, Grafana).


## Configure JMX
The JMX Metrics function is enabled by default. If you need to disable this function, you can set `jmx=false` in the parameters.
1. Enable JMX to connect to your Kafka installation
   
   - Establish a connection with the remote server. Please append the following content to the `KAFKA_JMX_OPTS` parameter in the Kafka Connect startup script:
     ````
     export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true
      -Dcom.sun.management.jmxremote.authenticate=false
      -Dcom.sun.management.jmxremote.ssl=false
      -Djava.rmi.server.hostname=<ip_address>
      -Dcom.sun.management.jmxremote.port=<jmx_port>"
     ````
     `<ip_address>` is the current Kafka Connect host address

     `<jmx_port>` specifies the port to listen to JMX (please ensure that the port is not occupied)

   - Establish a connection on the same server. Please add the `JMX_PORT` parameter in the Kafka Connect startup script
     ````
     export JMX_PORT=<port_number>
     ````
2. Restart Kafka Connect

## Use VeloDB Kafka Connector to manage Beans (MBeans)
Kafka Connector Doris provides MBeans for accessing management objects. Through these MBeans indicators, you can clearly understand the internal status of the connector.

The general format of Kafka Connector MBean object names is:
>`kafka.connector.doris:connector={connector_name}, task={task_id},category={category_name},name={metric_name}`

<br>`connector={connector_name}` The name specified in the Kafka Connect configuration file
<br>`{task_id}` The task_id assigned by default when Kafka Connect starts
<br>`{category_name}` specifies the name of the MBean. Each MBean contains specific indicators.
<br>`{metric_name}` metric name



The following is the Category and Metrics description of Doris Kafka Connector:<br>

<table>
    <tr>
        <th>Category</th><th>Metric Name</th><th>Data Type</th><th>Description</th>
    </tr>
    <tr>
        <td rowspan="1">offsets</td><td>committed-offset</td><td>long</td><td>Successfully submitted to Doris' offset</td>
    </tr>
    <tr>
        <td rowspan="3">total-processed</td><td>total-load-count</td><td>long</td><td>The total number of data successfully imported to doris through stream-load (or the total number of data files uploaded through copy-into)</td>
    </tr>
    <tr>
        <td>total-record-count</td><td>long</td><td>Total number of records processed cumulatively</td>
    </tr>
    <tr>
        <td>total-data-size</td><td>long</td><td>Cumulative processed data size (unit: byte)</td>
    </tr>
    <tr>
        <td rowspan="3">buffer</td><td>buffer-record-count</td><td>long</td><td>The number of Records in the current buffer when flushing Doris</td>
    </tr>
    <tr>
        <td>buffer-size-bytes</td><td>long</td><td>The size of bytes in the current buffer when flushing Doris</td>
    </tr>
    <tr>
        <td>buffer-memory-usage</td><td>long</td><td>The memory size currently occupied by buffer (unit: byte)</td>
    </tr>
</table>

For information about Kafka Connect’s default JMX Metrics, please refer to: [Monitoring Kafka Connect and Connectors
](https://docs.confluent.io/kafka-connectors/self-managed/monitoring.html#monitoring-kconnect-long-and-connectors)