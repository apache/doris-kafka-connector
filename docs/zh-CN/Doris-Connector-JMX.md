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

# 使用 Java Management Extensions (JMX) 监视 Doris Kafka Connector
本主题介绍如何使用 Java 管理扩展 (JMX) 监控 Doris Kafka Connector。 

Kafka Connect 提供了部分默认的 JMX Metrics，Doris Kafka Connector 也对这部分 Metrics 进行扩展，以便能获取到更多的监控指。
您也可以将这些指标导入到第三方的监控工具，包括（Prometheus、Grafana）。


## 配置 JMX
JMX Metrics 的功能默认是开启的，如果需要禁用这个功能，可以在参数中设置 `jmx=false`。
1. 启用 JMX 以连接到您的 Kafka 安装
   
   - 与远程服务器建立连接。请在 Kafka Connect 启动脚本 `KAFKA_JMX_OPTS` 参数中追加以下内容：
     ````
     export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true
      -Dcom.sun.management.jmxremote.authenticate=false
      -Dcom.sun.management.jmxremote.ssl=false
      -Djava.rmi.server.hostname=<ip_address>
      -Dcom.sun.management.jmxremote.port=<jmx_port>"
     ````
     `<ip_address>` 为当前 Kafka Connect 主机地址

     `<jmx_port>` 指定监听 JMX 的端口（请确保该端口未被占用）

   - 在同一台服务器上建立连接。请在 Kafka Connect 启动脚本中添加 `JMX_PORT` 参数
     ````
     export JMX_PORT=<port_number>
     ````
2. 重启 Kafka Connect

## 使用 Doris Kafka Connector 管理 Beans(MBeans)
Kafka Connector Doris 提供了用于访问管理对象的 MBeans，通过这些 MBeans 指标，以便清晰的了解到连接器内部的状态。

Kafka Connector MBean 对象名称的一般格式为：
>`kafka.connector.doris:connector={connector_name}, task={task_id},category={category_name},name={metric_name}`

<br>`connector={connector_name}` 在 Kafka Connect 配置文件中指定的名称
<br>`{task_id}` Kafka Connect 启动时默认分配的 task_id
<br>`{category_name}` 指定 MBean 的名称，每个 MBean 包含具体的各项指标
<br>`{metric_name}` 指标名称



以下为 Doris Kafka Connector 的 Category  与 Metrics 说明：<br>

<table>
    <tr>
        <th>Category</th><th>Metric Name</th><th>Data Type</th><th>Description</th>
    </tr>
    <tr>
        <td rowspan="1">offsets</td><td>committed-offset</td><td>long</td><td>已成功提交至 Doris 的 offset </td>
    </tr>
    <tr>
        <td rowspan="3">total-processed</td><td>total-load-count</td><td>long</td><td>累计通过 stream-load 成功导入数据到 doris 的总数（或者是通过 copy-into 上传的数据文件总数）</td>
    </tr>
    <tr>
        <td>total-record-count</td><td>long</td><td>累计处理的 record 总数</td>
    </tr>
    <tr>
        <td>total-data-size</td><td>long</td><td>累计处理的数据大小（单位：byte）</td>
    </tr>
    <tr>
        <td rowspan="3">buffer</td><td>buffer-record-count</td><td>long</td><td>往 Doris 进行 flush 时候当前 buffer 中的 Record 数量</td>
    </tr>
    <tr>
        <td>buffer-size-bytes</td><td>long</td><td>往 Doris 进行 flush 时候当前 buffer 中的 bytes 大小</td>
    </tr>
    <tr>
        <td>buffer-memory-usage</td><td>long</td><td>当前 buffer 所占用的内存大小（单位：byte）</td>
    </tr>
</table>

关于 Kafka Connect 默认的 JMX Metrics，可以参考：[Monitoring Kafka Connect and Connectors
](https://docs.confluent.io/kafka-connectors/self-managed/monitoring.html#monitoring-kconnect-long-and-connectors)