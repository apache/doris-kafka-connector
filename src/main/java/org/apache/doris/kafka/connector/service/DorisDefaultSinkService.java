/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.doris.kafka.connector.service;

import com.codahale.metrics.MetricRegistry;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.DorisSinkTask;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.connection.JdbcConnectionProvider;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.metrics.MetricsJmxReporter;
import org.apache.doris.kafka.connector.writer.CopyIntoWriter;
import org.apache.doris.kafka.connector.writer.DorisWriter;
import org.apache.doris.kafka.connector.writer.StreamLoadWriter;
import org.apache.doris.kafka.connector.writer.load.LoadModel;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is per task configuration. A task can be assigned multiple partitions. Major methods are
 * startTask, insert, getOffset and close methods.
 *
 * <p>StartTask: Called when partitions are assigned. Responsible for generating the POJOs.
 *
 * <p>Insert and getOffset are called when {@link DorisSinkTask#put(Collection)} and {@link
 * DorisSinkTask#preCommit(Map)} APIs are called.
 */
public class DorisDefaultSinkService implements DorisSinkService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisDefaultSinkService.class);

    private final ConnectionProvider conn;
    private final Map<String, DorisWriter> writer;
    private final DorisOptions dorisOptions;
    private final MetricsJmxReporter metricsJmxReporter;
    private final DorisConnectMonitor connectMonitor;

    DorisDefaultSinkService(Map<String, String> config) {
        this.dorisOptions = new DorisOptions(config);
        this.writer = new HashMap<>();
        this.conn = new JdbcConnectionProvider(dorisOptions);
        MetricRegistry metricRegistry = new MetricRegistry();
        this.metricsJmxReporter = new MetricsJmxReporter(metricRegistry, dorisOptions.getName());
        this.connectMonitor =
                new DorisConnectMonitor(
                        dorisOptions.isEnableCustomJMX(),
                        dorisOptions.getTaskId(),
                        this.metricsJmxReporter);
    }

    @Override
    public void startTask(TopicPartition topicPartition) {
        startTask(getTopicMapTableInConfig(topicPartition.topic()), topicPartition);
    }

    /**
     * Create new task
     *
     * @param tableName destination table name in doris
     * @param topicPartition TopicPartition passed from Kafka
     */
    @Override
    public void startTask(final String tableName, final TopicPartition topicPartition) {
        // check if the task is already started
        String writerKey =
                getWriterKey(topicPartition.topic(), topicPartition.partition(), tableName);
        if (writer.containsKey(writerKey)) {
            LOG.info("already start task");
        } else {
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            LoadModel loadModel = dorisOptions.getLoadModel();
            DorisWriter dorisWriter =
                    LoadModel.COPY_INTO.equals(loadModel)
                            ? new CopyIntoWriter(
                                    tableName, topic, partition, dorisOptions, conn, connectMonitor)
                            : new StreamLoadWriter(
                                    tableName,
                                    topic,
                                    partition,
                                    dorisOptions,
                                    conn,
                                    connectMonitor);
            writer.put(writerKey, dorisWriter);
            metricsJmxReporter.start();
        }
    }

    @Override
    public void insert(final Collection<SinkRecord> records) {
        // note that records can be empty
        for (SinkRecord record : records) {
            // skip null value records
            if (record.value() == null) {
                LOG.debug(
                        "Null valued record from topic '{}', partition {} and offset {} was skipped",
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset());
                continue;
            }
            // Might happen a count of record based flushing，buffer
            insert(record);
        }
        // check all sink writer to see if they need to be flushed
        for (DorisWriter writer : writer.values()) {
            // Time based flushing
            if (writer.shouldFlush()) {
                writer.flushBuffer();
            }
        }
    }

    @Override
    public void insert(SinkRecord record) {
        String tableName = getSinkDorisTableName(record);
        String writerKey = getWriterKey(record.topic(), record.kafkaPartition(), tableName);
        // init a new topic partition
        if (!writer.containsKey(writerKey)) {
            startTask(tableName, new TopicPartition(record.topic(), record.kafkaPartition()));
        }
        writer.get(writerKey).insert(record);
    }

    @Override
    public long getOffset(final TopicPartition topicPartition) {
        String tpName = getNameIndex(topicPartition.topic(), topicPartition.partition());
        // get all writers for the topic partition
        List<DorisWriter> writers =
                writer.entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(tpName))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
        if (writers.isEmpty()) {
            LOG.info(
                    "Topic: {} Partition: {} hasn't been initialized to get offset",
                    topicPartition.topic(),
                    topicPartition.partition());
            return 0;
        }
        // return the max offset of all writers
        return writers.stream().map(DorisWriter::getOffset).reduce(Long::max).orElse(0L);
    }

    @Override
    public int getPartitionCount() {
        return writer.size();
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.keySet()
                .forEach(
                        tp -> {
                            String tpName = getNameIndex(tp.topic(), tp.partition());
                            // commit all writers that match the topic and partition
                            for (Map.Entry<String, DorisWriter> entry : writer.entrySet()) {
                                if (entry.getKey().startsWith(tpName)) {
                                    entry.getValue().commit(tp.partition());
                                }
                            }
                        });
    }

    /**
     * Get the table name in doris for the given topic. If the table name is not found in the
     * config, use the topic name as the table name.
     *
     * @param topic topic name
     * @return table name in doris
     */
    private String getTopicMapTableInConfig(String topic) {
        String table = dorisOptions.getTopicMapTable(topic);
        if (StringUtils.isEmpty(table)) {
            return topic;
        }
        return table;
    }

    /**
     * Get the table name in doris for the given record.
     *
     * @param record sink record
     * @return table name in doris
     */
    private String getSinkDorisTableName(SinkRecord record) {
        String defaultTableName = getTopicMapTableInConfig(record.topic());
        String field = dorisOptions.getTableField();
        // if the field is not set, use the table name in the config
        if (StringUtils.isEmpty(field)) {
            return defaultTableName;
        }
        if (!(record.value() instanceof Map)) {
            LOG.warn("Only Map objects supported for The 'doris.table.field' configuration");
            return defaultTableName;
        }
        Map<String, Object> map = (Map<String, Object>) record.value();
        // if the field is not found in the record, use the table name in the config
        return map.getOrDefault(field, defaultTableName).toString();
    }

    private static String getNameIndex(String topic, int partition) {
        return topic + "_" + partition;
    }

    /**
     * Parse the writer unique key
     *
     * @param topic topic name
     * @param partition partition number
     * @param tableName table name
     * @return writer key
     */
    private static String getWriterKey(String topic, int partition, String tableName) {
        return topic + "_" + partition + "_" + tableName;
    }
}
