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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.DorisSinkTask;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.connection.JdbcConnectionProvider;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.metrics.MetricsJmxReporter;
import org.apache.doris.kafka.connector.model.BehaviorOnNullValues;
import org.apache.doris.kafka.connector.writer.CopyIntoWriter;
import org.apache.doris.kafka.connector.writer.DorisWriter;
import org.apache.doris.kafka.connector.writer.StreamLoadWriter;
import org.apache.doris.kafka.connector.writer.load.LoadModel;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
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
    private final ObjectMapper objectMapper;
    private final SinkTaskContext context;

    DorisDefaultSinkService(Map<String, String> config, SinkTaskContext context) {
        this.dorisOptions = new DorisOptions(config);
        this.context = context;
        this.objectMapper = new ObjectMapper();
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
        startTask(dorisOptions.getTopicMapTable(topicPartition.topic()), topicPartition);
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
            // skip records
            if (shouldSkipRecord(record)) {
                continue;
            }
            // check topic mutating SMTs
            checkTopicMutating(record);
            // Might happen a count of record based flushing，buffer
            insert(record);
        }
        // check all sink writer to see if they need to be flushed
        for (DorisWriter writer : writer.values()) {
            // Time based flushing
            if (writer.shouldFlush()) {
                LOG.info("trigger flush by time.");
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

    /** Check if the topic of record is mutated */
    public void checkTopicMutating(SinkRecord record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        if (!context.assignment().contains(tp)) {
            throw new ConnectException(
                    "Unexpected topic name: ["
                            + record.topic()
                            + "] that doesn't match assigned partitions. "
                            + "Connector doesn't support topic mutating SMTs. ");
        }
    }

    @VisibleForTesting
    public boolean shouldSkipRecord(SinkRecord record) {
        if (record.value() == null) {
            switch (dorisOptions.getBehaviorOnNullValues()) {
                case FAIL:
                    throw new DataException(
                            String.format(
                                    "Null valued record from topic %s, partition %s and offset %s was failed "
                                            + "(the configuration property '%s' is '%s').",
                                    record.topic(),
                                    record.kafkaPartition(),
                                    record.kafkaOffset(),
                                    DorisSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES,
                                    BehaviorOnNullValues.FAIL));
                case IGNORE:
                default:
                    LOG.debug(
                            "Null valued record from topic '{}', partition {} and offset {} was skipped",
                            record.topic(),
                            record.kafkaPartition(),
                            record.kafkaOffset());
                    return true;
            }
        }
        return false;
    }

    /**
     * Get the table name in doris for the given record.
     *
     * @param record sink record
     * @return table name in doris
     */
    @VisibleForTesting
    public String getSinkDorisTableName(SinkRecord record) {
        String defaultTableName = dorisOptions.getTopicMapTable(record.topic());
        String field = dorisOptions.getTableNameField();
        // if the field is not set, use the table name in the config
        if (StringUtils.isEmpty(field)) {
            return defaultTableName;
        }
        return parseRecordTableName(defaultTableName, field, record);
    }

    private String parseRecordTableName(
            String defaultTableName, String tableNameField, SinkRecord record) {
        Object recordValue = record.value();
        Map<String, Object> recordMap = Collections.emptyMap();
        if (recordValue instanceof Struct) {
            LOG.warn(
                    "The Struct type record not supported for The 'record.tablename.field' configuration, field={}",
                    tableNameField);
            return defaultTableName;
        } else if (recordValue instanceof Map) {
            recordMap = (Map<String, Object>) recordValue;
        } else if (recordValue instanceof String) {
            try {
                recordMap = objectMapper.readValue((String) recordValue, Map.class);
            } catch (JsonProcessingException e) {
                LOG.warn(
                        "The String type record failed to parse record value to map. record={}, field={}",
                        recordValue,
                        tableNameField,
                        e);
            }
        }
        // if the field is not found in the record, use the table name in the config
        if (!recordMap.containsKey(tableNameField)) {
            return defaultTableName;
        }
        return recordMap.get(tableNameField).toString();
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
    private String getWriterKey(String topic, int partition, String tableName) {
        if (dorisOptions.getTopicMapTable(topic).equals(tableName)) {
            return topic + "_" + partition;
        }
        return topic + "_" + partition + "_" + tableName;
    }
}
