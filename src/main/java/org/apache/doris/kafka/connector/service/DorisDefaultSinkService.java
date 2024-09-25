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
import java.util.Map;
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
        startTask(null, topicPartition);
    }

    /**
     * Create new task
     *
     * @param tableName destination table name in doris
     * @param topicPartition TopicPartition passed from Kafka
     */
    @Override
    public void startTask(final String tableName, final TopicPartition topicPartition) {
        // fetch topic partition
        String nameIndex = getNameIndex(topicPartition.topic(), topicPartition.partition());
        if (writer.containsKey(nameIndex)) {
            LOG.info("already start task");
        } else {
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            LoadModel loadModel = dorisOptions.getLoadModel();
            DorisWriter dorisWriter =
                    LoadModel.COPY_INTO.equals(loadModel)
                            ? new CopyIntoWriter(
                                    topic, partition, dorisOptions, conn, connectMonitor)
                            : new StreamLoadWriter(
                                    topic, partition, dorisOptions, conn, connectMonitor);
            writer.put(nameIndex, dorisWriter);
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
        String nameIndex = getNameIndex(record.topic(), record.kafkaPartition());
        // init a new topic partition
        if (!writer.containsKey(nameIndex)) {
            startTask(new TopicPartition(record.topic(), record.kafkaPartition()));
        }
        writer.get(nameIndex).insert(record);
    }

    @Override
    public long getOffset(final TopicPartition topicPartition) {
        String name = getNameIndex(topicPartition.topic(), topicPartition.partition());
        if (writer.containsKey(name)) {
            return writer.get(name).getOffset();
        } else {
            LOG.info(
                    "Topic: {} Partition: {} hasn't been initialized to get offset",
                    topicPartition.topic(),
                    topicPartition.partition());
            return 0;
        }
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
                            String name = getNameIndex(tp.topic(), tp.partition());
                            writer.get(name).commit(tp.partition());
                        });
    }

    private static String getNameIndex(String topic, int partition) {
        return topic + "_" + partition;
    }
}
