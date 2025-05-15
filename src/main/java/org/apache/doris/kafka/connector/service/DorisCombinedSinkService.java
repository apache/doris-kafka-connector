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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.doris.kafka.connector.writer.AsyncStreamLoadWriter;
import org.apache.doris.kafka.connector.writer.DorisWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Combined all partitions and write once. */
public class DorisCombinedSinkService extends DorisDefaultSinkService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisCombinedSinkService.class);
    private final Map<String, HashMap<Integer, Long>> topicPartitionOffset;

    DorisCombinedSinkService(Map<String, String> config, SinkTaskContext context) {
        super(config, context);
        this.topicPartitionOffset = new HashMap<>();
    }

    @Override
    public void init() {
        for (DorisWriter wr : writer.values()) {
            if (wr instanceof AsyncStreamLoadWriter) {
                // When the stream load asynchronous thread down,
                // it needs to be restarted when retrying
                ((AsyncStreamLoadWriter) wr).start();
            }
        }
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
        String writerKey = getWriterKey(topicPartition.topic(), tableName);
        if (writer.containsKey(writerKey)) {
            LOG.info("already start task with key {}", writerKey);
        } else {
            String topic = topicPartition.topic();

            // Only by topic
            int partition = -1;
            DorisWriter dorisWriter =
                    new AsyncStreamLoadWriter(
                            tableName, topic, partition, dorisOptions, conn, connectMonitor);

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

            String topic = record.topic();
            int partition = record.kafkaPartition();
            topicPartitionOffset
                    .computeIfAbsent(topic, k -> new HashMap<>())
                    .put(partition, record.kafkaOffset());
            // Might happen a count of record based flushing，buffer
            insert(record);
        }

        // check all sink writer to see if they need to be flushed
        for (DorisWriter writer : writer.values()) {
            // Time based flushing
            if (writer.shouldFlush()) {
                writer.flushBuffer(false);
            }
        }
    }

    @Override
    public void insert(SinkRecord record) {
        String tableName = getSinkDorisTableName(record);
        String writerKey = getWriterKey(record.topic(), tableName);
        // init a new topic partition
        if (!writer.containsKey(writerKey)) {
            startTask(tableName, new TopicPartition(record.topic(), -1));
        }
        writer.get(writerKey).insert(record);
    }

    @Override
    public long getOffset(final TopicPartition topicPartition) {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        if (topicPartitionOffset.containsKey(topic)
                && topicPartitionOffset.get(topic).containsKey(partition)) {
            return topicPartitionOffset.get(topic).get(partition);
        }
        return 0;
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Here we force flushing the data in memory once to
        // ensure that the offsets recorded in topicPartitionOffset have been flushed to doris
        for (DorisWriter writer : writer.values()) {
            writer.flushBuffer(true);
        }
    }

    /**
     * Parse the writer unique key
     *
     * @param topic topic name
     * @param tableName table name
     * @return writer key
     */
    private String getWriterKey(String topic, String tableName) {
        if (dorisOptions.getTopicMapTable(topic).equals(tableName)) {
            return topic;
        }
        return topic + "_" + tableName;
    }
}
