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
import java.util.Objects;
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

    protected final ConnectionProvider conn;
    protected final Map<WriterKey, DorisWriter> writer;
    protected final DorisOptions dorisOptions;
    protected final DorisSystemService dorisSystemService;
    protected final MetricsJmxReporter metricsJmxReporter;
    protected final DorisConnectMonitor connectMonitor;
    protected final ObjectMapper objectMapper;
    protected final SinkTaskContext context;

    DorisDefaultSinkService(Map<String, String> config, SinkTaskContext context) {
        this.dorisOptions = new DorisOptions(config);
        this.context = context;
        this.objectMapper = new ObjectMapper();
        this.writer = new HashMap<>();
        this.conn = new JdbcConnectionProvider(dorisOptions);
        this.dorisSystemService = new DorisSystemService(dorisOptions);
        MetricRegistry metricRegistry = new MetricRegistry();
        this.metricsJmxReporter = new MetricsJmxReporter(metricRegistry, dorisOptions.getName());
        this.connectMonitor =
                new DorisConnectMonitor(
                        dorisOptions.isEnableCustomJMX(),
                        dorisOptions.getTaskId(),
                        this.metricsJmxReporter);
    }

    @Override
    public void init() {}

    @Override
    public void close() {
        writer.values().forEach(DorisWriter::close);
        dorisSystemService.close();
        conn.closeConnection();
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
        WriterKey writerKey =
                new WriterKey(topicPartition.topic(), topicPartition.partition(), tableName);
        if (writer.containsKey(writerKey)) {
            LOG.info("already start task");
        } else {
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            LoadModel loadModel = dorisOptions.getLoadModel();
            DorisWriter dorisWriter =
                    LoadModel.COPY_INTO.equals(loadModel)
                            ? new CopyIntoWriter(
                                    tableName,
                                    topic,
                                    partition,
                                    dorisOptions,
                                    conn,
                                    dorisSystemService,
                                    connectMonitor)
                            : new StreamLoadWriter(
                                    tableName,
                                    topic,
                                    partition,
                                    dorisOptions,
                                    conn,
                                    dorisSystemService,
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
                writer.flushBuffer();
            }
        }
    }

    @Override
    public void insert(SinkRecord record) {
        String tableName = getSinkDorisTableName(record);
        WriterKey writerKey = new WriterKey(record.topic(), record.kafkaPartition(), tableName);
        // init a new topic partition
        if (!writer.containsKey(writerKey)) {
            startTask(tableName, new TopicPartition(record.topic(), record.kafkaPartition()));
        }
        writer.get(writerKey).insert(record);
    }

    @Override
    public long getOffset(final TopicPartition topicPartition) {
        // select exactly the writers of this topic-partition (all tables routed to it via
        // 'record.tablename.field'); a string-prefix match would wrongly include other
        // topic-partitions, e.g. writers of partition 1 when asked for partition 10
        List<DorisWriter> writers =
                writer.entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getKey()
                                                .isFor(
                                                        topicPartition.topic(),
                                                        topicPartition.partition()))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
        if (writers.isEmpty()) {
            LOG.info(
                    "Topic: {} Partition: {} hasn't been initialized to get offset",
                    topicPartition.topic(),
                    topicPartition.partition());
            return 0;
        }
        // A topic-partition may be routed to multiple tables via 'record.tablename.field', each
        // with its own writer. Reporting the max of per-writer offsets would claim records as
        // consumed that are still buffered in another writer; after a crash they would be lost.
        // The safe watermark is contiguous: the earliest offset still buffered in any writer, or
        // (when every writer's buffer is empty) the max of the per-writer committed offsets.
        // DorisSinkTask calls commit() (which commits buffered batches to Doris and throws on
        // failure) before getOffset(), so a writer whose buffer is empty has all its records
        // durably committed at this point.

        // Finalize every writer's commit state first: DorisWriter.getOffset() is not a pure
        // accessor (StreamLoadWriter advances committedOffset from flushedOffset and clears its
        // committable list, CopyIntoWriter clears its uploaded file list). Finalizing is valid
        // because preCommit runs commit() — which makes the flushed batches durable or throws,
        // aborting preCommit before any offset is returned — before getOffset(). Skipping the
        // finalization when another writer still buffers records would leave completed
        // transactions pending and re-commit them on the next cycle.
        long maxCommitted = 0;
        for (DorisWriter dorisWriter : writers) {
            maxCommitted = Math.max(maxCommitted, dorisWriter.getOffset());
        }

        long minFirstBufferedOffset =
                writers.stream()
                        .map(DorisWriter::getFirstBufferedOffset)
                        .filter(offset -> offset >= 0)
                        .reduce(Long::min)
                        .orElse(-1L);
        if (minFirstBufferedOffset >= 0) {
            return minFirstBufferedOffset;
        }
        return maxCommitted;
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
                            // commit exactly the writers of this topic-partition (all tables);
                            // same exact-identity selection as getOffset()
                            writer.entrySet().stream()
                                    .filter(
                                            entry ->
                                                    entry.getKey()
                                                            .isFor(tp.topic(), tp.partition()))
                                    .forEach(entry -> entry.getValue().commit(tp.partition()));
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

    /** Inserts the record and returns the (pre-existing or newly created) writer it went to. */
    @VisibleForTesting
    StreamLoadWriter insertAndReturnWriter(SinkRecord record) {
        insert(record);
        return writerOf(record.topic(), record.kafkaPartition(), getSinkDorisTableName(record));
    }

    /** The writer responsible for the given topic-partition + table, or null if none exists. */
    @VisibleForTesting
    StreamLoadWriter writerOf(String topic, int partition, String tableName) {
        return (StreamLoadWriter) writer.get(new WriterKey(topic, partition, tableName));
    }

    /**
     * Commits all writers of the given topic-partition, mirroring what {@link
     * DorisSinkTask#preCommit} does via {@link #commit(Map)}.
     */
    @VisibleForTesting
    void commitOffsets(TopicPartition topicPartition) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(0L));
        commit(offsets);
    }

    /**
     * Structured, exact-identity key of a writer: one Kafka topic-partition routed to one Doris
     * table. Replaces the previous string-prefix key matching, which could select writers of
     * another topic-partition (e.g. topic-partition 1 also matching the key prefix of
     * topic-partition 10) and aggregate unrelated writers into getOffset()/commit().
     */
    static final class WriterKey {
        private final String topic;
        private final int partition;
        private final String tableName;

        WriterKey(String topic, int partition, String tableName) {
            this.topic = topic;
            this.partition = partition;
            this.tableName = tableName;
        }

        /** Whether this key identifies a writer of exactly the given topic-partition. */
        boolean isFor(String topic, int partition) {
            return this.topic.equals(topic) && this.partition == partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WriterKey writerKey = (WriterKey) o;
            return partition == writerKey.partition
                    && topic.equals(writerKey.topic)
                    && tableName.equals(writerKey.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, tableName);
        }

        @Override
        public String toString() {
            return topic + "_" + partition + "_" + tableName;
        }
    }
}
