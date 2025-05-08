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

package org.apache.doris.kafka.connector;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.service.DorisSinkService;
import org.apache.doris.kafka.connector.service.DorisSinkServiceFactory;
import org.apache.doris.kafka.connector.utils.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DorisSinkTask implements SinkTask for Kafka Connect framework. */
public class DorisSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkTask.class);
    private DorisSinkService sink = null;
    private DorisOptions options;
    private int remainingRetries;

    /** default constructor, invoked by kafka connect framework */
    public DorisSinkTask() {}

    /**
     * start method handles configuration parsing and one-time setup of the task. loads
     * configuration
     *
     * @param parsedConfig - has the configuration settings
     */
    @Override
    public void start(final Map<String, String> parsedConfig) {
        LOG.info("kafka doris sink task start with {}", parsedConfig);
        this.options = new DorisOptions(parsedConfig);
        this.remainingRetries = options.getMaxRetries();
        this.sink = DorisSinkServiceFactory.getDorisSinkService(parsedConfig, context, options);
    }

    /**
     * stop method is invoked only once outstanding calls to other methods have completed. e.g.
     * after current put, and a final preCommit has completed.
     */
    @Override
    public void stop() {
        LOG.info("kafka doris sink task stopped");
    }

    /**
     * init ingestion task in Sink service
     *
     * @param partitions - The list of all partitions that are now assigned to the task
     */
    @Override
    public void open(final Collection<TopicPartition> partitions) {
        LOG.info("kafka doris sink task open with {}", partitions.toString());
        partitions.forEach(tp -> this.sink.startTask(tp));
    }

    /**
     * Closes sink service
     *
     * <p>Closes all running task because the parameter of open function contains all partition info
     * but not only the new partition
     *
     * @param partitions - The list of all partitions that were assigned to the task
     */
    @Override
    public void close(final Collection<TopicPartition> partitions) {
        LOG.info("kafka doris sink task closed with {}", partitions.toString());
    }

    /**
     * insert record to doris
     *
     * @param records - collection of records from kafka topic/partitions for this connector
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        LOG.info("Read {} records from Kafka", records.size());
        try {
            sink.insert(records);
        } catch (Exception ex) {
            LOG.error("Error inserting records to Doris", ex);
            if (remainingRetries > 0) {
                LOG.info(
                        "Retrying to insert records to Doris, remaining retries: {}",
                        remainingRetries);
                remainingRetries--;
                context.timeout(options.getRetryIntervalMs());
                throw new RetriableException(ex);
            }
            throw ex;
        }
    }

    /**
     * Sync committed offsets
     *
     * @param offsets - the current map of offsets as of the last call to put
     * @return an empty map if Connect-managed offset commit is not desired, otherwise a map of
     *     offsets by topic-partition that are safe to commit. If we return the same offsets that
     *     was passed in, Kafka Connect assumes that all offsets that are already passed to put()
     *     are safe to commit.
     * @throws RetriableException when meet any issue during processing
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) throws RetriableException {
        // return an empty map means that offset commitment is not desired
        if (sink == null || sink.getPartitionCount() == 0) {
            return new HashMap<>();
        }

        sink.commit(offsets);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        // it's ok to just log the error since commit can retry
        try {
            offsets.forEach(
                    (topicPartition, offsetAndMetadata) -> {
                        long offSet = sink.getOffset(topicPartition);
                        if (offSet != 0) {
                            committedOffsets.put(topicPartition, new OffsetAndMetadata(offSet));
                        }
                    });
        } catch (Exception e) {
            return new HashMap<>();
        }
        LOG.info("Returning committed offsets {}", committedOffsets);
        return committedOffsets;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
