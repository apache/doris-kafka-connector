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
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/** Background service of data sink, responsible to create/drop table and insert/delete files */
public interface DorisSinkService {

    /**
     * Start the Task.
     *
     * @param topicPartition TopicPartition passed from Kafka
     */
    void startTask(TopicPartition topicPartition);

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     *
     * @param tableName destination table name
     * @param topicPartition TopicPartition passed from Kafka
     */
    void startTask(String tableName, TopicPartition topicPartition);

    /**
     * insert a collections of JSON records will trigger time based flush
     *
     * @param records record content
     */
    void insert(final Collection<SinkRecord> records);

    /**
     * insert a JSON record will not trigger time based flush
     *
     * @param record record content
     */
    void insert(final SinkRecord record);

    /**
     * retrieve offset of last loaded record
     *
     * @param topicPartition topic and partition
     * @return offset, or -1 for empty
     */
    long getOffset(TopicPartition topicPartition);

    /**
     * get the number of partitions assigned to this sink service
     *
     * @return number of partitions
     */
    int getPartitionCount();

    /** commit data to doris. */
    void commit(Map<TopicPartition, OffsetAndMetadata> offsets);
}
