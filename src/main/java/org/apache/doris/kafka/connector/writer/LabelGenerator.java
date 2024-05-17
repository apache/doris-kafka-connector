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

package org.apache.doris.kafka.connector.writer;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/** Generator label for stream load. */
public class LabelGenerator {
    private String topic;
    private int partition;
    private String tableIdentifier;
    // The label of doris stream load cannot be repeated when loading.
    // Under special circumstances (usually load failure) when doris-kafka-connector is started,
    // stream load is performed at the same offset every time, which will cause label duplication.
    // For this reason, we use labelRandomSuffix to generate a random suffix at startup.
    private final AtomicLong labelRandomSuffix;

    public LabelGenerator(String topic, int partition, String tableIdentifier) {
        // The label of stream load can not contain `.`
        this.tableIdentifier = tableIdentifier.replaceAll("\\.", "_");
        this.topic = topic.replaceAll("\\.", "_");
        this.partition = partition;
        Random random = new Random();
        labelRandomSuffix = new AtomicLong(random.nextInt(1000));
    }

    public String generateLabel(long lastOffset) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(partition)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(tableIdentifier)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(lastOffset)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(labelRandomSuffix.getAndIncrement());
        return sb.toString();
    }
}
