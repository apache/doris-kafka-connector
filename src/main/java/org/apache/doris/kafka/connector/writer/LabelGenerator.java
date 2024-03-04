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

import java.util.UUID;

/** Generator label for stream load. */
public class LabelGenerator {
    private final String labelPrefix;
    private String topic;
    private int partition;
    private final boolean enable2PC;
    private String tableIdentifier;
    private int subtaskId;

    public LabelGenerator(String labelPrefix, boolean enable2PC) {
        this.labelPrefix = labelPrefix;
        this.enable2PC = enable2PC;
    }

    public LabelGenerator(
            String labelPrefix,
            boolean enable2PC,
            String topic,
            int partition,
            String tableIdentifier,
            int subtaskId) {
        this(labelPrefix, enable2PC);
        // The label of stream load can not contain `.`
        this.tableIdentifier = tableIdentifier.replace(".", "_");
        this.subtaskId = subtaskId;
        this.topic = topic;
        this.partition = partition;
    }

    public String generateLabel(long lastOffset) {
        StringBuilder sb = new StringBuilder();
        sb.append(labelPrefix)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(topic)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(partition)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(tableIdentifier)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(subtaskId)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(lastOffset)
                .append(LoadConstants.FILE_DELIM_DEFAULT)
                .append(System.currentTimeMillis());
        if (!enable2PC) {
            sb.append(LoadConstants.FILE_DELIM_DEFAULT).append(UUID.randomUUID());
        }
        return sb.toString();
    }
}
