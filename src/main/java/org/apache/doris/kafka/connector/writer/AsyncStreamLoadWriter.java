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

import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.writer.load.AsyncDorisStreamLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncStreamLoadWriter extends StreamLoadWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncStreamLoadWriter.class);
    private final LabelGenerator labelGenerator;
    private AsyncDorisStreamLoad dorisStreamLoad;

    public AsyncStreamLoadWriter(
            String tableName,
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisConnectMonitor connectMonitor) {
        super(tableName, topic, partition, dorisOptions, connectionProvider, connectMonitor);
        this.labelGenerator = new LabelGenerator(topic, partition, tableIdentifier);
        BackendUtils backendUtils = BackendUtils.getInstance(dorisOptions, LOG);
        this.dorisStreamLoad =
                new AsyncDorisStreamLoad(backendUtils, dorisOptions, topic, this.tableName);
    }

    protected void flush(final RecordBuffer buff) {
        String label = labelGenerator.generateLabel(buff.getLastOffset());
        dorisStreamLoad.asyncLoad(label, buff);

        // update metrics
        updateFlushedMetrics(buff);
        connectMonitor.addAndGetTotalSizeOfData(buff.getBufferSizeBytes());
        connectMonitor.addAndGetTotalNumberOfRecord(buff.getNumOfRecords());
    }

    public void flushBuffer() {
        if (!buffer.isEmpty()) {
            RecordBuffer tmpBuff = buffer;
            this.buffer = new RecordBuffer();
            flush(tmpBuff);
        }
        dorisStreamLoad.forceLoad();
    }
}
