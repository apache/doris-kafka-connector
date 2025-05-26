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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.writer.load.AsyncDorisStreamLoad;
import org.apache.doris.kafka.connector.writer.load.DefaultThreadFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncStreamLoadWriter extends DorisWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncStreamLoadWriter.class);
    private final LabelGenerator labelGenerator;
    private AsyncDorisStreamLoad dorisStreamLoad;
    private final transient ScheduledExecutorService scheduledExecutorService;

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
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1, new DefaultThreadFactory("stream-load-flush-interval"));
        // when uploading data in streaming mode, we need to regularly detect whether there are
        // exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(
                this::intervalFlush,
                dorisOptions.getFlushTime(),
                dorisOptions.getFlushTime(),
                TimeUnit.SECONDS);
    }

    /** start async thread stream load */
    public void start() {
        this.dorisStreamLoad.start();
    }

    public void insert(final SinkRecord dorisRecord) {
        checkFlushException();
        putBuffer(dorisRecord);
        if (buffer.getBufferSizeBytes() >= dorisOptions.getFileSize()
                || (dorisOptions.getRecordNum() != 0
                        && buffer.getNumOfRecords() >= dorisOptions.getRecordNum())) {
            LOG.info(
                    "trigger flush by buffer size or count, buffer size: {}, num of records: {}, lastoffset : {}",
                    buffer.getBufferSizeBytes(),
                    buffer.getNumOfRecords(),
                    buffer.getLastOffset());
            bufferFullFlush();
        }
    }

    private void bufferFullFlush() {
        doFlush(false, true);
    }

    private void intervalFlush() {
        LOG.debug("interval flush trigger");
        doFlush(false, false);
    }

    public void commitFlush() {
        doFlush(true, false);
    }

    private synchronized void doFlush(boolean waitUtilDone, boolean bufferFull) {
        if (waitUtilDone || bufferFull) {
            flushBuffer(waitUtilDone);
        } else if (dorisStreamLoad.hasCapacity()) {
            flushBuffer(false);
        }
    }

    public synchronized void flushBuffer(boolean waitUtilDone) {
        if (!buffer.isEmpty()) {
            RecordBuffer tmpBuff = buffer;

            String label = labelGenerator.generateLabel(tmpBuff.getLastOffset());
            dorisStreamLoad.flush(label, tmpBuff);
            this.buffer = new RecordBuffer();
        }

        if (waitUtilDone) {
            dorisStreamLoad.forceFlush();
        }
    }

    private void checkFlushException() {
        dorisStreamLoad.checkException();
    }

    @Override
    public void commit(int partition) {
        // Won't go here
    }

    @Override
    public long getOffset() {
        // Won't go here
        return 0;
    }

    @Override
    public void fetchOffset() {
        // Won't go here
    }
}
