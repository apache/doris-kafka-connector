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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.converter.RecordService;
import org.apache.doris.kafka.connector.exception.ArgumentsException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DorisWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.class);
    protected static final ObjectMapper objectMapper = new ObjectMapper();
    protected String tableName;
    protected String dbName;
    protected final String tableIdentifier;
    protected List<String> fileNames;
    private RecordBuffer buffer;
    protected final AtomicLong committedOffset; // loaded offset + 1
    protected final AtomicLong flushedOffset; // flushed offset
    protected final AtomicLong processedOffset; // processed offset
    protected long previousFlushTimeStamp;

    // make the initialization lazy
    private boolean hasInitialized = false;
    protected final AtomicLong offsetPersistedInDoris = new AtomicLong(-1);
    protected final ConnectionProvider connectionProvider;
    protected final DorisOptions dorisOptions;
    protected final String topic;
    protected final int partition;
    protected RecordService recordService;
    protected int taskId;
    protected final DorisConnectMonitor connectMonitor;

    public DorisWriter(
            String tableName,
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisConnectMonitor connectMonitor) {
        this.topic = topic;
        this.partition = partition;
        this.tableName = tableName;
        if (StringUtils.isNotEmpty(dorisOptions.getDatabase())) {
            this.dbName = dorisOptions.getDatabase();
        } else if (tableName.contains(".")) {
            String[] dbTbl = tableName.split("\\.");
            this.dbName = dbTbl[0];
            this.tableName = dbTbl[1];
        } else {
            LOG.error("Error params database {}, table {}, topic {}", dbName, tableName, topic);
            throw new ArgumentsException("Failed to get database and table names");
        }

        this.tableIdentifier = dbName + "." + this.tableName;
        this.fileNames = new ArrayList<>();
        this.buffer = new RecordBuffer();
        this.processedOffset = new AtomicLong(-1);
        this.flushedOffset = new AtomicLong(-1);
        this.committedOffset = new AtomicLong(0);
        this.previousFlushTimeStamp = System.currentTimeMillis();

        this.dorisOptions = dorisOptions;
        this.connectionProvider = connectionProvider;
        this.recordService = new RecordService(dorisOptions);
        this.connectMonitor = connectMonitor;
    }

    /** read offset from doris */
    public abstract void fetchOffset();

    public void insert(final SinkRecord record) {}

    protected void initRecord(final SinkRecord record) {
        // init offset
        if (!hasInitialized
                && DeliveryGuarantee.EXACTLY_ONCE.equals(dorisOptions.getDeliveryGuarantee())) {
            // This will only be called once at the beginning when an offset arrives for first time
            // after connector starts/rebalance
            LOG.info(
                    "read topic {} partition {} offset from doris",
                    record.topic(),
                    record.kafkaPartition());
            fetchOffset();
            this.hasInitialized = true;
        }
    }

    protected void insertRecord(final SinkRecord record) {
        // discard the record if the record offset is smaller or equal to server side offset
        if (record.kafkaOffset() > this.offsetPersistedInDoris.get()
                && record.kafkaOffset() > processedOffset.get()) {
            SinkRecord dorisRecord = record;
            RecordBuffer tmpBuff = null;
            processedOffset.set(dorisRecord.kafkaOffset());
            putBuffer(dorisRecord);
            if (buffer.getBufferSizeBytes() >= dorisOptions.getFileSize()
                    || (dorisOptions.getRecordNum() != 0
                            && buffer.getNumOfRecords() >= dorisOptions.getRecordNum())) {
                tmpBuff = buffer;
                this.buffer = new RecordBuffer();
            }

            if (tmpBuff != null) {
                flush(tmpBuff);
            }
        }
    }

    protected void updateFlushedMetrics(final RecordBuffer buffer) {
        // compute metrics which will be exported to JMX for now.
        connectMonitor.updateBufferMetrics(buffer.getBufferSizeBytes(), buffer.getNumOfRecords());
        this.previousFlushTimeStamp = System.currentTimeMillis();
        // This is safe and atomic
        flushedOffset.updateAndGet((value) -> Math.max(buffer.getLastOffset() + 1, value));
        connectMonitor.resetMemoryUsage();
        connectMonitor.addAndGetLoadCount();
    }

    protected void putBuffer(SinkRecord record) {
        String processedRecord = recordService.getProcessedRecord(record);
        if (buffer.getBufferSizeBytes() == 0L) {
            buffer.setFirstOffset(record.kafkaOffset());
        }
        buffer.insert(processedRecord);
        buffer.setLastOffset(record.kafkaOffset());
        connectMonitor.addAndGetBuffMemoryUsage(
                processedRecord.getBytes(StandardCharsets.UTF_8).length);
    }

    public boolean shouldFlush() {
        return (System.currentTimeMillis() - this.previousFlushTimeStamp)
                >= (dorisOptions.getFlushTime() * 1000);
    }

    public void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }
        RecordBuffer tmpBuff = buffer;
        this.buffer = new RecordBuffer();
        flush(tmpBuff);
    }

    public abstract void commit(int partition);

    public abstract long getOffset();

    protected void flush(final RecordBuffer buff) {
        if (buff == null || buff.isEmpty()) {
            return;
        }
        connectMonitor.addAndGetTotalSizeOfData(buff.getBufferSizeBytes());
        connectMonitor.addAndGetTotalNumberOfRecord(buff.getNumOfRecords());
    }

    @VisibleForTesting
    protected Long getOffsetPersistedInDoris() {
        return this.offsetPersistedInDoris.get();
    }

    protected RecordBuffer getBuffer() {
        return this.buffer;
    }
}
