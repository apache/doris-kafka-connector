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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.S3TvfOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.converter.RecordService;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.service.DorisSystemService;
import org.apache.doris.kafka.connector.writer.load.DefaultThreadFactory;
import org.apache.doris.kafka.connector.writer.s3.S3ClientObjectStore;
import org.apache.doris.kafka.connector.writer.s3.S3ObjectStore;
import org.apache.doris.kafka.connector.writer.s3.S3TvfLoad;
import org.apache.doris.kafka.connector.writer.s3.S3TvfRecordSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stages combined Kafka records as JSON Lines files and commits them through the S3 TVF. */
public class AsyncS3TvfWriter extends DorisWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncS3TvfWriter.class);
    private static final byte NEW_LINE = '\n';
    private static final int MAX_LABEL_LENGTH = 128;
    private static final int UPLOAD_QUEUE_SIZE = 1;
    private static final Runnable UPLOAD_BARRIER = () -> {};

    private S3ObjectStore objectStore;
    private S3TvfLoad load;
    private S3TvfRecordSerializer serializer;
    private ExecutorService uploadExecutor;
    private S3TvfOptions s3Options;
    private String normalizedLabelPrefix;
    private String normalizedTable;

    private final ByteArrayOutputStream tvfBuffer = new ByteArrayOutputStream();
    private final BlockingQueue<Runnable> uploadQueue;
    private final AtomicReference<Throwable> exception = new AtomicReference<>();
    private final List<String> uploadedObjectKeys = new ArrayList<>();
    private int bufferedRecords;
    private String batchUuid;
    private int fileNumber;

    public AsyncS3TvfWriter(
            String tableName,
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisSystemService dorisSystemService,
            DorisConnectMonitor connectMonitor) {
        super(
                tableName,
                topic,
                partition,
                dorisOptions,
                connectionProvider,
                dorisSystemService,
                connectMonitor);
        this.uploadQueue = new LinkedBlockingQueue<>(UPLOAD_QUEUE_SIZE);
        initialize(
                new S3ClientObjectStore(dorisOptions.getS3TvfOptions()),
                new S3TvfLoad(connectionProvider, dorisOptions, dbName, this.tableName),
                this.recordService,
                Executors.newSingleThreadExecutor(
                        new DefaultThreadFactory("s3-tvf-upload-" + dorisOptions.getTaskId())));
    }

    AsyncS3TvfWriter(
            String tableName,
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisSystemService dorisSystemService,
            DorisConnectMonitor connectMonitor,
            RecordService recordService,
            S3ObjectStore objectStore,
            S3TvfLoad load,
            ExecutorService uploadExecutor) {
        this(
                tableName,
                topic,
                partition,
                dorisOptions,
                connectionProvider,
                dorisSystemService,
                connectMonitor,
                recordService,
                objectStore,
                load,
                uploadExecutor,
                new LinkedBlockingQueue<>(UPLOAD_QUEUE_SIZE));
    }

    AsyncS3TvfWriter(
            String tableName,
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisSystemService dorisSystemService,
            DorisConnectMonitor connectMonitor,
            RecordService recordService,
            S3ObjectStore objectStore,
            S3TvfLoad load,
            ExecutorService uploadExecutor,
            BlockingQueue<Runnable> uploadQueue) {
        super(
                tableName,
                topic,
                partition,
                dorisOptions,
                connectionProvider,
                dorisSystemService,
                connectMonitor);
        this.uploadQueue = uploadQueue;
        initialize(objectStore, load, recordService, uploadExecutor);
    }

    private void initialize(
            S3ObjectStore objectStore,
            S3TvfLoad load,
            RecordService recordService,
            ExecutorService uploadExecutor) {
        this.objectStore = objectStore;
        this.load = load;
        this.recordService = recordService;
        this.uploadExecutor = uploadExecutor;
        this.s3Options = dorisOptions.getS3TvfOptions();
        this.serializer =
                new S3TvfRecordSerializer(
                        dorisOptions.getTvfColumns(), dorisOptions.isEnableDelete());
        this.normalizedLabelPrefix = normalize(dorisOptions.getLabelPrefix());
        this.normalizedTable = normalize(tableIdentifier);
        this.uploadExecutor.execute(this::runUploadLoop);
    }

    @Override
    public synchronized void insert(SinkRecord record) {
        checkException();
        String processedRecord = recordService.getProcessedRecord(record);
        if (processedRecord == null) {
            return;
        }
        String serialized = serializer.serialize(processedRecord);
        if (serialized.isEmpty()) {
            return;
        }
        byte[] bytes = serialized.getBytes(StandardCharsets.UTF_8);
        int bytesWithNewLine = bytes.length + 1;
        tvfBuffer.write(bytes, 0, bytes.length);
        tvfBuffer.write(NEW_LINE);
        bufferedRecords++;
        connectMonitor.addAndGetBuffMemoryUsage(bytesWithNewLine);

        if (tvfBuffer.size() >= dorisOptions.getFileSize()
                || (dorisOptions.getRecordNum() != 0
                        && bufferedRecords >= dorisOptions.getRecordNum())) {
            submitBuffer();
        }
    }

    @Override
    public synchronized void flushBuffer() {
        submitBuffer();
    }

    @Override
    public synchronized void commitFlush() {
        submitBuffer();
        if (!hasActiveBatch()) {
            return;
        }

        waitForUploads();
        List<String> objectKeys = Collections.unmodifiableList(new ArrayList<>(uploadedObjectKeys));
        String label = buildLabel();
        try {
            load.load(label, objectKeys);
        } catch (RuntimeException e) {
            exception.compareAndSet(null, e);
            throw e;
        } finally {
            finishBatch();
        }
    }

    private void submitBuffer() {
        if (tvfBuffer.size() == 0) {
            return;
        }
        startBatchIfNeeded();
        int currentFileNumber = fileNumber++;
        String label = buildLabel();
        String fileName =
                label + "_" + dorisOptions.getTaskId() + "_" + currentFileNumber + ".json";
        String objectKey = buildObjectKey(fileName);
        byte[] content = tvfBuffer.toByteArray();
        int recordCount = bufferedRecords;
        putUpload(
                () -> {
                    if (exception.get() != null) {
                        return;
                    }
                    long uploadStartedAtNanos = System.nanoTime();
                    try {
                        objectStore.put(objectKey, content);
                        uploadedObjectKeys.add(objectKey);
                        LOG.info(
                                "S3 TVF object upload completed, fileName={}, objectKey={}, "
                                        + "sizeBytes={}, uploadTimeMs={}",
                                fileName,
                                objectKey,
                                content.length,
                                TimeUnit.NANOSECONDS.toMillis(
                                        System.nanoTime() - uploadStartedAtNanos));
                    } catch (Exception e) {
                        LOG.warn(
                                "S3 TVF object upload failed, fileName={}, objectKey={}, "
                                        + "sizeBytes={}, uploadTimeMs={}",
                                fileName,
                                objectKey,
                                content.length,
                                TimeUnit.NANOSECONDS.toMillis(
                                        System.nanoTime() - uploadStartedAtNanos),
                                e);
                        exception.compareAndSet(
                                null,
                                new DorisException("Failed to upload S3 TVF file " + objectKey, e));
                    }
                });
        connectMonitor.updateBufferMetrics(content.length, recordCount);
        connectMonitor.addAndGetTotalSizeOfData(content.length);
        connectMonitor.addAndGetTotalNumberOfRecord(recordCount);
        connectMonitor.resetMemoryUsage();
        tvfBuffer.reset();
        bufferedRecords = 0;
        LOG.info(
                "Queued S3 TVF file {} for upload ({} bytes, {} records)",
                fileName,
                content.length,
                recordCount);
    }

    private void runUploadLoop() {
        LOG.info("S3 TVF upload worker started");
        try {
            while (!Thread.currentThread().isInterrupted()) {
                uploadQueue.take().run();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            LOG.info("S3 TVF upload worker stopped");
        }
    }

    private void waitForUploads() {
        for (int i = 0; i < UPLOAD_QUEUE_SIZE + 1; i++) {
            putUpload(UPLOAD_BARRIER);
        }
    }

    private void putUpload(Runnable upload) {
        checkException();
        try {
            uploadQueue.put(upload);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DorisException("Interrupted while queuing an S3 TVF upload", e);
        }
        checkException();
    }

    private void checkException() {
        if (exception.get() != null) {
            throw new DorisException(exception.get());
        }
    }

    /** Clears a failed batch so Kafka Connect can retry the records. */
    public synchronized void resetAfterFailure() {
        if (exception.get() == null) {
            return;
        }
        uploadQueue.clear();
        awaitUploadWorkerIdle();
        finishBatch();
        tvfBuffer.reset();
        bufferedRecords = 0;
        connectMonitor.resetMemoryUsage();
        exception.set(null);
        LOG.info("Reset failed S3 TVF batch for retry");
    }

    /**
     * Waits until the single upload worker has passed a queue barrier.
     *
     * <p>Clearing the queue cannot cancel a task that the worker has already dequeued. The upload
     * failure must remain visible until such a task finishes, otherwise it could upload an object
     * from the failed batch and add its key to the next batch.
     */
    private void awaitUploadWorkerIdle() {
        CountDownLatch drained = new CountDownLatch(1);
        try {
            uploadQueue.put(drained::countDown);
            drained.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DorisException("Interrupted while draining failed S3 TVF uploads", e);
        }
    }

    private void startBatchIfNeeded() {
        if (!hasActiveBatch()) {
            batchUuid = UUID.randomUUID().toString().replace("-", "");
        }
    }

    private boolean hasActiveBatch() {
        return batchUuid != null;
    }

    private void finishBatch() {
        uploadedObjectKeys.clear();
        batchUuid = null;
        fileNumber = 0;
    }

    private String buildLabel() {
        String suffix = "_" + batchUuid;
        String label = normalizedLabelPrefix + "_" + normalizedTable + suffix;
        return label.length() <= MAX_LABEL_LENGTH ? label : normalizedLabelPrefix + suffix;
    }

    private String buildObjectKey(String fileName) {
        String prefix = s3Options.getPrefix();
        while (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return prefix
                + "/"
                + normalizedLabelPrefix
                + "/"
                + normalizedTable
                + "/"
                + batchUuid
                + "/"
                + fileName;
    }

    private static String normalize(String value) {
        return value.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    @Override
    public void commit(int partition) {
        // Combined S3 TVF mode commits all task offsets after commitFlush succeeds.
    }

    @Override
    public long getOffset() {
        return 0;
    }

    @Override
    public void fetchOffset() {
        // S3 TVF mode relies on Kafka Connect offsets and does not persist offsets in Doris.
    }

    @Override
    public void close() {
        uploadExecutor.shutdownNow();
        objectStore.close();
    }
}
