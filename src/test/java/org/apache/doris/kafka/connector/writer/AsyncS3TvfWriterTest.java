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

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.converter.RecordService;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.service.DorisSystemService;
import org.apache.doris.kafka.connector.writer.s3.S3ObjectStore;
import org.apache.doris.kafka.connector.writer.s3.S3TvfLoad;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AsyncS3TvfWriterTest {
    private static final String LABEL_PREFIX = "tvf_demo_orders_";

    @Test
    public void testLogsUploadedObjectMetrics() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer = writer(options(1024, 100), store, load, records);
        StringWriter logs = new StringWriter();
        WriterAppender appender = new WriterAppender(new PatternLayout("%m%n"), logs);
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(AsyncS3TvfWriter.class);
        logger.addAppender(appender);

        try {
            writer.insert(record);
            writer.commitFlush();
        } finally {
            logger.removeAppender(appender);
            appender.close();
            writer.close();
        }

        String output = logs.toString();
        Assert.assertTrue(output.contains("S3 TVF object upload completed"));
        Assert.assertTrue(output.contains("objectKey=objects/tvf/demo_orders/"));
        Assert.assertTrue(output.contains("sizeBytes=24"));
        Assert.assertTrue(output.matches("(?s).*uploadTimeMs=\\d+.*"));
    }

    @Test
    public void testUsesTaskIdAndOneBatchLabelForAllFiles() throws Exception {
        DorisOptions options = options(1024, 1);
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord first = TestRecordBuffer.newSinkRecord("ignored", 1);
        SinkRecord second = TestRecordBuffer.newSinkRecord("ignored", 2);
        when(records.getProcessedRecord(first)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        when(records.getProcessedRecord(second)).thenReturn("{\"id\":2,\"name\":\"second\"}");

        AsyncS3TvfWriter writer = writer(options, store, load, records);
        writer.insert(first);
        writer.insert(second);
        writer.commitFlush();

        ArgumentCaptor<String> labelCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List> objectKeys = ArgumentCaptor.forClass(List.class);
        verify(load).load(labelCaptor.capture(), objectKeys.capture());
        String label = labelCaptor.getValue();
        Assert.assertTrue(label.matches(LABEL_PREFIX + "[0-9a-f]{32}"));
        String batchUuid = label.substring(LABEL_PREFIX.length());
        String directory = "objects/tvf/demo_orders/" + batchUuid + "/";
        Assert.assertEquals(2, store.objects.size());
        Assert.assertTrue(store.objects.containsKey(directory + label + "_7_0.json"));
        Assert.assertTrue(store.objects.containsKey(directory + label + "_7_1.json"));

        Assert.assertEquals(
                Arrays.asList(directory + label + "_7_0.json", directory + label + "_7_1.json"),
                objectKeys.getValue());
        writer.close();
    }

    @Test
    public void testLabelDoesNotExceedDorisLimit() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        String labelPrefix = "kafka_tvf_1787740455434";
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer =
                new AsyncS3TvfWriter(
                        "regression_test_stress_load_release_kafka_connector.kafka_connector_tvf_dup",
                        "orders-topic",
                        -1,
                        options(1024, 100, labelPrefix),
                        mock(ConnectionProvider.class),
                        mock(DorisSystemService.class),
                        mock(DorisConnectMonitor.class),
                        records,
                        store,
                        load,
                        Executors.newSingleThreadExecutor());

        writer.insert(record);
        writer.commitFlush();

        ArgumentCaptor<String> label = ArgumentCaptor.forClass(String.class);
        verify(load).load(label.capture(), anyList());
        Assert.assertTrue(label.getValue().matches(labelPrefix + "_[0-9a-f]{32}"));
        writer.close();
    }

    @Test
    public void testSuccessfulCommitStartsNewBatch() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer = writer(options(1024, 100), store, load, records);

        writer.insert(record);
        writer.commitFlush();
        writer.insert(record);
        writer.commitFlush();

        ArgumentCaptor<String> labels = ArgumentCaptor.forClass(String.class);
        verify(load, org.mockito.Mockito.times(2)).load(labels.capture(), anyList());
        Assert.assertTrue(labels.getAllValues().get(0).matches(LABEL_PREFIX + "[0-9a-f]{32}"));
        Assert.assertTrue(labels.getAllValues().get(1).matches(LABEL_PREFIX + "[0-9a-f]{32}"));
        Assert.assertNotEquals(labels.getAllValues().get(0), labels.getAllValues().get(1));
        writer.close();
    }

    @Test
    public void testCommitFlushWritesResidualBufferAndNormalizesRows() throws Exception {
        DorisOptions options = options(1024, 100);
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"extra\":\"drop\"}");

        AsyncS3TvfWriter writer = writer(options, store, load, records);
        writer.insert(record);
        Assert.assertTrue(store.objects.isEmpty());

        writer.commitFlush();

        Assert.assertEquals(1, store.objects.size());
        String content =
                new String(store.objects.values().iterator().next(), StandardCharsets.UTF_8);
        Assert.assertEquals("{\"id\":1,\"name\":null}\n", content);
        verify(load).load(anyString(), anyList());
        writer.close();
    }

    @Test
    public void testSizeThresholdSubmitsFileBeforeCommit() throws Exception {
        DorisOptions options = options(10, 100);
        BlockingObjectStore store = new BlockingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer = writer(options, store, load, records);

        try {
            writer.insert(record);

            Assert.assertTrue(store.uploadStarted.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(store.objects.isEmpty());
            verify(load, never()).load(anyString(), anyList());

            store.continueUpload.countDown();
            writer.commitFlush();

            Assert.assertEquals(1, store.objects.size());
            verify(load).load(anyString(), anyList());
        } finally {
            store.continueUpload.countDown();
            writer.close();
        }
    }

    @Test
    public void testSizeThresholdFlushesAfterAppendingWholeRecord() throws Exception {
        DorisOptions options = options(30, 100);
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord first = TestRecordBuffer.newSinkRecord("ignored", 1);
        SinkRecord second = TestRecordBuffer.newSinkRecord("ignored", 2);
        when(records.getProcessedRecord(first)).thenReturn("{\"id\":1,\"name\":\"a\"}");
        when(records.getProcessedRecord(second)).thenReturn("{\"id\":2,\"name\":\"b\"}");
        AsyncS3TvfWriter writer = writer(options, store, load, records);

        try {
            writer.insert(first);
            writer.insert(second);
            writer.commitFlush();

            Assert.assertEquals(1, store.objects.size());
            Assert.assertEquals(
                    "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n",
                    new String(store.objects.values().iterator().next(), StandardCharsets.UTF_8));
        } finally {
            writer.close();
        }
    }

    @Test
    public void testEmptyCommitDoesNotCreateFileOrLoad() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        AsyncS3TvfWriter writer =
                writer(options(1024, 100), store, load, mock(RecordService.class));

        writer.commitFlush();

        Assert.assertTrue(store.objects.isEmpty());
        verify(load, never()).load(anyString(), anyList());
        writer.close();
    }

    @Test(expected = DorisException.class)
    public void testUploadFailurePreventsLoad() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        store.putFailure = new IOException("upload failed");
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer = writer(options(1024, 100), store, load, records);
        try {
            writer.insert(record);
            writer.commitFlush();
        } finally {
            verify(load, never()).load(anyString(), anyList());
            writer.close();
        }
    }

    @Test
    public void testUploadFailureCanBeResetForRetry() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        store.putFailure = new IOException("upload failed");
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer = writer(options(1024, 100), store, load, records);
        try {
            writer.insert(record);
            try {
                writer.commitFlush();
                Assert.fail("Expected upload to fail");
            } catch (DorisException expected) {
                // Reset the failed batch before Kafka Connect retries the same records.
            }

            writer.resetAfterFailure();
            store.putFailure = null;
            writer.insert(record);
            writer.commitFlush();

            Assert.assertEquals(1, store.objects.size());
            verify(load).load(anyString(), anyList());
        } finally {
            writer.close();
        }
    }

    @Test
    public void testResetDrainsDequeuedUploadBeforeClearingFailure() throws Exception {
        PausingUploadQueue uploadQueue = new PausingUploadQueue();
        FailFirstUploadObjectStore store = new FailFirstUploadObjectStore(uploadQueue);
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord first = TestRecordBuffer.newSinkRecord("ignored", 1);
        SinkRecord second = TestRecordBuffer.newSinkRecord("ignored", 2);
        when(records.getProcessedRecord(first)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        when(records.getProcessedRecord(second)).thenReturn("{\"id\":2,\"name\":\"second\"}");
        AsyncS3TvfWriter writer = writer(options(1024, 1), store, load, records, uploadQueue);
        ExecutorService resetExecutor = Executors.newSingleThreadExecutor();

        try {
            writer.insert(first);
            try {
                writer.insert(second);
            } catch (DorisException expected) {
                // The second upload was queued before the first upload failure became visible.
            }
            Assert.assertTrue(uploadQueue.secondUploadDequeued.await(5, TimeUnit.SECONDS));

            Future<?> reset = resetExecutor.submit(writer::resetAfterFailure);
            Assert.assertTrue(uploadQueue.resetStarted.await(5, TimeUnit.SECONDS));
            uploadQueue.continueSecondUpload.countDown();

            Assert.assertTrue(uploadQueue.secondUploadFinished.await(5, TimeUnit.SECONDS));
            reset.get(5, TimeUnit.SECONDS);
            Assert.assertTrue(store.objects.isEmpty());
        } finally {
            uploadQueue.continueSecondUpload.countDown();
            resetExecutor.shutdownNow();
            writer.close();
        }
    }

    @Test
    public void testCommittedObjectsRemainStaged() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer = writer(options(1024, 100), store, load, records);

        writer.insert(record);
        writer.commitFlush();

        verify(load).load(anyString(), anyList());
        Assert.assertEquals(1, store.objects.size());
        writer.close();
    }

    @Test
    public void testLoadFailureRemainsVisibleUntilReset() throws Exception {
        RecordingObjectStore store = new RecordingObjectStore();
        S3TvfLoad load = mock(S3TvfLoad.class);
        doThrow(new DorisException("failed")).doNothing().when(load).load(anyString(), anyList());
        RecordService records = mock(RecordService.class);
        SinkRecord record = TestRecordBuffer.newSinkRecord("ignored", 1);
        when(records.getProcessedRecord(record)).thenReturn("{\"id\":1,\"name\":\"first\"}");
        AsyncS3TvfWriter writer = writer(options(1024, 100), store, load, records);
        writer.insert(record);

        try {
            writer.commitFlush();
            Assert.fail("Expected first commit to fail");
        } catch (DorisException expected) {
            // Kafka Connect can replay the records after the failed commit.
        }
        try {
            writer.insert(record);
            Assert.fail("Expected load failure to remain visible");
        } catch (DorisException expected) {
            // DorisSinkTask handles the persistent failure through its put retry budget.
        }
        writer.resetAfterFailure();
        writer.insert(record);
        writer.commitFlush();

        ArgumentCaptor<String> labels = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List> objectKeys = ArgumentCaptor.forClass(List.class);
        verify(load, org.mockito.Mockito.times(2)).load(labels.capture(), objectKeys.capture());
        Assert.assertNotEquals(labels.getAllValues().get(0), labels.getAllValues().get(1));
        Assert.assertEquals(1, objectKeys.getAllValues().get(0).size());
        Assert.assertEquals(1, objectKeys.getAllValues().get(1).size());
        Assert.assertNotEquals(
                objectKeys.getAllValues().get(0).get(0), objectKeys.getAllValues().get(1).get(0));
        Assert.assertEquals(2, store.objects.size());
        writer.close();
    }

    private static AsyncS3TvfWriter writer(
            DorisOptions options, S3ObjectStore store, S3TvfLoad load, RecordService records) {
        return writer(options, store, load, records, new LinkedBlockingQueue<>(1));
    }

    private static AsyncS3TvfWriter writer(
            DorisOptions options,
            S3ObjectStore store,
            S3TvfLoad load,
            RecordService records,
            BlockingQueue<Runnable> uploadQueue) {
        return new AsyncS3TvfWriter(
                "demo.orders",
                "orders-topic",
                -1,
                options,
                mock(ConnectionProvider.class),
                mock(DorisSystemService.class),
                mock(DorisConnectMonitor.class),
                records,
                store,
                load,
                Executors.newSingleThreadExecutor(),
                uploadQueue);
    }

    private static DorisOptions options(int bufferSize, int recordCount) throws IOException {
        return options(bufferSize, recordCount, "tvf");
    }

    private static DorisOptions options(int bufferSize, int recordCount, String labelPrefix)
            throws IOException {
        InputStream stream =
                AsyncS3TvfWriterTest.class
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        Properties properties = new Properties();
        properties.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) properties);
        properties.put("task_id", "7");
        properties.put(DorisSinkConnectorConfig.NAME, "connector");
        properties.put(DorisSinkConnectorConfig.DORIS_DATABASE, "");
        properties.put(DorisSinkConnectorConfig.LABEL_PREFIX, labelPrefix);
        properties.put(DorisSinkConnectorConfig.LOAD_MODEL, "tvf");
        properties.put(DorisSinkConnectorConfig.ENABLE_COMBINE_FLUSH, "true");
        properties.put(DorisSinkConnectorConfig.DELIVERY_GUARANTEE, "at_least_once");
        properties.put(DorisSinkConnectorConfig.BUFFER_SIZE_BYTES, String.valueOf(bufferSize));
        properties.put(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS, String.valueOf(recordCount));
        properties.put(DorisSinkConnectorConfig.SINK_S3_ENDPOINT, "https://s3.example.com");
        properties.put(DorisSinkConnectorConfig.SINK_S3_REGION, "us-east-1");
        properties.put(DorisSinkConnectorConfig.SINK_S3_BUCKET, "staging");
        properties.put(DorisSinkConnectorConfig.SINK_S3_PREFIX, "objects");
        properties.put(DorisSinkConnectorConfig.SINK_S3_ACCESS_KEY, "access-key");
        properties.put(DorisSinkConnectorConfig.SINK_S3_SECRET_KEY, "secret-key");
        properties.put(DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX + "columns", "id,name");
        return new DorisOptions((Map) properties);
    }

    private static class RecordingObjectStore implements S3ObjectStore {
        protected final Map<String, byte[]> objects = new LinkedHashMap<>();
        private IOException putFailure;

        @Override
        public synchronized void put(String objectKey, byte[] content) throws IOException {
            if (putFailure != null) {
                throw putFailure;
            }
            objects.put(objectKey, content);
        }

        @Override
        public void close() {}
    }

    private static class BlockingObjectStore extends RecordingObjectStore {
        private final CountDownLatch uploadStarted = new CountDownLatch(1);
        private final CountDownLatch continueUpload = new CountDownLatch(1);

        @Override
        public void put(String objectKey, byte[] content) throws IOException {
            uploadStarted.countDown();
            try {
                continueUpload.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Upload interrupted", e);
            }
            super.put(objectKey, content);
        }
    }

    private static class FailFirstUploadObjectStore extends RecordingObjectStore {
        private final PausingUploadQueue uploadQueue;
        private final AtomicInteger uploadCount = new AtomicInteger();

        private FailFirstUploadObjectStore(PausingUploadQueue uploadQueue) {
            this.uploadQueue = uploadQueue;
        }

        @Override
        public void put(String objectKey, byte[] content) throws IOException {
            if (uploadCount.incrementAndGet() == 1) {
                try {
                    uploadQueue.secondUploadQueued.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Upload interrupted", e);
                }
                throw new IOException("first upload failed");
            }
            super.put(objectKey, content);
        }
    }

    private static class PausingUploadQueue extends LinkedBlockingQueue<Runnable> {
        private final AtomicInteger putCount = new AtomicInteger();
        private final AtomicInteger takeCount = new AtomicInteger();
        private final CountDownLatch secondUploadQueued = new CountDownLatch(1);
        private final CountDownLatch secondUploadDequeued = new CountDownLatch(1);
        private final CountDownLatch continueSecondUpload = new CountDownLatch(1);
        private final CountDownLatch secondUploadFinished = new CountDownLatch(1);
        private final CountDownLatch resetStarted = new CountDownLatch(1);

        private PausingUploadQueue() {
            super(1);
        }

        @Override
        public void put(Runnable upload) throws InterruptedException {
            super.put(upload);
            if (putCount.incrementAndGet() == 2) {
                secondUploadQueued.countDown();
            }
        }

        @Override
        public Runnable take() throws InterruptedException {
            Runnable upload = super.take();
            if (takeCount.incrementAndGet() != 2) {
                return upload;
            }
            secondUploadDequeued.countDown();
            continueSecondUpload.await();
            return () -> {
                try {
                    upload.run();
                } finally {
                    secondUploadFinished.countDown();
                }
            };
        }

        @Override
        public void clear() {
            super.clear();
            resetStarted.countDown();
        }
    }
}
