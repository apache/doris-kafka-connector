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

package org.apache.doris.kafka.connector.writer.load;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.exception.StreamLoadException;
import org.apache.doris.kafka.connector.model.KafkaRespContent;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.utils.DorisUrlBuilder;
import org.apache.doris.kafka.connector.utils.HttpPutBuilder;
import org.apache.doris.kafka.connector.utils.HttpUtils;
import org.apache.doris.kafka.connector.writer.LoadConstants;
import org.apache.doris.kafka.connector.writer.LoadStatus;
import org.apache.doris.kafka.connector.writer.RecordBuffer;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncDorisStreamLoad extends DataLoad {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncDorisStreamLoad.class);
    private static final String LOAD_PATH_PATTERN = "/api/%s/%s/_stream_load";
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(LoadStatus.SUCCESS, LoadStatus.PUBLISH_TIMEOUT));

    // Doris answers a duplicate load label with the "Label Already Exists" status and, in
    // the same response, the status of the existing job: FINISHED means the earlier load
    // committed the data, RUNNING means it is still executing
    private static final String LABEL_ALREADY_EXISTS_STATUS = "Label Already Exists";
    private static final String EXISTING_JOB_STATUS_FINISHED = "FINISHED";
    private String loadUrl;
    private final DorisOptions dorisOptions;
    private final String topic;
    private String hostPort;
    private final CloseableHttpClient httpClient;
    private final BackendUtils backendUtils;
    private Queue<KafkaRespContent> respContents = new LinkedList<>();
    private final boolean enableGroupCommit;
    private final boolean enableGzCompress;
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<RecordBuffer> flushQueue = new LinkedBlockingDeque<>(1);
    private final AtomicBoolean started;
    private volatile boolean loadThreadAlive = false;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    // Set for the lifetime of this object once a load failed and its retries were exhausted:
    // the failed buffer is dropped and the records it held can only be recovered by replaying
    // from the last committed Kafka offsets (task restart). start() restarts the executor
    // thread but must not clear this latch, otherwise later commits could advance offsets
    // past the records that were dropped by the failed load.
    private final AtomicBoolean loadFailed = new AtomicBoolean(false);

    public AsyncDorisStreamLoad(
            BackendUtils backendUtils, DorisOptions dorisOptions, String topic, String table) {
        this(
                backendUtils,
                dorisOptions,
                topic,
                table,
                new HttpUtils(dorisOptions.getTlsOptions()).getHttpClient());
    }

    public AsyncDorisStreamLoad(
            BackendUtils backendUtils,
            DorisOptions dorisOptions,
            String topic,
            String table,
            CloseableHttpClient httpClient) {
        this.database = dorisOptions.getDatabase();
        this.table = table;
        this.user = dorisOptions.getUser();
        this.password = dorisOptions.getPassword();
        this.dorisOptions = dorisOptions;
        this.backendUtils = backendUtils;
        this.topic = topic;
        this.httpClient = httpClient;
        this.enableGroupCommit = dorisOptions.enableGroupCommit();
        this.enableGzCompress =
                LoadConstants.COMPRESS_TYPE_GZ.equals(
                        dorisOptions
                                .getStreamLoadProp()
                                .getProperty(LoadConstants.COMPRESS_TYPE, ""));
        this.loadAsyncExecutor = new LoadAsyncExecutor();
        this.loadExecutorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("streamload-executor"),
                        new ThreadPoolExecutor.AbortPolicy());

        this.started = new AtomicBoolean(true);
        start();
    }

    void load(String label, RecordBuffer buffer) throws IOException {
        loadAsyncExecutor.load(label, buffer);
    }

    public void start() {
        if (!loadThreadAlive) {
            this.loadExecutorService.execute(loadAsyncExecutor);
            this.exception.set(null);
        }
    }

    public void flush(String label, RecordBuffer buffer) {
        checkFlushException();
        buffer.setLabel(label);
        putRecordToFlushQueue(buffer);
    }

    public void forceFlush() {
        checkFlushException();
        waitAsyncLoadFinish();
    }

    private void waitAsyncLoadFinish() {
        // Make sure the data in the queue has been flushed
        for (int i = 0; i < 2; i++) {
            RecordBuffer empty = new RecordBuffer();
            putRecordToFlushQueue(empty);
        }
    }

    private void putRecordToFlushQueue(RecordBuffer buffer) {
        checkFlushException();
        if (!loadThreadAlive) {
            throw new RuntimeException("load thread already exit, write was interrupted");
        }
        try {
            flushQueue.put(buffer);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to put record buffer to flush queue");
        }
        checkFlushException();
    }

    private void checkFlushException() {
        if (exception.get() != null) {
            throw new DorisException(exception.get());
        }
        if (loadFailed.get()) {
            throw new DorisException(
                    "A previous async stream load failed and its retries were exhausted; its "
                            + "buffered records are no longer available for retry, the task must"
                            + " restart from the last committed Kafka offsets");
        }
    }

    public void close() {
        if (started.compareAndSet(true, false)) {
            LOG.info("close executorService");
            loadExecutorService.shutdownNow();
        }
        try {
            httpClient.close();
        } catch (IOException e) {
            LOG.warn("Failed to close asynchronous stream load HTTP client", e);
        }
    }

    public boolean hasCapacity() {
        return flushQueue.remainingCapacity() > 0;
    }

    public void checkException() {
        checkFlushException();
    }

    class LoadAsyncExecutor implements Runnable {

        @Override
        public void run() {
            LOG.info("LoadAsyncExecutor start");
            loadThreadAlive = true;
            while (started.get()) {
                try {
                    RecordBuffer buffer = flushQueue.poll(2000L, TimeUnit.MILLISECONDS);
                    if (buffer == null) {
                        continue;
                    }
                    if (buffer.getLabel() == null) {
                        // When the label is empty, it is the eof buffer for checkpoint flush.
                        continue;
                    }
                    loadWithRetry(buffer);
                    if (loadFailed.get()) {
                        break;
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (!started.get()) {
                        break;
                    }
                    loadFailed.set(true);
                    exception.set(e);
                    flushQueue.clear();
                    break;
                } catch (Exception e) {
                    LOG.error("worker running error", e);
                    loadFailed.set(true);
                    exception.set(e);
                    // clear queue to avoid writer thread blocking
                    flushQueue.clear();
                    break;
                }
            }
            LOG.info("LoadAsyncExecutor stop");
            loadThreadAlive = false;
        }

        /**
         * Loads one buffer, preserving both the buffer and its original load label.
         *
         * <p>For ordinary stream loads the load is retried with its original label, bounded by
         * max.retries with retry.interval.ms backoff: Doris registers a stream load label when the
         * load is submitted and rejects a second load with the same label while the first is still
         * running or has committed, so a same-label retry cannot load the same batch twice as long
         * as Doris retains the label's metadata and the label uniquely identifies this logical
         * load. A duplicate-label rejection whose existing job is FINISHED is accepted as
         * completion by the load() response handling. Any other failed attempt never proves the
         * data was not written, so the only further outcomes are a confirmed success (load returns
         * normally) or, once the retries are exhausted, the failure latch.
         *
         * <p>Group commit sends no client-controlled label, so a retry could re-send a batch Doris
         * already accepted; it fails closed to the latch on the first failure instead of retrying.
         */
        private void loadWithRetry(RecordBuffer buffer) throws InterruptedException {
            if (enableGroupCommit) {
                try {
                    load(buffer.getLabel(), buffer);
                    return;
                } catch (Exception e) {
                    LOG.error(
                            "stream load failed for label {}, latching the writer",
                            buffer.getLabel(),
                            e);
                    latchFailure(e);
                    return;
                }
            }
            int remainingRetries = dorisOptions.getMaxRetries();
            while (true) {
                try {
                    load(buffer.getLabel(), buffer);
                    return;
                } catch (Exception e) {
                    if (remainingRetries <= 0) {
                        LOG.error(
                                "stream load failed for label {} after retries, latching the writer",
                                buffer.getLabel(),
                                e);
                        latchFailure(e);
                        return;
                    }
                    remainingRetries--;
                    LOG.warn(
                            "stream load failed for label {}, retries left: {}",
                            buffer.getLabel(),
                            remainingRetries,
                            e);
                    Thread.sleep(dorisOptions.getRetryIntervalMs());
                }
            }
        }

        private void latchFailure(Exception e) {
            loadFailed.set(true);
            exception.set(e);
            // clear queue to avoid writer thread blocking
            flushQueue.clear();
        }

        /** execute stream load. */
        public void load(String label, RecordBuffer buffer) throws IOException {
            if (enableGroupCommit) {
                label = null;
            }

            refreshLoadUrl(database, table);
            String data = buffer.getData();
            ByteArrayEntity byteEntity = new ByteArrayEntity(data.getBytes(StandardCharsets.UTF_8));
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(loadUrl)
                    .baseAuth(user, password)
                    .setLabel(label)
                    .addCommonHeader()
                    .setEntity(
                            enableGzCompress ? new GzipCompressingEntity(byteEntity) : byteEntity)
                    .addHiddenColumns(dorisOptions.isEnableDelete())
                    .enable2PC(dorisOptions.enable2PC())
                    .addProperties(dorisOptions.getStreamLoadProp());

            if (enableGroupCommit) {
                LOG.info("stream load started with group commit on host {}", hostPort);
            } else {
                LOG.info("stream load started for {} on host {}", label, hostPort);
            }

            try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 && response.getEntity() != null) {
                    String loadResult = EntityUtils.toString(response.getEntity());
                    LOG.info("load Result {}", loadResult);
                    KafkaRespContent respContent =
                            OBJECT_MAPPER.readValue(loadResult, KafkaRespContent.class);
                    if (respContent == null || respContent.getMessage() == null) {
                        throw new StreamLoadException("response error : " + loadResult);
                    }
                    boolean confirmedFinishedDuplicate =
                            LABEL_ALREADY_EXISTS_STATUS.equals(respContent.getStatus())
                                    && EXISTING_JOB_STATUS_FINISHED.equals(
                                            respContent.getExistingJobStatus())
                                    && label != null
                                    && label.equals(respContent.getLabel());
                    if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())
                            && !confirmedFinishedDuplicate) {
                        // a duplicate-label rejection without a FINISHED existing job is
                        // ambiguous: the existing load may still be running or may have
                        // failed, so it keeps failing until it resolves or retries end
                        String errMsg =
                                String.format(
                                        "stream load error: %s, see more in %s",
                                        respContent.getMessage(), respContent.getErrorURL());
                        throw new StreamLoadException(errMsg);
                    }
                    if (confirmedFinishedDuplicate) {
                        LOG.info(
                                "stream load for label {} already finished by an earlier attempt: {}",
                                label,
                                respContent.getMessage());
                    }
                    respContent.setDatabase(database);
                    respContent.setTable(table);
                    respContent.setLastOffset(buffer.getLastOffset());
                    respContent.setTopic(topic);
                    respContents.add(respContent);
                }
            } catch (Exception ex) {
                String err;
                if (enableGroupCommit) {
                    err = "failed to stream load data with group commit";
                } else {
                    err = "failed to stream load data with label: " + label;
                }

                LOG.warn(err, ex);
                backendUtils.invalidateCache();
                throw new StreamLoadException(err, ex);
            }
        }

        private void refreshLoadUrl(String database, String table) {
            hostPort = backendUtils.getAvailableBackend();
            loadUrl =
                    DorisUrlBuilder.buildHttpUrl(
                            dorisOptions.getTlsOptions(),
                            hostPort,
                            String.format(LOAD_PATH_PATTERN, database, table));
        }
    }
}
