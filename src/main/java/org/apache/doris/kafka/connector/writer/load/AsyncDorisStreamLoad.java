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
import org.apache.doris.kafka.connector.utils.HttpPutBuilder;
import org.apache.doris.kafka.connector.utils.HttpUtils;
import org.apache.doris.kafka.connector.writer.LoadStatus;
import org.apache.doris.kafka.connector.writer.RecordBuffer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncDorisStreamLoad extends DataLoad {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncDorisStreamLoad.class);
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(LoadStatus.SUCCESS, LoadStatus.PUBLISH_TIMEOUT));
    private String loadUrl;
    private final DorisOptions dorisOptions;
    private final String topic;
    private String hostPort;
    private final CloseableHttpClient httpClient = new HttpUtils().getHttpClient();
    private final BackendUtils backendUtils;
    private Queue<KafkaRespContent> respContents = new LinkedList<>();
    private final boolean enableGroupCommit;
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<RecordBuffer> flushQueue = new LinkedBlockingDeque<>(1);
    private final AtomicBoolean started;
    private volatile boolean loadThreadAlive = false;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);

    public AsyncDorisStreamLoad(
            BackendUtils backendUtils, DorisOptions dorisOptions, String topic, String table) {
        this.database = dorisOptions.getDatabase();
        this.table = table;
        this.user = dorisOptions.getUser();
        this.password = dorisOptions.getPassword();
        this.loadUrl = String.format(LOAD_URL_PATTERN, hostPort, database, table);
        this.dorisOptions = dorisOptions;
        this.backendUtils = backendUtils;
        this.topic = topic;
        this.enableGroupCommit = dorisOptions.enableGroupCommit();
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

        start();
        this.started = new AtomicBoolean(true);
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
    }

    public void close() {
        if (started.compareAndSet(true, false)) {
            LOG.info("close executorService");
            loadExecutorService.shutdown();
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
                    load(buffer.getLabel(), buffer);

                } catch (Exception e) {
                    LOG.error("worker running error", e);
                    exception.set(e);
                    // clear queue to avoid writer thread blocking
                    flushQueue.clear();
                    break;
                }
            }
            LOG.info("LoadAsyncExecutor stop");
            loadThreadAlive = false;
        }

        /** execute stream load. */
        public void load(String label, RecordBuffer buffer) throws IOException {
            if (enableGroupCommit) {
                label = null;
            }

            refreshLoadUrl(database, table);
            String data = buffer.getData();
            ByteArrayEntity entity = new ByteArrayEntity(data.getBytes(StandardCharsets.UTF_8));
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(loadUrl)
                    .baseAuth(user, password)
                    .setLabel(label)
                    .addCommonHeader()
                    .setEntity(entity)
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
                    if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                        String errMsg =
                                String.format(
                                        "stream load error: %s, see more in %s",
                                        respContent.getMessage(), respContent.getErrorURL());
                        throw new StreamLoadException(errMsg);
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
                throw new StreamLoadException(err, ex);
            }
        }

        private void refreshLoadUrl(String database, String table) {
            hostPort = backendUtils.getAvailableBackend();
            loadUrl = String.format(LOAD_URL_PATTERN, hostPort, database, table);
        }
    }
}
