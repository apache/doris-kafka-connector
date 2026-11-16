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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.testutil.RecordingHttpClient;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.writer.AsyncStreamLoadWriter;
import org.apache.doris.kafka.connector.writer.load.AsyncDorisStreamLoad;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

/**
 * Regression tests for combined-flush mode: when a background stream load fails, the failed buffer
 * is dropped from the flush queue, so neither {@code put()} redelivery nor {@code preCommit()} may
 * advance offsets past the dropped records. The task must keep failing until it is restarted; on
 * restart, consumption resumes from the last committed offsets.
 */
public class TestCombineFlushFailedLoadCommits {
    private static final String TOPIC = "orders-topic";
    private static final String TABLE = "orders";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private MockedStatic<RestService> mockRestService;
    private Map<String, String> config;
    private SinkTaskContext context;

    @Before
    public void init() throws IOException {
        mockRestService = mockStatic(RestService.class);
        config = combineFlushConfig();
        context = mock(SinkTaskContext.class);
        when(context.assignment()).thenReturn(Collections.singleton(PARTITION));
    }

    @After
    public void close() {
        mockRestService.close();
    }

    private static Map<String, String> combineFlushConfig() throws IOException {
        InputStream stream =
                TestCombineFlushFailedLoadCommits.class
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        Properties properties = new Properties();
        properties.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) properties);
        properties.put("task_id", "7");
        properties.put(DorisSinkConnectorConfig.NAME, "combine-flush-failed-load-test");
        properties.put(DorisSinkConnectorConfig.DORIS_DATABASE, "demo");
        properties.put(DorisSinkConnectorConfig.JMX_OPT, "false");
        properties.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, TOPIC + ":" + TABLE);
        properties.put(DorisSinkConnectorConfig.LOAD_MODEL, "stream_load");
        properties.put(DorisSinkConnectorConfig.ENABLE_COMBINE_FLUSH, "true");
        properties.put(DorisSinkConnectorConfig.DELIVERY_GUARANTEE, "at_least_once");
        properties.put(DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "3600");
        return (Map<String, String>) (Map) properties;
    }

    private static SinkRecord record(long offset, String value) {
        return new SinkRecord(
                TOPIC,
                0,
                Schema.OPTIONAL_STRING_SCHEMA,
                "key",
                Schema.OPTIONAL_STRING_SCHEMA,
                value,
                offset);
    }

    /**
     * Replaces the writer's internal {@link AsyncDorisStreamLoad} with one backed by the recording
     * HTTP client, so the background load can be made to fail.
     */
    private static AsyncDorisStreamLoad injectRecordingClient(
            DorisCombinedSinkService service, DorisOptions options, RecordingHttpClient client)
            throws Exception {
        AsyncStreamLoadWriter writer =
                (AsyncStreamLoadWriter) service.writer.values().iterator().next();
        Field field = AsyncStreamLoadWriter.class.getDeclaredField("dorisStreamLoad");
        field.setAccessible(true);
        AsyncDorisStreamLoad original = (AsyncDorisStreamLoad) field.get(writer);
        original.close();
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("be:8040");
        AsyncDorisStreamLoad replacement =
                new AsyncDorisStreamLoad(backendUtils, options, TOPIC, TABLE, client);
        awaitLoadThread(replacement);
        field.set(writer, replacement);
        return replacement;
    }

    private static void awaitLoadThread(AsyncDorisStreamLoad streamLoad) {
        try {
            Field alive = AsyncDorisStreamLoad.class.getDeclaredField("loadThreadAlive");
            alive.setAccessible(true);
            long deadline = System.currentTimeMillis() + 10_000;
            while (!(boolean) alive.get(streamLoad)) {
                Assert.assertTrue(
                        "load executor thread did not start",
                        System.currentTimeMillis() < deadline);
                Thread.sleep(10);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void failedLoadFailsInsertAndCommitAcrossRestarts() throws Exception {
        DorisCombinedSinkService service = new DorisCombinedSinkService(config, context);
        DorisOptions options = new DorisOptions(config);
        service.startTask(TABLE, PARTITION);
        RecordingHttpClient client = new RecordingHttpClient();
        injectRecordingClient(service, options, client);

        // two puts buffer records in the writer
        service.insert(Collections.singleton(record(100, "{\"id\":100}")));
        service.insert(Collections.singleton(record(101, "{\"id\":101}")));

        // preCommit first calls commit(): the flush must fail and propagate, so preCommit
        // cannot return the (unsafe) offsets from topicPartitionOffset
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"insert rows exceed limit\"}");
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Collections.singletonMap(PARTITION, new OffsetAndMetadata(102));
        Assert.assertThrows(DorisException.class, () -> service.commit(offsets));

        // DorisSinkTask.put() catches the failure and calls sink.init(), which restarts the
        // async executor; both insert() and commit() must keep failing afterwards
        service.init();
        Assert.assertThrows(
                DorisException.class,
                () -> service.insert(Collections.singleton(record(102, "{\"id\":102}"))));
        Assert.assertThrows(DorisException.class, () -> service.commit(offsets));

        // getOffset() itself still reports the last seen offset, but preCommit never reaches
        // it: commit() runs first and throws
        Assert.assertEquals(102, service.getOffset(PARTITION));
        service.close();
    }

    @Test
    public void taskKeepsFailingUntilRetriesExhaustedThenNeverCommits() throws Exception {
        Map<String, String> taskConfig = new HashMap<>(combineFlushConfig());
        taskConfig.put(DorisSinkConnectorConfig.MAX_RETRIES, "2");
        taskConfig.put(DorisSinkConnectorConfig.RETRY_INTERVAL_MS, "0");
        org.apache.doris.kafka.connector.DorisSinkTask task =
                new org.apache.doris.kafka.connector.DorisSinkTask();
        task.initialize(context);
        task.start(taskConfig);
        task.open(Collections.singleton(PARTITION));

        Field sinkField =
                org.apache.doris.kafka.connector.DorisSinkTask.class.getDeclaredField("sink");
        sinkField.setAccessible(true);
        DorisCombinedSinkService service = (DorisCombinedSinkService) sinkField.get(task);
        RecordingHttpClient client = new RecordingHttpClient();
        injectRecordingClient(service, new DorisOptions(taskConfig), client);

        // put #1 buffers records; the task stays healthy
        task.put(Collections.singleton(record(100, "{\"id\":100}")));

        // preCommit flushes and the background load fails: no offsets are returned
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"insert rows exceed limit\"}");
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Collections.singletonMap(PARTITION, new OffsetAndMetadata(101));
        Assert.assertThrows(DorisException.class, () -> task.preCommit(offsets));

        // Kafka Connect re-delivers only the current put() batch; each redelivery keeps
        // failing retriable until the retries are exhausted, then the original exception
        // escapes and the task fails (it stays FAILED until restarted; on restart,
        // consumption resumes from the last committed offsets)
        Assert.assertThrows(
                RetriableException.class,
                () -> task.put(Collections.singleton(record(101, "{\"id\":101}"))));
        Assert.assertThrows(
                RetriableException.class,
                () -> task.put(Collections.singleton(record(101, "{\"id\":101}"))));
        Assert.assertThrows(
                DorisException.class,
                () -> task.put(Collections.singleton(record(101, "{\"id\":101}"))));

        // and preCommit still never returns offsets past the failed load
        Assert.assertThrows(DorisException.class, () -> task.preCommit(offsets));
        task.stop();
    }
}
