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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.connection.JdbcConnectionProvider;
import org.apache.doris.kafka.connector.exception.StreamLoadException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.service.DorisSystemService;
import org.apache.doris.kafka.connector.service.RestService;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.utils.HttpUtils;
import org.apache.doris.kafka.connector.writer.HttpEntityMock;
import org.apache.doris.kafka.connector.writer.RecordBuffer;
import org.apache.doris.kafka.connector.writer.StreamLoadWriter;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

/**
 * Tests that stream load fails loudly (throws) instead of returning normally when the HTTP response
 * is not a valid Doris success response: non-200 status, null entity, or empty body. Returning
 * normally would make upper layers advance Kafka offsets over data that never landed in Doris
 * (silent data loss, e.g. when a BE crashes and a proxy answers 502/503/504).
 */
public class TestStreamLoadResponseHandling {

    private static final String SUCCESS_BODY = "{\"Status\": \"Success\", \"Message\": \"OK\"}";

    private MockedStatic<RestService> restServiceMock;
    private DorisOptions dorisOptions;

    @Before
    public void init() throws IOException {
        InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        Properties props = new Properties();
        props.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        props.put("task_id", "1");
        props.put("name", "sink-connector-test");
        dorisOptions = new DorisOptions((Map) props);
    }

    @After
    public void close() {
        if (restServiceMock != null) {
            restServiceMock.close();
        }
    }

    private RecordBuffer newBuffer() {
        RecordBuffer buffer = new RecordBuffer();
        buffer.setFirstOffset(0);
        buffer.setLastOffset(0);
        buffer.insert("{\"id\":1}");
        return buffer;
    }

    /** Build a DorisStreamLoad with a mocked http client and injected backend. */
    private DorisStreamLoad newStreamLoad(CloseableHttpClient httpClient) throws Exception {
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("127.0.0.1:8040");
        restServiceMock = mockStatic(RestService.class);
        DorisStreamLoad streamLoad =
                new DorisStreamLoad(backendUtils, dorisOptions, "topic_test", "test_kafka_tbl");
        Field clientField = DorisStreamLoad.class.getDeclaredField("httpClient");
        clientField.setAccessible(true);
        clientField.set(streamLoad, httpClient);
        return streamLoad;
    }

    /** Build an AsyncDorisStreamLoad and drive its inner executor with a mocked http client. */
    private AsyncDorisStreamLoad.LoadAsyncExecutor newAsyncExecutor(CloseableHttpClient httpClient)
            throws Exception {
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("127.0.0.1:8040");
        restServiceMock = mockStatic(RestService.class);
        AsyncDorisStreamLoad streamLoad =
                new AsyncDorisStreamLoad(
                        backendUtils, dorisOptions, "topic_test", "test_kafka_tbl");
        Field clientField = AsyncDorisStreamLoad.class.getDeclaredField("httpClient");
        clientField.setAccessible(true);
        clientField.set(streamLoad, httpClient);
        streamLoad.close();
        return streamLoad.new LoadAsyncExecutor();
    }

    private CloseableHttpResponse mockResponse(int status, String body, boolean nullEntity) {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        StatusLine statusLine =
                new BasicStatusLine(
                        new org.apache.http.ProtocolVersion("http", 1, 1), status, "reason");
        when(response.getStatusLine()).thenReturn(statusLine);
        if (nullEntity) {
            when(response.getEntity()).thenReturn(null);
        } else {
            HttpEntityMock entity = new HttpEntityMock();
            entity.setValue(body);
            when(response.getEntity()).thenReturn(entity);
        }
        return response;
    }

    private CloseableHttpClient mockHttpClient(CloseableHttpResponse response) throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        when(httpClient.execute(any())).thenReturn(response);
        return httpClient;
    }

    /** Walks the cause chain so assertions work through the outer re-wrap. */
    private Throwable rootMessageContaining(Throwable t, String fragment) {
        Throwable cur = t;
        while (cur != null) {
            if (cur.getMessage() != null && cur.getMessage().contains(fragment)) {
                return cur;
            }
            cur = cur.getCause();
        }
        return null;
    }

    @Test
    public void syncLoadSuccess() throws Exception {
        DorisStreamLoad streamLoad =
                newStreamLoad(mockHttpClient(mockResponse(200, SUCCESS_BODY, false)));
        streamLoad.load("label_ok", newBuffer());
        Assert.assertEquals(1, streamLoad.getKafkaRespContents().size());
    }

    @Test
    public void syncLoadThrowsOnNon200() throws Exception {
        DorisStreamLoad streamLoad =
                newStreamLoad(mockHttpClient(mockResponse(503, "Bad Gateway from proxy", false)));
        try {
            streamLoad.load("label_503", newBuffer());
            Assert.fail("expected StreamLoadException for http 503");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "503"));
            Assert.assertNotNull(rootMessageContaining(e, "Bad Gateway"));
        }
        Assert.assertTrue(streamLoad.getKafkaRespContents().isEmpty());
    }

    @Test
    public void syncLoadThrowsOnNullEntity() throws Exception {
        DorisStreamLoad streamLoad = newStreamLoad(mockHttpClient(mockResponse(200, null, true)));
        try {
            streamLoad.load("label_null_entity", newBuffer());
            Assert.fail("expected StreamLoadException for null entity");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "entity is null"));
        }
        Assert.assertTrue(streamLoad.getKafkaRespContents().isEmpty());
    }

    @Test
    public void syncLoadThrowsOnEmptyBody() throws Exception {
        DorisStreamLoad streamLoad = newStreamLoad(mockHttpClient(mockResponse(200, "", false)));
        try {
            streamLoad.load("label_empty_body", newBuffer());
            Assert.fail("expected StreamLoadException for empty body");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "body is empty"));
        }
        Assert.assertTrue(streamLoad.getKafkaRespContents().isEmpty());
    }

    @Test
    public void syncLoadThrowsOnDorisFailStatus() throws Exception {
        DorisStreamLoad streamLoad =
                newStreamLoad(
                        mockHttpClient(
                                mockResponse(
                                        200,
                                        "{\"Status\": \"Failed\", \"Message\": \"insert error\"}",
                                        false)));
        try {
            streamLoad.load("label_fail_status", newBuffer());
            Assert.fail("expected StreamLoadException for Doris Failed status");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "insert error"));
        }
        Assert.assertTrue(streamLoad.getKafkaRespContents().isEmpty());
    }

    @Test
    public void asyncLoadSuccess() throws Exception {
        AsyncDorisStreamLoad.LoadAsyncExecutor executor =
                newAsyncExecutor(mockHttpClient(mockResponse(200, SUCCESS_BODY, false)));
        executor.load("label_ok", newBuffer());
    }

    @Test
    public void asyncLoadThrowsOnNon200() throws Exception {
        AsyncDorisStreamLoad.LoadAsyncExecutor executor =
                newAsyncExecutor(mockHttpClient(mockResponse(502, "proxy blew up", false)));
        try {
            executor.load("label_502", newBuffer());
            Assert.fail("expected StreamLoadException for http 502");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "502"));
            Assert.assertNotNull(rootMessageContaining(e, "proxy blew up"));
        }
    }

    @Test
    public void asyncLoadThrowsOnNullEntity() throws Exception {
        AsyncDorisStreamLoad.LoadAsyncExecutor executor =
                newAsyncExecutor(mockHttpClient(mockResponse(200, null, true)));
        try {
            executor.load("label_null_entity", newBuffer());
            Assert.fail("expected StreamLoadException for null entity");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "entity is null"));
        }
    }

    @Test
    public void asyncLoadThrowsOnEmptyBody() throws Exception {
        AsyncDorisStreamLoad.LoadAsyncExecutor executor =
                newAsyncExecutor(mockHttpClient(mockResponse(200, "", false)));
        try {
            executor.load("label_empty_body", newBuffer());
            Assert.fail("expected StreamLoadException for empty body");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "body is empty"));
        }
    }

    @Test
    public void asyncLoadThrowsOnDorisFailStatus() throws Exception {
        AsyncDorisStreamLoad.LoadAsyncExecutor executor =
                newAsyncExecutor(
                        mockHttpClient(
                                mockResponse(
                                        200,
                                        "{\"Status\": \"Failed\", \"Message\": \"insert error\"}",
                                        false)));
        try {
            executor.load("label_fail_status", newBuffer());
            Assert.fail("expected StreamLoadException for Doris Failed status");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "insert error"));
        }
    }

    @Test
    public void snippetHelperHandlesNullUnreadableAndLongBody() throws Exception {
        // null entity
        Assert.assertEquals("<no body>", errorBodySnippet(null));
        // empty / whitespace-only body
        HttpEntityMock emptyEntity = new HttpEntityMock();
        emptyEntity.setValue("   \n  ");
        Assert.assertEquals("<empty body>", errorBodySnippet(emptyEntity));
        // long body is truncated to the cap
        HttpEntityMock longEntity = new HttpEntityMock();
        StringBuilder longBody = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            longBody.append("abcdefghij");
        }
        longEntity.setValue(longBody.toString());
        String snippet = errorBodySnippet(longEntity);
        Assert.assertTrue(snippet.length() <= 516);
        Assert.assertTrue(snippet.endsWith("..."));
    }

    /**
     * Finding 3 regression: the error-path snippet must read at most 512+1 bytes of the body,
     * regardless of the body's actual size — a huge error page must not be read into memory before
     * truncation.
     */
    @Test
    public void errorSnippetReadsBoundedBytes() throws Exception {
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            huge.append("0123456789");
        }
        CountingContentEntity entity = new CountingContentEntity();
        entity.setValue(huge.toString());
        String snippet = errorBodySnippet(entity);
        Assert.assertTrue(snippet.length() <= 516);
        Assert.assertTrue(snippet.endsWith("..."));
        Assert.assertTrue(
                "body must not be read fully, read " + entity.bytesRead + " bytes",
                entity.bytesRead <= 513);
    }

    /** {@link HttpEntityMock} whose stream counts how many bytes were consumed. */
    private static final class CountingContentEntity extends HttpEntityMock {
        private int bytesRead;

        @Override
        public java.io.InputStream getContent() throws java.io.IOException {
            return new java.io.FilterInputStream(super.getContent()) {
                @Override
                public int read(byte[] b, int off, int len) throws java.io.IOException {
                    int n = super.read(b, off, len);
                    if (n > 0) {
                        bytesRead += n;
                    }
                    return n;
                }
            };
        }
    }

    /**
     * PR#1 end-to-end propagation: a BE death seen as a proxy 503 must surface from the writer
     * flush (StreamLoadWriter.flush), not just from the HTTP handler, so put()/preCommit() can
     * never treat the batch as flushed and advance Kafka offsets over it.
     */
    @Test
    public void streamLoadWriterFlushPropagatesHttpFailure() throws Exception {
        DorisStreamLoad streamLoad =
                newStreamLoad(mockHttpClient(mockResponse(503, "Bad Gateway from proxy", false)));
        StreamLoadWriter writer =
                new StreamLoadWriter(
                        "test_kafka_tbl",
                        "topic_test",
                        0,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        mock(DorisSystemService.class),
                        mock(DorisConnectMonitor.class));
        writer.setDorisStreamLoad(streamLoad);
        restServiceMock
                .when(() -> RestService.isUniqueKeyType(any(), any(), any()))
                .thenReturn(false);

        writer.insert(newSinkRecord("topic_test", "{\"id\":1}", 100));
        writer.insert(newSinkRecord("topic_test", "{\"id\":2}", 101));
        try {
            writer.flushBuffer();
            Assert.fail("expected the 503 to propagate out of flushBuffer");
        } catch (StreamLoadException e) {
            Assert.assertNotNull(rootMessageContaining(e, "503"));
        }
        // nothing may be acknowledged after the failure: put() surfaced the error, so Kafka
        // Connect cannot commit past the failed batch
        Assert.assertEquals(0L, writer.getOffset());
    }

    /**
     * PR#1 async propagation: the background load thread must capture the 503 failure and surface
     * it on the next flush/checkException call, so preCommit() can never advance offsets past a
     * batch that never landed in Doris.
     */
    @Test
    public void asyncLoadThreadSurfacesHttpFailureOnNextFlush() throws Exception {
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("127.0.0.1:8040");
        restServiceMock = mockStatic(RestService.class);
        AsyncDorisStreamLoad streamLoad =
                new AsyncDorisStreamLoad(
                        backendUtils, dorisOptions, "topic_test", "test_kafka_tbl");
        Field clientField = AsyncDorisStreamLoad.class.getDeclaredField("httpClient");
        clientField.setAccessible(true);
        clientField.set(
                streamLoad, mockHttpClient(mockResponse(503, "Bad Gateway from proxy", false)));

        RecordBuffer buffer = newBuffer();
        buffer.setLabel("label_async_503");
        streamLoad.flush("label_async_503", buffer);
        try {
            streamLoad.forceFlush();
            Assert.fail("expected the async 503 failure to surface on forceFlush");
        } catch (Exception e) {
            Assert.assertNotNull(rootMessageContaining(e, "503"));
        }
        streamLoad.close();
    }

    private static SinkRecord newSinkRecord(String topic, String value, long offset) {
        return new SinkRecord(
                topic,
                0,
                org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA,
                "key",
                org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA,
                value,
                offset);
    }

    private static String errorBodySnippet(HttpEntity entity) {
        // direct test hook for the protected helper
        try {
            java.lang.reflect.Method m =
                    DataLoad.class.getDeclaredMethod("errorBodySnippet", HttpEntity.class);
            m.setAccessible(true);
            return (String) m.invoke(null, entity);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // HttpUtils reference keeps the import meaningful for the fixture family.
    @SuppressWarnings("unused")
    private static final Class<HttpUtils> HTTP_UTILS = HttpUtils.class;
}
