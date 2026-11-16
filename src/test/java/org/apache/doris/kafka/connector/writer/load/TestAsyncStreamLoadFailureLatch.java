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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.testutil.RecordingHttpClient;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.writer.RecordBuffer;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for the failure latch of {@link AsyncDorisStreamLoad}: once a background load
 * failed, its buffered records were dropped from the flush queue, so the writer must keep failing
 * (also across executor restarts triggered by {@code DorisSinkTask.put()}) until the task is
 * restarted and consumption resumes from the last committed offsets.
 */
public class TestAsyncStreamLoadFailureLatch {
    private DorisOptions options;

    @Before
    public void init() throws IOException {
        InputStream stream =
                getClass().getClassLoader().getResourceAsStream("doris-connector-sink.properties");
        Properties props = new Properties();
        props.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        props.put("task_id", "0");
        props.put("name", "async-failure-latch-test");
        options = new DorisOptions((Map) props);
    }

    private AsyncDorisStreamLoad newStreamLoad(CloseableHttpClient client) {
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("be:8040");
        AsyncDorisStreamLoad streamLoad =
                new AsyncDorisStreamLoad(backendUtils, options, "orders-topic", "orders", client);
        awaitLoadThread(streamLoad);
        return streamLoad;
    }

    /**
     * The executor thread sets its alive flag as its first statement; wait for that so the first
     * flush() cannot lose the race against thread startup.
     */
    private static void awaitLoadThread(AsyncDorisStreamLoad streamLoad) {
        awaitLoadThreadState(streamLoad, true);
    }

    private static void awaitLoadThreadExit(AsyncDorisStreamLoad streamLoad) {
        awaitLoadThreadState(streamLoad, false);
    }

    private static void awaitLoadThreadState(AsyncDorisStreamLoad streamLoad, boolean wanted) {
        try {
            Field field = AsyncDorisStreamLoad.class.getDeclaredField("loadThreadAlive");
            field.setAccessible(true);
            long deadline = System.currentTimeMillis() + 10_000;
            while ((boolean) field.get(streamLoad) != wanted) {
                Assert.assertTrue(
                        "load executor thread did not reach alive=" + wanted,
                        System.currentTimeMillis() < deadline);
                Thread.sleep(10);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static RecordBuffer dataBuffer() {
        RecordBuffer buffer = new RecordBuffer();
        buffer.insert("{\"id\":1}");
        return buffer;
    }

    @Test
    public void failedLoadKeepsFailingAfterExecutorRestart() {
        RecordingHttpClient client = new RecordingHttpClient();
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"insert rows exceed limit\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        streamLoad.flush("label-1", dataBuffer());
        // the background load fails; the failure surfaces on the next flush interaction
        Assert.assertThrows(DorisException.class, streamLoad::forceFlush);

        // DorisSinkTask.put() retries via sink.init() -> AsyncStreamLoadWriter.start() ->
        // AsyncDorisStreamLoad.start(), which restarts the executor thread; the failure
        // must survive that restart, otherwise the dropped buffer is never recovered
        streamLoad.start();
        Assert.assertThrows(DorisException.class, streamLoad::checkException);
        Assert.assertThrows(DorisException.class, () -> streamLoad.flush("label-2", dataBuffer()));
        Assert.assertThrows(DorisException.class, streamLoad::forceFlush);
        streamLoad.close();
    }

    @Test
    public void connectionFailureKeepsFailingAfterExecutorRestart() {
        // no response configured -> the client throws IOException, like a connection
        // refused while the BE restarts
        RecordingHttpClient client = new RecordingHttpClient();
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        streamLoad.flush("label-1", dataBuffer());
        Assert.assertThrows(DorisException.class, streamLoad::forceFlush);

        streamLoad.start();
        Assert.assertThrows(DorisException.class, streamLoad::checkException);
        Assert.assertThrows(DorisException.class, () -> streamLoad.flush("label-2", dataBuffer()));
        Assert.assertThrows(DorisException.class, streamLoad::forceFlush);
        streamLoad.close();
    }

    @Test
    public void successfulLoadsDoNotLatch() {
        RecordingHttpClient client = new RecordingHttpClient();
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        streamLoad.flush("label-1", dataBuffer());
        streamLoad.forceFlush();

        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        streamLoad.flush("label-2", dataBuffer());
        streamLoad.forceFlush();

        streamLoad.start();
        streamLoad.checkException();
        streamLoad.close();
    }

    /**
     * The preCommit path calls forceFlush() while a load may still be in flight on the executor
     * thread. forceFlush() must not return before that load has finished: if the load then failed,
     * the failure (exception + latch) must be observable to the caller.
     *
     * <p>The barrier is structural: waitAsyncLoadFinish() enqueues two eof markers into the
     * capacity-1 flush queue, and the single sequential executor thread can only poll the second
     * marker after load() has returned. If load() failed, exception/loadFailed are set (and the
     * queue cleared) before the blocked marker put unblocks, so the post-put checkFlushException()
     * throws.
     */
    @Test
    public void forceFlushWaitsForInFlightLoadAndSurfacesItsFailure() throws Exception {
        BlockingFailHttpClient client = new BlockingFailHttpClient();
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        // the executor dequeues the buffer and the HTTP request is now in progress
        streamLoad.flush("label-1", dataBuffer());
        Assert.assertTrue(
                "http request did not start", client.awaitRequestStarted(10, TimeUnit.SECONDS));

        // forceFlush() on a second thread, as preCommit would
        AtomicBoolean returned = new AtomicBoolean(false);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread flusher =
                new Thread(
                        () -> {
                            try {
                                streamLoad.forceFlush();
                                returned.set(true);
                            } catch (Throwable t) {
                                thrown.set(t);
                            }
                        },
                        "force-flush-thread");
        flusher.start();
        // give forceFlush() every chance to return early; the barrier must hold while the
        // request is in flight
        Thread.sleep(500);
        Assert.assertFalse(
                "forceFlush returned while the load was still in flight", returned.get());
        Assert.assertNull(thrown.get());

        // let the in-flight request fail; forceFlush must surface the failure, not return
        client.failRequest();
        flusher.join(10_000);
        Assert.assertFalse("forceFlush returned normally despite a failed load", returned.get());
        Assert.assertTrue(
                "expected DorisException from forceFlush, got " + thrown.get(),
                thrown.get() instanceof DorisException);

        // and the failure stays latched across the put()-retry executor restart
        streamLoad.start();
        Assert.assertThrows(DorisException.class, streamLoad::checkException);
        streamLoad.close();
    }

    @Test
    public void closeDuringIdlePollDoesNotLatch() throws Exception {
        RecordingHttpClient client = new RecordingHttpClient();
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);
        // the executor is idle, blocked in flushQueue.poll(2000ms)

        streamLoad.close();
        awaitLoadThreadExit(streamLoad);

        // the shutdown interrupt must not have recorded any failure (a task restart
        // creates fresh writers, so a closed instance is never restarted in production)
        streamLoad.checkException();
    }

    /**
     * HTTP client whose single request blocks until the test releases it, then fails with an
     * IOException — a connection reset while the request is in flight.
     */
    private static final class BlockingFailHttpClient extends CloseableHttpClient {
        private final CountDownLatch requestStarted = new CountDownLatch(1);
        private final CountDownLatch releaseRequest = new CountDownLatch(1);

        boolean awaitRequestStarted(long timeout, TimeUnit unit) throws InterruptedException {
            return requestStarted.await(timeout, unit);
        }

        void failRequest() {
            releaseRequest.countDown();
        }

        @Override
        protected CloseableHttpResponse doExecute(
                HttpHost target, HttpRequest request, HttpContext context) throws IOException {
            requestStarted.countDown();
            try {
                releaseRequest.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("connection reset during load");
        }

        @Override
        @Deprecated
        public HttpParams getParams() {
            return null;
        }

        @Override
        @Deprecated
        public ClientConnectionManager getConnectionManager() {
            return null;
        }

        @Override
        public void close() {}
    }
}
