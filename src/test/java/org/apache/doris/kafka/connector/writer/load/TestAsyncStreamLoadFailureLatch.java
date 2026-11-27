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
 * Regression tests for the bounded retry and sticky terminal failure of {@link
 * AsyncDorisStreamLoad}: a failed background load is retried with its original buffer and a
 * distinct label per attempt; a transient failure recovers automatically, and only when the retries
 * are exhausted is the terminal failure published on the exception reference, where it sticks (also
 * across executor restarts triggered by {@code DorisSinkTask.put()}) until the task is restarted
 * and consumption resumes from the last committed offsets.
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
        // fast, bounded retries for the executor's load retry loop
        props.put(DorisSinkConnectorConfig.MAX_RETRIES, "2");
        props.put(DorisSinkConnectorConfig.RETRY_INTERVAL_MS, "10");
        // keep request payloads readable for the payload-preservation assertions
        props.put("sink.properties.compress_type", "");
        options = new DorisOptions((Map) props);
    }

    private AsyncDorisStreamLoad newStreamLoad(CloseableHttpClient client) {
        return newStreamLoad(client, options);
    }

    private AsyncDorisStreamLoad newStreamLoad(CloseableHttpClient client, DorisOptions options) {
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("be:8040");
        AsyncDorisStreamLoad streamLoad =
                new AsyncDorisStreamLoad(backendUtils, options, "orders-topic", "orders", client);
        awaitLoadThread(streamLoad);
        return streamLoad;
    }

    private AsyncDorisStreamLoad newStreamLoad(
            CloseableHttpClient client, int maxRetries, long retryIntervalMs) throws IOException {
        InputStream stream =
                getClass().getClassLoader().getResourceAsStream("doris-connector-sink.properties");
        Properties props = new Properties();
        props.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        props.put("task_id", "0");
        props.put("name", "retry-config-test");
        props.put(DorisSinkConnectorConfig.MAX_RETRIES, String.valueOf(maxRetries));
        props.put(DorisSinkConnectorConfig.RETRY_INTERVAL_MS, String.valueOf(retryIntervalMs));
        props.put("sink.properties.compress_type", "");
        return newStreamLoad(client, new DorisOptions((Map) props));
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
        // wait for the failed executor to exit, so start() deterministically restarts it
        awaitLoadThreadExit(streamLoad);

        // DorisSinkTask.put() retries via sink.init() -> AsyncStreamLoadWriter.start() ->
        // AsyncDorisStreamLoad.start(), which restarts the executor thread; the terminal
        // failure must survive that restart, otherwise the dropped buffer is never
        // recovered
        streamLoad.start();
        awaitLoadThread(streamLoad);
        Assert.assertThrows(DorisException.class, streamLoad::checkException);
        // repeated start() calls must not clear the terminal failure either
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
     * marker after load() has returned. If the load failed, the exception is set (and the queue
     * cleared) before the blocked marker put unblocks, so the post-put checkFlushException()
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

    @Test
    public void transientFailureRecoversWithoutLatch() {
        // a Doris Failed load result followed by a successful retry of the same load:
        // the writer must recover automatically, with no latch and no exception
        RecordingHttpClient client = new RecordingHttpClient();
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"BE is restarting\"}");
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        streamLoad.flush("label-transient", dataBuffer());
        // blocks through the failed attempt and its successful retry
        streamLoad.forceFlush();

        streamLoad.checkException();
        streamLoad.start();
        streamLoad.checkException();
        streamLoad.close();
    }

    @Test
    public void retryUsesDistinctLabelsDerivedFromBase() throws Exception {
        // every attempt carries a distinct label: the first uses the buffer's original
        // label and every retry derives a fresh label from it (across the attempt digit
        // boundary), so a retry can never be rejected as a duplicate of an earlier
        // attempt of the same load
        ScriptedHttpClient client = new ScriptedHttpClient();
        for (int i = 0; i < 13; i++) {
            client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"BE is restarting\"}");
        }
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client, 12, 10L);

        RecordBuffer multiRecord = new RecordBuffer();
        multiRecord.insert("{\"id\":\"a1\"}");
        multiRecord.insert("{\"id\":\"a2\"}");
        streamLoad.flush("label-reuse", multiRecord);
        Assert.assertThrows(DorisException.class, streamLoad::forceFlush);

        java.util.List<String> expected = new java.util.ArrayList<>();
        expected.add("label-reuse");
        for (int attempt = 1; attempt <= 12; attempt++) {
            expected.add("label-reuse_" + attempt);
        }
        Assert.assertEquals(expected, client.requestLabels());
        Assert.assertEquals(13, new java.util.HashSet<>(client.requestLabels()).size());
        // every attempt transmits the identical, complete payload of the buffer
        Assert.assertEquals(13, client.requestBodies().size());
        for (byte[] body : client.requestBodies()) {
            Assert.assertArrayEquals(client.requestBodies().get(0), body);
            String text = new String(body, java.nio.charset.StandardCharsets.UTF_8);
            Assert.assertTrue("payload must keep all records: " + text, text.contains("a1"));
            Assert.assertTrue("payload must keep all records: " + text, text.contains("a2"));
        }
        streamLoad.close();
    }

    @Test
    public void duplicateLabelRejectionRetriesThenLatches() {
        // a "Label Already Exists" rejection is an ordinary failure for a changed-label
        // retry: the rejection keeps the load failing, the retries are exhausted and the
        // terminal failure sticks, with no offset advancement
        ScriptedHttpClient client = new ScriptedHttpClient();
        client.addIoError("connection reset");
        client.addResponse(200, duplicateLabelBody("label-ambiguous_1"));
        client.addResponse(200, duplicateLabelBody("label-ambiguous_2"));
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        streamLoad.flush("label-ambiguous", dataBuffer());
        Assert.assertThrows(DorisException.class, streamLoad::forceFlush);

        Assert.assertEquals(
                java.util.Arrays.asList(
                        "label-ambiguous", "label-ambiguous_1", "label-ambiguous_2"),
                client.requestLabels());

        // the terminal failure survives the executor restart (the put() retry path)
        streamLoad.start();
        Assert.assertThrows(DorisException.class, streamLoad::checkException);
        streamLoad.close();
    }

    @Test
    public void lostResponseRetriesWithNewLabelAndMayResubmit() {
        // a lost response after Doris committed the load: the retry uses a new label and
        // re-sends the identical complete payload, so the same batch may now exist in Doris
        // twice. Combined-flush mode is at-least-once — this documents the duplicate
        // submission instead of claiming exactly-once delivery.
        ScriptedHttpClient client = new ScriptedHttpClient();
        client.addIoError("connection reset while the response was in flight");
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        RecordBuffer multiRecord = new RecordBuffer();
        multiRecord.insert("{\"id\":\"d1\"}");
        multiRecord.insert("{\"id\":\"d2\"}");
        streamLoad.flush("label-lost-response", multiRecord);
        // blocks through the lost attempt and its successful retry
        streamLoad.forceFlush();

        streamLoad.checkException();
        Assert.assertEquals(
                java.util.Arrays.asList("label-lost-response", "label-lost-response_1"),
                client.requestLabels());
        Assert.assertEquals(2, client.requestBodies().size());
        Assert.assertArrayEquals(client.requestBodies().get(0), client.requestBodies().get(1));
        streamLoad.close();
    }

    @Test
    public void connectionFailureRecoversWithoutLatch() {
        // a connection-level failure (connection refused while the BE restarts) followed by
        // a successful retry of the same load: automatic recovery, no latch
        ScriptedHttpClient client = new ScriptedHttpClient();
        client.addIoError("connection refused");
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        streamLoad.flush("label-conn", dataBuffer());
        streamLoad.forceFlush();
        streamLoad.checkException();
        streamLoad.close();
    }

    @Test
    public void retryReselectsBackendBetweenAttempts() {
        // each attempt re-selects the backend (refreshLoadUrl -> getAvailableBackend per
        // load call), so a retry can land on a healthy backend after the first one died
        ScriptedHttpClient client = new ScriptedHttpClient();
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"BE is restarting\"}");
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("be1:8040", "be2:8040");
        AsyncDorisStreamLoad streamLoad =
                new AsyncDorisStreamLoad(backendUtils, options, "orders-topic", "orders", client);
        awaitLoadThread(streamLoad);

        streamLoad.flush("label-failover", dataBuffer());
        streamLoad.forceFlush();
        streamLoad.checkException();

        Assert.assertEquals(java.util.Arrays.asList("be1:8040", "be2:8040"), client.requestHosts());
        streamLoad.close();
    }

    @Test
    public void closeDuringRetryBackoffInterruptsPromptly() throws Exception {
        // close() while the executor sleeps in the retry backoff must interrupt the sleep
        // and shut down promptly, without latching (the interrupt exit checks `started`)
        ScriptedHttpClient client = new ScriptedHttpClient();
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"BE is restarting\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client, 2, 60_000L);

        streamLoad.flush("label-backoff", dataBuffer());
        // the first attempt fails immediately; the executor then sleeps for 60s
        awaitLoadThreadBusyInBackoff(client);
        long before = System.currentTimeMillis();
        streamLoad.close();
        awaitLoadThreadExit(streamLoad);
        Assert.assertTrue(
                "close must interrupt the backoff promptly",
                System.currentTimeMillis() - before < 10_000);

        // a single served request proves the close interrupted the backoff before a second
        // attempt, and the interrupt while shutting down must not have latched the writer
        Assert.assertEquals(1, client.requestLabels().size());
        streamLoad.checkException();
    }

    @Test
    public void groupCommitFailureLatchesWithoutRetry() throws Exception {
        // group commit sends no client-controlled load label, so a retry could re-send a
        // batch Doris already accepted: the first failure latches instead of retrying
        ScriptedHttpClient client = new ScriptedHttpClient();
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"BE is restarting\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client, groupCommitOptions());

        streamLoad.flush("label-gc", dataBuffer());
        Assert.assertThrows(DorisException.class, streamLoad::forceFlush);

        // a single attempt only, carrying no client-controlled label: no retry may
        // resubmit an unlabelled batch
        Thread.sleep(200);
        Assert.assertEquals(1, client.requestLabels().size());
        Assert.assertNull(client.requestLabels().get(0));
        streamLoad.start();
        Assert.assertThrows(DorisException.class, streamLoad::checkException);
        streamLoad.close();
    }

    /** The documented duplicate-label response body. */
    private static String duplicateLabelBody(String label) {
        return String.format(
                "{\"Status\": \"Label Already Exists\", \"Label\": \"%s\","
                        + " \"Message\": \"label [%s] already exist\"}",
                label, label);
    }

    /** Options with group commit enabled (stream_load_prop.group_commit). */
    private DorisOptions groupCommitOptions() throws IOException {
        InputStream stream =
                getClass().getClassLoader().getResourceAsStream("doris-connector-sink.properties");
        Properties props = new Properties();
        props.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        props.put("task_id", "0");
        props.put("name", "group-commit-test");
        props.put(DorisSinkConnectorConfig.MAX_RETRIES, "2");
        props.put(DorisSinkConnectorConfig.RETRY_INTERVAL_MS, "10");
        props.put("sink.properties.compress_type", "");
        props.put("sink.properties.group_commit", "async_mode");
        return new DorisOptions((Map) props);
    }

    /**
     * Waits until the executor is deterministically inside the retry backoff: the first scripted
     * response has been handed to the executor thread, and that same thread is in TIMED_WAITING —
     * inside {@code loadWithRetry} the only timed wait after a served response is the backoff
     * sleep.
     */
    private static void awaitLoadThreadBusyInBackoff(ScriptedHttpClient client) {
        long deadline = System.currentTimeMillis() + 10_000;
        Thread executor;
        do {
            executor = client.firstServingThread();
            Assert.assertTrue(
                    "first load attempt did not start", System.currentTimeMillis() < deadline);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } while (executor == null || executor.getState() != Thread.State.TIMED_WAITING);
    }

    @Test
    public void gzipDefaultPayloadPreservedAcrossRetries() throws Exception {
        // with the default compress_type=gz, each attempt transmits a gzip entity: the
        // recorded (still compressed) payloads must still be identical across attempts,
        // and decompressing them must yield the complete original records
        ScriptedHttpClient client = new ScriptedHttpClient();
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"BE is restarting\"}");
        client.addResponse(200, "{\"Status\":\"Failed\",\"Message\":\"BE is restarting\"}");
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");

        InputStream stream =
                getClass().getClassLoader().getResourceAsStream("doris-connector-sink.properties");
        Properties gzipProps = new Properties();
        gzipProps.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) gzipProps);
        gzipProps.put("task_id", "0");
        gzipProps.put("name", "gzip-payload-test");
        gzipProps.put(DorisSinkConnectorConfig.MAX_RETRIES, "2");
        gzipProps.put(DorisSinkConnectorConfig.RETRY_INTERVAL_MS, "10");
        // no compress_type override: the default gz applies

        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("be:8040");
        AsyncDorisStreamLoad streamLoad =
                new AsyncDorisStreamLoad(
                        backendUtils,
                        new DorisOptions((Map) gzipProps),
                        "orders-topic",
                        "orders",
                        client);
        awaitLoadThread(streamLoad);

        RecordBuffer buffer = new RecordBuffer();
        buffer.insert("{\"id\":\"g1\"}");
        buffer.insert("{\"id\":\"g2\"}");
        streamLoad.flush("label-gzip", buffer);
        streamLoad.forceFlush();
        streamLoad.checkException();

        Assert.assertEquals(3, client.requestBodies().size());
        String decompressed = gunzip(client.requestBodies().get(0));
        for (byte[] body : client.requestBodies()) {
            Assert.assertArrayEquals(client.requestBodies().get(0), body);
            Assert.assertEquals(decompressed, gunzip(body));
        }
        Assert.assertTrue(
                "payload must keep all records: " + decompressed,
                decompressed.contains("g1") && decompressed.contains("g2"));
        streamLoad.close();
    }

    private static String gunzip(byte[] gzipped) throws java.io.IOException {
        java.io.ByteArrayInputStream bytes = new java.io.ByteArrayInputStream(gzipped);
        try (java.util.zip.GZIPInputStream gzip = new java.util.zip.GZIPInputStream(bytes)) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            byte[] chunk = new byte[1024];
            int n;
            while ((n = gzip.read(chunk)) > 0) {
                out.write(chunk, 0, n);
            }
            return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    @Test
    public void inFlightFailureRetainsPendingQueuedBuffer() throws Exception {
        // buffer A is in flight when buffer B is enqueued behind it; A's first attempt fails,
        // its retry succeeds, and B — still queued throughout — is loaded afterwards
        ScriptedHttpClient client = new ScriptedHttpClient();
        ScriptedHttpClient.BlockingStep blockedA = client.addBlockingStep();
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        RecordBuffer bufferA = new RecordBuffer();
        bufferA.insert("{\"id\":\"a1\"}");
        bufferA.insert("{\"id\":\"a2\"}");
        RecordBuffer bufferB = new RecordBuffer();
        bufferB.insert("{\"id\":\"b1\"}");
        streamLoad.flush("label-a", bufferA);
        Assert.assertTrue(blockedA.awaitStarted(10, TimeUnit.SECONDS));
        // A is in flight (polled out of the capacity-1 queue): B enters the queue behind it
        streamLoad.flush("label-b", bufferB);
        blockedA.release();

        // the forceFlush barrier waits for A's retry and B's load to complete
        streamLoad.forceFlush();
        streamLoad.checkException();

        Assert.assertEquals(
                java.util.Arrays.asList("label-a", "label-a_1", "label-b"), client.requestLabels());
        // A's two attempts carry the identical complete payload; B's is distinct and intact
        Assert.assertEquals(3, client.requestBodies().size());
        Assert.assertArrayEquals(client.requestBodies().get(0), client.requestBodies().get(1));
        Assert.assertFalse(
                java.util.Arrays.equals(
                        client.requestBodies().get(0), client.requestBodies().get(2)));
        Assert.assertTrue(
                new String(client.requestBodies().get(2), java.nio.charset.StandardCharsets.UTF_8)
                        .contains("b1"));
        streamLoad.close();
    }

    @Test
    public void forceFlushWaitsForInFlightLoadThenSucceeds() throws Exception {
        // the preCommit path may run while a load is still in flight; forceFlush must not
        // return before the load has finished, and after a successful load it returns
        // normally (no latch)
        ScriptedHttpClient client = new ScriptedHttpClient();
        ScriptedHttpClient.BlockingStep blocked = client.addBlockingStep();
        client.addResponse(200, "{\"Status\":\"Success\",\"Message\":\"OK\"}");
        AsyncDorisStreamLoad streamLoad = newStreamLoad(client);

        streamLoad.flush("label-blocking", dataBuffer());
        Assert.assertTrue(blocked.awaitStarted(10, TimeUnit.SECONDS));

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
                        "force-flush-success-thread");
        flusher.start();
        Thread.sleep(300);
        Assert.assertFalse(
                "forceFlush returned while the load was still in flight", returned.get());
        Assert.assertNull(thrown.get());

        blocked.release();
        flusher.join(10_000);
        Assert.assertTrue(
                "forceFlush must return normally after a successful load", returned.get());
        Assert.assertNull(thrown.get());
        streamLoad.checkException();
        streamLoad.close();
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
    /**
     * Scripted HTTP client for the retry tests: every executed request records its stream load
     * label header, and each request consumes one scripted step — either an HTTP response or an
     * IOException. A blocking step parks the request until the test releases it, simulating a load
     * that is still in flight.
     */
    private static final class ScriptedHttpClient extends CloseableHttpClient {
        private final java.util.List<String> labels =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private final java.util.List<byte[]> bodies =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private final java.util.List<String> hosts =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private final java.util.List<Thread> servingThreads =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private final java.util.Queue<Step> steps =
                new java.util.concurrent.ConcurrentLinkedQueue<>();

        Step addResponse(int status, String body) {
            Step step = new ResponseStep(status, body);
            steps.add(step);
            return step;
        }

        Step addIoError(String message) {
            Step step = new IoErrorStep(message);
            steps.add(step);
            return step;
        }

        BlockingStep addBlockingStep() {
            BlockingStep step = new BlockingStep();
            steps.add(step);
            return step;
        }

        java.util.List<String> requestLabels() {
            synchronized (labels) {
                return new java.util.ArrayList<>(labels);
            }
        }

        java.util.List<byte[]> requestBodies() {
            synchronized (bodies) {
                return new java.util.ArrayList<>(bodies);
            }
        }

        java.util.List<String> requestHosts() {
            synchronized (hosts) {
                return new java.util.ArrayList<>(hosts);
            }
        }

        Thread firstServingThread() {
            synchronized (servingThreads) {
                return servingThreads.isEmpty() ? null : servingThreads.get(0);
            }
        }

        @Override
        protected CloseableHttpResponse doExecute(
                HttpHost target, HttpRequest request, HttpContext context) throws IOException {
            labels.add(
                    request.getLastHeader("label") == null
                            ? null
                            : request.getLastHeader("label").getValue());
            hosts.add(target == null ? null : target.toHostString());
            servingThreads.add(Thread.currentThread());
            if (request instanceof org.apache.http.HttpEntityEnclosingRequest) {
                org.apache.http.HttpEntity entity =
                        ((org.apache.http.HttpEntityEnclosingRequest) request).getEntity();
                if (entity == null) {
                    bodies.add(null);
                } else {
                    // writeTo works for plain and gzip entities alike; getContent does not
                    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                    try {
                        entity.writeTo(out);
                    } catch (java.io.IOException e) {
                        throw new RuntimeException(e);
                    }
                    bodies.add(out.toByteArray());
                }
            } else {
                bodies.add(null);
            }
            Step step = steps.poll();
            if (step == null) {
                throw new IOException("no scripted step left");
            }
            return step.run();
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

        interface Step {
            CloseableHttpResponse run() throws IOException;
        }

        private static final class ResponseStep implements Step {
            private final int status;
            private final String body;

            ResponseStep(int status, String body) {
                this.status = status;
                this.body = body;
            }

            @Override
            public CloseableHttpResponse run() {
                CloseableHttpResponse response =
                        org.mockito.Mockito.mock(CloseableHttpResponse.class);
                org.mockito.Mockito.when(response.getStatusLine())
                        .thenReturn(
                                new org.apache.http.message.BasicStatusLine(
                                        new org.apache.http.ProtocolVersion("HTTP", 1, 1),
                                        status,
                                        ""));
                if (body != null) {
                    org.mockito.Mockito.when(response.getEntity())
                            .thenReturn(
                                    new org.apache.http.entity.StringEntity(
                                            body, java.nio.charset.StandardCharsets.UTF_8));
                }
                return response;
            }
        }

        private static final class IoErrorStep implements Step {
            private final String message;

            IoErrorStep(String message) {
                this.message = message;
            }

            @Override
            public CloseableHttpResponse run() throws IOException {
                throw new IOException(message);
            }
        }

        static final class BlockingStep implements Step {
            private final CountDownLatch started = new CountDownLatch(1);
            private final CountDownLatch release = new CountDownLatch(1);

            void release() {
                release.countDown();
            }

            boolean awaitStarted(long timeout, TimeUnit unit) throws InterruptedException {
                return started.await(timeout, unit);
            }

            @Override
            public CloseableHttpResponse run() throws IOException {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IOException("connection reset during load");
            }
        }
    }
}
