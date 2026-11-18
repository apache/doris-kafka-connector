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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.DorisSinkTask;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.CopyLoadException;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.writer.CopyIntoWriter;
import org.apache.doris.kafka.connector.writer.DorisWriter;
import org.apache.doris.kafka.connector.writer.HttpEntityMock;
import org.apache.doris.kafka.connector.writer.StreamLoadWriter;
import org.apache.doris.kafka.connector.writer.commit.DorisCommitter;
import org.apache.doris.kafka.connector.writer.load.CopyLoad;
import org.apache.doris.kafka.connector.writer.load.DorisStreamLoad;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
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
 * End-to-end regression tests for {@link DorisDefaultSinkService#getOffset(TopicPartition)} and
 * {@link DorisDefaultSinkService#commit(Map)} writer selection and watermark computation.
 *
 * <p>These tests drive real {@link StreamLoadWriter}s through the real buffer/flush/commit
 * lifecycle (with only the HTTP layer mocked), so the offsets come from actual buffer state
 * transitions, not from mocked getOffset()/getFirstBufferedOffset() values.
 *
 * <p>Covers: (a) the string-prefix writer-selection bug where topic-partition 1 also matched
 * partition 10 ("topic_1".startsWith prefix of "topic_10") and vice versa; (b) the MAX-offset
 * data-loss bug with 'record.tablename.field' multi-table routing; (c) crash-consistency: the
 * reported watermark must never pass a record that is not durably committed in Doris.
 */
public class TestDorisDefaultSinkServiceGetOffset {

    private static final String TOPIC = "topic_test";
    // "Status" is parsed by the stream-load response model; "status" (lowercase) is read
    // by DorisCommitter's 2PC commit response handling — both keys are needed
    private static final String SUCCESS_BODY =
            "{\"Status\": \"Success\", \"status\": \"Success\", \"Message\": \"OK\", \"TxnId\": 42}";

    // a load the server rejected: HTTP 200 with a Doris Failed status — fails the load
    // even without the non-200 handling of the other PR
    private static final String FAILED_BODY =
            "{\"Status\": \"Failed\", \"Message\": \"insert rows exceed limit\"}";

    private DorisDefaultSinkService service;
    private DorisOptions dorisOptions;
    private MockedStatic<RestService> restServiceMock;
    private CloseableHttpClient httpClient;
    private Properties props;

    @Before
    public void init() throws Exception {
        InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        props = new Properties();
        props.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        props.put("task_id", "1");
        props.put("name", "sink-connector-test");
        // 2PC commit transport (DorisCommitter) is not under test here; disabling it keeps
        // these unit tests off the network. The commit-before-getOffset ordering that the
        // watermark relies on is still exercised via commitOffsets() -> writer.commit().
        props.put("enable.2pc", "false");
        // multi-table routing: records carry {"table_name": "..."} and are routed by it
        props.put("record.tablename.field", "table_name");
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment())
                .thenReturn(
                        Sets.newHashSet(
                                new TopicPartition(TOPIC, 0),
                                new TopicPartition(TOPIC, 1),
                                new TopicPartition(TOPIC, 10),
                                new TopicPartition(TOPIC + "_1", 0)));
        dorisOptions = new DorisOptions((Map) props);
        service = new DorisDefaultSinkService((Map) props, context);
        restServiceMock = mockStatic(RestService.class);
        restServiceMock
                .when(() -> RestService.isUniqueKeyType(any(), any(), any()))
                .thenReturn(false);
        httpClient = mock(CloseableHttpClient.class);
        stubHttpClientSuccess();
    }

    /** Point the shared http client mock at a 200/Success response. */
    private void stubHttpClientSuccess() throws Exception {
        CloseableHttpResponse success = mockResponse(200, SUCCESS_BODY);
        when(httpClient.execute(any())).thenReturn(success);
    }

    @After
    public void close() {
        restServiceMock.close();
    }

    private CloseableHttpResponse mockResponse(int status, String body) {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        StatusLine statusLine =
                new BasicStatusLine(
                        new org.apache.http.ProtocolVersion("http", 1, 1), status, "reason");
        when(response.getStatusLine()).thenReturn(statusLine);
        HttpEntityMock entity = new HttpEntityMock();
        entity.setValue(body);
        when(response.getEntity()).thenReturn(entity);
        return response;
    }

    /**
     * A DorisStreamLoad bound to the mocked http client and a stub backend, to inject into writers
     * the service creates itself (their own DorisStreamLoad cannot reach a real backend in unit
     * tests).
     */
    private DorisStreamLoad newMockedStreamLoad(String topic, String table) throws Exception {
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("127.0.0.1:8040");
        return new DorisStreamLoad(backendUtils, dorisOptions, topic, table, httpClient);
    }

    /**
     * Inserts the record and returns the writer it went to, with its DorisStreamLoad replaced by
     * one bound to the mocked http client.
     */
    private StreamLoadWriter insertAndReturnWriter(SinkRecord record) throws Exception {
        return insertAndReturnWriter(service, record);
    }

    /** Inserts into the given service and returns the writer, with a mocked stream load. */
    private StreamLoadWriter insertAndReturnWriter(
            DorisDefaultSinkService targetService, SinkRecord record) throws Exception {
        targetService.insert(record);
        StreamLoadWriter writer =
                targetService.writerOf(
                        record.topic(), record.kafkaPartition(), getSinkDorisTableName(record));
        writer.setDorisStreamLoad(
                newMockedStreamLoad(record.topic(), getSinkDorisTableName(record)));
        return writer;
    }

    /** A copy-into service variant whose writers upload staged files instead of stream load. */
    private DorisDefaultSinkService newCopyIntoService() {
        Properties copyProps = new Properties();
        copyProps.putAll(props);
        copyProps.put("load.model", "copy_into");
        return new DorisDefaultSinkService((Map) copyProps, mock(SinkTaskContext.class));
    }

    private CopyIntoWriter copyIntoWriterOf(
            DorisDefaultSinkService targetService, String topic, int partition, String table) {
        DorisWriter writer =
                targetService.writer.get(
                        new DorisDefaultSinkService.WriterKey(topic, partition, table));
        return (CopyIntoWriter) writer;
    }

    private static void setCommitter(StreamLoadWriter writer, DorisCommitter committer)
            throws Exception {
        java.lang.reflect.Field field = StreamLoadWriter.class.getDeclaredField("dorisCommitter");
        field.setAccessible(true);
        field.set(writer, committer);
    }

    private String getSinkDorisTableName(SinkRecord record) {
        // routed table from the record's table_name field (test config uses it)
        return record.value().toString().contains("appoint_table_a")
                ? "appoint_table_a"
                : record.value().toString().contains("appoint_table_b")
                        ? "appoint_table_b"
                        : "test_kafka_tbl";
    }

    private SinkRecord record(String topic, int partition, long offset, String table) {
        return new SinkRecord(
                topic,
                partition,
                Schema.OPTIONAL_STRING_SCHEMA,
                "key",
                Schema.OPTIONAL_STRING_SCHEMA,
                "{\"id\":" + offset + ",\"table_name\":\"" + table + "\"}",
                offset);
    }

    /**
     * The reviewer's blocking scenario, end to end with real writers: partition 10 fully committed
     * ahead, partition 1 still buffering. The old string-prefix selection made getOffset(TOPIC,10)
     * also aggregate partition 1's writers (and getOffset(TOPIC,1) aggregate partition 10's),
     * because "topic_test_1" is a prefix of "topic_test_10". With exact identity each partition
     * reports only its own progress.
     */
    @Test
    public void partitionIdentityIsolationPartitions1And10() throws Exception {
        // partition 10, table default: offsets 0..4 flushed+committed, then 5..6 buffered
        StreamLoadWriter p10 = insertAndReturnWriter(record(TOPIC, 10, 0, "test_kafka_tbl"));
        for (long off = 1; off <= 4; off++) {
            service.insert(record(TOPIC, 10, off, "test_kafka_tbl"));
        }
        p10.flushBuffer(); // flush offsets 0..4
        service.commitOffsets(new TopicPartition(TOPIC, 10));
        Assert.assertEquals(5L, service.getOffset(new TopicPartition(TOPIC, 10)));

        // partition 1, table default: offsets 100..101 buffered only (nothing flushed)
        service.insert(record(TOPIC, 1, 100, "test_kafka_tbl"));
        service.insert(record(TOPIC, 1, 101, "test_kafka_tbl"));

        // partition 10 still reports 5: partition 1's state must not leak into it
        Assert.assertEquals(5L, service.getOffset(new TopicPartition(TOPIC, 10)));
        // partition 1 reports its first buffered offset (100), not partition 10's committed 5
        Assert.assertEquals(100L, service.getOffset(new TopicPartition(TOPIC, 1)));
    }

    /**
     * Topic-prefix collision: topic "topic_test_1" partition 0 must not be aggregated with topic
     * "topic_test" partition 1 (old key "topic_test_1_..." matched both).
     */
    @Test
    public void topicPrefixCollisionIsolation() throws Exception {
        // topic_test partition 1: fully committed at 50
        StreamLoadWriter tp1 = insertAndReturnWriter(record(TOPIC, 1, 50, "test_kafka_tbl"));
        tp1.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 1));
        Assert.assertEquals(51L, service.getOffset(new TopicPartition(TOPIC, 1)));

        // topic_test_1 partition 0: buffering at 7 (unrelated topic that shares the string prefix)
        service.insert(record(TOPIC + "_1", 0, 7, "test_kafka_tbl"));

        Assert.assertEquals(51L, service.getOffset(new TopicPartition(TOPIC, 1)));
        Assert.assertEquals(7L, service.getOffset(new TopicPartition(TOPIC + "_1", 0)));
    }

    /**
     * commit() isolation for partitions 1 and 10, the selection bug's second half: with both
     * writers present, a commit for partition 1 only must commit partition 1's writer and must not
     * commit partition 10's writer. The old string-prefix selection matched "topic_test_1" against
     * partition 10's "topic_test_10_..." key too, so a commit for partition 1 also committed
     * partition 10's writers.
     */
    @Test
    public void commitIsolationPartitions1And10() throws Exception {
        // writers for partitions 1 and 10 through the real insert path (exact keys)
        StreamLoadWriter p1 = spy(insertAndReturnWriter(record(TOPIC, 1, 100, "test_kafka_tbl")));
        StreamLoadWriter p10 = spy(insertAndReturnWriter(record(TOPIC, 10, 200, "test_kafka_tbl")));
        service.writer.put(new DorisDefaultSinkService.WriterKey(TOPIC, 1, "test_kafka_tbl"), p1);
        service.writer.put(new DorisDefaultSinkService.WriterKey(TOPIC, 10, "test_kafka_tbl"), p10);

        // commit for partition 1 only
        service.commitOffsets(new TopicPartition(TOPIC, 1));

        // partition 1's writer committed with its own partition id...
        verify(p1).commit(1);
        // ...partition 10's writer not committed at all
        verify(p10, never()).commit(anyInt());
    }

    /**
     * commit() isolation for the topic-prefix collision: a commit for topic "topic_test" partition
     * 1 must not commit the writer of topic "topic_test_1" partition 0. The old string-prefix
     * selection matched "topic_test_1" against the other topic-partition's "topic_test_1_0_..." key
     * as well.
     */
    @Test
    public void commitIsolationTopicPrefixCollision() throws Exception {
        StreamLoadWriter tp1 = spy(insertAndReturnWriter(record(TOPIC, 1, 50, "test_kafka_tbl")));
        StreamLoadWriter colliding =
                spy(insertAndReturnWriter(record(TOPIC + "_1", 0, 7, "test_kafka_tbl")));
        service.writer.put(new DorisDefaultSinkService.WriterKey(TOPIC, 1, "test_kafka_tbl"), tp1);
        service.writer.put(
                new DorisDefaultSinkService.WriterKey(TOPIC + "_1", 0, "test_kafka_tbl"),
                colliding);

        // commit for topic_test partition 1 only
        service.commitOffsets(new TopicPartition(TOPIC, 1));

        verify(tp1).commit(1);
        verify(colliding, never()).commit(anyInt());
    }

    /**
     * The original data-loss scenario, end to end with real writers and real buffer transitions:
     * one topic-partition routed to two tables; table A's writer flushed past an offset that is
     * still buffered in table B's writer. MAX would report A's offset and lose B's records on
     * crash; the watermark must report the earliest still-buffered offset.
     */
    @Test
    public void multiTableRoutingWatermarkBlocksOnSlowTable() throws Exception {
        // realistic routing: every Kafka record goes to exactly one table —
        // offsets 0,2,4 -> table A; offsets 1,3 -> table B
        StreamLoadWriter tableA = insertAndReturnWriter(record(TOPIC, 0, 0, "appoint_table_a"));
        service.insert(record(TOPIC, 0, 1, "appoint_table_b"));
        service.insert(record(TOPIC, 0, 2, "appoint_table_a"));
        service.insert(record(TOPIC, 0, 3, "appoint_table_b"));
        service.insert(record(TOPIC, 0, 4, "appoint_table_a"));
        tableA.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        Assert.assertEquals(5L, tableA.getOffset());

        // table B still buffers offsets 1,3: the watermark is 1 — MAX(5) would lose B's
        // records on crash
        Assert.assertEquals(1L, service.getOffset(new TopicPartition(TOPIC, 0)));

        // flush table B and commit both: watermark jumps forward to max committed (5)
        StreamLoadWriter tableB = service.writerOf(TOPIC, 0, "appoint_table_b");
        tableB.setDorisStreamLoad(newMockedStreamLoad(TOPIC, "appoint_table_b"));
        tableB.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        Assert.assertEquals(5L, service.getOffset(new TopicPartition(TOPIC, 0)));
    }

    /**
     * Crash-consistency after a flush failure: the failed batch must never be acknowledged. A
     * failed load from the proxy (BE died) makes flush throw; even after committing the other
     * table's writer, the watermark must stay below the failed batch's first offset. On recovery,
     * Kafka Connect redelivers from the last committed offset (the failed record's offset is
     * re-inserted).
     */
    @Test
    public void failedFlushIsNeverAcknowledged() throws Exception {
        // good table: offsets 0..1 flush+commit fine
        StreamLoadWriter good = insertAndReturnWriter(record(TOPIC, 0, 0, "appoint_table_a"));
        service.insert(record(TOPIC, 0, 1, "appoint_table_a"));
        good.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 0));

        // failing table: offset 2 flush fails with a failed load result (BE died)
        StreamLoadWriter failing = insertAndReturnWriter(record(TOPIC, 0, 2, "appoint_table_b"));
        CloseableHttpResponse failedLoad = mockResponse(200, FAILED_BODY);
        when(httpClient.execute(any())).thenReturn(failedLoad);
        try {
            failing.flushBuffer();
            Assert.fail("expected flush to throw on a failed load result");
        } catch (Exception expected) {
            // the failed load must propagate: put() surfaces it, so Kafka Connect commits nothing
            // new
        }

        // watermark pinned at the failed record's offset (2): no acknowledgment past it
        Assert.assertEquals(2L, service.getOffset(new TopicPartition(TOPIC, 0)));

        // recovery: proxy restored; the failed record stayed in the writer's buffer, so the
        // redelivered put() re-inserts nothing (dedup guard) and the retry flush succeeds
        stubHttpClientSuccess();
        service.insert(record(TOPIC, 0, 2, "appoint_table_b"));
        StreamLoadWriter recovered = service.writerOf(TOPIC, 0, "appoint_table_b");
        recovered.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        Assert.assertEquals(3L, service.getOffset(new TopicPartition(TOPIC, 0)));
    }

    /**
     * Finding 1 regression (size-triggered flush): a failed flush must not discard the buffered
     * records. put() #1 buffers offset 100 for table B and succeeds; put() #2 routes offset 101 to
     * table A and 102 to table B, whose size-triggered flush fails with a failed load result. Kafka
     * Connect redelivers only put() #2. With the buffer discarded (old behavior) the watermark
     * could pass 100 and the record was lost; with the buffer preserved, the watermark is pinned at
     * 100 until B's records are durably persisted.
     */
    @Test
    public void failedSizeFlushPreservesBufferedRecordsAcrossPutBatches() throws Exception {
        // dedicated service: size-trigger flush after 2 records per buffer
        Properties sizeProps = new Properties();
        sizeProps.putAll(props);
        sizeProps.put("buffer.count.records", "2");
        DorisDefaultSinkService sizeService =
                new DorisDefaultSinkService((Map) sizeProps, mock(SinkTaskContext.class));

        // put() #1: offset 100 -> table B, buffered, returns successfully
        sizeService.insert(record(TOPIC, 0, 100, "appoint_table_b"));
        sizeService
                .writerOf(TOPIC, 0, "appoint_table_b")
                .setDorisStreamLoad(newMockedStreamLoad(TOPIC, "appoint_table_b"));

        // put() #2: 101 -> table A, 102 -> table B; B's size-triggered flush fails with a failed
        // load result
        CloseableHttpResponse failedLoad = mockResponse(200, FAILED_BODY);
        when(httpClient.execute(any())).thenReturn(failedLoad);
        try {
            sizeService.insert(record(TOPIC, 0, 101, "appoint_table_a"));
            sizeService.insert(record(TOPIC, 0, 102, "appoint_table_b"));
            Assert.fail("expected the failed load to propagate out of the second put()");
        } catch (Exception expected) {
            // put() #2 failed; Kafka Connect redelivers only this batch
        }

        // B still holds offsets 100 and 102: the watermark is pinned at 100, it cannot
        // acknowledge the lost-flush records
        Assert.assertEquals(100L, sizeService.getOffset(new TopicPartition(TOPIC, 0)));

        // redelivery of put() #2 only (proxy restored): both records are already buffered and
        // must not be duplicated
        stubHttpClientSuccess();
        sizeService.insert(record(TOPIC, 0, 101, "appoint_table_a"));
        sizeService.insert(record(TOPIC, 0, 102, "appoint_table_b"));
        Assert.assertEquals(100L, sizeService.getOffset(new TopicPartition(TOPIC, 0)));

        // later traffic for B re-triggers the size flush; now it succeeds and 100 is durable
        sizeService.insert(record(TOPIC, 0, 103, "appoint_table_b"));
        sizeService.commitOffsets(new TopicPartition(TOPIC, 0));
        // A still buffers 101, so the watermark claims exactly through 100 (now durable for B)
        Assert.assertEquals(101L, sizeService.getOffset(new TopicPartition(TOPIC, 0)));
        Assert.assertEquals(104L, sizeService.writerOf(TOPIC, 0, "appoint_table_b").getOffset());
    }

    /**
     * Finding 1 regression (time-triggered flush): same scenario via the service's time-based flush
     * loop (writer.shouldFlush() -> flushBuffer()), with shouldFlush() forced on a real writer so
     * the flush triggers deterministically during put() #2.
     */
    @Test
    public void failedTimeFlushPreservesBufferedRecordsAcrossPutBatches() throws Exception {
        // put() #1: offset 100 -> table B, buffered
        service.insert(record(TOPIC, 0, 100, "appoint_table_b"));

        // wrap B in a spy that only forces shouldFlush(); buffer/flush/commit stay real
        StreamLoadWriter realB = service.writerOf(TOPIC, 0, "appoint_table_b");
        realB.setDorisStreamLoad(newMockedStreamLoad(TOPIC, "appoint_table_b"));
        StreamLoadWriter spyB = spy(realB);
        when(spyB.shouldFlush()).thenReturn(true);
        service.writer.put(
                new DorisDefaultSinkService.WriterKey(TOPIC, 0, "appoint_table_b"), spyB);

        // put() #2: 101 -> table A; the time-based flush of B fails with a failed load result.
        // put() delivers a
        // Collection, and the time-based flush loop runs within the same put() call
        CloseableHttpResponse failedLoad = mockResponse(200, FAILED_BODY);
        when(httpClient.execute(any())).thenReturn(failedLoad);
        try {
            service.insert(
                    java.util.Collections.singleton(record(TOPIC, 0, 101, "appoint_table_a")));
            Assert.fail("expected the failed load to propagate out of the second put()");
        } catch (Exception expected) {
            // put() #2 failed; only this batch is redelivered
        }

        // B still holds offset 100: the watermark cannot pass it
        Assert.assertEquals(100L, service.getOffset(new TopicPartition(TOPIC, 0)));

        // redelivery of put() #2 only (proxy restored): 101 is already buffered in A
        stubHttpClientSuccess();
        service.insert(java.util.Collections.singleton(record(TOPIC, 0, 101, "appoint_table_a")));
        // the forced time flush now succeeds for B's preserved buffer [100]
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        // B durable through 100; A still buffers 101
        Assert.assertEquals(101L, service.getOffset(new TopicPartition(TOPIC, 0)));

        // A flushes later; the watermark advances past everything durable
        StreamLoadWriter a = service.writerOf(TOPIC, 0, "appoint_table_a");
        a.setDorisStreamLoad(newMockedStreamLoad(TOPIC, "appoint_table_a"));
        a.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        Assert.assertEquals(102L, service.getOffset(new TopicPartition(TOPIC, 0)));
    }

    /**
     * Finding 2 regression: writer commit-state finalization must happen on every preCommit cycle
     * even when the partition-level watermark is limited by another writer's buffered records.
     * StreamLoadWriter.getOffset() advances committedOffset from flushedOffset and clears its
     * committable list; skipping it (early return) left completed transactions pending and
     * re-committed them on the next cycle.
     */
    @Test
    public void committedWritersFinalizedDespiteBufferedWriter() throws Exception {
        // writer A: offsets 0..1 flushed and committed
        StreamLoadWriter tableA = insertAndReturnWriter(record(TOPIC, 0, 0, "appoint_table_a"));
        service.insert(record(TOPIC, 0, 1, "appoint_table_a"));
        tableA.flushBuffer();
        // writer B: offset 5 still buffered
        service.insert(record(TOPIC, 0, 5, "appoint_table_b"));

        // cycle 1: watermark limited by B, but A must be finalized (nothing pending to re-commit)
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        Assert.assertEquals(5L, service.getOffset(new TopicPartition(TOPIC, 0)));
        Assert.assertTrue(
                "A's committable list must be cleared", committableListOf(tableA).isEmpty());

        // cycle 2: no new data; A must not have anything to re-commit
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        Assert.assertEquals(5L, service.getOffset(new TopicPartition(TOPIC, 0)));
        Assert.assertTrue(committableListOf(tableA).isEmpty());

        // B becomes durable; the watermark advances to the max committed offset
        StreamLoadWriter tableB = service.writerOf(TOPIC, 0, "appoint_table_b");
        tableB.setDorisStreamLoad(newMockedStreamLoad(TOPIC, "appoint_table_b"));
        tableB.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 0));
        Assert.assertEquals(6L, service.getOffset(new TopicPartition(TOPIC, 0)));
    }

    /**
     * Round-3 review regression: 2PC partial-commit failure. Writer A's transaction commits, writer
     * B's transaction commit fails: preCommit aborts (commit() propagates, getOffset() is never
     * reached, no offsets are returned), pending committables are retained, and the retry cycle
     * re-commits the already-committed transaction idempotently, commits B's, and only then
     * advances the watermark to the durable state.
     */
    @Test
    public void twoPhaseCommitPartialCommitFailureRetriesIdempotently() throws Exception {
        Properties twoPcProps = new Properties();
        twoPcProps.putAll(props);
        twoPcProps.put("enable.2pc", "true");
        DorisDefaultSinkService twoPcService =
                new DorisDefaultSinkService((Map) twoPcProps, mock(SinkTaskContext.class));

        // writer A: offsets 0..1 loaded successfully; its 2PC committer succeeds
        StreamLoadWriter tableA =
                insertAndReturnWriter(twoPcService, record(TOPIC, 0, 0, "appoint_table_a"));
        twoPcService.insert(record(TOPIC, 0, 1, "appoint_table_a"));
        DorisCommitter committerA = mock(DorisCommitter.class);
        setCommitter(tableA, committerA);

        // writer B: offset 2 loaded successfully; its 2PC committer fails on the first attempt
        StreamLoadWriter tableB =
                insertAndReturnWriter(twoPcService, record(TOPIC, 0, 2, "appoint_table_b"));
        DorisCommitter committerB = mock(DorisCommitter.class);
        doThrow(new DorisException("commit transaction failed"))
                .doNothing()
                .when(committerB)
                .commit(anyList());
        setCommitter(tableB, committerB);

        tableA.flushBuffer();
        tableB.flushBuffer();

        // cycle 1: the failing 2PC commit aborts preCommit before any offset is returned
        try {
            twoPcService.commitOffsets(new TopicPartition(TOPIC, 0));
            Assert.fail("expected the failed 2PC commit to abort preCommit");
        } catch (DorisException expected) {
            // preCommit propagates: getOffset() is never called, nothing is finalized
        }

        // the failed writer keeps its pending committables for the retry cycle
        Assert.assertFalse(
                "B must keep its pending committables", committableListOf(tableB).isEmpty());

        // cycle 2 (retry): pending transactions are committed again — the already-committed
        // one idempotently — and only then may the watermark advance
        twoPcService.commitOffsets(new TopicPartition(TOPIC, 0));
        // B's transaction: one failed attempt plus one successful retry. A's transaction was
        // committed in cycle 1 (or is committed here) and — being still pending — idempotently
        // again; writer-map iteration order is unspecified, so only the minimum is asserted.
        verify(committerB, times(2)).commit(anyList());
        verify(committerA, org.mockito.Mockito.atLeast(1)).commit(anyList());
        Assert.assertEquals(3L, twoPcService.getOffset(new TopicPartition(TOPIC, 0)));
        Assert.assertTrue(committableListOf(tableA).isEmpty());
        Assert.assertTrue(committableListOf(tableB).isEmpty());
    }

    /**
     * Round-3 review regression: COPY INTO buffer lifecycle. A failed file upload propagates and
     * preserves the buffered records — the watermark stays pinned at the failed batch — and the
     * retry uploads and copies them durably before the watermark advances.
     */
    @Test
    public void copyIntoFailedUploadPreservesBufferedRecords() throws Exception {
        DorisDefaultSinkService copyService = newCopyIntoService();

        // put(): offset 100 -> table B, buffered by the CopyIntoWriter
        copyService.insert(record(TOPIC, 0, 100, "appoint_table_b"));
        CopyIntoWriter tableB = copyIntoWriterOf(copyService, TOPIC, 0, "appoint_table_b");
        CopyLoad copyLoad = mock(CopyLoad.class);
        tableB.setCopyLoad(copyLoad);

        // the upload fails (object storage outage): flush propagates, buffer is preserved
        doThrow(new CopyLoadException("upload failed"))
                .when(copyLoad)
                .uploadFile(anyString(), anyString());
        try {
            tableB.flushBuffer();
            Assert.fail("expected the upload failure to propagate");
        } catch (Exception expected) {
            // put()/preCommit() fail; Kafka Connect commits nothing past the failed batch
        }
        Assert.assertEquals(100L, copyService.getOffset(new TopicPartition(TOPIC, 0)));

        // retry after recovery: upload + copy succeed, then the watermark advances
        doNothing().when(copyLoad).uploadFile(anyString(), anyString());
        tableB.flushBuffer();
        copyService.commitOffsets(new TopicPartition(TOPIC, 0));
        verify(copyLoad, times(1)).executeCopy(anyList());
        Assert.assertEquals(101L, copyService.getOffset(new TopicPartition(TOPIC, 0)));
    }

    /**
     * Round-3 review regression: COPY INTO commit failure. The upload succeeds but the COPY INTO
     * execution fails: preCommit aborts (no offsets returned) and the uploaded file stays pending;
     * the retry executes the copy before the watermark advances, and the file list is finalized
     * exactly once across consecutive cycles.
     */
    @Test
    public void copyIntoCopyFailureRetriesBeforeAdvancingOffsets() throws Exception {
        DorisDefaultSinkService copyService = newCopyIntoService();

        copyService.insert(record(TOPIC, 0, 100, "appoint_table_b"));
        CopyIntoWriter tableB = copyIntoWriterOf(copyService, TOPIC, 0, "appoint_table_b");
        CopyLoad copyLoad = mock(CopyLoad.class);
        doThrow(new CopyLoadException("copy into failed"))
                .doReturn(true)
                .when(copyLoad)
                .executeCopy(anyList());
        tableB.setCopyLoad(copyLoad);

        // upload succeeds; the copy execution fails -> preCommit aborts before getOffset()
        tableB.flushBuffer();
        try {
            copyService.commitOffsets(new TopicPartition(TOPIC, 0));
            Assert.fail("expected the copy failure to abort preCommit");
        } catch (Exception expected) {
            // preCommit propagates: no offsets returned, the file stays pending
        }

        // retry cycle: the pending file is copied durably, then the watermark advances
        copyService.commitOffsets(new TopicPartition(TOPIC, 0));
        verify(copyLoad, times(2)).executeCopy(anyList());
        Assert.assertEquals(101L, copyService.getOffset(new TopicPartition(TOPIC, 0)));

        // a third cycle has nothing pending: the file list was finalized exactly once
        copyService.commitOffsets(new TopicPartition(TOPIC, 0));
        verify(copyLoad, times(2)).executeCopy(anyList());
        Assert.assertEquals(101L, copyService.getOffset(new TopicPartition(TOPIC, 0)));
    }

    /**
     * Round-3 review regression: the full Kafka Connect task lifecycle. put() #1 buffers offset 100
     * for table B successfully; put() #2 (offset 101 -> table A) fails while flushing B's buffer
     * (Doris Failed status) and surfaces as RetriableException; the retried put() redelivers only
     * its own batch; preCommit() then returns a watermark that never passes B's undurable record,
     * and advances only after everything is durable.
     */
    @Test
    public void dorisSinkTaskPutPreCommitLifecycleWithFailedFlush() throws Exception {
        DorisSinkTask task = new DorisSinkTask();
        task.initialize(mock(SinkTaskContext.class));
        setTaskField(task, "sink", service);
        setTaskField(task, "options", dorisOptions);
        setTaskField(task, "remainingRetries", 2);

        TopicPartition tp = new TopicPartition(TOPIC, 0);

        // put() #1: offset 100 -> table B, buffered successfully
        task.put(java.util.Collections.singletonList(record(TOPIC, 0, 100, "appoint_table_b")));
        StreamLoadWriter realB = service.writerOf(TOPIC, 0, "appoint_table_b");
        realB.setDorisStreamLoad(newMockedStreamLoad(TOPIC, "appoint_table_b"));
        StreamLoadWriter spyB = spy(realB);
        when(spyB.shouldFlush()).thenReturn(true);
        service.writer.put(
                new DorisDefaultSinkService.WriterKey(TOPIC, 0, "appoint_table_b"), spyB);

        // put() #2: offset 101 -> table A; B's time-based flush fails with a failed load result
        CloseableHttpResponse failedLoad = mockResponse(200, FAILED_BODY);
        when(httpClient.execute(any())).thenReturn(failedLoad);
        try {
            task.put(java.util.Collections.singletonList(record(TOPIC, 0, 101, "appoint_table_a")));
            Assert.fail("expected put() to surface the failed load as a RetriableException");
        } catch (RetriableException expected) {
            // Kafka Connect retries this batch and commits no offsets in the meantime
        }

        // the watermark is pinned at 100 while B's record is undurable
        Assert.assertEquals(100L, service.getOffset(tp));

        // recovery: Kafka Connect retries the failed put() batch (only offset 101, already
        // buffered in A — no duplication); B's preserved buffer flushes successfully
        stubHttpClientSuccess();
        task.put(java.util.Collections.singletonList(record(TOPIC, 0, 101, "appoint_table_a")));

        // preCommit(): B is durable through 100, A still buffers 101 -> watermark 101
        Map<TopicPartition, OffsetAndMetadata> offsets =
                java.util.Collections.singletonMap(tp, new OffsetAndMetadata(0L));
        Map<TopicPartition, OffsetAndMetadata> committed = task.preCommit(offsets);
        Assert.assertEquals(101L, committed.get(tp).offset());

        // A flushes later; preCommit() advances past everything durable
        StreamLoadWriter tableA = service.writerOf(TOPIC, 0, "appoint_table_a");
        tableA.setDorisStreamLoad(newMockedStreamLoad(TOPIC, "appoint_table_a"));
        tableA.flushBuffer();
        committed = task.preCommit(offsets);
        Assert.assertEquals(102L, committed.get(tp).offset());
    }

    private static void setTaskField(DorisSinkTask task, String name, Object value)
            throws Exception {
        java.lang.reflect.Field field = DorisSinkTask.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(task, value);
    }

    private static java.util.List<?> committableListOf(StreamLoadWriter writer) throws Exception {
        java.lang.reflect.Field field = StreamLoadWriter.class.getDeclaredField("committableList");
        field.setAccessible(true);
        return (java.util.List<?>) field.get(writer);
    }

    /** An idle writer (no records at all) must not stall or shrink the watermark. */
    @Test
    public void idleWriterDoesNotStallWatermark() throws Exception {
        // two tables routed; only table A ever receives records
        StreamLoadWriter tableA = insertAndReturnWriter(record(TOPIC, 0, 10, "appoint_table_a"));
        service.insert(record(TOPIC, 0, 11, "appoint_table_a"));
        tableA.flushBuffer();
        service.commitOffsets(new TopicPartition(TOPIC, 0));

        // create writer B explicitly without ever routing records to it: an idle writer
        // (empty buffer, committed offset 0) must neither stall nor shrink the watermark
        service.startTask("appoint_table_b", new TopicPartition(TOPIC, 0));
        Assert.assertEquals(12L, service.getOffset(new TopicPartition(TOPIC, 0)));
    }
}
