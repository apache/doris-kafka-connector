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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.writer.AsyncS3TvfWriter;
import org.apache.doris.kafka.connector.writer.DorisWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Assert;
import org.junit.Test;

public class DorisCombinedSinkServiceTest {
    @Test
    public void testCreatesOneS3WriterForAllPartitionsOfTopicTable() throws Exception {
        DorisCombinedSinkService service = service();
        service.startTask("orders", new TopicPartition("orders-topic", 0));
        service.startTask("orders", new TopicPartition("orders-topic", 1));

        Assert.assertEquals(1, service.writer.size());
        Assert.assertTrue(service.writer.values().iterator().next() instanceof AsyncS3TvfWriter);
        service.close();
    }

    @Test
    public void testCommitFlushesAllWriters() throws Exception {
        DorisCombinedSinkService service = service();
        DorisWriter firstWriter = mock(DorisWriter.class);
        DorisWriter secondWriter = mock(DorisWriter.class);
        service.writer.put(
                new DorisDefaultSinkService.WriterKey("first", -1, "first"), firstWriter);
        service.writer.put(
                new DorisDefaultSinkService.WriterKey("second", -1, "second"), secondWriter);

        service.commit(Collections.emptyMap());

        verify(firstWriter).commitFlush();
        verify(secondWriter).commitFlush();
        service.close();
    }

    @Test
    public void testCommitPropagatesWriterFailure() throws Exception {
        DorisCombinedSinkService service = service();
        DorisWriter writer = mock(DorisWriter.class);
        doThrow(new DorisException("load failed")).when(writer).commitFlush();
        service.writer.put(new DorisDefaultSinkService.WriterKey("writer", -1, "writer"), writer);
        TopicPartition partition = new TopicPartition("orders-topic", 0);
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Collections.singletonMap(partition, new OffsetAndMetadata(6));

        Assert.assertThrows(DorisException.class, () -> service.commit(offsets));
        service.close();
    }

    @Test
    public void testCombinedInsertDoesNotUseTimeBasedFlush() throws Exception {
        DorisCombinedSinkService service = service();
        DorisWriter writer = mock(DorisWriter.class);
        when(writer.shouldFlush()).thenReturn(true);
        service.writer.put(
                new DorisDefaultSinkService.WriterKey("orders-topic", -1, "orders"), writer);
        SinkRecord record =
                new SinkRecord(
                        "orders-topic",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "{\"id\":1,\"name\":\"first\"}",
                        5);

        service.insert(Collections.singleton(record));

        verify(writer).insert(record);
        verify(writer, never()).shouldFlush();
        verify(writer, never()).flushBuffer();
        service.close();
    }

    private static DorisCombinedSinkService service() throws IOException {
        InputStream stream =
                DorisCombinedSinkServiceTest.class
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        Properties properties = new Properties();
        properties.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) properties);
        properties.put("task_id", "7");
        properties.put(DorisSinkConnectorConfig.NAME, "connector");
        properties.put(DorisSinkConnectorConfig.DORIS_DATABASE, "demo");
        properties.put(DorisSinkConnectorConfig.JMX_OPT, "false");
        properties.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "orders-topic:orders");
        properties.put(DorisSinkConnectorConfig.LOAD_MODEL, "tvf");
        properties.put(DorisSinkConnectorConfig.ENABLE_COMBINE_FLUSH, "true");
        properties.put(DorisSinkConnectorConfig.DELIVERY_GUARANTEE, "at_least_once");
        properties.put(DorisSinkConnectorConfig.SINK_S3_ENDPOINT, "https://s3.example.com");
        properties.put(DorisSinkConnectorConfig.SINK_S3_REGION, "us-east-1");
        properties.put(DorisSinkConnectorConfig.SINK_S3_BUCKET, "staging");
        properties.put(DorisSinkConnectorConfig.SINK_S3_PREFIX, "objects");
        properties.put(DorisSinkConnectorConfig.SINK_S3_ACCESS_KEY, "access-key");
        properties.put(DorisSinkConnectorConfig.SINK_S3_SECRET_KEY, "secret-key");
        properties.put(DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX + "columns", "id,name");
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.assignment())
                .thenReturn(Collections.singleton(new TopicPartition("orders-topic", 0)));
        return new DorisCombinedSinkService((Map) properties, context);
    }
}
