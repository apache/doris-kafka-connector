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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.connection.JdbcConnectionProvider;
import org.apache.doris.kafka.connector.exception.StreamLoadException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.writer.commit.DorisCommittable;
import org.apache.doris.kafka.connector.writer.load.DorisStreamLoad;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStreamLoadWriter {

    private DorisWriter dorisWriter;
    private DorisOptions dorisOptions;

    private final Map<String, String> label2Status = new HashMap<>();

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
        fillLabel2Status();
    }

    private void fillLabel2Status() {
        label2Status.put(
                "KC_avro-complex10__KC_2__KC_test_person_complex__KC_321__KC_1706149860395",
                "ABORT");
        label2Status.put(
                "KC_avro-complex10__KC_2__KC_test_person_complex__KC_983__KC_1706149860395",
                "ABORT");
        label2Status.put(
                "avro-complex10__KC_2__KC_test_person_complex__KC_781__KC_1706149860395",
                "VISIBLE");
        label2Status.put(
                "avro-complex10__KC_2__KC_test_person_complex__KC_832__KC_1706149860395",
                "VISIBLE");
    }

    @Test(expected = StreamLoadException.class)
    public void fetchOffset() {
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        dorisWriter =
                new StreamLoadWriter(
                        "avro-complex10",
                        2,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        dorisConnectMonitor);
        dorisWriter.fetchOffset();
        Assert.assertEquals(-1l, dorisWriter.getOffsetPersistedInDoris().longValue());
    }

    @Test
    public void fetchOffsetTest() {
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        StreamLoadWriter streamLoadWriter =
                spy(
                        new StreamLoadWriter(
                                "avro-complex10",
                                2,
                                dorisOptions,
                                new JdbcConnectionProvider(dorisOptions),
                                dorisConnectMonitor));

        doReturn(label2Status).when(streamLoadWriter).fetchLabel2Status();
        dorisWriter = streamLoadWriter;

        dorisWriter.fetchOffset();
        System.out.println(dorisWriter.getOffsetPersistedInDoris().longValue());
        Assert.assertEquals(832, dorisWriter.getOffsetPersistedInDoris().longValue());
    }

    @Test
    public void flush() throws Exception {
        DorisStreamLoad streamLoad = mock(DorisStreamLoad.class);
        doNothing().when(streamLoad).load(anyString(), any());
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        List<DorisCommittable> mockDorisCommittableList = new ArrayList<>();
        mockDorisCommittableList.add(new DorisCommittable("", "", 1, 3, "", 0, ""));

        StreamLoadWriter streamLoadWriter =
                new StreamLoadWriter(
                        "avro-complex10",
                        0,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        dorisConnectMonitor);
        streamLoadWriter.setDorisStreamLoad(streamLoad);

        dorisWriter = streamLoadWriter;
        dorisWriter.insert(TestRecordBuffer.newSinkRecord("doris-1", 1));
        dorisWriter.insert(TestRecordBuffer.newSinkRecord("doris-2", 2));
        dorisWriter.flushBuffer();
        Assert.assertEquals(dorisWriter.getOffsetPersistedInDoris().longValue(), -1L);

        streamLoadWriter.setCommittableList(mockDorisCommittableList);
        Assert.assertEquals(dorisWriter.getOffset(), 3);
    }

    @Test
    public void putBuffer() {
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        dorisWriter =
                new StreamLoadWriter(
                        "avro-complex10",
                        0,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        dorisConnectMonitor);
        SinkRecord record = TestRecordBuffer.newSinkRecord("doris-1", 2);
        dorisWriter.putBuffer(record);
        Assert.assertEquals(2, dorisWriter.getBuffer().getLastOffset());
    }
}
