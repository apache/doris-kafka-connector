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

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.connection.JdbcConnectionProvider;
import org.apache.doris.kafka.connector.exception.CopyLoadException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.writer.load.CopyLoad;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCopyIntoWriter {

    private DorisWriter dorisWriter;
    private DorisOptions dorisOptions;

    private final String[] listLoadFiles = {
        "s3://doris-online-cn-shanghai-bucket1667717444/ALSH08NM/stage/admin/admin/connect-test-sink299__KC_connect-test299__KC_5__KC_168142036__KC_1669260859111",
        "s3://doris-online-cn-shanghai-bucket1667717444/ALSH08NM/stage/admin/admin/connect-test-sink299__KC_connect-test299__KC_5__KC_168152036__KC_1669260859362",
        "s3://doris-online-cn-shanghai-bucket1667717444/ALSH08NM/stage/admin/admin/connect-test-sink299__KC_connect-test299__KC_5__KC_168162036__KC_1669260859548",
        "s3://doris-online-cn-shanghai-bucket1667717444/ALSH08NM/stage/admin/admin/connect-test-sink299__KC_connect-test299__KC_5__KC_168172036__KC_1669260859799"
    };

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
        props.put("name", "connect-test-sink299");
        props.put("load.model", "copy_into");
        dorisOptions = new DorisOptions((Map) props);
    }

    @Test(expected = CopyLoadException.class)
    public void fetchOffset() {
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        dorisWriter =
                new CopyIntoWriter(
                        "test5",
                        0,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        dorisConnectMonitor);
        dorisWriter.fetchOffset();
        Assert.assertEquals(-1l, dorisWriter.getOffsetPersistedInDoris().longValue());
    }

    @Test
    public void fetchOffsetTest() {
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        CopyIntoWriter copyIntoWriter =
                spy(
                        new CopyIntoWriter(
                                "connect-test299",
                                5,
                                dorisOptions,
                                new JdbcConnectionProvider(dorisOptions),
                                dorisConnectMonitor));
        doReturn(Arrays.asList(listLoadFiles)).when(copyIntoWriter).listLoadFiles();
        dorisWriter = copyIntoWriter;
        dorisWriter.fetchOffset();
        System.out.println(dorisWriter.getOffsetPersistedInDoris().longValue());
        Assert.assertEquals(168172036, dorisWriter.getOffsetPersistedInDoris().longValue());
    }

    @Test
    public void flush() throws Exception {
        CopyLoad mockCopyLoad = mock(CopyLoad.class);
        doCallRealMethod().when(mockCopyLoad).uploadFile("testFile", "dorisvalue");
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        CopyIntoWriter copyIntoWriter =
                new CopyIntoWriter(
                        "test5",
                        0,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        dorisConnectMonitor);
        copyIntoWriter.setCopyLoad(mockCopyLoad);

        dorisWriter = copyIntoWriter;
        dorisWriter.insert(TestRecordBuffer.newSinkRecord("doris-1", 1));
        dorisWriter.insert(TestRecordBuffer.newSinkRecord("doris-2", 2));
        dorisWriter.flushBuffer();
        Assert.assertEquals(dorisWriter.getOffsetPersistedInDoris().longValue(), -1L);
        Assert.assertEquals(dorisWriter.getOffset(), 3);
    }

    @Test
    public void putBuffer() {
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        dorisWriter =
                new CopyIntoWriter(
                        "test5",
                        0,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        dorisConnectMonitor);
        SinkRecord record = TestRecordBuffer.newSinkRecord("doris-1", 2);
        dorisWriter.putBuffer(record);
        Assert.assertEquals(2, dorisWriter.getBuffer().getLastOffset());
    }
}
