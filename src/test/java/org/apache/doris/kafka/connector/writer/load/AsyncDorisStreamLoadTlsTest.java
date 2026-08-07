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

import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.testutil.DorisOptionsTestUtils;
import org.apache.doris.kafka.connector.testutil.RecordingHttpClient;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.writer.RecordBuffer;
import org.junit.Assert;
import org.junit.Test;

public class AsyncDorisStreamLoadTlsTest {

    @Test
    public void testAsyncStreamLoadUsesHttps() throws Exception {
        DorisOptions options = DorisOptionsTestUtils.tlsOptions("fe", 8030);
        BackendUtils backendUtils = mock(BackendUtils.class);
        when(backendUtils.getAvailableBackend()).thenReturn("be:8040");
        RecordingHttpClient httpClient = new RecordingHttpClient();
        httpClient.addResponse(
                200,
                "{\"TxnId\":1,\"Status\":\"Success\",\"Message\":\"OK\",\"TwoPhaseCommit\":\"true\"}");
        AsyncDorisStreamLoad streamLoad =
                new AsyncDorisStreamLoad(backendUtils, options, "topic", "table", httpClient);
        RecordBuffer buffer = new RecordBuffer();
        buffer.insert("{\"id\":1}");

        streamLoad.load("label", buffer);

        Assert.assertEquals(
                "https://be:8040/api/db/table/_stream_load", httpClient.getLastRequestUri());
        Assert.assertEquals("PUT", httpClient.getLastRequestMethod());
        streamLoad.close();
        Assert.assertTrue(httpClient.isClosed());
    }
}
