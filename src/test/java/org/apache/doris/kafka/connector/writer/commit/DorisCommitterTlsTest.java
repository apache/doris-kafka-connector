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

package org.apache.doris.kafka.connector.writer.commit;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.testutil.DorisOptionsTestUtils;
import org.apache.doris.kafka.connector.testutil.RecordingHttpClient;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.junit.Assert;
import org.junit.Test;

public class DorisCommitterTlsTest {

    @Test
    public void testCommitUsesHttps() throws Exception {
        DorisOptions options = DorisOptionsTestUtils.tlsOptions("fe", 8030);
        RecordingHttpClient httpClient = new RecordingHttpClient();
        httpClient.addResponse(200, "{\"status\":\"Success\",\"msg\":\"\"}");
        DorisCommitter committer =
                new DorisCommitter(options, mock(BackendUtils.class), httpClient);

        committer.commit(
                Collections.singletonList(
                        new DorisCommittable("be:8040", "db", 1, 0, "topic", 0, "table")));

        Assert.assertEquals(
                "https://be:8040/api/db/_stream_load_2pc", httpClient.getLastRequestUri());
        Assert.assertEquals("PUT", httpClient.getLastRequestMethod());

        committer.close();
        Assert.assertTrue(httpClient.isClosed());
    }
}
