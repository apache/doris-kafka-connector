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

import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.model.doris.Schema;
import org.apache.doris.kafka.connector.testutil.DorisOptionsTestUtils;
import org.apache.doris.kafka.connector.testutil.HttpsTestServer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class RestServiceTlsTest {

    @Test
    public void testGetSchemaUsesConfiguredTls() throws Exception {
        String response =
                "{\"data\":{\"status\":200,\"keysType\":\"UNIQUE_KEYS\",\"properties\":[]}}";
        try (HttpsTestServer server = new HttpsTestServer(response)) {
            DorisOptions options = DorisOptionsTestUtils.tlsOptions("localhost", server.getPort());

            Schema schema =
                    RestService.getSchema(
                            options, "db", "table", LoggerFactory.getLogger(getClass()));

            Assert.assertEquals("UNIQUE_KEYS", schema.getKeysType());
            Assert.assertEquals("/api/db/table/_schema", server.getLastRequestPath());
            Assert.assertEquals("GET", server.getLastRequestMethod());
        }
    }
}
