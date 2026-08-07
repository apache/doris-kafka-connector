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

package org.apache.doris.kafka.connector.converter.schema;

import static org.mockito.Mockito.mock;

import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.service.DorisSystemService;
import org.apache.doris.kafka.connector.testutil.DorisOptionsTestUtils;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Test;

public class SchemaChangeManagerTlsTest {

    @Test
    public void testSchemaChangeUsesHttps() throws Exception {
        DorisOptions options = DorisOptionsTestUtils.tlsOptions("fe", 8030);
        SchemaChangeManager manager =
                new SchemaChangeManager(options, mock(DorisSystemService.class));

        HttpPost request = manager.buildHttpPost("ALTER TABLE t ADD COLUMN c INT", "db");

        Assert.assertEquals(
                "https://fe:8030/api/query/default_cluster/db", request.getURI().toString());
    }
}
