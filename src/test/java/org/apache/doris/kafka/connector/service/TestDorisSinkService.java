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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDorisSinkService {

    private DorisDefaultSinkService dorisDefaultSinkService;

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
        props.put("record.tablename.field", "table_name");
        dorisDefaultSinkService = new DorisDefaultSinkService((Map) props);
    }

    @Test
    public void getSinkDorisTableName() {
        SinkRecord record1 =
                new SinkRecord(
                        "topic_test",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "val",
                        1);
        Assert.assertEquals(
                "test_kafka_tbl", dorisDefaultSinkService.getSinkDorisTableName(record1));

        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("col1", "val");
        valueMap.put("table_name", "appoint_table");
        SinkRecord record2 =
                new SinkRecord(
                        "topic_test",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                .optional()
                                .build(),
                        valueMap,
                        1);
        Assert.assertEquals(
                "appoint_table", dorisDefaultSinkService.getSinkDorisTableName(record2));
    }
}
